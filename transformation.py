# -*- coding: utf-8 -*-
# FREDDEX - Transformation layer: BaMaRa -> FREDD business rules
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Le NOM de la maladie (diaCli_diagMR_nom / ...Add) est écrit dans le même ordre trié que sa
# charge (code ORPHA, statut, gènes, variations, âges) -- voir diag_block_treatment.
# SKEZIA n'expose que pro_nom1..pro_nom3 pour les essais cliniques -- voir
# handle_FREDD_columns, columns_to_deploy.

import re
import numpy as np
import pandas as pd
from datetime import date

from mapping import load_mapping_file
from orphanet import completer_codes_et_noms, _code_valide
from investigations import selectionner_derniere_investigation, COL_ID_INVEST, _cle_maladie
from consentements import (patients_opposes, consentement_ern_par_patient, COL_OPPOSITION_TOTALE,
                           COL_OPPOSITION_GENETIQUE, COL_AIDE_CONSENTEMENT)
import json


COL_AIDE_STATUT = "FREDDEX_statut_resultat"   # colonne technique, supprimée avant envoi


def _classe_fredd(v):
    """Classe du variant au format attendu par SKEZIA : « 3 » (et non « 3.0 »), vide si absente."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    try:
        f = float(v)
        return str(int(f)) if f.is_integer() else str(v).strip()
    except (TypeError, ValueError):
        s = str(v).strip()
        return s or None


def traiter_donnees(liste_MR, mapping_file, df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_rec,
                    df_invest, nomenclature=None):
    """
    Returns the processed dataframe (df_filtred), handled sheet by sheet, where each sheet corresponds to those extracted from the BaMaRa Excel file.

    Arguments:
    - liste MR : ORPHA codes to be retained
    - mapping_file : Excel mapping file between BaMaRa and FREDD
    - df_admin : "Données administratives" sheet from the input file
    - df_prises : "Prises en charge" sheet from the input file
    - df_diagnostic : "Diagnostic" sheet from the input file
    - df_genes : "Gènes" sheet (GENETIC export, new BaMaRa format)
    - df_var  "Variations nucléotidiques" sheet (GENETIC export, formerly "Variations")
    - df_neo : "Anté-néonatal" sheet from the input file
    - df_rec : "Recherche" sheet from the input file
    - df_invest : "Investigations génétiques" sheet (GENETIC export) -- used to keep only the
      most recent investigation per disease (see investigations.py)
    - nomenclature : optional orphanet.Nomenclature (Orphanet thesaurus, French) used as the
      level-2 fallback to match a disease NAME with its ORPHA CODE (see orphanet.py). None =
      level 1 only (pairs found in the export itself).

    The BaMaRa Excel file input file must therefore strictly contain these sheets, named exactly as specified.
    """

    # Load the mapping file (cached: see mapping.load_mapping_file)
    df_mapping = load_mapping_file(mapping_file)

    # Filtering mappings for the 'Données administratives' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Données administratives']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    if not all(col in df_admin.columns for col in colonnes_mapping):
        missing_cols = [col for col in colonnes_mapping if col not in df_admin.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Données administratives' : {missing_cols}")
    df_final = df_admin[colonnes_mapping].copy()

    # Oppositions et consentements (voir consentements.py).
    # Opposition à toute réutilisation des données : le patient n'est pas importé.
    opposes_total = patients_opposes(df_admin, COL_OPPOSITION_TOTALE)
    if opposes_total:
        df_final = df_final[~df_final['ID BaMaRa'].isin(opposes_total)]
        print(f"⛔ {len(opposes_total)} patient(s) non importé(s) : opposition à toute réutilisation "
              f"de leurs données (ID BaMaRa : {', '.join(map(str, sorted(opposes_total, key=str)))}).")
    # Consentement ERN -> leg_patient_REDgistryCons (colonne technique, utilisée par
    # handle_FREDD_columns puis supprimée).
    consentements_ern = consentement_ern_par_patient(df_admin)
    if consentements_ern:
        df_final[COL_AIDE_CONSENTEMENT] = df_final['ID BaMaRa'].map(consentements_ern)

    # Filtering mappings for the 'Prises en charge' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Prises en charge']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]  # mandatory
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_prises.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_prises.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Prises en charge' : {missing_cols}")
    df_prises_filtre = df_prises[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_prises_filtre,
        on=['ID BaMaRa'],  # key for joint
        how="left"
    )

    # Filtering mappings for the 'Diagnostic' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Diagnostics']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa", "Identifiant du bloc diagnostic"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_diagnostic.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_diagnostic.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Diagnostics' : {missing_cols}")
    df_diag_filtre = df_diagnostic[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_diag_filtre,
        on=['ID BaMaRa'],
        how="left"
    )
    df_final.rename(columns={"Identifiant du bloc diagnostic": "ID diagnostic", "Statut": "Statut_y"}, inplace=True)

    # Filtering mappings for the 'Gènes' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Gènes']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    # Chaque gène est rattaché à une investigation génétique.
    colonnes_additionnelles = ["ID diagnostic", COL_ID_INVEST, "ID gène"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    colonnes_genetiques = [c for c in colonnes_a_extraire if c != "ID diagnostic"]
    if not all(col in df_genes.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_genes.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Gènes' : {missing_cols}")
    df_genes_filtre = df_genes[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_genes_filtre,
        on=['ID diagnostic'],
        how="left"
    )

    # Filtering mappings for the 'Variations nucléotidiques' sheet (formerly 'Variations')
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Variations nucléotidiques']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID gène"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_var.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_var.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Variations nucléotidiques' : {missing_cols}")
    df_var_filtre = df_var[colonnes_a_extraire]
    colonnes_genetiques += [c for c in colonnes_a_extraire if c not in colonnes_genetiques]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_var_filtre,
        on=['ID gène'],
        how="left"
    )

    # Filtering mappings for the 'Anté-neonatal' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Anté-néonatal']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_neo.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_neo.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Anté-néonatal' : {missing_cols}")
    df_neo_filtre = df_neo[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_neo_filtre,
        on=['ID BaMaRa'],
        how="left"
    )

    # Filtering mappings for the 'Recherche' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Recherche']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_rec.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_rec.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Recherche' : {missing_cols}")
    df_rec_filtre = df_rec[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_rec_filtre,
        on=['ID BaMaRa'],
        how="left"
    )

    # Conversion of ORPHA in numbers
    df_final['Code orphanet'] = pd.to_numeric(
        df_final['Code orphanet'],
        errors='coerce'
    )

    # Un bloc diagnostic BaMaRa peut porter le code ORPHA, le nom de la maladie (thésaurus
    # Orphanet), ou les deux. Avant le filtrage, on complète le code manquant à partir du nom
    # et le nom manquant à partir du code. Voir orphanet.completer_codes_et_noms ; aucune
    # valeur déjà renseignée n'est modifiée.
    col_nom_maladie = df_mapping[df_mapping['Nom standardisé'] == 'maladie']['Nom BaMaRa'].dropna().iloc[0]
    df_final, rapport_orpha = completer_codes_et_noms(
        df_final, 'Code orphanet', col_nom_maladie, 'ID BaMaRa', nomenclature)
    print(f"Correspondance nom <-> code ORPHA (lignes) : "
          f"{rapport_orpha['codes_niveau1'] + rapport_orpha['codes_niveau2']} code(s) retrouvé(s) "
          f"(export : {rapport_orpha['codes_niveau1']}, nomenclature : {rapport_orpha['codes_niveau2']}), "
          f"{rapport_orpha['noms_niveau1'] + rapport_orpha['noms_niveau2']} nom(s) retrouvé(s), "
          f"{rapport_orpha['non_resolus'] + rapport_orpha['ambigus']} non résolu(s) "
          f"(dont {rapport_orpha['ambigus']} ambigu(s)).")

    # Counting total lines before filtering
    total_lignes = len(df_final)
    print(f"Nombre total de lignes lues : {total_lignes}")

    # Cleaning of description clinique column
    desc = (
        df_final['Description clinique']
        .fillna('')
        .astype(str)
        .str.strip()
    )

    # Defintion of filters on BaMaRa "Status" column
    cond_orpha_valide = (
        df_final['Statut_y'].isin(["probable", "confirmé"]) &
        df_final['Code orphanet'].isin(liste_MR)
    )
    # add "Indeterminé" status if HPO codes presents
    cond_indet_desc = (
        (df_final['Statut_y'] == 'indéterminé') &
        (desc != '')
    )

    # Final filtering
    df_filtered = df_final[cond_orpha_valide | cond_indet_desc]

    # Counting filtered lines
    gardees_code_orpha = cond_orpha_valide.sum()
    gardees_indet_desc = cond_indet_desc.sum()
    lignes_gardees = (cond_orpha_valide | cond_indet_desc).sum()
    lignes_filtrees = total_lignes - lignes_gardees

    # Display filetring results
    print(f"Lignes conservées (statut BaMaRa probable/confirmé + code ORPHA valide) : {gardees_code_orpha}")
    print(f"Lignes conservées (statut BaMaRa indéterminé + description clinique) : {gardees_indet_desc}")
    print(f"Total des lignes conservées : {lignes_gardees}")
    print(f"Lignes filtrées (exclues) : {lignes_filtrees}")

    # Opposition à la réutilisation des données génétiques : AUCUNE génétique importée pour ce
    # patient (gènes, variations, investigations -> ni « Test génétique » ni statut déduits).
    opposes_gen = patients_opposes(df_invest, COL_OPPOSITION_GENETIQUE)
    if opposes_gen:
        masque_gen = df_filtered['ID BaMaRa'].isin(opposes_gen)
        if masque_gen.any():
            df_filtered = df_filtered.copy()
            cols_vider = [c for c in colonnes_genetiques if c in df_filtered.columns]
            df_filtered.loc[masque_gen, cols_vider] = np.nan
            df_filtered = df_filtered[~df_filtered.duplicated(keep="first")]
        df_invest = df_invest[~df_invest['ID BaMaRa'].isin(opposes_gen)]
        print(f"⛔ {len(opposes_gen)} patient(s) importé(s) SANS données génétiques : opposition à la "
              f"réutilisation de leurs données génétiques (ID BaMaRa : "
              f"{', '.join(map(str, sorted(opposes_gen, key=str)))}).")

    # Ne garder, pour chaque maladie, que la génétique de la dernière investigation (date du
    # compte rendu). Voir investigations.py pour les règles.
    _nom_bamara = lambda cle: df_mapping[df_mapping['Nom standardisé'] == cle]['Nom BaMaRa'].dropna().iloc[0]
    df_filtered, rapport_inv = selectionner_derniere_investigation(
        df_filtered, df_invest, 'Code orphanet', col_nom_maladie, colonnes_genetiques,
        col_causal=_nom_bamara('var_caus'), col_classe=_nom_bamara('var_classe'))
    # « Test génétique » coché automatiquement pour toute maladie ayant au moins une
    # investigation génétique. On l'ajoute au champ BaMaRa des investigations réalisées
    # (déprécié, souvent vide) ; handle_FREDD_columns en déduit diaCli_invesGen[Add].
    col_invest = _nom_bamara('invest')
    blocs_inv = rapport_inv['blocs_avec_investigation']
    if col_invest in df_filtered.columns and blocs_inv:
        df_filtered = df_filtered.copy()
        masque = df_filtered['ID diagnostic'].isin(blocs_inv)
        actuel = df_filtered.loc[masque, col_invest].fillna('').astype(str)
        df_filtered.loc[masque, col_invest] = [
            t if 'test génétique' in t else (f"{t}, test génétique" if t.strip() else "test génétique")
            for t in actuel]
    # « Statut du résultat » de l'investigation retenue, par maladie -> diaGen_statut_analyse
    # (handle_FREDD_columns). Transporté dans une colonne technique, par patient, supprimée avant envoi.
    if rapport_inv['statut_par_maladie']:
        par_patient = {}
        for (id_patient, cle), statut in rapport_inv['statut_par_maladie'].items():
            par_patient.setdefault(id_patient, {})[cle] = statut
        df_filtered = df_filtered.copy()
        df_filtered[COL_AIDE_STATUT] = df_filtered['ID BaMaRa'].map(
            lambda i: json.dumps(par_patient[i], ensure_ascii=False) if i in par_patient else None)
    print(f"Investigations génétiques : {rapport_inv['investigations']} rattachée(s) aux patients "
          f"retenus, {rapport_inv['retenues']} retenue(s), {rapport_inv['ecartees']} écartée(s) "
          f"(plus anciennes) ; {rapport_inv['ex_aequo']} cas d'ex æquo, {rapport_inv['sans_date']} "
          f"maladie(s) sans investigation datée.")
    print("✅ Étape 1 terminée.")

    return df_filtered


def diag_block_treatment(df, mapping_file):
    """
    This function processes a DataFrame so that there is only one row per BaMaRa ID. For each patient (BaMaRa ID),
    it reconciles each diagnosis with its associated variables and with each associated gene, which in turn is linked
    to multiple variations.

    Rare disease 1  — Gene 1    — Variation 1
                                — Variation 2
                    — Gene 2    — Variation 3
                                — Variation 4
    Rare disease 2  — Gene 3    — Variation 5
                                — Variation 6
                    — Gene 4    — Variation 7
                                — Variation 8

    The code identifies each patient by their BaMaRa ID, then builds a hierarchical structure for each patient where:
    - a patient can have multiple rare diseases,
    - each disease can be linked to multiple genes,
    - each gene can have multiple variations.

    The code consolidates redundant rows by updating existing information using the most recent data entered in BaMaRa
    (latest diagnostic block).

    Arguments:
    - df (pd.DataFrame): Input DataFrame
    - mapping_file: BaMaRa / FREDD mapping file

    Returns the processed DataFrame (df).
    """
    mapping_df = load_mapping_file(mapping_file)

    # Filter rows where all key columns are present in the mapping file
    mapping_standard_to_bamara = (
        mapping_df[
            mapping_df["Nom standardisé"].notna()
            & mapping_df["Nom BaMaRa"].notna()
            & mapping_df["Nom FREDD"].notna()
        ]
        .groupby("Nom standardisé")[["Nom BaMaRa", "Nom FREDD"]]  # we use the column "Nom standardisé" of the mapping file
        .apply(lambda df: df.to_dict(orient="records"))
        .to_dict()
    )

    # These do not depend on the row being processed, so they are computed once here instead
    # of on every iteration of the loop below.
    cles = [
        "maladie", "code", "statut", "signesAss",
        #"signesInh",
        "invest", "diaCara",
        "PS", "agePS", "diaCli", "ageDC", "diaGen", "ageDG", "spor", "gene_nom", "var_nom",
        "var_refseq", "var_classe", "var_stat", "var_caus", "var_trans", "var_par"
    ]
    # Maps each standardized key (cles, plus "id_bamara") to its BaMaRa column name, so the
    # loop below can do row_dict[col_for[key]] instead of repeating the
    # mapping_standard_to_bamara[key][0].get("Nom BaMaRa") lookup on every single row.
    col_for = {key: mapping_standard_to_bamara[key][0].get("Nom BaMaRa") for key in ["id_bamara"] + cles}
    noms_bamara = [col_for[cle] for cle in cles]
    noms_bamara.extend(["Statut_y", "ID diagnostic", COL_ID_INVEST, "ID gène", "genes"])  # We add these manually created variables
    noms_bamara.append(mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+"Add")  # idem

    processed_data = {}  # Dictionary to store consolidated rows
    # La hiérarchie maladie -> gènes -> variations est indexée par le CODE ORPHA (le nom
    # seulement à défaut de code), pour qu'un même diagnostic ne devienne pas deux maladies
    # selon que le nom est renseigné ou non. Le nom est conservé à part dans nom_par_cle et
    # réécrit à l'export.
    nom_par_cle = {}
    var_att = {}
    maladie_att = {}
    patient_multiple_bloc = 0
    patient_multiple_diag = 0
    # Bulk-convert the whole DataFrame to a list of plain dicts, one per row, in the same
    # order as the DataFrame -- this replaces df.iterrows() (which would build one pandas
    # Series per row before this same to_dict() call). Same row dicts, same order, no change
    # in what gets computed; see the performance note in this function's docstring.
    for row_dict in df.to_dict(orient="records"):
        id_bamara = row_dict[col_for["id_bamara"]]
        maladie = row_dict[col_for["maladie"]]
        code = row_dict[col_for["code"]]
        statut = row_dict["Statut_y"]  # This column was renamed earlier because it has the same name as "Status" in the "Variation" sheet.
        signesAss = row_dict[col_for["signesAss"]]
        #signesInh = row_dict[col_for["signesInh"]]  # removed from the lastest Export version
        invest = row_dict[col_for["invest"]]
        diaCara = row_dict[col_for["diaCara"]]
        PS = row_dict[col_for["PS"]]
        agePS = row_dict[col_for["agePS"]]
        diaCli = row_dict[col_for["diaCli"]]
        ageDC = row_dict[col_for["ageDC"]]
        diaGen = row_dict[col_for["diaGen"]]
        ageDG = row_dict[col_for["ageDG"]]
        spor = row_dict[col_for["spor"]]
        gene_nom = row_dict[col_for["gene_nom"]]
        variation_nom = row_dict[col_for["var_nom"]]
        var_refseq = row_dict[col_for["var_refseq"]]
        var_classe = row_dict[col_for["var_classe"]]
        var_stat = row_dict[col_for["var_stat"]]
        var_caus = row_dict[col_for["var_caus"]]
        var_trans = row_dict[col_for["var_trans"]]
        var_par = row_dict[col_for["var_par"]]

        # Clé de la maladie : code ORPHA, ou nom à défaut. `maladie` désigne cette clé dans
        # toute la suite de la boucle ; le nom lisible est `nom_maladie`, mémorisé (premier nom
        # non vide rencontré) dans nom_par_cle.
        nom_maladie = row_dict[col_for["maladie"]]
        code_cle = _code_valide(code)
        maladie = code_cle if code_cle is not None else nom_maladie
        if pd.notna(nom_maladie) and pd.isna(nom_par_cle.get((id_bamara, maladie))):
            nom_par_cle[id_bamara, maladie] = nom_maladie

        if id_bamara not in processed_data:  # The BaMaRa ID has never been encountered: this is a new patient.
            # Initialisation of the whole structure (reuses row_dict built above instead of
            # calling row.to_dict() a second time).
            processed_data[id_bamara] = row_dict
            nom_fredd = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")
            processed_data[id_bamara][nom_fredd] = nom_maladie  # Diagnostis assigned (name; rewritten at export)
            processed_data[id_bamara][nom_fredd+'Add'] = []  # Preparing the case where a second disease is present
            processed_data[id_bamara]['genes'] = {maladie: {gene_nom: {variation_nom}}}  # Define the hierarchy of variables (1 diagnosis = X genes = Y variations)
            maladie_att[id_bamara, maladie] = [code, statut, signesAss,
                                               #signesInh,
                                               invest, diaCara,  # Definition of diagnosis attributes
                                               PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
            var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,var_refseq,  # Definition of variantion attributes
                                                                    var_trans,var_par]
        else:  # The ID BaMaRa has already been seen
            current_data = processed_data[id_bamara]
            if pd.notna(maladie) and maladie not in current_data['genes']:  # It is a new disease for this patient
                nom_fredd = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")
                current_data[nom_fredd+'Add'].append(nom_maladie)
                current_data['genes'][maladie] = {gene_nom: {variation_nom}}
                var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,var_refseq,var_trans,var_par]
                maladie_att[id_bamara, maladie] = [code, statut, signesAss,
                                                   #signesInh,
                                                   invest, diaCara,PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                print(f"Le patient {id_bamara} a plusieurs maladies rares de l'œil")
                patient_multiple_diag += 1
            elif maladie not in current_data['genes']:

                print(f"⚠️ Patient {id_bamara} : bloc diagnostic sans nom de maladie ignoré "
                      f"(aucune maladie rare à laquelle rattacher ce bloc).")
            else:
                if pd.notna(gene_nom) and gene_nom not in current_data['genes'][maladie]:  # It is a new gene related to this disease
                    current_data['genes'][maladie][gene_nom] = {variation_nom}
                    var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,
                                                                            var_refseq,var_trans,var_par]
                    maladie_att[id_bamara, maladie] = [code, statut, signesAss,
                                                       #signesInh,
                                                       invest, diaCara,
                                               PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                else:  # It is the same gene for the same disease
                    if pd.notna(variation_nom) and variation_nom not in current_data['genes'][maladie][gene_nom]:  # It is a new variation
                        current_data['genes'][maladie][gene_nom].add(variation_nom)
                        var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,
                                                                                var_refseq,var_trans,var_par]
                        maladie_att[id_bamara, maladie] = [code, statut, signesAss,
                                                           #signesInh,
                                                           invest, diaCara,
                                                   PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                    else:  # It is the same disease, the same gene, and the same variation, so the variables for this diagnostic block are updated with the most recent values
                        print(f"Attention, le patient {id_bamara} a plusieurs blocs de diagnostic pour la maladie : "
                              f"{nom_par_cle.get((id_bamara, maladie), maladie)}")
                        patient_multiple_bloc += 1
                        for column, new_value in row_dict.items():
                            if (pd.isna(current_data.get(column)) and pd.notna(new_value)) or (current_data.get(column) != new_value and pd.notna(new_value)):  # Compare current values
                                print(f"Patient {id_bamara} : mise à jour de la colonne {column} : {current_data.get(column)} -> {new_value}")
                                current_data[column] = new_value
                                # Update of other values related to maladie_att
                                maladie_att[id_bamara, maladie] = [
                                    current_data.get(mapping_standard_to_bamara["code"][0].get("Nom BaMaRa")),
                                    current_data.get("Statut_y"),
                                    current_data.get(mapping_standard_to_bamara["signesAss"][0].get("Nom BaMaRa")),
                                    #current_data.get(mapping_standard_to_bamara["signesInh"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["invest"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaCara"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["PS"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["agePS"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaCli"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["ageDC"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaGen"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["ageDG"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["spor"][0].get("Nom BaMaRa"))
                                ]

    # Converting the dictionary into a DataFrame, expanding each variable for each diagnostic block
    output_data = []
    for id_bamara, data in processed_data.items():
        row_dict = {key: value for key, value in data.items() if key not in noms_bamara}  # to avoid filling the manually processed columns twice


        maladie_index = 1  # used to keep track of the disease concerned (maximum of 2 rare diseases per patient)

        # Tri par NOM pour fixer l'ordre maladie principale / additionnelle, la clé (code) à défaut.
        def _nom(cle):
            nom = nom_par_cle.get((id_bamara, cle))
            return nom if pd.notna(nom) else cle
        maladies_triees = sorted(data['genes'].items(), key=lambda x: str(_nom(x[0])))
        if len(maladies_triees) > 2:
            extras = [str(_nom(m)) for m, _ in maladies_triees[2:]]
            print(f"⚠️ Patient {id_bamara} : {len(maladies_triees)} maladies rares détectées, "
                  f"or FREDD n'en gère que 2 par patient. Maladie(s) non exportée(s), à traiter "
                  f"manuellement : {', '.join(extras)}")
            maladies_triees = maladies_triees[:2]
        for maladie_index, (maladie, genes) in enumerate(maladies_triees, start=1):

            nom_fredd_mal = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")
            nom_export = nom_par_cle.get((id_bamara, maladie), np.nan)  # nom lisible, pas la clé
            if maladie_index == 1:
                row_dict[nom_fredd_mal] = nom_export
            else:
                row_dict[nom_fredd_mal + "Add"] = nom_export
            gene_index = 1  # Used to keep track of the gene concerned (any number of genes per disease)
            for gene, variations in sorted(genes.items(), key=lambda x: str(x[0])):
                if maladie_index == 1:
                    malID = ""
                    value = maladie_att.get((id_bamara, maladie), None)
                    if value is not None:
                        row_dict[mapping_standard_to_bamara["code"][0].get("Nom FREDD")] = value[0]
                        row_dict[mapping_standard_to_bamara["statut"][0].get("Nom FREDD")] = value[1]
                        row_dict[mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")] = value[2]
                        #row_dict[mapping_standard_to_bamara["signesInh"][0].get("Nom FREDD")] = value[3]
                        row_dict[mapping_standard_to_bamara["invest"][0].get("Nom FREDD")] = value[3]
                        row_dict[mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD")] = value[4]
                        row_dict[mapping_standard_to_bamara["PS"][0].get("Nom FREDD")] = value[5]
                        row_dict[mapping_standard_to_bamara["agePS"][0].get("Nom FREDD")] = value[6]
                        row_dict[mapping_standard_to_bamara["diaCli"][0].get("Nom FREDD")] = value[7]
                        row_dict[mapping_standard_to_bamara["ageDC"][0].get("Nom FREDD")] = value[8]
                        row_dict[mapping_standard_to_bamara["diaGen"][0].get("Nom FREDD")] = value[9]
                        row_dict[mapping_standard_to_bamara["ageDG"][0].get("Nom FREDD")] = value[10]
                        row_dict[mapping_standard_to_bamara["spor"][0].get("Nom FREDD")] = value[11]
                else:
                    malID = "Add"
                    value = maladie_att.get((id_bamara, maladie), None)
                    if value is not None:
                        row_dict[mapping_standard_to_bamara["code"][0].get("Nom FREDD")+"Add"] = value[0]
                        row_dict[mapping_standard_to_bamara["statut"][0].get("Nom FREDD")+"Add"] = value[1]
                        row_dict[mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")+"Add"] = value[2]
                        #row_dict[mapping_standard_to_bamara["signesInh"][0].get("Nom FREDD")+"Add"] = value[3]
                        row_dict[mapping_standard_to_bamara["invest"][0].get("Nom FREDD")+"Add"] = value[3]
                        row_dict[mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD")+"Add"] = value[4]
                        row_dict[mapping_standard_to_bamara["PS"][0].get("Nom FREDD")+"Add"] = value[5]
                        row_dict[mapping_standard_to_bamara["agePS"][0].get("Nom FREDD")+"Add"] = value[6]
                        row_dict[mapping_standard_to_bamara["diaCli"][0].get("Nom FREDD")+"Add"] = value[7]
                        row_dict[mapping_standard_to_bamara["ageDC"][0].get("Nom FREDD")+"Add"] = value[8]
                        row_dict[mapping_standard_to_bamara["diaGen"][0].get("Nom FREDD")+"Add"] = value[9]
                        row_dict[mapping_standard_to_bamara["ageDG"][0].get("Nom FREDD")+"Add"] = value[10]
                        row_dict[mapping_standard_to_bamara["spor"][0].get("Nom FREDD")+"Add"] = value[11]
                # We now populate the genetic variables associated with each disease
                row_dict[mapping_standard_to_bamara["gene_nom"][0].get("Nom FREDD")+"_"+ f"{gene_index}{malID}"] = gene
                for i, variation in enumerate(sorted(variations, key=str), start=1):
                    row_dict[mapping_standard_to_bamara["var_nom"][0].get("Nom FREDD")+f"_{gene_index}_{i}{malID}"] = variation
                    value = var_att.get((id_bamara, maladie, gene, variation), None)
                    if value is not None:
                        # _classe_fredd normalise la classe du variant : SKEZIA attend "3",
                        # pas "3.0" (classe lue comme nombre par Excel) ni "nan".
                        row_dict[mapping_standard_to_bamara["var_classe"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"] = _classe_fredd(value[0])
                        row_dict[mapping_standard_to_bamara["var_stat"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[1]
                        row_dict[mapping_standard_to_bamara["var_caus"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[2]
                        row_dict[mapping_standard_to_bamara["var_refseq"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[3]
                        row_dict[mapping_standard_to_bamara["var_trans"][0].get("Nom FREDD") + f"_{gene_index}{malID}"]  = value[4]
                        row_dict[mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[5]
                gene_index += 1

        output_data.append(row_dict)

    df = pd.DataFrame(output_data)
    print(f"{patient_multiple_bloc} bloc(s) de diagnostic en double pour la même maladie chez un même patient")
    print(f"{patient_multiple_diag} patient(s) atteint(s) de 2 maladies rares de l'œil")
    print("✅ Étape 2 terminée.")
    return df


def handle_FREDD_columns(df_output, mapping_file, centre, numero_centre):
    """
    Function to process and transform a DataFrame specific to FREDD (hard-coded FREDD column names).
    This function performs several operations to adapt the DataFrame to FREDD dataset requirements. It applies specific
    transformations to certain columns, fills missing columns, and performs calculations on the data to make it compatible with
    the SKEZIA FREDD e-CRF.

    Arguments:
    - df_output: DataFrame containing the data to be processed.
    - mapping_file: Path to an Excel file containing mappings between BaMaRa and FREDD column names.
    - centre: Name of the treatment center.
    - numero_centre: Identifier of the FREDD data collection center.
    """
    def deploy_column_signs(df, column_name, max_elements, prefix=""):
        """
        Generic function to expand values contained in a column into multiple columns based on a maximum number of elements.
        Arguments:
        - df: DataFrame
        - column_name: Name of the column to process
        - max_elements: Maximum number of allowed elements (10 for clinical signs, 5 for unusual signs, etc.)
        - prefix: Prefix used to name the new columns
        """
        if column_name+prefix in df.columns and df[column_name+prefix].notna().any():
            max_signes = df[column_name+prefix].dropna().str.split(',').map(len).max()
            if max_signes > max_elements:
                max_signes = max_elements
            for i in range(max_signes):
                if 'Add' in prefix:
                    col_name = column_name+str(i+1)+'Add'
                else:
                    col_name = column_name+str(i+1)
                df[col_name] = df[column_name+prefix].fillna('').str.split(',').str[i]
            # Supress the original column
            df = df.drop(columns=[column_name+prefix])
        return df

    def compute_chained_counts(df, base_name, target_base, suffixes, chain_len=3, group_indices=None):
        """
        Calculates the number of non-empty chained columns and fills a target column. Used to compute the number of genes
        and variants based on columns containing filled-in names.
        - df: DataFrame
        - base_name: column prefix (e.g. 'diaGen_var_hgcn' or 'diaGen_var_nom')
        - target_base: base name of the target column (e.g. 'diaGen_var_nbgene')
        - suffixes: list of suffixes to process (e.g. ['', 'Add'])
        - chain_len: chain length (default 3)
        - group_indices: list of indices for grouped variants (e.g. [1, 2, 3]) or None
        """
        for suffix in suffixes:
            if group_indices is None:
                # Cases diaGen_var_hgcn_1, _2, _3
                target_col = f"{target_base}{suffix}"
                #df[target_col] = 0

                col_names = [f"{base_name}_{i}{suffix}" for i in range(1, chain_len + 1)]
                if col_names[0] in df.columns:
                    mask = df[col_names[0]] != ""
                    df.loc[mask, target_col] = 1
                    for i in range(1, chain_len):
                        if col_names[i] in df.columns:
                            mask = mask & (df[col_names[i]] != "")
                            df.loc[mask, target_col] = i + 1
            else:
                # case for Cas diaGen_var_nom_1_1, _1_2, etc.
                for idx in group_indices:
                    target_col = f"{target_base}_{idx}{suffix}"
                    #df[target_col] = 0

                    col_names = [f"{base_name}_{idx}_{i}{suffix}" for i in range(1, chain_len + 1)]
                    if col_names[0] in df.columns:
                        mask = df[col_names[0]] != ""
                        df.loc[mask, target_col] = 1
                        for i in range(1, chain_len):
                            if col_names[i] in df.columns:
                                mask = mask & (df[col_names[i]] != "")
                                df.loc[mask, target_col] = i + 1
        return df

    df_mapping = load_mapping_file(mapping_file)

    # Building the renaming dictionary using the standardised names
    mapping_standard_to_bamara = (
        df_mapping[
            df_mapping["Nom standardisé"].notna()
            & df_mapping["Nom BaMaRa"].notna()
            & df_mapping["Nom FREDD"].notna()
        ]
        .groupby("Nom standardisé")[["Nom BaMaRa", "Nom FREDD"]]
        .apply(lambda df: df.to_dict(orient="records"))
        .to_dict()
    )
    # Notice FREDD OK, REDgistry KO for the moment
    df_output['leg_notice_info'] = 'true'
    # Consentement ERN (REDgistry) : 1 = Oui si « Consentements ERN » contient « EYE », sinon
    # 0 = Non (voir consentements.py) ; colonne technique supprimée ensuite.
    if COL_AIDE_CONSENTEMENT in df_output.columns:
        df_output['leg_patient_REDgistryCons'] = df_output[COL_AIDE_CONSENTEMENT].fillna('0').replace('', '0')
        df_output = df_output.drop(columns=[COL_AIDE_CONSENTEMENT])
    else:
        df_output['leg_patient_REDgistryCons'] = '0'
    df_output['leg_date_incFREDD'] = date.today()

    df_output['leg_site_inc_nom'] = centre
    df_output['leg_site_inc'] = numero_centre

    df_output['adm_patient_MR'] = 'true'  #because of deletion of "Non malade" column in BaMaRa -> diseased by default

    # Filling of boolean variables related to clinical signs: set to "Yes" if at least one clinical sign boolean is "Yes"
    col_signesAss_base = mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")
    for suffix in ["", "Add"]:
        col_signesAss = col_signesAss_base + suffix
        col_signesAss_boo = col_signesAss_base + "Boo" + suffix
        if col_signesAss in df_output.columns:
            non_vide = (df_output[col_signesAss].notna()) & (df_output[col_signesAss] != "")
            df_output[col_signesAss_boo] = non_vide.map({True: "true", False: "false"})

    # Expansion of columns containing multiple fields
    columns_to_deploy = [
        ("signesAss", 10),  # max 10 clinical signs
        #("signesInh", 5),  #max 5
        ("pro_name", 3)     # max 3 essais cliniques : SKEZIA n'expose que pro_nom1..pro_nom3
    ]                       # (pro_nom4 rejeté à l'import). Le 4e essai et au-delà sont tronqués.
    for key, max_count in columns_to_deploy:
        col_base = mapping_standard_to_bamara[key][0].get("Nom FREDD")
        df_output = deploy_column_signs(df_output, col_base, max_count)

        if key != "pro_name":  # no "Add" for pro_name
            df_output = deploy_column_signs(df_output, col_base, max_count, "Add")
        df_output = df_output.fillna('')


    col_pays = mapping_standard_to_bamara["pays"][0].get("Nom FREDD")
    if col_pays in df_output.columns:
        mask = df_output[col_pays].str.contains(r"\(", na=False)
        df_output.loc[mask, "adm_ville_naissance"] = df_output.loc[mask, col_pays].str.replace("|", " ", regex=False)
        df_output.loc[mask, col_pays] = "France"

    # Genetic characterization → analysis status set to "Completed"
    for suffix in ["", "Add"]:
        col_cara = mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD") + suffix
        col_statut = "diaGen_statut_analyse" + suffix
        if col_cara in df_output.columns:
            df_output[col_statut] = df_output[col_cara].eq("Oui").map({True: "2", False: None})
    # Une investigation retenue avec un « Statut du résultat » (Concluant / Non conclusif)
    # signifie une analyse terminée -> 2 = Terminé.
    if COL_AIDE_STATUT in df_output.columns:
        col_code = mapping_standard_to_bamara["code"][0].get("Nom FREDD")
        col_nom = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")
        for suffix in ["", "Add"]:
            if col_code + suffix not in df_output.columns and col_nom + suffix not in df_output.columns:
                continue
            def _termine(row, suffix=suffix):
                aide = row.get(COL_AIDE_STATUT)
                if not isinstance(aide, str) or not aide:
                    return False
                cle = _cle_maladie(row.get(col_code + suffix), row.get(col_nom + suffix))
                return cle in json.loads(aide)
            termine = df_output.apply(_termine, axis=1)
            if termine.any():
                df_output.loc[termine, "diaGen_statut_analyse" + suffix] = "2"
        df_output = df_output.drop(columns=[COL_AIDE_STATUT])

    # Expansion of the list of investigations
    mapping = {
        "clinique": "diaCli_invesCli",
        "biochimique": "diaCli_invesBio",
        "biologique": "diaCli_invesBiol",
        "imagerie": "diaCli_invesIma",
        "exploration fonctionnelle": "diaCli_invesExp",
        "anatomopathologie": "diaCli_invesAna",
        "test génétique": "diaCli_invesGen"
    }

    # Add "Boo" (for boolean) columns with default value "true".
    for suffix in ["", "Add"]:
        nom_fredd = mapping_standard_to_bamara["invest"][0].get("Nom FREDD")
        df_output[nom_fredd + "Boo" + suffix] = "true"
        col_check = nom_fredd + suffix
        if col_check in df_output.columns:
            # Select the column to be tested for each suffix
            valid_rows = df_output[col_check].notna() & df_output[col_check].str.contains("|".join(mapping.keys()), na=False)
            # Update the values of "Boo" based on the investigations found
            df_output.loc[valid_rows, nom_fredd + "Boo" + suffix] = "false"
            # Populate the corresponding investigation columns
            for investigation, target_col in mapping.items():
                valid_investigation_rows = valid_rows & df_output[col_check].str.contains(investigation, na=False)
                df_output.loc[valid_investigation_rows, target_col + suffix] = "1"
            df_output = df_output.drop(columns=[col_check])  # suppression of original column

    # Calculation of the number of genes from diaGen_var_hgcn
    df_output = compute_chained_counts(
        df_output,
        base_name=mapping_standard_to_bamara["gene_nom"][0].get("Nom FREDD"),
        target_base="diaGen_var_nbgene",
        suffixes=["", "Add"]
    )
    # Calculation of the number of variants from diaGen_var_nom
    df_output = compute_chained_counts(
        df_output,
        base_name=mapping_standard_to_bamara["var_nom"][0].get("Nom FREDD"),
        target_base="diaGen_var_nbvar",
        suffixes=["", "Add"],
        group_indices=[1, 2, 3]
    )

    # Check whether the patient has two rare diseases
    nomFREDD = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+"Add"
    if nomFREDD in df_output.columns:
        df_output["diaCli_nb_MR"] = df_output[nomFREDD].ne("").map({True: "true", False: "false"})
    else:
        df_output["diaCli_nb_MR"] = "false"

    # Variant origin = "de novo" → mode of inheritance = "de novo". Same for "unknown" → "undetermined"
    for suffix in ["", "Add"]:
        for i in range(1, 4):
            col_trans = mapping_standard_to_bamara["var_trans"][0].get("Nom FREDD") + f"_{i}{suffix}"
            if col_trans in df_output.columns:
                mask_denovo = pd.Series(False, index=df_output.index)
                mask_inconnue = pd.Series(False, index=df_output.index)

                for j in range(1, 4):
                    col_parent = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{i}_{j}{suffix}"
                    if col_parent in df_output.columns:
                        mask_denovo |= df_output[col_parent] == "de novo"
                        mask_inconnue |= df_output[col_parent] == "inconnue"

                df_output.loc[mask_denovo, col_trans] = "de novo"
                df_output.loc[mask_inconnue, col_trans] = "non déterminé"

                # Clear the source columns containing "de novo" or "unknown"
                for j in range(1, 4):
                    col_parent = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{i}_{j}{suffix}"
                    if col_parent in df_output.columns:
                        df_output.loc[mask_denovo & (df_output[col_parent] == "de novo"), col_parent] = ""
                        df_output.loc[mask_inconnue & (df_output[col_parent] == "inconnue"), col_parent] = ""

    # If a parent is indicated as carrying the same genetic variant, "Parent carries the variant?" is set to "true"
    for suffix in ["", "Add"]:
        for i in range(1, 4):
            for j in range(1, 4):
                base_name = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + "_" + str(i) + "_" + str(j)
                col_boo = f"{base_name}Boo{suffix}"
                col_source = base_name + suffix
                if col_source in df_output.columns:
                    df_output[col_boo] = df_output[col_source].apply(lambda x: "1" if pd.notna(x) and x != "" else "")

    # Enforcement of integer type for columns that must be integers
    cols_to_convert = [
        "diaGen_var_nbgene", "diaGen_var_nbgeneAdd", "diaCli_diagMR_code",
        "his_age_psignPrec", "his_age_diagMRPrec", "diaGen_agePrec",
        "diaCli_diagMR_codeAdd", "his_age_psignPrecAdd", "his_age_diagMRPrecAdd",
        "diaGen_agePrecAdd", "hisPer_sa"
    ]

    # Addition of dynamic columns: diaGen_var_nbvar_{i}[Add], diaGen_var_classe_{i}_{j}[Add]
    for i in range(1, 4):
        cols_to_convert.append(f"diaGen_var_nbvar_{i}")
        cols_to_convert.append(f"diaGen_var_nbvar_{i}Add")

    for col in cols_to_convert:
        if col in df_output.columns:
            df_output[col] = pd.to_numeric(df_output[col], errors='coerce').astype('Int64')

    print("✅ Étape 3 terminée.")
    return df_output


def appliquer_remplacements(df, mapping_file):
    """
    Function that applies, for each value, the replacement of BaMaRa values to make them match the expected values in the FREDD
    questionnaire.

    Arguments:
    - input df to be processed
    - mapping_file: BaMaRa/FREDD mapping file
    """
    def charger_remplacements_depuis_excel(fichier_excel):
        """
        Loads an Excel file containing mappings between BaMaRa and FREDD values and transforms it into a dictionary to facilitate
        subsequent replacements during data processing.

        Arguments:
        - fichier_excel: path to the Excel file containing the mappings to load.
        Returns remplacements (dict): a dictionary where keys are FREDD field names, and values are dictionaries mapping BaMaRa values to their corresponding FREDD values.
        """
        df = load_mapping_file(fichier_excel)
        df = df.dropna(subset=['Nom BaMaRa', 'Valeurs BaMaRa', 'Nom FREDD', 'Valeurs FREDD'])
        remplacements = {}

        for _, row in df.iterrows():
            nom_fredd = row['Nom FREDD']
            valeur_bamara = ' '.join(str(row['Valeurs BaMaRa']).split())
            valeur_fredd = ' '.join(str(row['Valeurs FREDD']).split())
            if nom_fredd not in remplacements:
                remplacements[nom_fredd] = {}
            remplacements[nom_fredd][valeur_bamara] = valeur_fredd
        return remplacements

    def nettoyer_chaine(s):
        """
        Cleans a string by:
        - Replacing multiple spaces with a single space.
        - Removing non-printable characters.
        - Stripping leading and trailing spaces.
        """
        if pd.isna(s):
            return s
        s = str(s)
        s = re.sub(r'\s+', ' ', s)  # replace all spaces by a single one
        s = ''.join(c for c in s if c.isprintable())  # Removes non-printable characters
        return s.strip()

    remplacements_valeurs = charger_remplacements_depuis_excel(mapping_file)
    # All FREDD field names that have their own value-replacement table. Used just below to
    # resolve prefix collisions.
    noms_fredd = set(remplacements_valeurs.keys())
    for nom_fredd, mapping in remplacements_valeurs.items():
        # Search for columns whose names start with the FREDD name prefix. The startswith is
        # deliberately broad: expanded columns are named <field><n>, <field>Boo, <field>Add,
        # <field>_1_1... with no single clean separator, so a "_"-boundary regex would wrongly
        # miss <field>1 / <field>Boo. That breadth, however, would let a SHORTER field name
        # capture the columns of a DIFFERENT, more specific field sharing its prefix (e.g.
        # "diaGen" grabbing "diaGen_var_nom_1_1"), applying the wrong value map to it.
        # Rule: when another, longer field name in the map is also a prefix of `col`, that more
        # specific field owns the column -- skip it here so only its own (correct) mapping
        # applies. An exact match (col == nom_fredd) always belongs to this field.
        colonnes_cibles = [
            col for col in df.columns
            if col == nom_fredd or (
                col.startswith(nom_fredd)
                and not any(len(autre) > len(nom_fredd) and col.startswith(autre)
                            for autre in noms_fredd)
            )
        ]
        for col in colonnes_cibles:
            df[col] = df[col].apply(nettoyer_chaine)
            df[col] = df[col].replace(mapping)

    df_clean = df.replace([np.inf, -np.inf], np.nan).infer_objects().where(pd.notnull(df), None)

    print("✅ Étape 4 terminée.")
    return df_clean