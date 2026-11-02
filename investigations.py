# -*- coding: utf-8 -*-
# FREDDEX - Sélection de la dernière investigation génétique
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Chaque gène et chaque variation sont rattachés à une INVESTIGATION génétique
# (ID Investigation), elle-même rattachée à un bloc diagnostic (ID diagnostic). Un patient peut
# avoir plusieurs investigations successives pour une même maladie (panel puis exome,
# réanalyse...).
#
# Règles métier :
#   1) Granularité : on garde la DERNIÈRE investigation PAR MALADIE (patient + code ORPHA, ou
#      nom à défaut de code), pas par patient : un patient à 2 maladies rares garde la
#      génétique de chacune.
#   2) UNE SEULE investigation par maladie (l'e-CRF FREDD n'en accepte qu'une) :
#      - au moins une datée -> la plus récente « Date du compte rendu » ; les non datées sont
#        écartées (alerte) ;
#      - aucune datée -> toutes sont candidates ;
#      - plusieurs candidates (même date, ou aucune datée) -> la PLUS CONCLUSIVE, critères
#        comparés dans l'ordre (le suivant ne sert qu'à départager une égalité) :
#          0) « Statut du résultat » = Concluant (onglet Investigations ; autre valeur
#             BaMaRa : « Non conclusif ») ;
#          a) nb de variants « Variant causal du phénotype » = Oui ;
#          b) nb de variants causaux « partiellement » ;
#          c) nb de variants de classe 4 ou 5 (ACMG/AMP ; classe 3 = VSI non conclusive) ;
#          d) complétude = nb de champs génétiques renseignés (hors identifiants) ;
#      - égalité persistante -> l'ID Investigation le plus élevé (dernier recours arbitraire
#        mais reproductible), avec alerte « À VÉRIFIER ».
#   3) Les gènes / variations des investigations écartées ne sont pas exportés ; le bloc
#      diagnostic lui-même est conservé (sans génétique s'il ne lui en reste aucune).
#   4) Si la dernière investigation n'a AUCUN gène alors qu'une investigation plus ancienne
#      en avait, la règle s'applique quand même (dernière = référence) mais le patient est
#      signalé : la génétique antérieure n'est pas exportée.
#   6) Toute maladie ayant au moins une investigation génétique (retenue ou non, quel que soit
#      son résultat) est marquée « test génétique » dans les investigations réalisées
#      (FREDD diaCli_invesGen / diaCli_invesGenAdd) -- voir transformation.traiter_donnees.
#   5) Un gène dont l'ID Investigation est absent de l'onglet « Investigations génétiques »
#      (incohérence d'export) est signalé et son investigation traitée comme NON DATÉE : elle
#      reste soumise à la règle 2 (jamais deux investigations exportées pour une maladie).

import numpy as np
import pandas as pd

from orphanet import _code_valide, normaliser_nom

COL_ID_PATIENT = "ID BaMaRa"
COL_ID_DIAG = "ID diagnostic"
COL_ID_INVEST = "ID Investigation"
COL_DATE_CR = "Date du compte rendu"
COL_STATUT_RESULTAT = "Statut du résultat"   # valeurs BaMaRa : « Concluant » / « Non conclusif »
COLONNES_INVESTIGATIONS = [COL_ID_PATIENT, COL_ID_DIAG, COL_ID_INVEST, COL_DATE_CR]


def _cle_maladie(code, nom):
    """Clé de regroupement d'une maladie : code ORPHA, sinon nom normalisé, sinon aucune."""
    code = _code_valide(code)
    if code is not None:
        return f"ORPHA:{code}"
    nom = normaliser_nom(nom)
    return f"NOM:{nom}" if nom else "AUCUNE"


def _dates(serie):
    """Convertit « Date du compte rendu » en datetime (formats Excel ou jj/mm/aaaa)."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie
    return pd.to_datetime(serie, errors="coerce", dayfirst=True)


CLASSES_CONCLUSIVES = {"4", "5"}  # ACMG/AMP (Richards et al., Genet Med 2015) ; 3 = VSI


def _est_causal(v):
    return isinstance(v, str) and v.strip().casefold() == "oui"


def _est_concluant(v):
    return isinstance(v, str) and v.strip().casefold() == "concluant"


def _est_partiellement_causal(v):
    return isinstance(v, str) and v.strip().casefold() == "partiellement"


def _cle_id(v):
    """Clé de tri d'un ID Investigation : numérique si possible, sinon texte."""
    try:
        return (0, float(v), "")
    except (TypeError, ValueError):
        return (1, 0.0, str(v))


def _est_classe_conclusive(v):
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return False
    s = str(v).strip().casefold().removeprefix("classe").strip()
    s = s[:-2] if s.endswith(".0") else s
    return s in CLASSES_CONCLUSIVES


def selectionner_derniere_investigation(df, df_invest, col_code, col_nom, colonnes_genetiques,
                                        col_causal="Variant causal du phénotype",
                                        col_classe="Classe de la variation"):
    """
    Applique les règles ci-dessus à `df` (une ligne par bloc diagnostic x gène x variation,
    après fusion des onglets), à l'aide de l'onglet « Investigations génétiques ».

    Arguments :
    - df : DataFrame fusionné et filtré (contient ID BaMaRa, ID diagnostic, ID Investigation,
      le code ORPHA, le nom de la maladie et les colonnes génétiques).
    - df_invest : onglet « Investigations génétiques ».
    - col_code / col_nom : colonnes BaMaRa du code ORPHA et du nom de la maladie.
    - colonnes_genetiques : colonnes issues des onglets Gènes et Variations nucléotidiques
      (vidées pour les investigations écartées).
    - col_causal / col_classe : colonnes BaMaRa « Variant causal du phénotype » et « Classe de
      la variation » (critères de conclusivité, cas « aucune investigation datée »).

    Renvoie (df, rapport) ; rapport est un dict de compteurs.
    """
    rapport = {"investigations": 0, "retenues": 0, "ecartees": 0, "ex_aequo": 0,
               "non_datees_ecartees": 0, "sans_date": 0, "derniere_sans_gene": 0,
               "genes_investigation_inconnue": 0, "departage_par_id": 0,
               "blocs_avec_investigation": set(), "statut_par_maladie": {}}
    if df.empty:
        return df, rapport
    manquantes = [c for c in COLONNES_INVESTIGATIONS if c not in df_invest.columns]
    if manquantes:
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille "
                         f"'Investigations génétiques' : {manquantes}")
    df = df.copy()
    colonnes_genetiques = [c for c in colonnes_genetiques if c in df.columns]

    # Maladie de chaque bloc diagnostic retenu à l'étape 1.
    blocs = df[[COL_ID_PATIENT, COL_ID_DIAG, col_code, col_nom]].drop_duplicates(COL_ID_DIAG)
    blocs["cle"] = [_cle_maladie(c, n) for c, n in zip(blocs[col_code], blocs[col_nom])]
    blocs = blocs[[COL_ID_DIAG, COL_ID_PATIENT, "cle"]]

    cols_inv = [COL_ID_DIAG, COL_ID_INVEST, COL_DATE_CR] + (
        [COL_STATUT_RESULTAT] if COL_STATUT_RESULTAT in df_invest.columns else [])
    inv = (df_invest[cols_inv].dropna(subset=[COL_ID_INVEST]).drop_duplicates(COL_ID_INVEST))
    inv = inv.assign(date=_dates(inv[COL_DATE_CR]))
    concluantes = set(inv.loc[inv[COL_STATUT_RESULTAT].map(_est_concluant), COL_ID_INVEST]) \
        if COL_STATUT_RESULTAT in inv.columns else set()

    # Gènes rattachés à une investigation ABSENTE de l'onglet « Investigations génétiques »
    # (incohérence d'export) : traités comme des investigations NON DATÉES de leur bloc, pour
    # rester soumis à la règle « une seule investigation par maladie » (contrainte e-CRF FREDD).
    orphelines = df[df[COL_ID_INVEST].notna() & ~df[COL_ID_INVEST].isin(set(inv[COL_ID_INVEST]))]
    if len(orphelines):
        rapport["genes_investigation_inconnue"] = int(orphelines[COL_ID_INVEST].nunique())
        for id_patient in orphelines[COL_ID_PATIENT].unique():
            print(f"⚠️ Patient {id_patient} : gène(s) rattaché(s) à une investigation absente de "
                  f"l'onglet « Investigations génétiques » -- traitée comme une investigation non datée.")
        inv = pd.concat([inv, orphelines[[COL_ID_DIAG, COL_ID_INVEST]].drop_duplicates(COL_ID_INVEST)
                         .assign(**{COL_DATE_CR: np.nan, "date": pd.NaT})], ignore_index=True)
    inv = inv.merge(blocs, on=COL_ID_DIAG, how="inner")
    rapport["investigations"] = len(inv)

    invest_avec_genes = set(df[COL_ID_INVEST].dropna())

    # Conclusivité de chaque investigation, calculée sur ses lignes gène x variation
    # distinctes : (nb causaux « Oui », nb causaux « partiellement », nb classe 4/5, complétude).
    champs = [c for c in colonnes_genetiques if not c.startswith("ID ")]
    lignes_gen = df[df[COL_ID_INVEST].notna()][[COL_ID_INVEST] + champs].drop_duplicates()
    par_inv = lignes_gen[COL_ID_INVEST]
    vide = pd.Series(None, index=lignes_gen.index, dtype=object)
    causal = lignes_gen[col_causal] if col_causal in lignes_gen else vide
    classe = lignes_gen[col_classe] if col_classe in lignes_gen else vide
    completude = (lignes_gen[champs].notna() & (lignes_gen[champs].astype(str).apply(
        lambda c: c.str.strip()) != "")).sum(axis=1).groupby(par_inv).sum()
    nb_causaux = causal.map(_est_causal).groupby(par_inv).sum()
    nb_partiels = causal.map(_est_partiellement_causal).groupby(par_inv).sum()
    nb_classe45 = classe.map(_est_classe_conclusive).groupby(par_inv).sum()

    def _score(id_inv):
        return (int(id_inv in concluantes), int(nb_causaux.get(id_inv, 0)), int(nb_partiels.get(id_inv, 0)),
                int(nb_classe45.get(id_inv, 0)), int(completude.get(id_inv, 0)))

    def _departager(candidats, id_patient, maladie, contexte):
        """Réduit `candidats` à UNE investigation : conclusivité, puis ID le plus élevé."""
        if len(candidats) == 1:
            return candidats
        scores = candidats[COL_ID_INVEST].map(_score)
        meilleur = max(scores)
        finalistes = candidats[scores == meilleur]
        detail = (f"{'concluant' if meilleur[0] else 'non concluant'}, {meilleur[1]} variant(s) causal(aux), "
                  f"{meilleur[2]} partiellement causal(aux), {meilleur[3]} de classe 4/5, "
                  f"{meilleur[4]} champ(s) renseigné(s)")
        if len(finalistes) == 1:
            print(f"⚠️ Patient {id_patient} ({maladie}) : {len(candidats)} investigations {contexte} -- "
                  f"la plus conclusive est conservée ({detail}).")
            return finalistes
        choisie = finalistes.loc[[max(finalistes.index, key=lambda i: _cle_id(finalistes.at[i, COL_ID_INVEST]))]]
        rapport["departage_par_id"] += 1
        print(f"⚠️ Patient {id_patient} ({maladie}) : {len(finalistes)} investigations {contexte} aussi "
              f"conclusives l'une que l'autre ({detail}) -- conservée : ID Investigation "
              f"{choisie[COL_ID_INVEST].iloc[0]} (la plus élevée, par défaut). À VÉRIFIER.")
        return choisie

    retenues = set()
    for (id_patient, cle), groupe in inv.groupby([COL_ID_PATIENT, "cle"], sort=False):
        maladie = cle.split(":", 1)[-1]
        datees = groupe[groupe["date"].notna()]
        if len(datees):
            plus_recentes = datees[datees["date"] == datees["date"].max()]
            if len(plus_recentes) > 1:
                rapport["ex_aequo"] += 1
            garder = _departager(plus_recentes, id_patient, maladie,
                                 f"à la même date de compte rendu ({plus_recentes['date'].iloc[0]:%d/%m/%Y})")
            nb_non_datees = len(groupe) - len(datees)
            if nb_non_datees:
                rapport["non_datees_ecartees"] += nb_non_datees
                print(f"⚠️ Patient {id_patient} ({maladie}) : {nb_non_datees} investigation(s) sans "
                      f"date de compte rendu écartée(s) au profit de la plus récente datée.")
        else:
            rapport["sans_date"] += 1
            garder = _departager(groupe, id_patient, maladie, "sans date de compte rendu")
        ids_gardes = set(garder[COL_ID_INVEST])
        retenues |= ids_gardes
        if COL_STATUT_RESULTAT in garder.columns:
            statut = garder[COL_STATUT_RESULTAT].iloc[0]
            if isinstance(statut, str) and statut.strip():
                rapport["statut_par_maladie"][(id_patient, cle)] = statut.strip()
        if not (ids_gardes & invest_avec_genes) and (set(groupe[COL_ID_INVEST]) & invest_avec_genes):
            rapport["derniere_sans_gene"] += 1
            print(f"⚠️ Patient {id_patient} ({maladie}) : la dernière investigation n'a aucun gène "
                  f"renseigné ; les gènes d'investigations antérieures ne sont pas exportés.")
    # Blocs diagnostic dont la maladie a AU MOINS une investigation génétique (retenue ou non,
    # quel que soit son résultat) : sert à cocher « Test génétique » dans FREDD (règle 6).
    maladies_investiguees = set(zip(inv[COL_ID_PATIENT], inv["cle"]))
    rapport["blocs_avec_investigation"] = {
        b for b, p, c in zip(blocs[COL_ID_DIAG], blocs[COL_ID_PATIENT], blocs["cle"])
        if (p, c) in maladies_investiguees}
    rapport["retenues"] = len(retenues)
    rapport["ecartees"] = len(inv) - len(retenues)
    connues = set(inv[COL_ID_INVEST])

    # Investigations écartées : on vide leurs colonnes génétiques. Dans un bloc qui garde au
    # moins une ligne génétique retenue, les lignes vidées sont supprimées (sinon elles
    # créeraient un « gène vide » dans diag_block_treatment) ; un bloc sans plus aucune
    # génétique garde une seule ligne, sans gène.
    ecartee = df[COL_ID_INVEST].isin(connues) & ~df[COL_ID_INVEST].isin(retenues)
    if ecartee.any():
        df.loc[ecartee, colonnes_genetiques] = np.nan
        blocs_gardant_genetique = set(df.loc[~ecartee, COL_ID_DIAG])
        a_supprimer = ecartee & df[COL_ID_DIAG].isin(blocs_gardant_genetique)
        df = df[~a_supprimer]
        df = df[~df.duplicated(keep="first")]
    return df, rapport
