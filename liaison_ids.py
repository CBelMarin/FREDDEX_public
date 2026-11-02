# -*- coding: utf-8 -*-
# FREDDEX - Liaison des ID SKEZIA (étape 2 du mode hors-ligne)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Le mode hors-ligne se déroule en DEUX temps, sans jamais contacter l'API :
#   1. FREDDEX produit l'export de données (FREDD_export_*.xlsx) et le fichier d'import des
#      participants (FREDD_participants_*.csv, voir participants.py) ;
#   2. les participants sont importés dans la plateforme, qui leur attribue un ID SKEZIA ; on
#      exporte alors la liste des participants (colonne « id ») et CE MODULE ajoute à l'export de
#      données une colonne portant l'ID SKEZIA de chaque patient, pour lier ses données.
#
# Le lien se fait par IDENTITÉ (nom, prénom, date de naissance), faute d'autre clé commune : le
# modèle d'import des participants n'a pas de colonne pour l'ID BaMaRa. La règle qui définit
# NAME / FIRST_NAME / BIRTHDAY est celle de participants.identite_participant -- la même que pour
# créer le participant -- et la comparaison tolère casse, accents, ponctuation et format de date
# (skezia_api.normalize_name / normalize_birth_date, comme l'anti-doublon de l'import en ligne).
#
# Principe de prudence : RELIER DES DONNÉES DE SANTÉ AU MAUVAIS PATIENT est pire que ne pas les
# relier. Une ligne n'est donc liée que si l'identité correspond à EXACTEMENT un participant ;
# sinon la case reste vide et l'ID BaMaRa est listé dans le rapport, avec la raison :
#   - introuvable            : aucun participant de cet export ne correspond ;
#   - plusieurs participants : homonymes de même date de naissance (ou doublon dans la plateforme) ;
#   - date inversée possible : introuvable, MAIS un participant correspond si l'on inverse jour et
#                              mois -- probable erreur de lecture de la date à l'import ; JAMAIS
#                              appliqué automatiquement, à vérifier à la main ;
#   - identité incomplète    : nom, prénom ou date manquant (ce patient n'était pas dans le
#                              fichier des participants).
#
# Le classeur de données est modifié avec openpyxl (une colonne ajoutée à une COPIE), et non relu
# puis réécrit par pandas : un aller-retour pandas convertirait au passage des valeurs (ex. un
# identifiant « 00123 » deviendrait 123). L'original n'est jamais modifié.

import os
import re
import unicodedata

import pandas as pd
from openpyxl import load_workbook

from participants import (
    COLONNES_MODELE, COL_ID, COL_NOM_NAISSANCE, COL_NOM_USAGE, COL_PRENOM, COL_DATE_NAISSANCE,
    identite_participant,
)
from skezia_api import normalize_name, normalize_birth_date

# Nom de la colonne ajoutée à l'export de données (modifiable : option --colonne du script).
COLONNE_ID_PAR_DEFAUT = "ID"  # nom attendu par l'import des données dans la plateforme (1re colonne)

# Colonnes de l'export de données lues pour retrouver l'identité (noms FREDD, comme dans
# create_patient_info et participants.py).
COLONNES_DONNEES = [COL_ID, COL_NOM_NAISSANCE, COL_NOM_USAGE, COL_PRENOM, COL_DATE_NAISSANCE]

# Noms de colonnes acceptés dans l'export des participants de la plateforme, comparés après
# normalisation (minuscules, sans accents ni séparateurs : « First_Name » -> « firstname »).
# La colonne d'ID est « id » ; les trois autres reprennent les noms du modèle d'import.
_ALIAS_EXPORT = {
    "id": {"id"},
    "nom": {"name", "nom", "lastname", "familyname", "nomdusage"},
    "prenom": {"firstname", "prenom", "given", "givenname"},
    # Facultative : l'export de la plateforme n'a pas toujours de date de naissance (seule
    # « adm_date_naissance », réponse au questionnaire, existe, et elle est vide tant que les
    # données ne sont pas importées).
    "date": {"birthday", "birthdate", "datenaissance", "datedenaissance", "dateofbirth", "dob",
             "admdatenaissance"},
}
_ROLES_FACULTATIFS = {"date"}

# Raisons de non-liaison, dans l'ordre d'affichage du rapport.
RAISONS = {
    "introuvable": "introuvable dans l'export des participants",
    "plusieurs": "plusieurs participants possibles (homonymes)",
    "date_inversee": "jour et mois peut-être INVERSÉS (non lié, à vérifier)",
    "incomplete": "nom, prénom ou date de naissance manquant",
}


def _cle_colonne(nom):
    """« First_Name », « Prénom », « date de naissance » -> « firstname », « prenom », ..."""
    sans_accents = unicodedata.normalize("NFD", str(nom)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", sans_accents.lower())


def _texte_id(valeur):
    """ID en texte propre ; « 123.0 » (entier lu comme décimal) devient « 123 »."""
    if valeur is None or (not isinstance(valeur, str) and pd.isna(valeur)):
        return ""
    texte = str(valeur).strip()
    return texte[:-2] if re.fullmatch(r"\d+\.0", texte) else texte


def lire_export_participants(chemin):
    """
    Lit l'export des participants de la plateforme (CSV ou Excel) ; tout en texte, rien n'est
    converti. Pour un CSV, le séparateur (« ; » ou « , ») est détecté, et l'encodage essayé en
    UTF-8 (avec ou sans BOM) puis Windows-1252.
    """
    extension = os.path.splitext(str(chemin))[1].lower()
    if extension in (".xlsx", ".xlsm"):
        return pd.read_excel(chemin, dtype=str, keep_default_na=False)
    if extension in (".csv", ".txt"):
        derniere_erreur = None
        for encodage in ("utf-8-sig", "cp1252"):
            try:
                return pd.read_csv(chemin, sep=None, engine="python", dtype=str,
                                   keep_default_na=False, encoding=encodage)
            except UnicodeDecodeError as e:
                derniere_erreur = e
        raise derniere_erreur
    raise ValueError(f"Format d'export non reconnu ({extension or 'sans extension'}) : "
                     f"attendu .csv ou .xlsx.")


def trouver_colonnes_export(df_export):
    """
    Repère, dans l'export des participants, les colonnes ID / nom / prénom / date de naissance.
    Lève une ValueError qui LISTE les colonnes réellement présentes si l'une manque ou si le
    repérage est ambigu : mieux vaut s'arrêter que lier sur une mauvaise colonne.
    """
    par_cle = {}
    for colonne in df_export.columns:
        par_cle.setdefault(_cle_colonne(colonne), []).append(colonne)

    # Erreur fréquente : avoir choisi le fichier d'IMPORT des participants (FREDD_participants_*.csv,
    # colonnes EMAIL;NAME;FIRST_NAME;...) au lieu de l'export de la plateforme (Answers.csv).
    if "id" not in par_cle and all(_cle_colonne(c) in par_cle for c in COLONNES_MODELE):
        raise ValueError(
            "Ce fichier est le fichier d'IMPORT des participants (FREDD_participants_….csv), pas l'export "
            "de la plateforme. Il faut l'export de la plateforme (par exemple Answers.csv), qui contient "
            "la colonne « id ».")

    trouvees, problemes = {}, []
    for role, alias in _ALIAS_EXPORT.items():
        candidates = [c for cle, cols in par_cle.items() if cle in alias for c in cols]
        if len(candidates) == 1:
            trouvees[role] = candidates[0]
        elif role in _ROLES_FACULTATIFS and not candidates:
            trouvees[role] = None
        else:
            problemes.append(f"{role} ({'ambigu : ' + ', '.join(map(repr, candidates)) if candidates else 'absente'})")
    if problemes:
        raise ValueError(
            "Colonnes de l'export des participants non reconnues -- " + " ; ".join(problemes)
            + f". Colonnes présentes : {', '.join(map(repr, df_export.columns))}.")
    return trouvees


def _cle_identite(nom, prenom, date):
    """Clé de comparaison (NOM, PRÉNOM, date ISO) ; None si l'une des trois est inexploitable."""
    cle = (normalize_name(nom), normalize_name(prenom), normalize_birth_date(date))
    return cle if all(cle) else None


def _date_inversee(date_iso):
    """Même date avec jour et mois échangés (AAAA-JJ-MM lu comme AAAA-MM-JJ), ou None si impossible."""
    annee, mois, jour = date_iso.split("-")
    if int(jour) > 12 or jour == mois:
        return None
    return f"{annee}-{jour}-{mois}"


def construire_index(df_export, colonnes):
    """
    {(NOM, PRÉNOM) : [(ID SKEZIA, date ISO ou None), ...]} à partir de l'export des
    participants, et le nombre de lignes inutilisables (sans ID, ou sans nom/prénom). La date
    vaut None si l'export n'a pas de colonne date, ou si elle est vide/illisible sur la ligne.
    """
    index, ignorees = {}, 0
    colonne_date = colonnes.get("date")
    dates = df_export[colonne_date] if colonne_date else [None] * len(df_export)
    for id_skezia, nom, prenom, date in zip(df_export[colonnes["id"]], df_export[colonnes["nom"]],
                                            df_export[colonnes["prenom"]], dates):
        id_skezia = _texte_id(id_skezia)
        cle = (normalize_name(nom), normalize_name(prenom))
        if not id_skezia or not all(cle):
            ignorees += 1
            continue
        entree = (id_skezia, normalize_birth_date(date) or None)
        if entree not in index.setdefault(cle, []):
            index[cle].append(entree)
    return index, ignorees


class RapportLiaison:
    def __init__(self, chemin_sortie, nom_colonne):
        self.chemin_sortie = chemin_sortie
        self.nom_colonne = nom_colonne
        self.lignes = 0
        self.liees = 0
        self.non_liees = {raison: [] for raison in RAISONS}  # raison -> ID BaMaRa
        self.export_ignorees = 0
        self.ecrit = True  # False : aucune ligne liée, aucun fichier écrit (voir lier_classeur)
        self.export_lues = 0  # participants distincts lus dans l'export de la plateforme
        self.liees_sans_date = 0  # liées sur nom + prénom seuls (pas de date de naissance dans l'export)

    def lignes_texte(self):
        """Rapport lisible. Que des ID BaMaRa (pseudonymes) : ni nom ni date n'y figurent."""
        total_non_liees = sum(len(v) for v in self.non_liees.values())
        if self.liees == 0:
            lignes = [f"❌ Aucune ligne liée (0/{self.lignes})"
                      + (" : aucun fichier n'a été créé." if not self.ecrit else ".")]
        else:
            lignes = [f"✅ {self.liees}/{self.lignes} ligne(s) liée(s) (colonne « {self.nom_colonne} »)."]
        lignes.append(f"ℹ️ {self.export_lues} participant(s) lu(s) dans l'export de la plateforme.")
        if self.export_ignorees:
            lignes.append(f"ℹ️ {self.export_ignorees} ligne(s) de l'export des participants ignorée(s) "
                          f"(sans ID, nom ou prénom).")
        if self.liees_sans_date:
            lignes.append(f"ℹ️ {self.liees_sans_date} ligne(s) liée(s) sur nom + prénom seuls (pas de date "
                          f"de naissance dans l'export des participants).")
        if total_non_liees:
            lignes.append(f"⚠️ {total_non_liees} ligne(s) non liée(s), laissées vides :")
            for raison, libelle in RAISONS.items():
                ids = self.non_liees[raison]
                if ids:
                    lignes.append(f"   • {len(ids)} {libelle} — ID BaMaRa : {', '.join(ids)}")
        return lignes


def lier_classeur(chemin_donnees, df_export, chemin_sortie, nom_colonne=COLONNE_ID_PAR_DEFAUT,
                  ecrire_si_aucune_liaison=True):
    """
    Écrit dans `chemin_sortie` une COPIE du classeur de données avec, en PREMIÈRE colonne,
    l'ID SKEZIA de chaque ligne (voir l'en-tête du module pour les règles). Renvoie un
    RapportLiaison. Si ecrire_si_aucune_liaison est False et qu'aucune ligne n'a pu être liée, rien
    n'est écrit (rapport.ecrit vaut False) : une copie sans aucun ID ne servirait à rien et
    bloquerait la relance (un fichier existant n'est jamais écrasé). Lève ValueError (colonne manquante / déjà présente / export non reconnu)
    ou FileExistsError (la sortie existe déjà : rien n'est jamais écrasé).
    """
    if os.path.splitext(str(chemin_donnees))[1].lower() not in (".xlsx", ".xlsm"):
        raise ValueError("L'export de données FREDDEX doit être un fichier Excel (.xlsx), "
                         "par exemple FREDD_export_….xlsx.")
    if os.path.exists(chemin_sortie):
        raise FileExistsError(f"Le fichier de sortie existe déjà : {chemin_sortie} (rien n'a été écrasé).")

    index, ignorees = construire_index(df_export, trouver_colonnes_export(df_export))

    classeur = load_workbook(chemin_donnees)
    feuille = classeur.worksheets[0]
    colonnes = {str(c.value).strip(): c.column for c in feuille[1] if c.value is not None}
    manquantes = [c for c in COLONNES_DONNEES if c not in colonnes]
    if manquantes:
        raise ValueError(f"Colonne(s) absente(s) de l'export de données : {', '.join(manquantes)}.")
    if nom_colonne.lower() in {c.lower() for c in colonnes}:
        raise ValueError(f"La colonne « {nom_colonne} » existe déjà dans l'export de données.")

    rapport = RapportLiaison(chemin_sortie, nom_colonne)
    rapport.export_ignorees = ignorees
    rapport.export_lues = sum(len(v) for v in index.values())
    ids_par_ligne = {}  # ligne -> ID SKEZIA ; écrits à la fin, dans une colonne insérée en tête

    # Passe préalable : dates de naissance distinctes par (nom, prénom) dans les DONNÉES. Sert à
    # refuser de lier sur le seul nom quand deux patients différents le partagent.
    dates_par_nom = {}
    for ligne in range(2, feuille.max_row + 1):
        v = {c: feuille.cell(row=ligne, column=colonnes[c]).value for c in COLONNES_DONNEES}
        k = _cle_identite(*identite_participant(v[COL_NOM_NAISSANCE], v[COL_NOM_USAGE], v[COL_PRENOM],
                                                v[COL_DATE_NAISSANCE]))
        if k:
            dates_par_nom.setdefault((k[0], k[1]), set()).add(k[2])

    for ligne in range(2, feuille.max_row + 1):
        valeurs = {c: feuille.cell(row=ligne, column=colonnes[c]).value for c in COLONNES_DONNEES}
        if all(v is None for v in valeurs.values()):
            continue  # ligne vide
        rapport.lignes += 1
        id_bamara = _texte_id(valeurs[COL_ID]) or f"(ligne {ligne})"

        nom, prenom, date = identite_participant(
            valeurs[COL_NOM_NAISSANCE], valeurs[COL_NOM_USAGE], valeurs[COL_PRENOM], valeurs[COL_DATE_NAISSANCE])
        cle = _cle_identite(nom, prenom, date)
        if cle is None:
            rapport.non_liees["incomplete"].append(id_bamara)
            continue

        entrees = index.get((cle[0], cle[1]), [])
        date = cle[2]
        sans_date = {i for i, d in entrees if d is None}   # participants dont la date n'est pas connue
        avec_date = {i for i, d in entrees if d == date}    # participants dont la date correspond
        trouves = avec_date | sans_date                     # (une date DIFFÉRENTE exclut le participant)
        if sans_date and len(dates_par_nom[(cle[0], cle[1])]) > 1:
            # Plusieurs patients de mêmes nom et prénom dans les données, et l'export ne permet
            # pas de les distinguer : on ne devine pas.
            rapport.non_liees["plusieurs"].append(id_bamara)
        elif len(trouves) == 1:
            ids_par_ligne[ligne] = next(iter(trouves))
            rapport.liees += 1
            if not avec_date:
                rapport.liees_sans_date += 1
        elif len(trouves) > 1:
            rapport.non_liees["plusieurs"].append(id_bamara)
        else:
            inversee = _date_inversee(date)
            if inversee and any(d == inversee for _, d in entrees):
                rapport.non_liees["date_inversee"].append(id_bamara)
            else:
                rapport.non_liees["introuvable"].append(id_bamara)

    if rapport.liees == 0 and not ecrire_si_aucune_liaison:
        rapport.ecrit = False
        return rapport

    feuille.insert_cols(1)
    feuille.cell(row=1, column=1, value=nom_colonne)
    for ligne, id_skezia in ids_par_ligne.items():
        feuille.cell(row=ligne, column=1, value=id_skezia)

    classeur.save(chemin_sortie)
    return rapport
