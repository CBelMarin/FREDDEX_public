# -*- coding: utf-8 -*-
# FREDDEX - Fichier d'import des participants (mode hors-ligne)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# En mode hors-ligne, FREDDEX produit un fichier de données (FREDD_export_*.xlsx) qui ne peut
# être importé qu'APRÈS la création des participants dans la plateforme cible. Ce module produit
# le fichier d'import de ces participants, au format du modèle « import_model.csv » :
#
#   EMAIL;NAME;FIRST_NAME;PHONE;BIRTHDAY;ADDRESS;NIR;GENDER;BIOLOGICAL_GENDER;CITY_OF_BIRTH;COUNTRY_OF_BIRTH
#
# Choix délibéré : seuls NAME, FIRST_NAME et BIRTHDAY sont renseignés. Les autres colonnes
# (e-mail, téléphone, adresse, NIR, sexe, lieu de naissance) ne sont pas obligatoires pour
# l'import : elles sont conservées dans l'en-tête, pour respecter le modèle, mais laissées vides
# -- FREDDEX n'exporte donc pas plus de données identifiantes que nécessaire (minimisation).
#
# Ce fichier contient des données directement identifiantes (nom, prénom, date de naissance) :
# il est écrit dans le dossier « output/ », exclu de Git (voir .gitignore), et ne doit être ni
# versionné ni transmis par un canal non sécurisé.

import pandas as pd

from skezia_api import normalize_birth_date  # même règle de lecture des dates que l'import en ligne

# Colonnes du modèle d'import, dans l'ordre exact du modèle fourni (import_model.csv).
COLONNES_MODELE = [
    "EMAIL", "NAME", "FIRST_NAME", "PHONE", "BIRTHDAY", "ADDRESS", "NIR",
    "GENDER", "BIOLOGICAL_GENDER", "CITY_OF_BIRTH", "COUNTRY_OF_BIRTH",
]

# Colonnes de df_final (après rename_columns, donc noms FREDD) dont dérivent les 3 colonnes
# renseignées. Ce sont les mêmes que celles lues par skezia_api.create_patient_info().
COL_ID = "adm_identifiant_bamara"
COL_NOM_NAISSANCE = "adm_nom_naissance"
COL_NOM_USAGE = "adm_nom_usage"
COL_PRENOM = "adm_prenom"
COL_DATE_NAISSANCE = "adm_date_naissance"
COLONNES_SOURCE = [COL_ID, COL_NOM_NAISSANCE, COL_NOM_USAGE, COL_PRENOM, COL_DATE_NAISSANCE]

FORMAT_DATE_MODELE = "%d/%m/%Y"  # JJ/MM/AAAA, comme dans le modèle (14/03/1984)


def _texte(valeur):
    """Texte nettoyé (espaces de début/fin et doublons retirés), ou "" si la valeur est vide/NaN."""
    if pd.isna(valeur):
        return ""
    if isinstance(valeur, float) and valeur.is_integer():  # ID lu comme 12345.0 -> "12345"
        return str(int(valeur))
    return " ".join(str(valeur).split())


def _date_modele(valeur):
    """
    Date de naissance au format JJ/MM/AAAA, ou "" si elle est absente ou illisible.
    S'appuie sur skezia_api.normalize_birth_date (ISO ou français JJ/MM/AAAA, jour en premier),
    pour que le fichier participants et l'import en ligne lisent une date de la même façon.
    """
    iso = normalize_birth_date(valeur)
    if not iso:
        return ""
    return pd.to_datetime(iso, format="%Y-%m-%d").strftime(FORMAT_DATE_MODELE)


def identite_participant(nom_naissance, nom_usage, prenom, date_naissance):
    """
    (NAME, FIRST_NAME, BIRTHDAY) d'un patient, "" pour ce qui manque. UNE SEULE définition de
    ces trois valeurs, partagée par le fichier des participants (ici) et par la liaison des ID
    SKEZIA (liaison_ids.py) : la liaison retrouve un participant par la même identité que celle
    qui a servi à le créer, donc les deux ne doivent jamais diverger.
      - NAME : nom d'usage, sinon nom de naissance (comme le nom envoyé à SKEZIA) ;
      - BIRTHDAY : JJ/MM/AAAA.
    """
    nom = _texte(nom_usage) or _texte(nom_naissance)
    return nom, _texte(prenom), _date_modele(date_naissance)


def construire_fichier_participants(df_final):
    """
    Construit le tableau des participants à partir du résultat de la transformation (df_final).

    Règles, alignées sur l'import en ligne (skezia_api.create_patient_info / process_patient) :
      - NAME : nom d'usage, sinon nom de naissance (comme le nom envoyé à SKEZIA) ;
      - FIRST_NAME : premier prénom de l'acte de naissance ;
      - BIRTHDAY : date de naissance, JJ/MM/AAAA ;
      - un patient sans nom, sans prénom ou sans date de naissance exploitable est EXCLU, et son
        ID BaMaRa est renvoyé -- jamais d'exclusion silencieuse (cf. create_patient_info) ;
      - un même participant (même nom + prénom + date) n'apparaît qu'une fois.
    Les noms sont repris tels qu'écrits dans BaMaRa (casse comprise), seuls les espaces superflus
    sont retirés.

    Renvoie (df_participants, ids_exclus, nb_doublons) :
      df_participants : DataFrame aux colonnes COLONNES_MODELE (texte, "" = vide) ;
      ids_exclus      : ID BaMaRa (str) des patients exclus faute de donnée obligatoire ;
      nb_doublons     : nombre de lignes écartées car identiques à un participant déjà listé.
    Lève ValueError si une colonne source attendue est absente de df_final.
    """
    manquantes = [c for c in COLONNES_SOURCE if c not in df_final.columns]
    if manquantes:
        raise ValueError(
            "Impossible de construire le fichier des participants : colonne(s) absente(s) du "
            f"résultat de la transformation : {', '.join(manquantes)}.")

    source = df_final[COLONNES_SOURCE]
    identites = [
        identite_participant(nom_naissance, nom_usage, prenom, date_naissance)
        for nom_naissance, nom_usage, prenom, date_naissance in zip(
            source[COL_NOM_NAISSANCE], source[COL_NOM_USAGE], source[COL_PRENOM], source[COL_DATE_NAISSANCE])
    ]
    participants = pd.DataFrame(identites, columns=["NAME", "FIRST_NAME", "BIRTHDAY"], index=source.index)

    incomplet = (participants == "").any(axis=1)
    ids_exclus = [_texte(i) or "ID inconnu" for i in source.loc[incomplet, COL_ID]]
    participants = participants.loc[~incomplet]

    nb_avant = len(participants)
    participants = participants.drop_duplicates()
    nb_doublons = nb_avant - len(participants)

    # Toutes les colonnes du modèle, dans son ordre ; celles non renseignées restent vides.
    participants = participants.reindex(columns=COLONNES_MODELE, fill_value="").reset_index(drop=True)
    return participants, ids_exclus, nb_doublons


def ecrire_fichier_participants(df_participants, chemin):
    """
    Écrit le tableau au format du modèle : CSV, séparateur « ; », UTF-8 sans BOM, fins de ligne
    CRLF (caractéristiques constatées sur import_model.csv).
    """
    df_participants.to_csv(chemin, sep=";", index=False, encoding="utf-8", lineterminator="\r\n")
