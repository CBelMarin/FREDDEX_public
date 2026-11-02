# -*- coding: utf-8 -*-
# FREDDEX - configuration loading (centers, ORPHA codes, mapping/survey file locations)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License

import os
import pandas as pd

from paths import resource_path

# Fichier généré par FREDDEX-base.spec au moment du build, contenant le nom du centre de
# cet exécutable (voir lire_nom_centre ci-dessous).
CENTRE_BUILD_FILE = "files/centre_build.txt"


def lire_nom_centre():
    """
    Nom du centre pour lequel tourne FREDDEX, avec UNE seule source par contexte :
    1) exécutable : files/centre_build.txt, écrit par FREDDEX-base.spec au build (le centre est
       choisi une seule fois, via la variable FREDDEX_CENTRE) ;
    2) développement (python FREDDEX-base.py) : variable d'environnement FREDDEX_CENTRE ;
    3) à défaut : "TEST".
    Le numéro du centre est toujours lu dans
    fichier_config.csv (voir obtenir_numero_centre / initialiser_ressources).
    """
    chemin = resource_path(CENTRE_BUILD_FILE)
    if os.path.exists(chemin):
        with open(chemin, encoding="utf-8") as f:
            nom = f.read().strip()
        if nom:
            return nom
    return os.environ.get("FREDDEX_CENTRE", "").strip() or "TEST"


# Name of the center this build of FREDDEX is configured for, and the version date shown
# in the report. Each participating center ships its own FREDDEX build.
NOM_CENTRE = lire_nom_centre()
DATE_VERSION = "13-10-2026"


def charger_codes_mr(fichier_path):
    """
    Loads rare disease ORPHA codes from a text file. Reads a comma-separated list, converts
    valid entries to integers, and filters out empty or invalid values.
    """
    with open(fichier_path, 'r', encoding='utf-8') as f:
        contenu = f.read()
    codes = [int(code.strip()) for code in contenu.split(',') if code.strip().isdigit()]
    return codes


def lire_centres(fichier_csv):
    """
    Reads a CSV file containing center information and returns a structured dictionary
    indexed by center name.
    """
    df = pd.read_csv(fichier_csv, sep=';')

    df = df.apply(lambda col: col.map(lambda x: x.strip('"') if isinstance(x, str) else x))

    colonnes_attendues = {"Nom_centre", "Numero_centre", "Ficher_codes_MR", "questionnaire_id"}
    if not colonnes_attendues.issubset(df.columns):
        raise ValueError(
            f"Warning : your center file must contain the following columns : {', '.join(colonnes_attendues)}."
        )

    dict_centres = (
        df
        .set_index("Nom_centre")[["Numero_centre", "Ficher_codes_MR", "questionnaire_id"]]
        .to_dict(orient="index")
    )
    return dict_centres


def obtenir_numero_centre(centre):
    """
    Returns just the Numero_centre value configured for `centre` in fichier_config.csv, or an
    error message (str) if the file/format is invalid -- without loading anything else (ORPHA
    codes, mapping file, Survey.csv).

    Used at startup (see FREDDEX-base.py's __main__) to know which SKEZIA Fernet key to check
    for (see key_setup.py) before doing any of the heavier resource loading that
    initialiser_ressources() below performs on every actual import. 
    """
    try:
        centres_file = resource_path("files/fichier_config.csv")
        if not os.path.exists(centres_file):
            return "❌ Fichier fichier_config.csv not found."
        centres_dict = lire_centres(centres_file)
        if centre not in centres_dict:
            return f"❌ Centre {centre} n'existe pas."
        return centres_dict[centre]["Numero_centre"]
    except Exception as e:
        return f"❌ Unexpected error : {str(e)}"


def initialiser_ressources(centre):
    """
    Initializes the resources required for the import process:
    - ORPHA rare disease codes
    - available collecting centers
    - BaMaRa-FREDD mapping file path
    - FREDD questionnaire structure (Survey.csv)

    Args:
    centre (str): Name of the center for which the application is launched. Each center has
    its own FREDDEX configuration.

    Returns either a tuple (liste_codes_MR, numero_centre, questionnaire_centre, map_file,
    survey), or an error message (str) if a required file is missing/invalid.

    The loading helpers (lire_centres, charger_codes_mr) are module-level functions so that
    each of them can be read and tested independently.
    """

    contact = " Contactez l'équipe FREDD."

    try:
        # Load centers
        centres_file = resource_path("files/fichier_config.csv")
        if not os.path.exists(centres_file):
            return f"❌ Fichier de configuration des centres introuvable (fichier_config.csv).{contact}"
        centres_dict = lire_centres(centres_file)

        if centre not in centres_dict:
            return f"❌ Le centre « {centre} » n'est pas configuré dans FREDDEX.{contact}"

        infos_centre = centres_dict[centre]

        numero_centre = infos_centre["Numero_centre"]
        questionnaire_centre = infos_centre["questionnaire_id"]
        codesMR_file = resource_path(f"files/codes_MR/{infos_centre['Ficher_codes_MR']}")  # load ORPHA codes for the center

        print(f"Application lancée pour le centre : {centre}")

        if not os.path.exists(codesMR_file):
            return f"❌ Fichier des codes ORPHA introuvable pour ce centre.{contact}"
        liste_codes_MR = charger_codes_mr(codesMR_file)

        map_file = resource_path("files/map_BaMaRa_FREDD.xlsx")
        if not os.path.exists(map_file):
            return f"❌ Fichier de correspondance BaMaRa/FREDD introuvable (map_BaMaRa_FREDD.xlsx).{contact}"

        csv_file = resource_path("files/Survey.csv")
        if not os.path.exists(csv_file):
            return f"❌ Fichier de structure du questionnaire introuvable (Survey.csv).{contact}"
        survey = pd.read_csv(csv_file, sep=";")

        return liste_codes_MR, numero_centre, questionnaire_centre, map_file, survey

    except Exception as e:
        return f"❌ Erreur inattendue lors du chargement de la configuration : {e}.{contact}"
