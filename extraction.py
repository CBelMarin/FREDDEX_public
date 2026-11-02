# -*- coding: utf-8 -*-
# FREDDEX - Extraction layer: reading the BaMaRa Excel exports
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# La génétique est sortie de l'export principal. FREDDEX lit DEUX fichiers :
#   - l'export principal : Données administratives, Prises en charge, Diagnostics,
#     Anté-néonatal, Recherche ;
#   - l'export génétique : Investigations génétiques, Gènes, Variations nucléotidiques
#     (ex-« Variations »). Chaque gène / variation est rattaché à une investigation
#     (ID Investigation), elle-même rattachée à un bloc diagnostic (ID diagnostic).
# Les onglets « Anomalies par chromosomes » et « Expansions » de l'export génétique ne sont
# pas lus (hors périmètre FREDD à ce jour). L'ancien export en un seul fichier n'est plus
# accepté.

import pandas as pd

# Onglets attendus dans chacun des deux exports.
SHEETS_PRINCIPAL = {
    "admin": "Données administratives",
    "prises": "Prises en charge",
    "diagnostic": "Diagnostics",
    "neo": "Anté-néonatal",
    "recherche": "Recherche",
}
SHEETS_GENETIQUE = {
    "invest": "Investigations génétiques",
    "genes": "Gènes",
    "var": "Variations nucléotidiques",
}
# Rétro-compatibilité des imports : vue d'ensemble de tous les onglets lus.
SHEET_NAMES = {**SHEETS_PRINCIPAL, **SHEETS_GENETIQUE}


def _onglets(fichier):
    """Noms des onglets d'un classeur Excel, sans lire les données."""
    with pd.ExcelFile(fichier) as xls:
        return set(xls.sheet_names)


def identifier_export(fichier):
    """
    Renvoie "principal", "genetique" ou None selon les onglets présents dans `fichier`.
    Sert à vérifier le choix de l'utilisateur dès la sélection (GUI), et à tolérer que les
    deux fichiers soient fournis dans l'ordre inverse.
    """
    onglets = _onglets(fichier)
    if set(SHEETS_PRINCIPAL.values()) <= onglets:
        return "principal"
    if set(SHEETS_GENETIQUE.values()) <= onglets:
        return "genetique"
    return None


def onglets_manquants(fichier, attendu):
    """Liste des onglets de `attendu` ("principal" / "genetique") absents de `fichier`."""
    feuilles = SHEETS_PRINCIPAL if attendu == "principal" else SHEETS_GENETIQUE
    onglets = _onglets(fichier)
    return [s for s in feuilles.values() if s not in onglets]


def ordonner_exports(fichier_principal, fichier_genetique):
    """
    Remet les deux fichiers dans l'ordre (principal, génétique) s'ils ont été inversés, et
    lève une ValueError explicite si l'un des deux n'est pas un export BaMaRa reconnu.
    """
    type_1, type_2 = identifier_export(fichier_principal), identifier_export(fichier_genetique)
    if (type_1, type_2) == ("genetique", "principal"):
        return fichier_genetique, fichier_principal
    if (type_1, type_2) != ("principal", "genetique"):
        manq_p = onglets_manquants(fichier_principal, "principal")
        manq_g = onglets_manquants(fichier_genetique, "genetique")
        raise ValueError(
            "Les fichiers sélectionnés ne correspondent pas aux deux exports BaMaRa attendus. "
            f"Export principal, onglet(s) manquant(s) : {manq_p or 'aucun'} ; "
            f"export génétique, onglet(s) manquant(s) : {manq_g or 'aucun'}.")
    return fichier_principal, fichier_genetique


def read_bamara_export(fichier_principal, fichier_genetique):
    """
    Lit les onglets requis des deux exports BaMaRa (ordre des fichiers indifférent).

    Renvoie un tuple de DataFrames, dans l'ordre attendu par transformation.traiter_donnees() :
    (df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_recherche, df_invest).
    df_invest (onglet « Investigations génétiques ») est ajouté EN DERNIER pour ne pas décaler
    les positions existantes.
    """
    fichier_principal, fichier_genetique = ordonner_exports(fichier_principal, fichier_genetique)
    dataframes = {}
    for fichier, feuilles in ((fichier_principal, SHEETS_PRINCIPAL), (fichier_genetique, SHEETS_GENETIQUE)):
        for key, sheet_name in feuilles.items():
            print(f"Traitement de la feuille {sheet_name}...")
            dataframes[key] = pd.read_excel(fichier, sheet_name=sheet_name)

    return (
        dataframes["admin"],
        dataframes["prises"],
        dataframes["diagnostic"],
        dataframes["genes"],
        dataframes["var"],
        dataframes["neo"],
        dataframes["recherche"],
        dataframes["invest"],
    )


def compter_patients_bruts(fichier_principal):
    """
    Counts the rows in the "Données administratives" sheet of the MAIN export only, without
    reading the other sheets or running any of the FREDDEX filtering/transformation logic.
    """
    df_admin = pd.read_excel(fichier_principal, sheet_name=SHEETS_PRINCIPAL["admin"])
    return len(df_admin)
