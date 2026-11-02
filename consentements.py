# -*- coding: utf-8 -*-
# FREDDEX - Oppositions et consentements
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
#   1) « Opposition à toute réutilisation de ses données » (Données administratives) = Oui
#      -> le patient n'est PAS importé dans FREDD.
#   2) « Le patient s'oppose à la réutilisation de ses données de génétique dans le cadre de la
#      recherche » (Investigations génétiques) = Oui sur au moins une investigation du patient
#      -> AUCUNE donnée génétique du patient n'est importée (gènes, variations, investigations ;
#      donc ni « Test génétique » ni statut d'analyse déduits des investigations).
#   3) « Consentements ERN » contient « EYE » (ERN-EYE, quelle que soit la casse)
#      -> leg_patient_REDgistryCons = 1 (Oui) ; sinon 0 (Non).
# Une opposition est retenue dès que la réponse commence par « oui » (prudence).

import pandas as pd

COL_OPPOSITION_TOTALE = "Opposition à toute réutilisation de ses données"
COL_OPPOSITION_GENETIQUE = ("Le patient s'oppose à la réutilisation de ses données de génétique "
                            "dans le cadre de la recherche")
COL_CONSENTEMENT_ERN = "Consentements ERN"
COL_AIDE_CONSENTEMENT = "FREDDEX_consentement_ERN"   # colonne technique, supprimée avant envoi

def _texte(v):
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    return str(v).strip().casefold()


def est_opposition(v):
    return _texte(v).startswith("oui")


def est_consentement(v):
    """Le champ BaMaRa « Consentements ERN » contient « EYE »."""
    return "eye" in _texte(v)


def patients_opposes(df, col, id_col="ID BaMaRa"):
    """IDs des patients ayant répondu Oui à la colonne d'opposition `col` (si elle existe)."""
    if df is None or col not in df.columns:
        return set()
    return set(df.loc[df[col].map(est_opposition), id_col])


def consentement_ern_par_patient(df_admin, id_col="ID BaMaRa"):
    """{ID BaMaRa: '1' ou '0'} d'après « Consentements ERN » (vide si la colonne est absente)."""
    if COL_CONSENTEMENT_ERN not in df_admin.columns:
        return {}
    return {i: ("1" if est_consentement(v) else "0")
            for i, v in zip(df_admin[id_col], df_admin[COL_CONSENTEMENT_ERN])}
