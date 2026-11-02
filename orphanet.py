# -*- coding: utf-8 -*-
# FREDDEX - Correspondance code ORPHA <-> nom de maladie (thésaurus Orphanet)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Dans BaMaRa, un bloc diagnostic peut porter le code ORPHA, le nom de la maladie
# (« Maladie rare (Orphanet) », choisi dans le thésaurus Orphanet, jamais en texte libre),
# ou les deux -- aucun des deux n'est obligatoire. Ce module complète, avant le filtrage, le
# code manquant à partir du nom et le nom manquant à partir du code, en deux niveaux :
#   1) l'export lui-même : blocs portant nom ET code (même version du thésaurus que les
#      données, donc la source la plus fiable) ;
#   2) repli sur la nomenclature Orphanet en français (fr_product1.xml, Orphadata,
#      CC BY 4.0) : terme préféré + synonymes, pour les noms saisis avec une version
#      antérieure du thésaurus ou absents du niveau 1.
# Toute correspondance ambiguë (un nom -> plusieurs codes) n'est JAMAIS utilisée : le bloc
# reste non résolu et est signalé, plutôt que de risquer d'attribuer la mauvaise maladie.

import os
import unicodedata
import xml.etree.ElementTree as ET

import pandas as pd

from paths import resource_path

# Emplacement par défaut de la nomenclature (à télécharger sur Orphadata et à mettre à jour
# à chaque nouvelle version du thésaurus) :
#   https://www.orphadata.com/data/xml/fr_product1.xml
NOMENCLATURE_FILE = "files/orphanet/fr_product1.xml"

_AMBIGU = object()  # marqueur : ce nom correspond à plusieurs codes ORPHA distincts


def normaliser_nom(nom):
    """
    Normalise un nom de maladie pour la comparaison : Unicode NFC, apostrophes typographiques
    -> apostrophe droite, espaces multiples réduits, casse ignorée. Les accents sont CONSERVÉS
    (le nom vient du thésaurus, pas d'une saisie libre : un accent différent est un autre
    terme). Renvoie None pour une valeur manquante ou vide.
    """
    if nom is None or (not isinstance(nom, str) and pd.isna(nom)):
        return None
    s = unicodedata.normalize("NFC", str(nom))
    s = s.replace("’", "'").replace("‘", "'").replace(" ", " ")
    s = " ".join(s.split()).casefold()
    return s or None


def _code_valide(code):
    """Convertit un code ORPHA en int, ou None s'il est manquant/invalide."""
    if code is None:
        return None
    try:
        if pd.isna(code):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(float(code))
    except (TypeError, ValueError):
        return None


def _ajouter(nom_vers_code, nom_norm, code):
    """Ajoute nom -> code ; marque le nom comme ambigu s'il pointe déjà vers un autre code."""
    if nom_norm is None or code is None:
        return
    actuel = nom_vers_code.get(nom_norm)
    if actuel is None:
        nom_vers_code[nom_norm] = code
    elif actuel is not _AMBIGU and actuel != code:
        nom_vers_code[nom_norm] = _AMBIGU


class Nomenclature:
    """Table de correspondance code ORPHA <-> nom (terme préféré + synonymes)."""

    def __init__(self, nom_vers_code=None, code_vers_nom=None, source=""):
        self.nom_vers_code = nom_vers_code or {}   # nom normalisé -> code (ou _AMBIGU)
        self.code_vers_nom = code_vers_nom or {}   # code -> terme préféré (casse d'origine)
        self.source = source

    def code_pour(self, nom):
        code = self.nom_vers_code.get(normaliser_nom(nom))
        return None if code is _AMBIGU else code

    def est_ambigu(self, nom):
        return self.nom_vers_code.get(normaliser_nom(nom)) is _AMBIGU

    def nom_pour(self, code):
        return self.code_vers_nom.get(_code_valide(code))

    def __len__(self):
        return len(self.code_vers_nom)


def charger_nomenclature_xml(chemin):
    """
    Lit fr_product1.xml (Orphadata, « Rare diseases and alignment with terminologies ») et
    renvoie une Nomenclature. Pour chaque <Disorder>, on lit uniquement ses enfants DIRECTS
    <OrphaCode>, <Name> et <SynonymList>/<Synonym> -- d'autres balises <Name> existent plus
    bas dans l'arbre (type de désordre, groupe, associations...) et ne doivent pas être
    confondues avec le nom de la maladie. Lecture en flux (iterparse) : le fichier fait
    ~50 Mo.

    NOTE : structure vérifiée sur la description publique de product1, mais pas sur le
    fichier réel depuis l'environnement de développement (téléchargement bloqué) ; le test
    tests/test_orphanet.py utilise un extrait synthétique de même structure. Vérifier une
    fois avec le vrai fichier (voir le message affiché au chargement : nombre d'entrées lues,
    ~11 600 attendues pour la version juin 2026).
    """
    nom_vers_code, code_vers_nom = {}, {}
    for _event, elem in ET.iterparse(chemin, events=("end",)):
        if elem.tag != "Disorder":
            continue
        code = _code_valide(elem.findtext("OrphaCode"))
        nom = elem.findtext("Name")
        if code is not None:
            if nom:
                code_vers_nom.setdefault(code, " ".join(nom.split()))
                _ajouter(nom_vers_code, normaliser_nom(nom), code)
            for syn in elem.findall("SynonymList/Synonym"):
                _ajouter(nom_vers_code, normaliser_nom(syn.text), code)
        elem.clear()  # libère la mémoire au fil de la lecture
    return Nomenclature(nom_vers_code, code_vers_nom, source=os.path.basename(chemin))


def charger_nomenclature_defaut():
    """
    Charge la nomenclature Orphanet embarquée (NOMENCLATURE_FILE). Si le fichier est absent
    ou illisible, renvoie None avec un avertissement explicite : FREDDEX continue avec le
    seul niveau 1 (correspondances trouvées dans l'export), plutôt que de bloquer l'import.
    """
    chemin = resource_path(NOMENCLATURE_FILE)
    if not os.path.exists(chemin):
        print(f"⚠️ Nomenclature Orphanet introuvable ({NOMENCLATURE_FILE}) : la correspondance "
              f"nom <-> code ORPHA se limitera aux blocs de l'export qui portent les deux.")
        return None
    try:
        nomenclature = charger_nomenclature_xml(chemin)
    except Exception as e:  # fichier corrompu / format inattendu : on le dit, sans planter
        print(f"⚠️ Nomenclature Orphanet illisible ({NOMENCLATURE_FILE}) : {e}. "
              f"Correspondance nom <-> code limitée aux blocs de l'export.")
        return None
    print(f"Nomenclature Orphanet chargée : {len(nomenclature)} maladies ({nomenclature.source}).")
    return nomenclature


def table_depuis_export(df, col_code, col_nom):
    """Niveau 1 : construit la correspondance à partir des lignes portant nom ET code."""
    nom_vers_code, code_vers_nom = {}, {}
    paires = df[[col_code, col_nom]].dropna().drop_duplicates()
    for code, nom in paires.itertuples(index=False):
        code = _code_valide(code)
        nom_norm = normaliser_nom(nom)
        if code is None or nom_norm is None:
            continue
        code_vers_nom.setdefault(code, str(nom).strip())
        _ajouter(nom_vers_code, nom_norm, code)
    return Nomenclature(nom_vers_code, code_vers_nom, source="export BaMaRa")


def completer_codes_et_noms(df, col_code, col_nom, col_id, nomenclature=None):
    """
    Complète, ligne par ligne, le code ORPHA manquant à partir du nom et le nom manquant à
    partir du code (niveau 1 = export, niveau 2 = nomenclature Orphanet). Ne modifie JAMAIS
    une valeur déjà renseignée. Les chaînes vides sont traitées comme des valeurs manquantes.

    Renvoie (df complété, rapport) où rapport est un dict de compteurs, et affiche un
    avertissement par patient dont un bloc « nom seul » n'a pas pu être relié à un code.
    """
    df = df.copy()
    noms = df[col_nom].where(df[col_nom].map(normaliser_nom).notna())  # "" / espaces -> NaN
    df[col_nom] = noms
    codes = df[col_code]

    niveau1 = table_depuis_export(df, col_code, col_nom)
    rapport = {"codes_niveau1": 0, "codes_niveau2": 0, "noms_niveau1": 0, "noms_niveau2": 0,
               "non_resolus": 0, "ambigus": 0}
    non_resolus = {}

    nouveaux_codes, nouveaux_noms = codes.copy(), noms.copy()
    for idx in df.index:
        code, nom = _code_valide(codes.at[idx]), noms.at[idx]
        if code is None and pd.notna(nom):
            trouve = niveau1.code_pour(nom)
            if trouve is not None:
                rapport["codes_niveau1"] += 1
            elif nomenclature is not None:
                trouve = nomenclature.code_pour(nom)
                if trouve is not None:
                    rapport["codes_niveau2"] += 1
            if trouve is not None:
                nouveaux_codes.at[idx] = trouve
            else:
                ambigu = niveau1.est_ambigu(nom) or (nomenclature is not None and nomenclature.est_ambigu(nom))
                rapport["ambigus" if ambigu else "non_resolus"] += 1
                non_resolus.setdefault(df.at[idx, col_id], set()).add(
                    f"{nom}{' (nom associé à plusieurs codes ORPHA)' if ambigu else ''}")
        elif code is not None and pd.isna(nom):
            trouve = niveau1.nom_pour(code)
            if trouve is not None:
                rapport["noms_niveau1"] += 1
            elif nomenclature is not None:
                trouve = nomenclature.nom_pour(code)
                if trouve is not None:
                    rapport["noms_niveau2"] += 1
            if trouve is not None:
                nouveaux_noms.at[idx] = trouve

    df[col_code] = pd.to_numeric(nouveaux_codes, errors="coerce")
    df[col_nom] = nouveaux_noms

    for id_patient, liste in non_resolus.items():
        print(f"⚠️ Patient {id_patient} : maladie sans code ORPHA retrouvé "
              f"({'; '.join(sorted(liste))}) -- bloc non retenu par le filtre ORPHA, à vérifier.")
    return df, rapport
