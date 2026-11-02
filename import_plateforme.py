"""
Fichier d'IMPORT DES DONNÉES pour la plateforme SKEZIA (mode hors ligne, étape 2/2).

La plateforme vérifie chaque colonne selon son type et n'accepte que des formats précis. Règles
établies par essais d'import réels (un fichier par type de champ, octobre 2026) :

    Type du champ (Survey.csv)                          Écriture de la valeur dans le CSV
    --------------------------------------------------  ---------------------------------
    texte (Field type text, saisie text)                "valeur"      entre guillemets
    liste déroulante (dropdown)                         "valeur"      entre guillemets
    bouton radio (radiogroup)                           "code"        entre guillemets
    choix multiple (tagbox)                             ["code1","code2"]   liste JSON
    nombre (saisie number), booléen, date               tels quels (true/false, 2005-01-08)

Dans un CSV, les guillemets littéraux sont doublés ; la ligne contient donc trois guillemets de chaque côté de TEST.
NON ÉPROUVÉS faute de colonne dans les exports de test : `checkbox` (traité comme tagbox) et
`matrixdropdown_radiogroup` (valeur telle quelle). Plusieurs valeurs dans un tagbox : séparées par
« ; » ou « , » dans l'export, écrites en liste JSON (format non vérifié pour plus d'une valeur).

Une valeur radio / liste / tagbox qui n'est pas dans les choix du questionnaire ferait rejeter le
fichier ENTIER par la plateforme : elle est vidée et signalée dans le rapport (jamais silencieuse).
"""
import csv
import json
import os
import re

import pandas as pd

GENRE_TEXTE, GENRE_LISTE, GENRE_RADIO, GENRE_MULTI = "texte", "liste", "radio", "multi"
GENRE_NOMBRE, GENRE_BOOLEEN, GENRE_DATE, GENRE_AUTRE = "nombre", "booleen", "date", "autre"

_DATE_OK = re.compile(r"^(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})$")
_NOMBRE_OK = re.compile(r"^-?\d+([.,]\d+)?$")


def _codes_choix(choix):
    """'1, Oui | 0, Non' -> ['1', '0'] ; chaîne vide -> None (pas de liste de choix)."""
    if not isinstance(choix, str) or not choix.strip():
        return None
    return [c.split(",", 1)[0].strip() for c in choix.split(" | ")]


def regles_import(survey):
    """{variable: (genre, codes|None)} déduit de Survey.csv."""
    regles = {}
    for _, ligne in survey.iterrows():
        nom = ligne["Variable / Field name"]
        type_champ = "" if pd.isna(ligne["Field type"]) else str(ligne["Field type"])
        saisie = "" if pd.isna(ligne["Field input type"]) else str(ligne["Field input type"])
        codes = _codes_choix(ligne.get("Choices"))
        if saisie == "number":
            genre = GENRE_NOMBRE
        elif saisie == "date":
            genre = GENRE_DATE
        elif type_champ == "boolean":
            genre = GENRE_BOOLEEN
        elif type_champ == "radiogroup":
            genre = GENRE_RADIO
        elif type_champ == "dropdown":
            genre = GENRE_LISTE
        elif type_champ in ("tagbox", "checkbox"):
            genre = GENRE_MULTI
        elif type_champ == "text":
            genre = GENRE_TEXTE
        else:
            genre = GENRE_AUTRE
        regles[nom] = (genre, codes)
    return regles


def _formater(genre, codes, valeur):
    """Renvoie (texte_pour_le_csv, motif_du_rejet|None). Valeur vide -> ('', None)."""
    v = str(valeur).strip()
    if v == "" or v.lower() == "nan":
        return "", None
    if genre == GENRE_TEXTE:
        return f'"{v}"', None
    if genre == GENRE_LISTE:
        if codes is not None and v not in codes:
            return "", "hors des choix"
        return f'"{v}"', None
    if genre == GENRE_RADIO:
        if v.endswith(".0"):
            v = v[:-2]
        if codes is not None and v not in codes:
            return "", "hors des choix"
        return f'"{v}"', None
    if genre == GENRE_MULTI:
        valeurs = [x.strip() for x in re.split(r"[;,]", v) if x.strip()]
        if codes is not None and any(x not in codes for x in valeurs):
            return "", "hors des choix"
        return json.dumps(valeurs, ensure_ascii=False, separators=(",", ":")), None
    if genre == GENRE_NOMBRE:
        if v.endswith(".0"):
            v = v[:-2]
        return (v, None) if _NOMBRE_OK.match(v) else ("", "nombre illisible")
    if genre == GENRE_BOOLEEN:
        bas = v.lower()
        return (bas, None) if bas in ("true", "false") else ("", "booléen illisible")
    if genre == GENRE_DATE:
        return (v, None) if _DATE_OK.match(v) else ("", "date illisible")
    return v, None


def ecrire_csv_import(chemin_xlsx_avec_id, chemin_csv, survey, colonne_id="ID"):
    """
    Lit l'export de données (avec la colonne ID en tête) et écrit le CSV d'import (« ; », UTF-8 avec
    BOM). Refuse d'écraser un fichier existant (FileExistsError). Renvoie (nb_lignes, rapport) où
    rapport = {"rejets": {(colonne, motif): nb}, "hors_questionnaire": [colonnes]}.
    """
    if os.path.exists(chemin_csv):
        raise FileExistsError(chemin_csv)
    df = pd.read_excel(chemin_xlsx_avec_id, dtype=str, keep_default_na=False)
    regles = regles_import(survey)
    rejets, hors = {}, []
    colonnes = [c for c in df.columns if c != colonne_id]
    for c in colonnes:
        if c not in regles:
            hors.append(c)
    with open(chemin_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([colonne_id] + colonnes)
        for _, ligne in df.iterrows():
            sortie = [str(ligne[colonne_id]).strip()]
            for c in colonnes:
                genre, codes = regles.get(c, (GENRE_AUTRE, None))
                texte, motif = _formater(genre, codes, ligne[c])
                if motif:
                    rejets[(c, motif)] = rejets.get((c, motif), 0) + 1
                sortie.append(texte)
            w.writerow(sortie)
    return len(df), {"rejets": rejets, "hors_questionnaire": hors}


def lignes_rapport(chemin_csv, nb_lignes, rapport):
    lignes = [f"📄 Fichier prêt pour l'import des données dans SKEZIA : {os.path.basename(chemin_csv)} "
              f"({nb_lignes} ligne(s))"]
    if rapport["rejets"]:
        total = sum(rapport["rejets"].values())
        lignes.append(f"⚠️ {total} valeur(s) non conforme(s) au questionnaire laissée(s) vide(s) "
                      f"(la plateforme aurait rejeté tout le fichier) :")
        for (col, motif), n in sorted(rapport["rejets"].items()):
            lignes.append(f"   • {col} — {motif} : {n}")
    if rapport["hors_questionnaire"]:
        lignes.append("⚠️ Colonne(s) absente(s) du questionnaire (Survey.csv), écrites telles quelles : "
                      + ", ".join(rapport["hors_questionnaire"]))
    return lignes
