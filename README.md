# FREDDEX

## Présentation

FREDDEX est un outil de traitement et d'intégration de données issues de BaMaRa vers la structure FREDD.

FREDDEX réalise les étapes suivantes :

- lecture des fichiers BaMaRa (Excel multi-feuilles) ;
- transformation et normalisation des données vers le modèle FREDD ;
- application de règles métiers (gènes, variations, diagnostics) ;
- génération des structures compatibles avec la base cible ;
- (optionnel) création des patients et envoi des questionnaires via l'API SKEZIA.

Deux modes d'utilisation existent : **avec** ou **sans** l'API SKEZIA (mode « Hors ligne »).

## ⚠️ Important

- Ce code n'est **pas générique**. Il est fortement couplé à la structure de données FREDD, à l'e-CRF SKEZIA et à son API. Il nécessite des adaptations pour tout autre contexte.
- Les fichiers de métadonnées propres à FREDD (correspondances de variables, structure du questionnaire, codes de maladies, configuration des centres, identifiants chiffrés) **ne sont pas fournis** dans ce dépôt.
- Les dépendances sont listées dans `requirements.txt` (Python ≥ 3.10).
- Ce logiciel ne prétend pas être exempt de défauts ; tout retour d'expérience est bienvenu.

## 1. Configuration préalable

Dans un dossier `files/` à placer à côté du code, à compléter selon votre contexte :

| Fichier | Rôle |
|---|---|
| `files/map_BaMaRa_FREDD.xlsx` | Correspondance entre variables BaMaRa et variables cibles, et entre modalités de réponse. Les « noms standardisés » ne doivent pas être modifiés. |
| `files/Survey.csv` | Export du questionnaire (`Variable / Field name`, `Field type`, `Field input type`, `Choices`) : types et formats des variables. |
| `files/codes_MR/*.txt` | Codes ORPHA retenus (séparés par des virgules). |
| `files/fichier_config.csv` | Un centre par ligne : `Nom_centre`, `Numero_centre`, `Ficher_codes_MR`, `questionnaire_id`. Si l'API n'est pas utilisée, `questionnaire_id` peut valoir `0`. |
| `files/orphanet/*.xml` | Nomenclature Orphanet. |
| `files/cles/` | Clés et identifiants chiffrés (mode API uniquement, voir ci-dessous). |

## 2. Mode complet (avec l'API SKEZIA)

Création des patients dans SKEZIA et envoi des questionnaires remplis.

Pour chaque projet SKEZIA, générer un couple clé / identifiants chiffrés : `python scripts_admin/create_keys.py` (première création) ou `python scripts_admin/rotate_keys.py` (rotation). **La clé ne doit jamais être distribuée avec le fichier chiffré** : elle est lue en priorité dans la variable d'environnement `FREDDEX_FERNET_KEY_<numéro du centre>`.

Si le réseau inspecte le HTTPS avec une autorité interne, FREDDEX utilise le magasin de certificats du système (`truststore`) ; `FREDDEX_TRUSTSTORE=0` le désactive.

## 3. Mode hors ligne (sans l'API)

Choisir « Hors ligne » dans l'écran d'accueil. Aucune connexion réseau n'est utilisée.

**Étape 1/2** — transformation des données BaMaRa. Deux fichiers sont produits dans le dossier `output/` :

- `FREDD_export_<date>.xlsx` : les données transformées ;
- `FREDD_participants_<date>.csv` : fichier d'import des participants (nom, prénom, date de naissance) à charger dans SKEZIA **avant** les données.

**Étape 2/2** — après import des participants, exporter leur liste depuis SKEZIA (`Answers.csv`), puis la fournir avec l'export de données. FREDDEX associe l'identifiant SKEZIA de chaque patient (colonne `ID`, en première position) et écrit `FREDD_export_<date>_import_skezia.csv`, au format attendu par l'import de données de la plateforme (valeurs entre guillemets pour les champs texte, listes et boutons radio ; liste JSON pour les choix multiples). Les valeurs hors des choix du questionnaire sont laissées vides et signalées.

## 4. Générer un .exe

```
set FREDDEX_CENTRE=NOM_DU_CENTRE
python -m PyInstaller FREDDEX-base.spec
```

Le fichier `.spec` embarque les fichiers de `files/` et le nom du centre ; il suppose que `files/` est complet.

## Licence

Code distribué sous licence MIT. Développé par Camille Beluffi-Marin ; l'Unité INSERM UMR 1112 en est titulaire des droits (Copyright (c) 2026 INSERM UMR 1112).

Archive Software Heritage : [swh:1:dir:158393792c8d88a59f4afa24914773112407b0dc](https://archive.softwareheritage.org/swh:1:dir:158393792c8d88a59f4afa24914773112407b0dc)

FREDDEX s'inscrit dans un cadre de recherche clinique dédié à l'interopérabilité des données de santé, notamment avec BaMaRa et la Banque Nationale de Données Maladies Rares (BNDMR).

Ce travail est soutenu par l'État français à travers l'Agence Nationale de la Recherche (ANR), dans le cadre du programme d'investissement France 2030 (ANR-21-PMRB-0009).
