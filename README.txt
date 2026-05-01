FREDDEX
Présentation

FREDDEX est un outil de traitement et d’intégration de données issues de BaMaRa vers la structure FREDD.

FREDDEX réalise les étapes suivantes :

- Lecture des fichiers BaMaRa (Excel multi-feuilles)
- Transformation et normalisation des données vers le modèle FREDD
- Application de règles métiers complexes (gènes, variations, diagnostics)
- Génération des structures compatibles avec la base cible
- (Optionnel) Création des patients et envoi des questionnaires via API SKEZIA

2 Modes d’utilisation sont possibles : avec ou sans exploitation de l'API.

-------------------------
⚠️ Important 
-------------------------

- Ce code n’est pas générique. Il est fortement couplé à l’architecture FREDD/SKEZIA. 
Le code partagé repose donc fortement sur :
	- la structure de données FREDD,
	- l’environnement SKEZIA,
	- ainsi que l’API SKEZIA pour la création de patients et l’envoi des questionnaires.
Il nécessite obligatoirement des adaptations pour tout autre contexte.
- FREDDEX nécessite l’installation des dépendances listées dans le fichier requirements.txt afin de fonctionner correctement.
- Ce document et le logiciel associé ne prétendent pas être exempts de défauts ni constituer une solution parfaite. Ils s’inscrivent dans une démarche évolutive, où des améliorations restent possibles. Tout retour d’expérience ou suggestion est donc vivement encouragé afin de contribuer à leur amélioration continue.

-------------------------
0. Avant utilisation
-------------------------

Peu importe le mode d'utilisation choisi, il faut avant tout réaliser une configuration préalable reposant sur plusieurs fichiers de mapping permettant d’assurer la correspondance entre les données BaMaRa et la structure cible FREDD implémentée dans l’e-CRF SKEZIA. 
- Le fichier principal files/mapping_BaMaRa_FREDD.xlsx doit être complété pour définir le lien entre les variables BaMaRa et les variables de la base cible, ainsi que les correspondances des modalités de réponse. Dans le code partagé, ces éléments sont directement alignés avec la configuration de l’e-CRF FREDD dans SKEZIA (métadonnées et valeurs autorisées). Les variables standardisées ne doivent pas être modifiées car elles assurent la cohérence du lien entre les deux systèmes.

- Un fichier Survey.csv est également requis avec une structure identique (notamment Variable / Field name, Field type, Field input type) afin de décrire les caractéristiques des variables sources, en particulier leur type et leur format d’entrée.

- Le fichier files/codes_MR/code_MR.txt doit contenir la liste des codes ORPHA (séparés par des virgules), utilisés pour filtrer les patients BaMaRa selon les maladies rares

- Configurer les centres fournisseurs de données à partir des informations contenues dans le fichier files/fichier_config.csv. Pour chaque centre, les paramètres suivants doivent être correctement définis : ID du centre, questionnaire associé et fichiers liés.

Une attention particulière doit être portée à la définition de l’ID du questionnaire SKEZIA :

   - si l’API est utilisée, l’ID du questionnaire doit être correctement renseigné ;
   - si l’API n’est pas utilisée, le champ questionnaire ID peut être initialisé à "0".

-------------------------
1. Mode complet (avec API SKEZIA)
-------------------------

Ce mode permet :

- la création des patients dans SKEZIA
- l’envoi des questionnaires remplis

Prérequis :

Avant exécution, il est nécessaire de :

- Configurer les centres fournisseurs de données à partir des informations contenues dans le fichier files/fichier_config.csv. Pour chaque centre, les paramètres suivants doivent être correctement définis : ID du centre, questionnaire associé et fichiers liés.

Une attention particulière doit être portée à la définition de l’ID du questionnaire SKEZIA :

   - si l’API est utilisée, l’ID du questionnaire doit être correctement renseigné ;
   - si l’API n’est pas utilisée, le champ questionnaire ID peut être initialisé à "0".

- Générer les clés API -> Utiliser le script create_keys (python create_keys.py) pour générer un coucle de clés dans le dossier cle. Ces clés permettent l’authentification à l’API SKEZIA et d'envoyer des données vers l'API de manière sécurisée. Attention : il faut générer un couple de clé pour chaque projet SKEZIA

-------------------------
2. Mode offline (sans API)
-------------------------

Ce mode permet uniquement :
- la transformation des données BaMaRa → FREDD
- la génération d’un fichier Excel final structuré
Activation

Pour activer ce mode :

Ouvrir FREDDEX-base.py et commenter le bloc indiqué entre (*) dans la fonction "traitement_complet".

-------------------------
3. Generer un .exe
-------------------------
Si souhaité, il est possible de générer un exécutable (.exe) afin de faciliter la diffusion et l’utilisation du logiciel sur d’autres environnements ne disposant pas de Python.
La génération de l’exécutable peut être réalisée avec PyInstaller à partir du fichier de spécification fourni :

python -m PyInstaller FREDDEX-base.spec

-------------------------
LICENSE
-------------------------
Le code source de FREDDEX est distribué sous licence MIT. Il est développé par Camille Beluffi-Marin, et l’Unité INSERM UMR 1112 en est le titulaire des droits (Copyright (c) 2026 INSERM UMR 1112).

FREDDEX s’inscrit dans un cadre de recherche clinique dédié à l’interopérabilité des données de santé, notamment avec BaMaRa et la Banque Nationale de Données Maladies Rares (BNDMR).

Ce travail est soutenu par l’État français à travers l’Agence Nationale de la Recherche (ANR), dans le cadre du programme d’investissement France 2030 (ANR-21-PMRB-0009).