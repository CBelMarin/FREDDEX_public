FREDDEX
Overview

FREDDEX is a tool for processing and integrating data from BaMaRa into the FREDD structure.
FREDDEX performs the following steps:
1) Reading of a BaMaRa files (multi-sheet Excel files)
2) Transformation and normalization of data into the FREDD model
3) Application of complex business rules (genes, variants, diagnoses)
4) Generation of structures compatible with the target database
5) (Optional) Patient creation and questionnaire submission via the SKEZIA API

Two usage modes are available: with or without API integration.

-----------------
⚠️ Important
-----------------
- This code is not generic. It is tightly coupled to the FREDD/SKEZIA architecture.
	The shared code strongly depends on:
	- the FREDD data structure,
	- the SKEZIA environment,
	- and the SKEZIA API for patient creation and questionnaire submission.
	Adaptations are therefore required for any other context.
- FREDDEX requires the installation of the dependencies listed in the requirements.txt file in order to function properly.
- This document and the associated software do not claim to be free of defects or to constitute a perfect solution. They are part of an evolving approach, where improvements remain possible. Any feedback or suggestions are therefore strongly encouraged to support continuous improvement.

-----------------
0. Before use
-----------------
Regardless of the selected mode, a preliminary configuration step is required, based on several mapping files ensuring consistency between BaMaRa data and the target FREDD structure implemented in the SKEZIA e-CRF.
- The main file files/mapping_BaMaRa_FREDD.xlsx must be completed to define the mapping between BaMaRa variables and target database variables, as well as the correspondence between response values. In the shared code, these elements are directly aligned with the FREDD e-CRF configuration in SKEZIA (metadata and authorized values). Standardized variables must not be modified, as they ensure consistency between the two systems.
- A Survey.csv file is also required, with a similar structure (notably Variable / Field name, Field type, Field input type), in order to describe the characteristics of the source variables, particularly their type and input format.
The file files/codes_MR/code_MR.txt must contain a list of ORPHA codes (comma-separated), used to filter BaMaRa patients based on rare diseases.
- Data provider centers must be configured using the information contained in the file files/fichier_config.csv. For each center, the following parameters must be properly defined: center ID, associated questionnaire, and related files.
- Particular attention must be paid to the SKEZIA questionnaire ID:
	- if the API is used, the questionnaire ID must be correctly specified;
	- if the API is not used, the questionnaire ID field can be set to "0".

-----------------
1. Full mode (with SKEZIA API)
-----------------
This mode enables:
- patient creation in SKEZIA
- submission of completed questionnaires

Prerequisites
Before execution, it is necessary to:

- Configure data provider centers using the file files/fichier_config.csv, ensuring that the following parameters are properly defined: center ID, associated questionnaire, and related files.
- Particular attention must be paid to the SKEZIA questionnaire ID:
	- if the API is used, the questionnaire ID must be correctly specified;
	- if the API is not used, the questionnaire ID field can be set to "0".
- Generate API keys → Use the create_keys script (python create_keys.py) to generate a key pair in the cle folder. These keys are used to authenticate with the SKEZIA API and securely send data.
⚠️ A key pair must be generated for each SKEZIA project.

-----------------
2. Offline mode (without API)
-----------------
This mode allows only:
- transformation of BaMaRa data → FREDD
- generation of a structured final Excel file

Activation
To activate this mode, open FREDDEX-base.py and comment out the block indicated between (*) in the traitement_complet function.

-----------------
3. Generating an executable (.exe)
-----------------
If desired, an executable (.exe) can be generated to facilitate distribution and usage on environments without Python.
The executable can be generated using PyInstaller with the provided specification file:

python -m PyInstaller FREDDEX-base.spec

-----------------
LICENSE
-----------------
The FREDDEX source code is distributed under the MIT License.
It is developed by Camille Beluffi-Marin, and the INSERM UMR 1112 unit is the legal rights holder (Copyright (c) 2026 INSERM UMR 1112).
FREDDEX is part of a clinical research framework focused on health data interoperability, particularly with BaMaRa and the French National Bank of Rare Diseases (BNDMR).
This work is supported by the French State through the French National Research Agency (ANR), under the France 2030 investment program (ANR-21-PMRB-0009).
