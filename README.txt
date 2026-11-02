FREDDEX

Overview
FREDDEX processes and integrates data from BaMaRa into the FREDD structure: reading BaMaRa Excel exports,
transforming them to the FREDD model (genes, variations, diagnoses), and optionally creating patients and
sending questionnaires through the SKEZIA API. Two modes exist: with the API, or "Hors ligne" (offline).

Important
This code is not generic: it is tightly coupled to the FREDD data structure and to the SKEZIA e-CRF and API.
The FREDD-specific metadata files (variable mapping, questionnaire structure, disease codes, centre
configuration, encrypted credentials) are NOT provided in this repository. Dependencies: requirements.txt
(Python >= 3.10).

Full documentation (in French): see README.md.
