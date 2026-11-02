# -*- coding: utf-8 -*-
# FREDDEX - main entry point
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# This file is a thin orchestrator: it wires together the extraction, transformation and
# loading layers (each in its own module) and drives the GUI. The business logic lives in:
# paths.py, logging_utils.py, config.py, mapping.py, extraction.py, transformation.py,
# skezia_api.py, participants.py, liaison_ids.py, import_plateforme.py, gui.py.

# --- GESTION DU SPLASH SCREEN PYINSTALLER ---
try:
    import pyi_splash
    pyi_splash.close()
except Exception:
    pass

import os
import shutil
import sys
import tempfile
import time
import traceback
from datetime import datetime
from urllib.parse import urlparse

# --- MAGASIN DE CERTIFICATS DU SYSTÈME (doit rester AVANT tout import de requests/urllib3) ---
# Certains établissements de santé inspectent le trafic HTTPS : leur proxy re-signe les
# certificats avec une autorité interne, déployée dans le magasin Windows des postes mais
# inconnue du magasin embarqué avec Python (certifi). Sans ceci, la connexion à SKEZIA échoue
# avec « CERTIFICATE_VERIFY_FAILED : self-signed certificate in certificate chain ». truststore
# fait valider les certificats par le système (CryptoAPI sous Windows), comme le navigateur.
# Désactivable en cas de souci : variable d'environnement FREDDEX_TRUSTSTORE=0 (c'est aussi ce
# que fait tests/conftest.py, pour ne pas modifier le module ssl du processus de test).
# Le résultat est consigné dans STATUT_MAGASIN_CERTIFICATS et écrit dans le journal au
# démarrage (le journal n'est pas encore configuré à ce stade).
if os.environ.get("FREDDEX_TRUSTSTORE", "1").strip() == "0":
    STATUT_MAGASIN_CERTIFICATS = "désactivé (FREDDEX_TRUSTSTORE=0) : magasin de certificats embarqué (certifi)"
else:
    try:
        import truststore
        truststore.inject_into_ssl()
        STATUT_MAGASIN_CERTIFICATS = "magasin de certificats du système (truststore)"
    except Exception as e:  # jamais bloquant : repli sur certifi, comme avant
        STATUT_MAGASIN_CERTIFICATS = (f"indisponible ({type(e).__name__}: {e}) : "
                                      f"repli sur le magasin embarqué (certifi)")

import pandas as pd
import requests
from cryptography.fernet import InvalidToken

from logging_utils import setup_logging, log_dir
from config import NOM_CENTRE, DATE_VERSION, initialiser_ressources, obtenir_numero_centre
from mapping import rename_columns
from extraction import read_bamara_export, compter_patients_bruts, ordonner_exports
from transformation import traiter_donnees, diag_block_treatment, handle_FREDD_columns, appliquer_remplacements
from orphanet import charger_nomenclature_defaut
from skezia_api import (
    def_questions_type,
    normaliser_dates_export,
    create_patient_info,
    charger_token_manager,
    create_patient_profiles,
    send_questionnaire_responses,
    ExistingPatientsFetchError,
    SkeziaCertificateError,
    SKEZIA_BASE_URL,
)
from participants import construire_fichier_participants, ecrire_fichier_participants
from gui import afficher_bienvenue_et_lancer, creer_root
from key_setup import cle_disponible, assurer_cle_disponible
from liaison_ids import COLONNE_ID_PAR_DEFAUT, lire_export_participants, lier_classeur
from import_plateforme import ecrire_csv_import, lignes_rapport
from paths import resource_path


def lier_ids_hors_ligne(fichier_donnees, fichier_participants):
    """
    ÉTAPE 2 du mode hors-ligne, appelée par le bouton de la fenêtre de sélection : ajoute l'ID
    SKEZIA à l'export de données, à partir de l'export des participants de la plateforme, et écrit
    le fichier prêt à importer (<export>_import_skezia.csv, jamais écrasé ; formats imposés par la
    plateforme, voir import_plateforme.py). L'export de données d'origine n'est jamais modifié ; la
    copie intermédiaire avec ID est temporaire et supprimée. Aucun accès réseau.

    Renvoie (message, chemin_sortie) ; chemin_sortie vaut None si rien n'a été écrit. Ne lève
    jamais : toute erreur attendue (fichier illisible, colonne absente, fichier déjà existant)
    devient un message ❌ en français, affiché tel quel à l'utilisateur.
    """
    sortie = os.path.splitext(fichier_donnees)[0] + "_import_skezia.csv"
    ext_donnees, ext_export = (os.path.splitext(f)[1].lower() for f in (fichier_donnees, fichier_participants))
    if ext_donnees in (".csv", ".txt") and ext_export in (".xlsx", ".xlsm"):
        return ("❌ Les deux fichiers semblent inversés : l'export de la plateforme (.csv) va dans la "
                "case 1, l'export de données FREDDEX (.xlsx) dans la case 2."), None
    if os.path.exists(sortie):
        return (f"❌ Le fichier {os.path.basename(sortie)} existe déjà dans ce dossier ; FREDDEX ne "
                "l'écrase jamais. Renommez-le ou déplacez-le, puis recommencez."), None
    dossier_tmp = tempfile.mkdtemp(prefix="freddex_")
    intermediaire = os.path.join(dossier_tmp, "avec_id.xlsx")
    try:
        df_export = lire_export_participants(fichier_participants)
        rapport = lier_classeur(fichier_donnees, df_export, intermediaire, COLONNE_ID_PAR_DEFAUT,
                                ecrire_si_aucune_liaison=False)
        if not rapport.ecrit:
            return ("\n".join(rapport.lignes_texte()) + "\n\nVérifiez que l'export de la plateforme est celui "
                    "des participants de CE lot (même noms et prénoms)."), None
        survey = pd.read_csv(resource_path("files/Survey.csv"), sep=";")
        nb, rapport_csv = ecrire_csv_import(intermediaire, sortie, survey)
    except (ValueError, OSError) as e:
        return f"❌ {e}", None
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        return f"❌ Le fichier d'import des données n'a PAS pu être généré : {e}", None
    finally:
        shutil.rmtree(dossier_tmp, ignore_errors=True)
    message = "\n".join(rapport.lignes_texte()) + "\n\n" + "\n".join(lignes_rapport(sortie, nb, rapport_csv))
    return message, sortie


def exporter_participants(df_final, output_dir, horodatage):
    """
    Écrit, à côté de l'export de données du mode hors-ligne, le fichier d'import des
    participants (voir participants.py : NAME, FIRST_NAME, BIRTHDAY uniquement). Les
    participants doivent être importés AVANT le fichier de données.

    Renvoie le texte à ajouter au message final. Ne lève jamais : un échec ici ne doit pas
    faire perdre l'export de données déjà écrit, mais il est signalé en toutes lettres (un
    fichier manquant passé sous silence ferait croire à l'utilisateur que tout est prêt).
    """
    try:
        df_participants, ids_exclus, nb_doublons = construire_fichier_participants(df_final)
        chemin = os.path.abspath(os.path.join(output_dir, f"FREDD_participants_{horodatage}.csv"))
        ecrire_fichier_participants(df_participants, chemin)
    except Exception:
        try:
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
        except Exception:
            pass
        message = ("⚠️ Le fichier des participants n'a PAS pu être généré (détail dans "
                   "logs/error_log.txt). Le fichier de données, lui, est bien écrit.")
        print(message)
        return message

    lignes = [f"👥 Fichier des participants : {len(df_participants)} participant(s) dans {chemin}",
              "   À importer AVANT le fichier de données."]
    if nb_doublons:
        lignes.append(f"ℹ️ {nb_doublons} ligne(s) identique(s) (même nom, prénom et date de naissance) "
                      f"regroupée(s) en un seul participant.")
    if ids_exclus:
        lignes.append(f"⚠️ {len(ids_exclus)} patient(s) absent(s) du fichier des participants "
                      f"(nom, prénom ou date de naissance manquant) : ID BaMaRa {', '.join(ids_exclus)}")
    for ligne in lignes:
        print(ligne)
    return "\n".join(lignes)


def apercu_fichier(fichier_entree, centre=NOM_CENTRE):
    """
    Lightweight preview for the GUI's pre-launch confirmation screen (see
    gui.lancer_interface_et_traitement's fonction_apercu parameter): a short, human-readable
    string describing what a real import of fichier_entree would involve, computed WITHOUT
    running any of the actual pipeline (no transformation, no SKEZIA connection).

    Deliberately cheap: reads only the "Données administratives" sheet (via
    extraction.compter_patients_bruts) instead of all 7, and does none of
    transformation.traiter_donnees()'s filtering -- so the number shown is a raw row count,
    not a guarantee of how many patients will actually be imported (the FREDDEX
    ORPHA/status filters, applied later, normally narrow it down). Good enough for a sanity
    check ("does this look like the right file, roughly the right size") without making the
    user wait through the full pipeline just to see a preview.

    Returns a string on success. Never raises: any failure to read the file (wrong format,
    missing sheet, unreadable file...) is caught and turned into an explanatory string
    instead, since the confirmation screen must still be able to show *something* and let the
    user decide whether to go back and pick a different file.
    """
    try:
        # NOUVEAU FORMAT : fichier_entree = (export principal, export génétique).
        # ordonner_exports vérifie les deux fichiers dès l'aperçu (onglets attendus).
        fichier_principal, _ = ordonner_exports(*fichier_entree)
        nombre_patients = compter_patients_bruts(fichier_principal)
    except Exception as e:
        return f"⚠️ Aperçu indisponible (fichier illisible ou format inattendu : {e})."
    return (f"Le fichier contient {nombre_patients} patient(s) (avant filtrage FREDDEX -- "
            f"Attention : le nombre réellement importé de patients pourra être inférieur (filtrage ultérieur).")


def traitement_complet(fichier_entree, centre=NOM_CENTRE, cancel_event=None, mode_hors_ligne=False):
    """
    Main function:
    0) Opens a window to select the BaMaRa input file to be processed
    1) Processes the BaMaRa Excel file sheet by sheet
    2) Processes all retrieved data using the traiter_donnees function
    3) Handles diagnostic blocks using the diag_block_treatment function
    4) Applies column transformation rules using the handle_columns function
    5) Inserts properly formatted questionnaire responses using the appliquer_remplacements function
    6) Connects to the SKEZIA API (skipped entirely in offline mode -- see mode_hors_ligne)
    7) Creates patient profiles and sends them to the API
    8) Creates patient questionnaires and sends them to the API

    cancel_event: optional threading.Event for cooperative cancellation from the GUI's
    "Annuler" button (see gui.lancer_interface_et_traitement). Checked once here, right
    before any SKEZIA call is made -- cancelling during extraction/transformation (steps
    1-5, all local, no network calls) therefore stops the whole import cleanly with nothing
    sent. Once SKEZIA sending has started, cancel_event is forwarded to
    create_patient_profiles/send_questionnaire_responses, which check it per-patient: see
    their docstrings for why cancelling mid-send necessarily leaves a partial import.

    mode_hors_ligne: if True, stops after step 5 (all local, no network) and writes the
    structured result straight to an Excel file instead of contacting SKEZIA at all -- for
    centers that don't use the SKEZIA API, or to inspect the transformed data before sending
    it for real. Selected via the GUI's "Mode hors-ligne" checkbox (see
    gui.lancer_interface_et_traitement). Replaces the previous approach (still described as
    a fallback in the README) of manually commenting out the SKEZIA-sending code block below.
    """

    # Loading required files and variables
    result = initialiser_ressources(centre)
    if isinstance(result, str):
        print(result)
        return result

    liste_codes_MR, numero_centre, questionnaire_centre, map_file, survey = result

    # START
    if fichier_entree:
        start_time = time.time()
        try:
            print(f"FREDDEX version du {DATE_VERSION} pour le centre {NOM_CENTRE}")
            print(f"Fichiers d'entrée sélectionnés : {' + '.join(map(str, fichier_entree))}")
            (df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_recherche,
             df_invest) = read_bamara_export(*fichier_entree)
            print("Traitement en cours...")

            nomenclature = charger_nomenclature_defaut()  # thésaurus Orphanet (correspondance nom <-> code ORPHA)
            df_output = traiter_donnees(liste_codes_MR, map_file, df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_recherche,
                                        df_invest, nomenclature=nomenclature)
            if df_output.empty:
                print("Aucun patient sélectionné selon les critères FREDDEX. Traitement arrêté.")
                return "ℹ️ Aucun patient n'a été sélectionné selon les critères FREDDEX. Traitement arrêté."

            #df_output.to_excel("resultat_1.xlsx", index=False)  # uncomment to see what your output file look like at this point
            df_output = diag_block_treatment(df_output, map_file)
            #df_output.to_excel("resultat_2.xlsx", index=False) # uncomment to see what your output file look like at this point
            df_output = rename_columns(df_output, map_file)
            df_output = handle_FREDD_columns(df_output, map_file, centre, numero_centre)  # this function is usefull only for FREDD data
            df_final = appliquer_remplacements(df_output, map_file)

            if mode_hors_ligne:

                output_dir = "output"
                os.makedirs(output_dir, exist_ok=True)
                horodatage = datetime.now().strftime('%Y%m%d_%H%M%S')  # commun aux deux fichiers produits
                output_path = os.path.abspath(os.path.join(output_dir, f"FREDD_export_{horodatage}.xlsx"))
                # Dates au format accepté par l'import de la plateforme (AAAA-MM-JJ, texte), comme
                # l'envoi en ligne ; le fichier des participants, lui, part de df_final.
                df_export, dates_illisibles = normaliser_dates_export(df_final, def_questions_type(survey))
                df_export.to_excel(output_path, index=False)
                avert_dates = ""
                if dates_illisibles:
                    avert_dates = (f"⚠️ {sum(dates_illisibles.values())} date(s) illisible(s) laissée(s) vide(s) "
                                   f"dans l'export (colonnes : {', '.join(dates_illisibles)}).\n")
                    print(avert_dates.strip())
                print(f"ℹ️ Mode hors-ligne : fichier structuré généré ({len(df_final)} ligne(s)) : {output_path}")
                resume_participants = exporter_participants(df_final, output_dir, horodatage)
                return (f"✅ Mode hors-ligne : {len(df_final)} ligne(s) exportée(s) dans {output_path}.\n"
                        f"{avert_dates}{resume_participants}\n"
                        f"Aucune donnée n'a été envoyée à SKEZIA.")

            question_types = def_questions_type(survey)
            patients_info = create_patient_info(df_final)

            if cancel_event is not None and cancel_event.is_set():
                # Cancelled before any network call: extraction/transformation above are
                # local-only, so nothing has been sent to SKEZIA yet -- a clean, complete stop.
                print("🛑 Annulé par l'utilisateur avant la connexion à SKEZIA. Aucune donnée n'a été envoyée.")
                return "🛑 Traitement annulé avant tout envoi. Aucune donnée n'a été transmise à SKEZIA."

            if not cle_disponible(numero_centre):
                message = ("❌ Aucune clé de sécurité SKEZIA n'est configurée pour ce centre. "
                            "Fermez puis relancez FREDDEX pour la renseigner, ou contactez "
                            "l'équipe FREDD'")
                print(message)
                return message

            print("Connexion à l'API SKEZIA...")

            try:
                token_manager = charger_token_manager(numero_centre)
            except InvalidToken:
                message = ("❌ Connexion à SKEZIA impossible : la clé de sécurité enregistrée "
                            "pour ce centre ne correspond pas aux identifiants stockés."
                            "Contactez l'équipe FREDD.")
                print(message)
                return message
            except OSError as e:
                message = (f"❌ Fichier de sécurité introuvable pour ce centre "
                            f"({e.filename or e}). Contactez l'équipe FREDD")
                            
                print(message)
                return message

            print("Envoi des profils patients...")

            try:
                created_profiles = create_patient_profiles(patients_info, token_manager, cancel_event=cancel_event)
            except SkeziaCertificateError as e:
                # Certificat du serveur non reconnu par ce poste : quasi toujours une inspection
                # HTTPS du réseau de l'établissement (la DSI est la bonne interlocutrice, pas
                # l'équipe FREDD). Levée avant toute création : rien n'a été envoyé.
                hote = urlparse(SKEZIA_BASE_URL).hostname
                message = (f"❌ Connexion sécurisée à SKEZIA impossible : le certificat du serveur "
                            f"({hote}) n'est pas reconnu par ce poste. Aucune donnée n'a été envoyée. "
                            f"Cela vient en général du réseau de l'établissement, qui inspecte les "
                            f"connexions HTTPS : demandez à votre DSI d'exclure {hote} de cette "
                            f"inspection (ou de fournir son certificat racine), puis réessayez. "
                            f"Si le problème persiste, contactez l'équipe FREDD.")
                print(message)
                print(f"(détail technique : {e})")
                return message
            except ExistingPatientsFetchError as e:

                message = ("❌ Impossible de vérifier la liste des patients déjà présents dans "
                            "SKEZIA : l'import a été interrompu AVANT toute création, pour éviter "
                            "de créer des doublons. Vérifiez la connexion à SKEZIA puis réessayez ; "
                            "si le problème persiste, contactez l'équipe FREDD. ")
                            
                print(message)
                print(f"(détail technique : {e})")
                return message
            except requests.exceptions.RequestException:
                message = ("❌ Impossible de joindre le serveur SKEZIA. Vérifiez la connexion "
                            "Internet de ce poste puis réessayez. Si le problème persiste, "
                            "contactez l'équipe FREDD'.")
                print(message)
                return message

            if created_profiles:
                print("Envoi des questionnaires patients...")
                sent_responses = send_questionnaire_responses(
                    created_profiles,
                    df_final,
                    question_types,
                    token_manager,
                    questionnaire_id=questionnaire_centre,
                    cancel_event=cancel_event,
                )
                print(f"{len(created_profiles)} profil(s) créé(s).")
                print(f"{len(sent_responses)} questionnaire(s) envoyé(s).")

                resume = ""
                for patient in created_profiles:
                    id_barama = str(patient.get("id_barama", "N/A"))
                    nom1 = str(patient.get("family_nai", "N/A"))
                    nom2 = str(patient.get("family_us", "N/A"))
                    prenom = str(patient.get("given", "N/A"))
                    resume += f"ID BaMaRa: {id_barama}, NOM: {nom1} {nom2}, Prénom: {prenom}\n"
                end_time = time.time()
                duration = (end_time - start_time)/60

                if cancel_event is not None and cancel_event.is_set():
                    return (f"⚠️ Traitement annulé par l'utilisateur pendant l'envoi : import PARTIEL -- "
                            f"{len(created_profiles)} profil(s) créé(s) et {len(sent_responses)} "
                            f"questionnaire(s) envoyé(s) en {duration:.2f} minutes pour :\n{resume}")

                return f"✅ {len(created_profiles)} profil(s) créé(s) et {len(sent_responses)} questionnaire(s) envoyé(s) en {duration:.2f} minutes pour :\n{resume}"
            else:
                print("Tous les patients sont déjà présents dans FREDD.")
                return "ℹ️ Tous les patients sont déjà présents dans FREDD. Aucun profil n'a été créé."

        except Exception:

            try:
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
            except Exception:
                pass
            return ("❌ Une erreur inattendue s'est produite pendant l'import. Le détail "
                    "technique a été enregistré dans le fichier logs/error_log.txt — "
                    "transmettez-le à la l'équipe FREDD. ")
                    
    else:
        print("Aucun fichier sélectionné, arrêt du traitement.")
        return "❌ Aucun fichier n'a été sélectionné. Traitement arrêté."


if __name__ == "__main__":
    setup_logging()
    print(f"Vérification des certificats HTTPS : {STATUT_MAGASIN_CERTIFICATS}")

    root = creer_root()

    numero_centre_demarrage = obtenir_numero_centre(NOM_CENTRE)
    if isinstance(numero_centre_demarrage, str):

        print(numero_centre_demarrage)
    else:
        assurer_cle_disponible(numero_centre_demarrage, root=root)

    afficher_bienvenue_et_lancer(
        lambda f, cancel_event=None, mode_hors_ligne=False: traitement_complet(
            f, NOM_CENTRE, cancel_event=cancel_event, mode_hors_ligne=mode_hors_ligne,
        ),
        fonction_apercu=lambda f: apercu_fichier(f, NOM_CENTRE),
        dossier_logs=log_dir,
        root=root,
        fonction_liaison=lier_ids_hors_ligne,
    )
