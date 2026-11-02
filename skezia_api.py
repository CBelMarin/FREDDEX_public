# -*- coding: utf-8 -*-
# FREDDEX - Loading layer: SKEZIA API (authentication, patient creation, questionnaire responses)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Warning: everything in this file is SKEZIA-specific (endpoints, FHIR-flavoured payloads,
# token flow). Adapting FREDDEX to another target system means rewriting this module only —
# the extraction/mapping/transformation layers do not depend on it.

import json
import os
import re
import threading
import time
import traceback
import unicodedata

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.fernet import Fernet

from logging_utils import print_lock
from paths import resource_path

# Default timeout (seconds) applied to every outgoing HTTP request to the SKEZIA API.
# Without an explicit timeout, requests can hang indefinitely if the API stops responding,
# which would freeze the whole import.
API_TIMEOUT = 30

# Base URL of the SKEZIA API, used to build every endpoint below -- switching SKEZIA
# environments (e.g. a staging/sandbox instance) only requires changing this one constant.
SKEZIA_BASE_URL = "https://api.skezi.eu"

MAX_SEND_ATTEMPTS = 5
BACKOFF_CAP_SECONDS = 30
# Transient server-side HTTP codes worth retrying. 401 is handled separately (it forces a
# fresh token first). Anything else -- especially 400/422 validation errors -- is a definitive
# answer: retrying would fail identically, so we return it at once and report it.
_RETRYABLE_STATUS = (500, 502, 503, 504)


def _backoff_delay(attempt):
    """Exponential back-off delay (seconds) before retry number `attempt` (1-based), capped at
    BACKOFF_CAP_SECONDS: 2, 4, 8, 16, 30, 30..."""
    return min(2 ** attempt, BACKOFF_CAP_SECONDS)


def _post_with_retries(url, json_data, token_manager, label, cancel_event=None):
    """
    POSTs json_data to url, retrying TRANSIENT failures with exponential back-off:
      - network errors (requests.RequestException: DNS, connection reset...) -- - timeouts,
      - HTTP 401 (invalidates the token so a brand-new one is fetched before the next attempt),
      - HTTP 500/502/503/504.
    A 2xx, or a non-retryable 4xx (e.g. a 400/422 validation error such as the
    "questionnaire is not a valid canonical URL" one), is returned IMMEDIATELY -- the caller
    decides what to do with it, and SKEZIA is not hammered once per patient for a systematic
    config error. Returns the final requests.Response, or None if every attempt failed with a
    transient error before any HTTP answer was obtained.

    `label` is a pseudonymous identifier (e.g. a BaMaRa ID) used only for log messages -- no
    health data is logged here. `cancel_event`, if set, stops retrying between attempts.

    Idempotency note: retrying a POST after an AMBIGUOUS failure (the request may have reached
    SKEZIA just before the connection dropped) can in theory create a duplicate. In practice
    the dominant failure mode is a DNS/name-resolution error (getaddrinfo failed), where the
    request never left the machine, so retrying is safe. This is a deliberate trade-off: the
    previous no-retry behaviour dropped patients on every brief blip.
    """
    response = None
    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        if cancel_event is not None and cancel_event.is_set():
            return response
        headers = {
            "Authorization": f"Bearer {token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        retryable = False
        try:
            response = requests.post(url, headers=headers, json=json_data, timeout=API_TIMEOUT)
            if response.status_code == 401:
                # Locally-valid but server-rejected token: force a fresh one before retrying,
                # otherwise the next attempt would resend the exact same rejected token.
                token_manager.invalidate_token()
                retryable = True
                with print_lock:
                    print(f"Jeton refusé (401) pour {label} (tentative {attempt}) — nouveau jeton, nouvel essai.")
            elif response.status_code in _RETRYABLE_STATUS:
                retryable = True
                with print_lock:
                    print(f"Erreur {response.status_code} pour {label} (tentative {attempt}), nouvel essai...")
            else:
                return response  # 2xx, or a non-retryable 4xx -> caller handles it
        except requests.exceptions.SSLError as e:
            # Certificate verification failure is DETERMINISTIC (typically a network that
            # inspects HTTPS, see SkeziaCertificateError): retrying would fail identically
            # MAX_SEND_ATTEMPTS times, with back-off delays, for every patient. Stop at once;
            # None = "no HTTP answer was obtained", the same contract as exhausted retries.
            with print_lock:
                print(f"❌ Certificat SKEZIA non reconnu pour {label} (pas de nouvel essai) : {e}")
            return None
        except requests.exceptions.Timeout:
            retryable = True
            with print_lock:
                print(f"⏱️ Délai dépassé (tentative {attempt}) pour {label}.")
        except requests.exceptions.RequestException as e:
            # Transient network error: retried instead of dropping the patient.
            retryable = True
            with print_lock:
                print(f"❌ Erreur réseau (tentative {attempt}) pour {label} : {e}")

        if not retryable:
            return response
        if attempt < MAX_SEND_ATTEMPTS:
            time.sleep(_backoff_delay(attempt))

    with print_lock:
        print(f"❌ Abandon pour {label} après {MAX_SEND_ATTEMPTS} tentatives (réseau/serveur).")
    return response


class ExistingPatientsFetchError(Exception):
    """
    Raised when the list of existing SKEZIA patients cannot be fully retrieved during
    pagination (a page keeps returning a non-200 status after retries).

    This is deliberately fatal to the import: the existing-patients list is the ONLY
    anti-duplicate safeguard (see create_patient_profiles). Continuing with a partial list
    would silently re-create, as duplicates, every patient that lives on a page we failed to
    read. In a health registry, silently creating duplicate patient records is worse than
    stopping with a clear message, so the import is aborted instead (see
    FREDDEX-base.traitement_complet, which turns this into a user-facing French message).
    """


class SkeziaCertificateError(Exception):
    """
    Raised when the TLS certificate presented for the SKEZIA API cannot be verified
    (requests.exceptions.SSLError, e.g. "CERTIFICATE_VERIFY_FAILED: self-signed certificate in
    certificate chain").

    Unlike a transient network error, this is deterministic -- retrying cannot succeed -- and
    its usual cause is not FREDDEX or SKEZIA but the center's network: a proxy that inspects
    HTTPS and re-signs certificates with an internal CA this machine's Python does not trust.
    It therefore gets its own exception (and its own user-facing message, see
    FREDDEX-base.traitement_complet) pointing at the center's IT department, instead of the
    generic "check your connection / contact the FREDD team". Raised while reading the existing
    patients, i.e. before anything is created: nothing has been sent when it surfaces.
    """


class TokenManager:
    """
    Class responsible for managing the authentication token for the Skezia API.

    This class ensures that the token is valid before each use.
    It automatically generates a new token when the previous one has expired or does not exist.
    It also handles a safety margin to prevent expiration during critical API calls.
    """
    def __init__(self, client_id, client_secret):
        """
        Initializes the manager with the application credentials.
        - client_id: Application identifier provided by the API.
        - client_secret: Secret key associated with the application.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expiry_time = 0

        self._lock = threading.Lock()

    def get_token(self):
        """
        Retourne a valid token. Generates a new one if needed. Thread-safe (see _lock).
        """
        with self._lock:
            current_time = time.time()
            if self.token is None or current_time >= self.expiry_time:
                print("Nouveau jeton d'authentification généré")
                self.token, self.expiry_time = self._generate_token()
            return self.token

    def invalidate_token(self):
        """
        Forces the next get_token() call to request a brand-new token, even if the current
        one has not reached its expiry margin yet. Used when the API answers 401 despite a
        locally-still-valid token (e.g. it was revoked server-side): without this, retrying
        with get_headers() would just resend the exact same rejected token.
        """
        with self._lock:
            self.token = None
            self.expiry_time = 0

    def _generate_token(self):
        """
        Sends a request to the API to obtain a new token. Warning : SKEZIA specific
        """
        response = requests.post(f"{SKEZIA_BASE_URL}/auth/token", data={
            "appId": self.client_id,
            "appSecret": self.client_secret
        }, timeout=API_TIMEOUT)

        if response.status_code in (200, 201):  # 200 = OK, 201 = Created → success of authentification
            token = response.json()["access_token"]
            expires_in = response.json().get("expires_in", 600)
            expiry_time = time.time() + expires_in - 30  # for security
            return token, expiry_time
        else:
            raise Exception(f"Erreur d'authentification SKEZIA : {response.status_code} - {response.text}")


def charger_token_manager(numero_centre):
    """
    Loads encrypted credentials for one center, decrypts them, and returns an initialized
    TokenManager instance.

    SECURITY NOTE: a Fernet key is all that is needed to decrypt secrets_<n>.enc, so the key
    must never be distributed alongside that file -- e.g. bundled into the same PyInstaller
    executable. Anyone holding both halves can trivially
    read the API credentials back out, which defeats the point of encrypting them.
    The key is read from the FREDDEX_FERNET_KEY_<numero_centre> environment variable first.
    The local files/cles/secret_<n>.key file is kept only as a fallback, for the local test
    version -- it must not be shipped together with the .enc file in a real deployment.
    """
    env_var_name = f"FREDDEX_FERNET_KEY_{numero_centre}"
    key_from_env = os.environ.get(env_var_name)
    if key_from_env is not None:
        key = key_from_env.encode()
    else:
        key_path = resource_path(f"files/cles/secret_{numero_centre}.key")
        with open(key_path, "rb") as key_file:
            key = key_file.read()

    # Loading and decrypting secrets
    enc_path = resource_path(f"files/cles/secrets_{numero_centre}.enc")
    with open(enc_path, "rb") as f:
        encrypted_data = f.read()

    fernet = Fernet(key)
    decrypted = fernet.decrypt(encrypted_data)
    secrets = json.loads(decrypted.decode())

    client_id = secrets["appId"]
    client_secret = secrets["secretId"]
    return TokenManager(client_id, client_secret)


def normalize_name(name):
    """
    Normalizes a name by removing accents and non-letter characters, then returns it in uppercase.

    Defined at module level so it can be unit-tested in isolation.
    """
    if pd.isna(name):
        return ""
    name = unicodedata.normalize("NFD", str(name))
    name = name.encode("ascii", "ignore").decode("utf-8")
    name = re.sub(r"[^A-Za-z]", "", name)
    return name.upper()


def normalize_birth_date(birth_date):
    """
    Normalizes a birth date by converting different input formats (including French format) into ISO format (YYYY-MM-DD).

    Defined at module level so it can be unit-tested in isolation.
    """
    if pd.isna(birth_date):
        return None
    try:
        birth_date = str(birth_date).strip()
        # CASE 1 — already ISO → we don't change it
        if re.match(r"\d{4}-\d{2}-\d{2}", birth_date):
            return birth_date[:10]
        # CASE 2 — FR format
        dt = pd.to_datetime(birth_date, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except:
        return None


def generate_possible_dates(date_str):
    """
    Generates possible date formats (YYYY-MM-DD and YYYY-DD-MM) from a given input date string.
    The alternative reading (day and month swapped) lets patients created earlier with an
    inverted birth date still be matched through the API.

    Defined at module level so it can be unit-tested in isolation.
    """
    try:
        d = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
        if pd.isna(d):
            return []

        normal = d.strftime("%Y-%m-%d")
        swapped = d.strftime("%Y-%d-%m")

        return list(set([normal, swapped]))
    except:
        return []


# Number of times a single existing-patients page is attempted before giving up on the whole
# import (see fetch_existing_patients). Small: this is meant to ride out a brief transient
# blip (one 500/502/timeout), not to keep hammering a genuinely down API.
MAX_PAGE_RETRIES = 3


def fetch_existing_patients(url, get_headers, token_manager=None, cancel_event=None):
    """
    Retrieves the full list of existing SKEZIA patients, one page at a time.
    """
    existing_patients = []
    page = 1

    while True:
        if cancel_event is not None and cancel_event.is_set():
            with print_lock:
                print("🛑 Annulation demandée : récupération des patients existants interrompue.")
            return existing_patients
        response = None
        for attempt in range(1, MAX_PAGE_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    headers=get_headers(),
                    params={"_page": page, "_count": 50},
                    timeout=API_TIMEOUT,
                )
            except requests.exceptions.SSLError as e:
                # Deterministic failure (see SkeziaCertificateError): no retry, and a dedicated
                # exception so the caller can tell the user to contact their IT department.
                with print_lock:
                    print(f"❌ Certificat SKEZIA non reconnu (patients existants, page {page}) : {e}")
                raise SkeziaCertificateError(str(e)) from e
            except requests.exceptions.RequestException as e:

                with print_lock:
                    print(f"❌ Erreur réseau (patients existants, page {page}, tentative {attempt}) : {e}")
                response = None
                if attempt < MAX_PAGE_RETRIES:
                    time.sleep(_backoff_delay(attempt))
                continue
            if response.status_code == 200:
                break
            if attempt < MAX_PAGE_RETRIES:
                if response.status_code == 401 and token_manager is not None:
                    # Locally-valid but server-rejected token: force a fresh one before the
                    # next attempt, otherwise get_headers() would resend the same token.
                    token_manager.invalidate_token()
                else:
                    time.sleep(_backoff_delay(attempt))  # back-off for a transient server-side error

        if response is None or response.status_code != 200:
            statut = response.status_code if response is not None else "aucune réponse"
            raise ExistingPatientsFetchError(
                f"Impossible de récupérer la liste des patients existants dans SKEZIA "
                f"(page {page}, statut {statut}). Import interrompu pour éviter de créer "
                f"des doublons."
            )

        data = response.json()
        entries = data.get("entry", [])

        if not entries:
            break

        existing_patients.extend(entries)

        if len(entries) < 50:
            break

        page += 1

    return existing_patients


def build_bamara_to_skezia_index(existing_patients):
    """
    Builds a {BaMaRa ID (str) -> SKEZIA patient reference id} map from the existing-patients
    list returned by fetch_existing_patients. Used by the "resend failed questionnaires" tool
    (resend_failed.py) to reach patients whose profile was already created (so a normal
    re-import would SKIP them, dedup) but whose questionnaire never went through -- e.g. the
    ones lost to the 22/08 network outage.

    The BaMaRa ID is read from the identifier whose system is "BaMaRa" (the value FREDDEX
    sends at creation). The SKEZIA reference id is the resource's logical `id` (what a
    Patient/<id> reference uses), falling back to identifier[0].value (what
    create_patient_profiles captured as the SKEZIA id) if `id` is absent.

    NOTE: this depends on the exact shape of the Patient resources SKEZIA returns -- validate
    on 1-2 patients before a bulk resend.
    """
    index = {}
    for r in existing_patients:
        resource = r.get("resource", r)
        bamara_id = None
        for ident in resource.get("identifier", []):
            if str(ident.get("system", "")).strip().lower() == "bamara":
                bamara_id = str(ident.get("value", "")).strip()
                break
        if not bamara_id:
            continue
        skezia_id = resource.get("id")
        if not skezia_id:
            idents = resource.get("identifier", [])
            skezia_id = idents[0].get("value") if idents else None
        if skezia_id:
            index[bamara_id] = skezia_id
    return index


def create_patient_profiles(patients_info, token_manager, max_workers=5, cancel_event=None):
    """
    Creates patient profiles in SKEZIA in parallel, with automatic handling of expired tokens and parallel retrieval of
    existing patients.
    This function is SKEZIA specific.

    cancel_event: optional threading.Event for cooperative cancellation (see gui.py's
    "Annuler" button). Checked once per patient, right before that patient's HTTP work
    starts: a patient not yet started when cancel_event is set is skipped outright, while a
    patient whose HTTP call already started runs to completion (there is no safe way to abort
    an in-flight request without risking an inconsistent state on the SKEZIA side, and doing
    so brings no benefit -- the call has already been made by the time we could check).
    Callers that don't need cancellation simply omit the parameter; behaviour is identical to
    before it existed.

    Raises ExistingPatientsFetchError if the existing-patients list cannot be fully retrieved
    (see fetch_existing_patients): the import aborts before creating anything rather than
    risk duplicates from an incomplete anti-duplicate index.
    """
    url = f"{SKEZIA_BASE_URL}/skezia/Patient"
    created_patients = []
    # Note: reuses the module-level print_lock (from logging_utils) instead of creating a
    # second, separate Lock() here, since both protect the same "print to log file" resource.

    def get_headers():
        """
        Returns the HTTP headers required for API requests, including the authorization token and content type.
        """
        return {"Authorization": f"Bearer {token_manager.get_token()}",
                "Content-Type": "application/json"}

    # Full retrieval of existing patients (robust pagination handling). A failure here aborts
    # the import (ExistingPatientsFetchError) instead of silently continuing with a partial
    # anti-duplicate list -- see fetch_existing_patients for the full rationale. cancel_event
    # is forwarded so a cancellation during a long pagination stops responsively.
    existing_patients = fetch_existing_patients(url, get_headers, token_manager, cancel_event)

    print(f"Nombre de patients existants récupérés : {len(existing_patients)}")

    # Dédoublonnage, 1er niveau : l'ID BaMaRa. FREDDEX l'envoie à la création (identifier
    # system "BaMaRa"), on le relit donc dans les patients existants. S'il est retrouvé, le
    # patient est déjà dans FREDD, sans ambiguïté possible. S'il ne l'est pas (patient créé
    # hors FREDDEX, ou identifiant non renvoyé par SKEZIA), on retombe sur le rapprochement
    # par identité ci-dessous.
    existing_bamara_ids = set(build_bamara_to_skezia_index(existing_patients))
    print(f"Dont patients identifiés par leur ID BaMaRa : {len(existing_bamara_ids)}")

    existing_index = {}

    for r in existing_patients:
        resource = r.get("resource", r)

        given = normalize_name(resource.get("name", [{}])[0].get("given", [""])[0])
        family = normalize_name(resource.get("name", [{}])[0].get("family", ""))
        birth = normalize_birth_date(resource.get("birthDate"))

        key = (given, family)
        if key not in existing_index:
            existing_index[key] = set()

        existing_index[key].add(birth)

    def process_patient_creation(patient):
        """
        Processes a patient by checking for existing records (with tolerant matching on names and birth date formats) and
        creating a new patient in SKEZIA if no match is found. Includes parallel execution with thread pooling and automatic
        handling of expired authentication tokens.

        Named distinctly from the top-level function "process_patient" (which sends the
        questionnaire responses).
        """
        try:
            patient_id_bamara = str(patient["ID BaMaRa"])

            if cancel_event is not None and cancel_event.is_set():
                with print_lock:
                    print(f"🛑 Annulé par l'utilisateur : patient BaMaRa ID {patient_id_bamara} ignoré "
                          f"(non commencé).")
                return None

            if patient_id_bamara in existing_bamara_ids:
                with print_lock:
                    print(f"✅ Patient déjà présent dans FREDD (même ID BaMaRa) : {patient_id_bamara}")
                return None

            patient_given = normalize_name(patient["given"])
            patient_birth_date = normalize_birth_date(patient["birthDate"])
            family_nai = normalize_name(patient["family_nai"])
            family_us = normalize_name(patient["family_us"])

            if not patient_given:  # we don't handle patients with no first name
                with print_lock:
                    print(f"⚠️ Patient ignoré (prénom manquant) : ID BaMaRa {patient_id_bamara}")
                return None

            key1 = (patient_given, family_nai)  # this allows to handle patient where the birth family name is indicated but not the used family name, and vice-versa
            key2 = (patient_given, family_us)

            possible_dates = generate_possible_dates(patient["birthDate"])  # also try the day/month-swapped reading


            if not possible_dates:
                with print_lock:
                    print(f"⚠️ Attention : le patient BaMaRa ID {patient_id_bamara} n'a pas de date de naissance "
                          f"valide — la vérification anti-doublon n'a pas pu être appliquée pour ce patient.")

            # 2e niveau : identité (prénom + nom de naissance ou d'usage + date de naissance),
            # en tolérant l'inversion jour/mois. Le message distingue une correspondance
            # exacte d'une correspondance sur la date inversée.
            match_found = None  # None, "exacte" ou "inversee"

            for key in [key1, key2]:
                existing_dates = existing_index.get(key, set())
                if patient_birth_date and patient_birth_date in existing_dates:
                    match_found = "exacte"
                    break
                if any(d in existing_dates for d in possible_dates):
                    match_found = "inversee"
                    break

            if match_found:
                with print_lock:
                    if match_found == "exacte":
                        print(f"✅ Patient déjà présent dans FREDD (même prénom, nom et date de naissance, "
                              f"ID BaMaRa différent ou absent) : {patient_id_bamara}")
                    else:
                        print(f"✅ Patient déjà présent dans FREDD (même prénom et nom, date de naissance "
                              f"avec jour et mois inversés) : {patient_id_bamara}")
                return None


            gender_code = patient.get("gender")
            gender_str = "" if pd.isna(gender_code) else str(gender_code).strip()
            if gender_str == "1":
                gender_fhir = "male"
            elif gender_str == "2":
                gender_fhir = "female"
            else:
                with print_lock:
                    print(f"⚠️ Patient BaMaRa ID {patient_id_bamara} ignoré : sexe non reconnu "
                          f"(valeur '{gender_str}'). SKEZIA exige un sexe renseigné (masculin/"
                          f"féminin) — corrigez la donnée dans BaMaRa puis relancez l'import.")
                return None

            # Patient creation
            data = {
                "resourceType": "Patient",
                "identifier": [{"system": "BaMaRa", "value": patient_id_bamara}],
                "name": [{"family": patient["family_us"] or patient["family_nai"], "given": [patient["given"]]}],
                "gender": gender_fhir,
                "birthDate": patient_birth_date
            }


            response_post = _post_with_retries(
                url, data, token_manager, f"patient BaMaRa ID {patient_id_bamara}", cancel_event
            )

            if response_post is not None and response_post.status_code == 201:
                patient_id_skezi = response_post.json()["identifier"][0]["value"]
                with print_lock:

                    print(f"✅ Profil créé pour le patient BaMaRa ID {patient_id_bamara} -> SKEZIA ID {patient_id_skezi}")
                return {
                    "family_us": patient["family_us"],
                    "family_nai": patient["family_nai"],
                    "given": patient["given"],
                    "id_barama": patient_id_bamara,
                    "id": patient_id_skezi
                }
            else:
                with print_lock:
                    statut = response_post.status_code if response_post is not None else "aucune réponse"
                    detail = response_post.text if response_post is not None else ""
                    print(f"❌ Erreur lors de la création du patient BaMaRa ID {patient_id_bamara} : "
                          f"{statut} - {detail}")
                return None

        except Exception as e:
            with print_lock:

                id_for_log = patient.get("ID BaMaRa", "unknown")
                print(f"❌ Exception lors du traitement du patient BaMaRa ID {id_for_log} : {e}")
                traceback.print_exc()
            return None

    total = len(patients_info)
    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_patient_creation, patient) for _, patient in patients_info.iterrows()]
        for future in as_completed(futures):
            result = future.result()
            if result:
                created_patients.append(result)
            completed += 1
            with print_lock:

                print(f"PROGRESS:patients_crees:{completed}/{total}")

    with print_lock:
        if cancel_event is not None and cancel_event.is_set():
            print(f"🛑 Création des patients arrêtée par annulation utilisateur : {len(created_patients)}/{total} "
                  f"profil(s) créé(s) avant l'annulation.")
        else:
            print("📌 Création des patients terminée")

    return created_patients


def process_patient(patient, responses_data, question_types, token_manager, questionnaire_id, url, cancel_event=None):
    """
    Processes responses for a given patient and sends them to the SKEZIA API.

    Steps performed:
    1) Retrieves patient responses from the dataset.
    2) Formats responses according to their expected type (boolean, numeric, date, text, etc.).
    3) Builds the structured QuestionnaireResponse payload in memory.
    4) Sends the data to the SKEZIA API through _post_with_retries (retries network errors,
       timeouts, 401 and 5xx with exponential back-off -- ).
    5) Logs all errors and statuses in a thread-safe manner using a lock.

    Parameters:
    - patient: dictionary containing patient identifiers (BaMaRa and SKEZIA).
    - responses_data: pandas DataFrame containing all responses to process.
    - question_types: dictionary mapping each linkId to its expected type (e.g. "boolean", "number").
    - token_manager: handles retrieval and refresh of the authentication token.
    - questionnaire_id: SKEZIA questionnaire identifier.
    - url: full SKEZIA endpoint URL for sending QuestionnaireResponse.
    - cancel_event: optional threading.Event for cooperative cancellation (see gui.py's
      "Annuler" button and create_patient_profiles' matching parameter for the full
      rationale). Checked once, before this patient's HTTP work starts.

    Returns:
    A dictionary containing patient and response identifiers if successful, otherwise None.
    """
    id_barama = patient["id_barama"]
    id_skezi = patient["id"]
    sent_response = None

    if cancel_event is not None and cancel_event.is_set():
        with print_lock:
            print(f"🛑 Annulé par l'utilisateur : patient BaMaRa ID {id_barama} ignoré (non commencé).")
        return None

    if id_skezi is None:
        with print_lock:
            print(f"Le patient BaMaRa ID {id_barama} n'a pas d'ID SKEZIA. Ignoré.")
        return None

    id_barama_str = str(id_barama).strip()
    patient_responses = responses_data[
        responses_data['adm_identifiant_bamara'].astype(str).str.strip() == id_barama_str
    ]
    if patient_responses.empty:
        with print_lock:
            print(f"Aucune donnée trouvée pour le patient BaMaRa ID {id_barama}. Ignoré.")
        return None

    items = []
    for linkId, response in patient_responses.iloc[0].items():
        if linkId == 'ID BaMaRa':
            continue

        if pd.isna(response) or (isinstance(response, str) and response.strip() == ""):
            continue

        try:
            question_type = question_types.get(linkId, "string")  # retrive the type of the field to normalize it

            if question_type == "boolean":
                answer = {"valueBoolean": str(response).strip().lower() == "true"}

            elif question_type == "number":
                if pd.notna(response) and str(response).strip() not in ["", "nan", "NaN"]:
                    try:
                        response_clean = str(response).strip()
                        response_float = float(response_clean)
                        if response_float.is_integer():
                            response_clean = str(int(response_float))
                        if '.' in response_clean:
                            answer = {"valueDecimal": float(response_clean)}
                        else:
                            answer = {"valueInteger": int(response_clean)}
                    except (ValueError, TypeError):
                        with print_lock:
                            print(f"Valeur numérique ignorée : {response}")
                        continue
                else:
                    continue

            elif question_type == "date":

                if response == MARQUEUR_DATE_INCONNUE:
                    continue  # explicit "unknown date" marker: omit the field, send nothing
                try:
                    valeur_date = normaliser_date_questionnaire(response)
                except Exception:
                    valeur_date = None
                    with print_lock:
                        print(f"⚠️ Date non reconnue pour linkId={linkId} : champ ignoré.")
                if valeur_date is None:
                    continue
                answer = {"valueDate": valeur_date}

            elif "diaGen_var_parents" in linkId and "Boo" not in linkId:
                valeurs_separees = [val.strip() for val in str(response).split(";") if val.strip()]
                answers = [{"valueString": value} for value in valeurs_separees]
                items.append({
                    "linkId": linkId,
                    "answer": answers
                })
                continue

            else:
                if not (isinstance(response, str) and response.strip() == ""):
                    try:
                        response_float = float(response)
                        if response_float.is_integer():
                            answer = {"valueString": str(int(response_float))}
                        else:
                            answer = {"valueString": str(response_float)}
                    except (ValueError, TypeError):
                        answer = {"valueString": str(response).strip()}

            items.append({
                "linkId": linkId,
                "answer": [answer]
            })

        except Exception as e:
            with print_lock:

                print(f"Erreur de traitement pour linkId={linkId} : {e}")
            continue

    if not items:
        with print_lock:
            print(f"Aucune donnée valide pour le patient BaMaRa ID {id_barama}. Ignoré.")
        return None

    data = {
        "resourceType": "QuestionnaireResponse",
        "questionnaire": f"{SKEZIA_BASE_URL}/skezia/Questionnaire/{questionnaire_id}",
        "subject": {"reference": f"Patient/{id_skezi}"},
        "item": items
    }

    response = _post_with_retries(url, data, token_manager, f"patient {id_barama}", cancel_event)
    if response is not None and response.status_code == 201:
        try:
            response_id = response.json().get("id")
        except ValueError:
            response_id = None
        with print_lock:
            print(f"Réponses envoyées avec succès pour le patient {id_barama}")
        sent_response = {
            "id_barama": id_barama,
            "id_skezi": id_skezi,
            "response_id": response_id,
        }
    elif response is not None:
        with print_lock:
            print(f"Erreur {response.status_code} pour le patient {id_barama} : {response.text}")
    # response is None -> every attempt failed transiently; _post_with_retries already logged it.

    time.sleep(1)  # 1 second break after each patient (SKEZIA throttling)
    return sent_response


def send_questionnaire_responses(patients_profiles, responses_data, question_types, token_manager, questionnaire_id,
                                  cancel_event=None):
    """
    Parallelizes the sending of responses for all patients to the SKEZIA API.
    1) Creates a thread pool (ThreadPoolExecutor) to process multiple patients in parallel.
    2) Executes the process_patient function for each patient.
    3) Collects results as threads complete.
    4) Displays a confirmation message once processing is finished.
    Parameters:
    - patients_profiles: list of dictionaries, each containing patient identifiers.
    - responses_data: pandas DataFrame containing all responses imported from the source file.
    - question_types: dictionary mapping linkIds to SKEZIA expected types.
    - token_manager: object providing the current authentication token for the API.
    - questionnaire_id: identifier of the questionnaire used on the SKEZIA platform.
    - cancel_event: optional threading.Event for cooperative cancellation, forwarded to each
      process_patient() call (see gui.py's "Annuler" button and create_patient_profiles'
      matching parameter for the full rationale).
    Returns:
    List of successfully sent responses, including BaMaRa ID, SKEZIA ID, and response ID.
    """
    url = f"{SKEZIA_BASE_URL}/skezia/QuestionnaireResponse"
    sent_responses = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(
                process_patient,
                patient,
                responses_data,
                question_types,
                token_manager,
                questionnaire_id,
                url,
                cancel_event
            )
            for patient in patients_profiles
        ]

        total = len(patients_profiles)
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            if result:
                sent_responses.append(result)
            completed += 1
            with print_lock:

                print(f"PROGRESS:questionnaires_envoyes:{completed}/{total}")

    with print_lock:
        if cancel_event is not None and cancel_event.is_set():
            print(f"🛑 Envoi des questionnaires arrêté par annulation utilisateur : {len(sent_responses)}/{total} "
                  f"réponse(s) envoyée(s) avant l'annulation.")
        else:
            print("Envoi des réponses terminé.")

    return sent_responses


MARQUEUR_DATE_INCONNUE = "UNK/UNK/UNK"  # marqueur BaMaRa « date inconnue » : le champ est omis


def normaliser_date_questionnaire(valeur):
    """
    Date d'une réponse de questionnaire au format AAAA-MM-JJ (jour en premier pour une date
    française JJ/MM/AAAA), ou None si la réponse est vide, est le marqueur « date inconnue »
    ou n'est pas lisible. UNE SEULE règle, partagée par l'envoi en ligne
    (send_questionnaire_responses) et par l'export du mode hors-ligne (normaliser_dates_export).
    Lève ValueError si la valeur est non vide et illisible (l'appelant décide d'avertir).
    """
    if valeur is None or (not isinstance(valeur, str) and pd.isna(valeur)):
        return None
    if isinstance(valeur, str) and (not valeur.strip() or valeur.strip() == MARQUEUR_DATE_INCONNUE):
        return None
    return pd.to_datetime(valeur, dayfirst=True, errors="raise").strftime("%Y-%m-%d")


def normaliser_dates_export(df, question_types):
    """
    Copie de `df` où chaque colonne de type « date » du questionnaire (question_types, voir
    def_questions_type) est écrite en texte AAAA-MM-JJ, comme le fait l'envoi en ligne. Sans cela,
    l'export hors-ligne contient des vraies dates Excel (ex. leg_date_incFREDD) que l'import de la
    plateforme refuse (« Erreur format »). Une date illisible est laissée VIDE, jamais devinée.
    Renvoie (df_normalise, {colonne: nombre de dates illisibles}) -- aucune valeur dans le dict.
    """
    df = df.copy()
    illisibles = {}
    for colonne in df.columns:
        if question_types.get(colonne) != "date":
            continue
        valeurs, nb = [], 0
        for v in df[colonne]:
            try:
                valeurs.append(normaliser_date_questionnaire(v))
            except Exception:
                valeurs.append(None)
                nb += 1
        df[colonne] = pd.Series(valeurs, index=df.index, dtype=object)
        if nb:
            illisibles[colonne] = nb
    return df, illisibles


def def_questions_type(survey):
    """
    Function that defines the FREDD field format in SKEZIA based on the Survey.csv file. This file must be downloaded
    from SKEZIA (by exporting the questionnaire data) in order to obtain the exact format of all fields.
    """

    question_types = {}

    for _, row in survey.iterrows():
        variable = row["Variable / Field name"]

        field_type_raw = row["Field type"]
        field_type = "" if pd.isna(field_type_raw) else str(field_type_raw)
        input_type = row["Field input type"]

        if input_type == "number":
            question_types[variable] = "number"
        elif input_type == "date":
            question_types[variable] = "date"
        elif "boolean" in field_type:
            question_types[variable] = "boolean"
        elif field_type in ["radiogroup", "dropdown", "checkbox", "tagbox"]:
            question_types[variable] = "string"  # by default for multiple choices
        else:
            question_types[variable] = "string"  # by default
    return question_types


def create_patient_info(df):
    """
    Function that creates the information required to build a patient profile in SKEZIA.

    A patient is excluded entirely if ANY of the 6 required columns is missing (dropna()
    below) -- SKEZIA needs all of them to create a profile. The number of excluded patients
    and their BaMaRa IDs are logged, so a data manager reviewing the report can tell an
    expected exclusion from a data quality problem worth investigating.
    """
    required_columns = ["adm_identifiant_bamara", "adm_nom_naissance", "adm_nom_usage",
                         "adm_prenom", "adm_sexe", "adm_date_naissance"]
    subset = df[required_columns].copy()

    incomplete_mask = subset.isna().any(axis=1)
    if incomplete_mask.any():
        excluded_ids = subset.loc[incomplete_mask, "adm_identifiant_bamara"].fillna("ID inconnu").astype(str).tolist()
        print(f"⚠️ {incomplete_mask.sum()} patient(s) exclu(s) de la création de profil SKEZIA "
              f"(nom/sexe/date de naissance manquant) : ID(s) BaMaRa {', '.join(excluded_ids)}")

    patients_info = subset.dropna()
    patients_info.columns = ["ID BaMaRa", "family_nai", "family_us", "given", "gender", "birthDate"]

    return patients_info
