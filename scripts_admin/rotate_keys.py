# -*- coding: utf-8 -*-
# FREDDEX - Fernet key / SKEZIA credentials rotation utility
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
#
# What this script does, for one center at a time:
# 1) Asks for the NEW appId/secretId (already regenerated on the SKEZIA side -- this script
#    only encrypts what you give it, it cannot create or revoke SKEZIA credentials itself).
# 2) Backs up any existing secret_<n>.key / secrets_<n>.enc for that center (never silently
#    overwritten) into a timestamped backup folder.
# 3) Generates a brand-new Fernet key and writes a brand-new encrypted secrets_<n>.enc from
#    the credentials just entered.
# 4) Prints the new key once, clearly labelled, with next-step reminders -- it is never
#    written to any log file (see logging_utils.py; this script does not import it and does
#    not call setup_logging(), so nothing it prints is redirected to disk).

import argparse
import getpass
import json
import os
import shutil
from datetime import datetime

from cryptography.fernet import Fernet

# Chemin ancré sur la racine FREDDEX (parent de scripts_admin/), peu importe le dossier
# depuis lequel ce script est lancé.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLES_DIR = os.path.join(ROOT, "files", "cles")


def _read_credentials(numero_centre):
    """
    Returns (app_id, secret_id) for the new credentials to encrypt.

    Prefers the FREDDEX_NEW_APPID_<n> / FREDDEX_NEW_SECRET_<n> environment variables (for
    scripted/non-interactive use, e.g. a controlled rotation run from a secrets manager);
    falls back to an interactive, hidden prompt (getpass) otherwise, so the values never
    appear in shell history or on-screen when typed by hand.
    """
    env_app_id = os.environ.get(f"FREDDEX_NEW_APPID_{numero_centre}")
    env_secret = os.environ.get(f"FREDDEX_NEW_SECRET_{numero_centre}")
    if env_app_id and env_secret:
        print(f"Nouveaux identifiants lus depuis les variables d'environnement pour le centre {numero_centre}.")
        return env_app_id, env_secret

    print(f"\nSaisie des nouveaux identifiants SKEZIA pour le centre {numero_centre}.")
    print("⚠️ Ces identifiants doivent déjà avoir été régénérés côté SKEZIA (voir la procédure "
          "dans le README) -- ce script se contente de les chiffrer, il ne les crée pas.")
    app_id = getpass.getpass("Nouvel appId (saisie masquée) : ").strip()
    secret_id = getpass.getpass("Nouveau secretId (saisie masquée) : ").strip()
    if not app_id or not secret_id:
        raise ValueError("appId et secretId ne peuvent pas être vides.")
    return app_id, secret_id


def _sauvegarder_anciens_fichiers(numero_centre, dossier):
    """
    Moves any existing secret_<n>.key / secrets_<n>.enc for this center into a timestamped
    backup subfolder, instead of overwriting them in place -- a rotation that silently
    destroys the previous key/secret pair with no way back is worse than one that is simply
    inconvenient to redo. Returns the backup folder path, or None if there was nothing to
    back up (first-time key generation for this center).
    """
    key_path = os.path.join(dossier, f"secret_{numero_centre}.key")
    enc_path = os.path.join(dossier, f"secrets_{numero_centre}.enc")

    if not os.path.exists(key_path) and not os.path.exists(enc_path):
        return None

    backup_dir = os.path.join(dossier, f"backup_{numero_centre}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(backup_dir, exist_ok=True)
    for path in (key_path, enc_path):
        if os.path.exists(path):
            shutil.move(path, os.path.join(backup_dir, os.path.basename(path)))
    return backup_dir


def rotate(numero_centre, dossier=CLES_DIR):
    """
    Performs the rotation for one center. Kept free of stdout/getpass I/O beyond the
    credential prompt so it can be unit-tested directly.

    Returns a 4-tuple (new_key, backup_dir, key_path, enc_path):
    - new_key (bytes): the freshly generated Fernet key -- the CALLER (main()) is responsible
      for displaying it.
    - backup_dir (str | None): timestamped backup folder for the previous key/secret pair, or
      None if there was nothing to back up (first-time generation for this center).
    - key_path (str), enc_path (str): where the new key and encrypted secrets were written.
    """
    app_id, secret_id = _read_credentials(numero_centre)

    os.makedirs(dossier, exist_ok=True)
    backup_dir = _sauvegarder_anciens_fichiers(numero_centre, dossier)

    new_key = Fernet.generate_key()
    fernet = Fernet(new_key)
    encrypted = fernet.encrypt(json.dumps({"appId": app_id, "secretId": secret_id}).encode())

    key_path = os.path.join(dossier, f"secret_{numero_centre}.key")
    enc_path = os.path.join(dossier, f"secrets_{numero_centre}.enc")
    with open(key_path, "wb") as f:
        f.write(new_key)
    with open(enc_path, "wb") as f:
        f.write(encrypted)

    return new_key, backup_dir, key_path, enc_path


def main():
    parser = argparse.ArgumentParser(
        description="Régénère la clé Fernet et les identifiants SKEZIA chiffrés d'un centre FREDDEX. "
                     "Les nouveaux appId/secretId doivent déjà avoir été régénérés côté SKEZIA."
    )
    parser.add_argument("--centre", "-n", required=True,
                         help="Numéro du centre à faire tourner (ex. 1), tel qu'utilisé dans "
                              "secret_<n>.key / secrets_<n>.enc / FREDDEX_FERNET_KEY_<n>.")
    args = parser.parse_args()
    numero_centre = args.centre

    try:
        new_key, backup_dir, key_path, enc_path = rotate(numero_centre)
    except ValueError as e:
        print(f"❌ {e}")
        raise SystemExit(1)

    print(f"\n✅ Rotation terminée pour le centre {numero_centre}.")
    if backup_dir:
        print(f"   Ancienne clé/secret déplacés (non écrasés) dans : {backup_dir}")
    print(f"   Nouveau fichier chiffré : {enc_path}")
    print(f"   Nouvelle clé locale (repli test uniquement) : {key_path}")

    print("\n" + "=" * 70)
    print(f"NOUVELLE CLÉ FERNET (centre {numero_centre}) -- à définir en variable d'environnement")
    print(f"FREDDEX_FERNET_KEY_{numero_centre}={new_key.decode()}")
    print("=" * 70)
    print("⚠️ Cette clé n'est affichée qu'une seule fois ici (jamais écrite dans un fichier de "
          "log FREDDEX). Copiez-la immédiatement dans le gestionnaire de secrets / la variable "
          "d'environnement de production, puis fermez ce terminal.")

    print("\nProchaines étapes (voir la procédure complète dans le README) :")
    print(f" 1. Définir FREDDEX_FERNET_KEY_{numero_centre} sur chaque poste/serveur qui exécute "
          f"FREDDEX pour ce centre, avec la valeur ci-dessus.")
    print(" 2. Relancer un import de test (mode hors-ligne ou avec un petit fichier) pour "
          "confirmer que les nouveaux identifiants fonctionnent.")
    if backup_dir:
        print(f" 3. Une fois la révocation des anciens identifiants confirmée côté SKEZIA, "
              f"supprimer définitivement le dossier de sauvegarde : {backup_dir}")
    print(" 4. Ne jamais committer ce dossier de sauvegarde ni le fichier secret_<n>.key dans git "
          "(voir .gitignore) ni les embarquer dans un exécutable distribué (voir FREDDEX-base.spec).")


if __name__ == "__main__":
    main()
