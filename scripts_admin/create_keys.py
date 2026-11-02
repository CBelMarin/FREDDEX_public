# -*- coding: utf-8 -*-
# FREDDEX - initial Fernet key / encrypted-credentials generator (one center)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Legacy helper for the FIRST-TIME creation of a center's secret_<n>.key + secrets_<n>.enc
# pair. For ROUTINE rotation of an existing center (regenerate + back up the old pair), use
# rotate_keys.py instead -- it is the maintained path and includes the full safety procedure.

import argparse
import getpass
import json
import os

from cryptography.fernet import Fernet

# Chemin ancré sur la racine FREDDEX (parent de scripts_admin/), peu importe le dossier
# depuis lequel ce script est lancé.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLES_DIR = os.path.join(ROOT, "files", "cles")


def generer_cle_centre(numero_centre, app_id, secret_id, dossier=CLES_DIR):
    """
    Creates a fresh Fernet key and the matching encrypted secrets file for one center:
    secret_<numero_centre>.key and secrets_<numero_centre>.enc, written under `dossier`.

    Returns the generated Fernet key (bytes). Does NOT back up any pre-existing files -- this
    is the first-time-creation helper; for an existing center use rotate_keys.py, which backs
    the old pair up first. Kept as a plain function (no argparse/getpass) so it can be
    unit-tested directly.
    """
    os.makedirs(dossier, exist_ok=True)

    key = Fernet.generate_key()
    key_path = os.path.join(dossier, f"secret_{numero_centre}.key")
    with open(key_path, "wb") as key_file:
        key_file.write(key)

    fernet = Fernet(key)
    encrypted = fernet.encrypt(json.dumps({"appId": app_id, "secretId": secret_id}).encode())
    enc_path = os.path.join(dossier, f"secrets_{numero_centre}.enc")
    with open(enc_path, "wb") as f:
        f.write(encrypted)

    return key


def main():
    parser = argparse.ArgumentParser(
        description="Génère la clé Fernet et le fichier d'identifiants chiffrés d'un centre FREDDEX "
                    "(première création). Pour un centre déjà configuré, utilisez plutôt rotate_keys.py."
    )
    parser.add_argument("--centre", "-n", required=True,
                        help="Numéro du centre (ex. 6 pour le centre de TEST), utilisé pour nommer "
                             "secret_<n>.key / secrets_<n>.enc / FREDDEX_FERNET_KEY_<n>.")
    args = parser.parse_args()

    app_id = getpass.getpass("appId (saisie masquée) : ").strip()
    secret_id = getpass.getpass("secretId (saisie masquée) : ").strip()
    if not app_id or not secret_id:
        print("❌ appId et secretId ne peuvent pas être vides.")
        raise SystemExit(1)

    key = generer_cle_centre(args.centre, app_id, secret_id)
    print(f"✅ Clé et identifiants chiffrés créés pour le centre {args.centre} "
          f"(dossier {CLES_DIR}).")
    print("=" * 70)
    print(f"FREDDEX_FERNET_KEY_{args.centre}={key.decode()}")
    print("=" * 70)
    print("⚠️ Définissez cette clé en variable d'environnement en production ; ne committez "
          "jamais secret_<n>.key ni ne l'embarquez dans l'exécutable distribué.")


if __name__ == "__main__":
    main()
