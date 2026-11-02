# -*- mode: python ; coding: utf-8 -*-
import sys
import glob
from PyInstaller.utils.hooks import collect_submodules, collect_data_files, collect_all
import csv
import os

crypto_datas, crypto_binaries, crypto_hiddenimports = collect_all('cryptography')

block_cipher = None

# --- Centre de ce build : UN SEUL choix, tout le reste est déduit de fichier_config.csv ---
# Usage :  set FREDDEX_CENTRE=NOM_DU_CENTRE  puis  python -m PyInstaller FREDDEX-base.spec
NOM_CENTRE = os.environ.get("FREDDEX_CENTRE", "").strip()
with open('files/fichier_config.csv', encoding='utf-8-sig') as f:
    centres = {l["Nom_centre"].strip('"'): l for l in csv.DictReader(f, delimiter=';')}
if NOM_CENTRE not in centres:
    raise SystemExit(f"FREDDEX_CENTRE='{NOM_CENTRE}' absent de files/fichier_config.csv. "
                     f"Centres possibles : {', '.join(centres)}")
NUM_CENTRE = str(centres[NOM_CENTRE]["Numero_centre"]).strip('"')
fichier_enc = f'files/cles/secrets_{NUM_CENTRE}.enc'
if not os.path.exists(fichier_enc):
    raise SystemExit(f"Identifiants chiffrés introuvables pour {NOM_CENTRE} : {fichier_enc}")
# Nom du centre embarqué dans l'exécutable (lu par config.lire_nom_centre). Écrit dans build/
# et non dans files/, pour ne pas imposer le dernier centre construit en mode développement.
os.makedirs('build', exist_ok=True)
with open('build/centre_build.txt', 'w', encoding='utf-8') as f:
    f.write(NOM_CENTRE)
print(f"=== Build FREDDEX pour {NOM_CENTRE} (centre n°{NUM_CENTRE}) ===")

a = Analysis(
    ['FREDDEX-base.py'],
    pathex=[],
    binaries=crypto_binaries,  # <-- Ajout des binaires de cryptography (contient le module _rust)
    datas=[
        ('files/Survey.csv', 'files'),
        ('files/icone.png', 'files'),
        (fichier_enc, 'files/cles'),
        ('build/centre_build.txt', 'files'),
        ('files/fichier_config.csv', 'files'),
        ('files/map_BaMaRa_FREDD.xlsx', 'files'),
        *[(p, 'files/codes_MR') for p in glob.glob('files/codes_MR/*')],
        *[(p, 'files/orphanet') for p in glob.glob('files/orphanet/*.xml')],
        *collect_data_files('openpyxl'),
        *crypto_datas,          # <-- Ajout des fichiers de données de cryptography
    ],

    hiddenimports=collect_submodules('pandas') + collect_submodules('numpy') + collect_submodules('truststore') + crypto_hiddenimports + [ # <-- Ajout des hiddenimports
        'openpyxl.cell._writer',
        'paths', 'logging_utils', 'config', 'mapping', 'extraction', 'transformation', 'skezia_api', 'gui',
        'key_setup', 'orphanet', 'investigations', 'consentements', 'participants',
        'liaison_ids', 'import_plateforme',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)

splash = Splash(
    'files/icone.png',  # Chemin vers ton image
    binaries=a.binaries,
    datas=a.datas,
    text_pos=(10, 50),  # Position du texte (facultatif)
    text_size=12,
    text_color='black'
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    splash,
    splash.binaries,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='FREDDEX-'+NOM_CENTRE,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,          # désactivé : UPX peut corrompre _rust*.pyd
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
	icon='files/icone.ico'
)