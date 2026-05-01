# -*- mode: python ; coding: utf-8 -*-
import sys
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

block_cipher = None
NOM_CENTRE="Center"

a = Analysis(
    ['FREDDEX-base.py'],
    pathex=[],
    datas=[
        ('files/Survey.csv', 'files'),
		('files/icone.png', 'files'),
		('files/cles/secrets_1.enc', 'files/cles'),
		('files/cles/secret_1.key', 'files/cles'),
		('files/fichier_config.csv', 'files'),
		('files/map_BaMaRa_FREDD.xlsx', 'files'),
		('files/codes_MR/codes_MR.txt', 'files/codes_MR'),
		*collect_data_files('openpyxl'),
    ],
    hiddenimports=collect_submodules('pandas') + collect_submodules('numpy') +['openpyxl.cell._writer'],
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
    debug=True,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False, 
	icon='files/icone.ico'
)
