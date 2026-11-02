# -*- coding: utf-8 -*-
# FREDDEX - BaMaRa/FREDD mapping file loading and column renaming
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License

import pandas as pd

# Simple in-memory cache for the BaMaRa/FREDD mapping Excel file. The same mapping_file path
# is read from several processing steps (traiter_donnees, diag_block_treatment,
# handle_FREDD_columns, appliquer_remplacements, rename_columns); caching it avoids
# re-reading the same file from disk up to 5 times per run.
_mapping_file_cache = {}


def load_mapping_file(mapping_file):
    """
    Reads the BaMaRa/FREDD mapping Excel file and caches the result in memory, so that
    repeated calls with the same path do not re-read the file from disk.
    """
    key = str(mapping_file)
    if key not in _mapping_file_cache:
        _mapping_file_cache[key] = pd.read_excel(mapping_file)
    return _mapping_file_cache[key]


def rename_columns(df, mapping_file):
    """
    Renames the columns of a DataFrame according to a mapping file between BaMaRa and FREDD.
    Reads an Excel file containing two columns: "Nom BaMaRa" and "Nom FREDD", then renames
    the columns of df, but only for columns with a BaMaRa name.

    - df: Input DataFrame containing data with BaMaRa column names.
    - mapping_file: Path to the Excel mapping file BaMaRa <-> FREDD.
    """
    df_mapping = load_mapping_file(mapping_file)

    # Filtering rows that contain both a BaMaRa name and a FREDD name
    df_mapping_filtered = df_mapping[["Nom BaMaRa", "Nom FREDD"]].dropna()

    # Building the renaming dictionary
    renaming_dict = dict(zip(df_mapping_filtered["Nom BaMaRa"], df_mapping_filtered["Nom FREDD"]))

    # Create a renaming dictionary only for columns that are not already FREDD named
    columns_to_rename = {ba: fredd for ba, fredd in renaming_dict.items()
                         if ba in df.columns and fredd not in df.columns}

    # Apply renaming only to the required columns
    df.rename(columns=columns_to_rename, inplace=True)

    return df
