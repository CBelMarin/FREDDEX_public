# -*- coding: utf-8 -*-
# FREDDEX
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License

import sys
import os

# --- GESTION DU SPLASH SCREEN PYINSTALLER ---
try:
    import pyi_splash
    pyi_splash.close()
except:
    pass

import pandas as pd 
import numpy as np
import json
import traceback
from datetime import datetime, date
import re
import requests
import tkinter as tk
import unicodedata
import random
import string
import threading
import time
from tkinter import filedialog, messagebox, ttk
from cryptography.fernet import Fernet
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path


NOM_CENTRE="Center"
DATE_VERSION="27-04-2026"

print_lock = Lock()

# Lod folder creation for error and report files
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"rapport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
error_file = os.path.join(log_dir, "error_log.txt")

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

    def get_token(self):
        """
        Retourne a valid token. Generates a new one if needed. 
        """
        current_time = time.time()
        if self.token is None or current_time >= self.expiry_time:
            print("New token generated")
            self.token, self.expiry_time = self._generate_token()
        return self.token

    def _generate_token(self):
        """
        Sends a request to the API to obtain a new token. Warning : SKEZIA specific
        """
        response = requests.post("https://api.skezi.eu/auth/token", data={
            "appId": self.client_id,
            "appSecret": self.client_secret
        })
        
        if response.status_code in (200, 201): # 200 = OK, 201 = Created → success of authentification
            token = response.json()["access_token"]
            expires_in = response.json().get("expires_in", 600)
            expiry_time = time.time() + expires_in - 30  # for security
            return token, expiry_time
        else:
            raise Exception(f"Error of authentification : {response.status_code} - {response.text}")

def setup_logging():
    """
    Open the log files
    """
    sys.stdout = open(log_file, "w", encoding="utf-8")
    sys.stderr = open(error_file, "w", encoding="utf-8")
    
def traiter_donnees(liste_MR, mapping_file, df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_rec):
    """
    Returns the processed dataframe (df_filtred), handled sheet by sheet, where each sheet corresponds to those extracted from the BaMaRa Excel file.

    Arguments:
    - liste MR : ORPHA codes to be retained
    - mapping_file : Excel mapping file between BaMaRa and FREDD
    - df_admin : "Données administratives" sheet from the input file
    - df_prises : "Prises en charge" sheet from the input file
    - df_diagnostic : "Diagnostic" sheet from the input file
    - df_genes : "Gènes" sheet from the input file
    - df_var  "Variations" sheet from the input file
    - df_neo : "Anté-néonatal" sheet from the input file
    - df_rec : "Recherche" sheet from the input file

    The BaMaRa Excel file input file must therefore strictly contain these sheets, named exactly as specified.
    """
                             
    # Load the mapping file
    df_mapping = pd.read_excel(mapping_file)  
    
    # Filtering mappings for the 'Données administratives' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Données administratives']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    if not all(col in df_admin.columns for col in colonnes_mapping):
        missing_cols = [col for col in colonnes_mapping if col not in df_admin.columns]
        raise ValueError(f"Les colonnes suivantes sont manquantes dans la feuille 'Données administratives' : {missing_cols}")        
    df_final = df_admin[colonnes_mapping] 

    # Filtering mappings for the 'Prises en charge' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Prises en charge']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]  # mandatory
    # Fusion 
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_prises.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_prises.columns]
        raise ValueError(f"The following columns are missing in the 'Prises en charge' sheet : {missing_cols}")        
    df_prises_filtre = df_prises[colonnes_a_extraire]
    # Fusion using the ID BaMaRa 
    df_final = df_final.merge(
        df_prises_filtre,
        on=['ID BaMaRa'],# key for joint
        how="left"
    )

    # Filtering mappings for the 'Diagnostic' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Diagnostics']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa", "Identifiant du bloc diagnostic"]
    # Fusion 
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_diagnostic.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_diagnostic.columns]
        raise ValueError(f"The following columns are missing in the 'Diagnostic' sheet : {missing_cols}")
    df_diag_filtre = df_diagnostic[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_diag_filtre,
        on=['ID BaMaRa'],
        how="left"
    )
    df_final.rename(columns={"Identifiant du bloc diagnostic": "ID diagnostic", "Statut": "Statut_y"}, inplace=True)
    
    # Filtering mappings for the 'Gènes' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Gènes']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID diagnostic", "ID gène"]
    # Fusion 
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_genes.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_genes.columns]
        raise ValueError(f"The following columns are missing in the 'Gènes' sheet : {missing_cols}")
    df_genes_filtre = df_genes[colonnes_a_extraire]
    # Fusion using the ID BaMaRa
    df_final = df_final.merge(
        df_genes_filtre,
        on=['ID diagnostic'],  
        how="left"
    )
    
    # Filtering mappings for the 'Variations' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Variations']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID gène"]
    # Fusion 
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_var.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_var.columns]
        raise ValueError(f"The following columns are missing for the 'Variations' sheet : {missing_cols}")
    df_var_filtre = df_var[colonnes_a_extraire]
    # Fusion using the ID BaMaRa 
    df_final = df_final.merge(
        df_var_filtre,
        on=['ID gène'],  
        how="left"
    )

    # Filtering mappings for the 'Anté-neonatal' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Anté-néonatal']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]
    # Fusion
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_neo.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_neo.columns]
        raise ValueError(f"The following columns are missing from the 'Anté-néonatal' sheet : {missing_cols}")
    df_neo_filtre = df_neo[colonnes_a_extraire]
    # Fusion using the ID BaMaRa 
    df_final = df_final.merge(
        df_neo_filtre,
        on=['ID BaMaRa'],
        how="left"
    )
    
    # Filtering mappings for the 'Recherche' sheet
    mapping_diagnostic = df_mapping[df_mapping['Onglet BaMaRa'] == 'Recherche']
    colonnes_mapping = mapping_diagnostic['Nom BaMaRa'].dropna().unique().tolist()
    colonnes_additionnelles = ["ID BaMaRa"]
    # Fusion 
    colonnes_a_extraire = list(dict.fromkeys(colonnes_mapping + colonnes_additionnelles))
    if not all(col in df_rec.columns for col in colonnes_a_extraire):
        missing_cols = [col for col in colonnes_a_extraire if col not in df_rec.columns]
        raise ValueError(f"The following colums are missing in the 'Recherche' sheet : {missing_cols}")
    df_rec_filtre = df_rec[colonnes_a_extraire]
    # Fusion using the ID BaMaRa 
    df_final = df_final.merge(
        df_rec_filtre,
        on=['ID BaMaRa'],
        how="left"
    )
    
    # Conversion of ORPHA in numbers
    df_final['Code orphanet'] = pd.to_numeric(
        df_final['Code orphanet'],
        errors='coerce'
    )

    # Counting total lines before filtering
    total_lignes = len(df_final)
    print(f"Total read lines : {total_lignes}")

    # Cleaning of description clinique column
    desc = (
        df_final['Description clinique']
        .fillna('')
        .astype(str)
        .str.strip()
    )

    # Defintion of filters on BaMaRa "Status" column
    cond_orpha_valide = (
        df_final['Statut_y'].isin(["probable", "confirmé"]) &
        df_final['Code orphanet'].isin(liste_MR)
    )
    # add "Indeterminé" status if HPO codes presents
    cond_indet_desc = (
        (df_final['Statut_y'] == 'indéterminé') &
        (desc != '')
    )

    # Final filtering
    df_filtered = df_final[cond_orpha_valide | cond_indet_desc]

    # Counting filtered lines
    gardees_code_orpha = cond_orpha_valide.sum()
    gardees_indet_desc = cond_indet_desc.sum()
    lignes_gardees = (cond_orpha_valide | cond_indet_desc).sum()
    lignes_filtrees = total_lignes - lignes_gardees

    # Display filetring results
    print(f"Kept lines (BaMaRa status probable/confirmé + valid ORPHA) : {gardees_code_orpha}")
    print(f"Kept lines (BaMaRa status indéterminé + clinical description) : {gardees_indet_desc}")
    print(f"Total kept lines : {lignes_gardees}")
    print(f"Filtered lines : {lignes_filtrees}")
    print("✅ Process 1 finished.")

    return df_filtered

def diag_block_treatment(df, mapping_file):
    """
    This function processes a DataFrame so that there is only one row per BaMaRa ID. For each patient (BaMaRa ID), 
    it reconciles each diagnosis with its associated variables and with each associated gene, which in turn is linked 
    to multiple variations.

    Rare disease 1  — Gene 1    — Variation 1
                                — Variation 2
                    — Gene 2    — Variation 3
                                — Variation 4
    Rare disease 2  — Gene 3    — Variation 5
                                — Variation 6
                    — Gene 4    — Variation 7
                                — Variation 8

    The code identifies each patient by their BaMaRa ID, then builds a hierarchical structure for each patient where:
    - a patient can have multiple rare diseases,
    - each disease can be linked to multiple genes,
    - each gene can have multiple variations.

    The code consolidates redundant rows by updating existing information using the most recent data entered in BaMaRa 
    (latest diagnostic block).

    Arguments:
    - df (pd.DataFrame): Input DataFrame
    - mapping_file: BaMaRa / FREDD mapping file

    Returns the processed DataFrame (df).
    """
    mapping_df = pd.read_excel(mapping_file)

    # Filter rows where all key columns are present in the mapping file
    mapping_standard_to_bamara = (
        mapping_df[
            mapping_df["Nom standardisé"].notna()
            & mapping_df["Nom BaMaRa"].notna()
            & mapping_df["Nom FREDD"].notna()
        ]
        .groupby("Nom standardisé")[["Nom BaMaRa", "Nom FREDD"]] # we use the column "Nom standardisé" of the mapping file 
        .apply(lambda df: df.to_dict(orient="records"))
        .to_dict()
    )
        
    processed_data = {}  # Dictionary to store consolidated rows
    var_att = {}
    maladie_att = {}
    patient_multiple_bloc = 0
    patient_multiple_diag = 0
    for _, row in df.iterrows():
        valeurs = row.to_dict()
        id_bamara = row[mapping_standard_to_bamara["id_bamara"][0].get("Nom BaMaRa")]
        maladie = row[mapping_standard_to_bamara["maladie"][0].get("Nom BaMaRa")]
        code = row[mapping_standard_to_bamara["code"][0].get("Nom BaMaRa")]
        statut = row["Statut_y"] # This column was renamed earlier because it has the same name as "Status" in the "Variation" sheet.
        signesAss = row[mapping_standard_to_bamara["signesAss"][0].get("Nom BaMaRa")]
        #signesInh = row[mapping_standard_to_bamara["signesInh"][0].get("Nom BaMaRa")]  # removed from the lastest Export version
        invest = row[mapping_standard_to_bamara["invest"][0].get("Nom BaMaRa")]
        diaCara = row[mapping_standard_to_bamara["diaCara"][0].get("Nom BaMaRa")]
        PS = row[mapping_standard_to_bamara["PS"][0].get("Nom BaMaRa")]
        agePS = row[mapping_standard_to_bamara["agePS"][0].get("Nom BaMaRa")]
        diaCli = row[mapping_standard_to_bamara["diaCli"][0].get("Nom BaMaRa")]
        ageDC = row[mapping_standard_to_bamara["ageDC"][0].get("Nom BaMaRa")]
        diaGen = row[mapping_standard_to_bamara["diaGen"][0].get("Nom BaMaRa")]
        ageDG = row[mapping_standard_to_bamara["ageDG"][0].get("Nom BaMaRa")]
        spor = row[mapping_standard_to_bamara["spor"][0].get("Nom BaMaRa")]
        gene_nom = row[mapping_standard_to_bamara["gene_nom"][0].get("Nom BaMaRa")]
        variation_nom = row[mapping_standard_to_bamara["var_nom"][0].get("Nom BaMaRa")]
        var_refseq = row[mapping_standard_to_bamara["var_refseq"][0].get("Nom BaMaRa")]
        var_classe = row[mapping_standard_to_bamara["var_classe"][0].get("Nom BaMaRa")]
        var_stat = row[mapping_standard_to_bamara["var_stat"][0].get("Nom BaMaRa")]
        var_caus = row[mapping_standard_to_bamara["var_caus"][0].get("Nom BaMaRa")]
        var_trans = row[mapping_standard_to_bamara["var_trans"][0].get("Nom BaMaRa")]
        var_par = row[mapping_standard_to_bamara["var_par"][0].get("Nom BaMaRa")]

        cles = [
            "maladie", "code", "statut", "signesAss", 
            #"signesInh", 
            "invest", "diaCara",
            "PS", "agePS", "diaCli", "ageDC", "diaGen", "ageDG", "spor", "gene_nom", "var_nom",
            "var_refseq", "var_classe", "var_stat", "var_caus", "var_trans", "var_par"
        ]
        noms_bamara = [mapping_standard_to_bamara[cle][0].get("Nom BaMaRa") for cle in cles]
        noms_bamara.extend(["Statut_y", "ID diagnostic", "ID gène", "genes"])  # We add these manually created variables
        noms_bamara.append(mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+"Add")  # idem
        
        if id_bamara not in processed_data:  # The BaMaRa ID has never been encountered: this is a new patient.
            # Initialisation of the whole structure
            processed_data[id_bamara] = row.to_dict()
            nom_fredd = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD") 
            processed_data[id_bamara][nom_fredd] = maladie # Diagnostis assigned
            processed_data[id_bamara][nom_fredd+'Add'] = [] # Preparing the case where a second disease is present
            processed_data[id_bamara]['genes'] = {maladie: {gene_nom: {variation_nom}}} # Define the hierarchy of variables (1 diagnosis = X genes = Y variations)
            maladie_att[id_bamara, maladie] = [code, statut, signesAss, 
                                               #signesInh, 
                                               invest, diaCara, # Definition of diagnosis attributes
                                               PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
            var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,var_refseq,  # Definition of variantion attributes
                                                                    var_trans,var_par]
        else:  # The ID BaMaRa has already been seen
            current_data = processed_data[id_bamara]
            if pd.notna(maladie) and maladie not in current_data['genes']:   # It is a new disease for this patient
                nom_fredd = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")
                current_data[nom_fredd+'Add'].append(maladie)
                current_data['genes'][maladie] = {gene_nom: {variation_nom}}
                var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,var_refseq,var_trans,var_par]
                maladie_att[id_bamara, maladie] = [code, statut, signesAss, 
                                                   #signesInh, 
                                                   invest, diaCara,PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                print(f"The patient {id_bamara} has several rare eye diseases")
                patient_multiple_diag += 1
            else:
                if pd.notna(gene_nom) and gene_nom not in current_data['genes'][maladie]: # It is a new gene related to this disease
                    current_data['genes'][maladie][gene_nom] = {variation_nom}
                    var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,
                                                                            var_refseq,var_trans,var_par]
                    maladie_att[id_bamara, maladie] = [code, statut, signesAss, 
                                                       #signesInh, 
                                                       invest, diaCara,
                                               PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                else: # It is the same gene for the same disease
                    if pd.notna(variation_nom) and variation_nom not in current_data['genes'][maladie][gene_nom]: # It is a new variation
                        current_data['genes'][maladie][gene_nom].add(variation_nom)
                        var_att[id_bamara, maladie, gene_nom, variation_nom] = [var_classe,var_stat,var_caus,
                                                                                var_refseq,var_trans,var_par]
                        maladie_att[id_bamara, maladie] = [code, statut, signesAss, 
                                                           #signesInh, 
                                                           invest, diaCara,
                                                   PS, agePS, diaCli, ageDC, diaGen, ageDG, spor]
                    else: #It is the same disease, the same gene, and the same variation, so the variables for this diagnostic block are updated with the most recent values
                        print(f"Warning the patient {id_bamara} has multiple diagnosis blocks for the disease : {maladie}")
                        patient_multiple_bloc += 1
                        for column in row.index:  
                            if (pd.isna(current_data.get(column)) and pd.notna(row[column])) or (current_data.get(column) != row[column] and pd.notna(row[column])):  # Compare current values
                                print(f"Patient {id_bamara} : mise à jour de la colonne {column} pour le patient colonne : {current_data.get(column)} -> {row[column]}")
                                current_data[column] = row[column]
                                # Update of other values related to maladie_att
                                maladie_att[id_bamara, maladie] = [
                                    current_data.get(mapping_standard_to_bamara["code"][0].get("Nom BaMaRa")),
                                    current_data.get("Statut_y"),
                                    current_data.get(mapping_standard_to_bamara["signesAss"][0].get("Nom BaMaRa")),
                                    #current_data.get(mapping_standard_to_bamara["signesInh"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["invest"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaCara"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["PS"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["agePS"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaCli"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["ageDC"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["diaGen"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["ageDG"][0].get("Nom BaMaRa")),
                                    current_data.get(mapping_standard_to_bamara["spor"][0].get("Nom BaMaRa"))
                                ]

    # Converting the dictionary into a DataFrame, expanding each variable for each diagnostic block
    output_data = []
    for id_bamara, data in processed_data.items():
        row_dict = {key: value for key, value in data.items() if key not in noms_bamara}   # to avoid filling the manually processed columns twice
        for i, maladie in enumerate(data[mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+'Add'], start=1):
            row_dict[mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+'Add'] = maladie
               
        maladie_index = 1  # used to keep track of the disease concerned (maximum of 2 rare diseases per patient)
        for maladie_index, (maladie, genes) in enumerate(sorted(data['genes'].items(), key=lambda x: str(x[0])), start=1):
            gene_index = 1  # Used to keep track of the gene concerned (any number of genes per disease)
            for gene, variations in sorted(genes.items(), key=lambda x: str(x[0])):
                if maladie_index == 1:
                    malID = ""
                    value = maladie_att.get((id_bamara, maladie), None)
                    if value is not None: 
                        row_dict[mapping_standard_to_bamara["code"][0].get("Nom FREDD")] = value[0]
                        row_dict[mapping_standard_to_bamara["statut"][0].get("Nom FREDD")] = value[1]
                        row_dict[mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")] = value[2]
                        #row_dict[mapping_standard_to_bamara["signesInh"][0].get("Nom FREDD")] = value[3]
                        row_dict[mapping_standard_to_bamara["invest"][0].get("Nom FREDD")] = value[3]
                        row_dict[mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD")] = value[4]
                        row_dict[mapping_standard_to_bamara["PS"][0].get("Nom FREDD")] = value[5]
                        row_dict[mapping_standard_to_bamara["agePS"][0].get("Nom FREDD")] = value[6]
                        row_dict[mapping_standard_to_bamara["diaCli"][0].get("Nom FREDD")] = value[7]
                        row_dict[mapping_standard_to_bamara["ageDC"][0].get("Nom FREDD")] = value[8]
                        row_dict[mapping_standard_to_bamara["diaGen"][0].get("Nom FREDD")] = value[9]
                        row_dict[mapping_standard_to_bamara["ageDG"][0].get("Nom FREDD")] = value[10]
                        row_dict[mapping_standard_to_bamara["spor"][0].get("Nom FREDD")] = value[11] 
                else:
                    malID = "Add"
                    value = maladie_att.get((id_bamara, maladie), None)
                    if value is not None:              
                        row_dict[mapping_standard_to_bamara["code"][0].get("Nom FREDD")+"Add"] = value[0]
                        row_dict[mapping_standard_to_bamara["statut"][0].get("Nom FREDD")+"Add"] = value[1]
                        row_dict[mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")+"Add"] = value[2]
                        #row_dict[mapping_standard_to_bamara["signesInh"][0].get("Nom FREDD")+"Add"] = value[3]
                        row_dict[mapping_standard_to_bamara["invest"][0].get("Nom FREDD")+"Add"] = value[3]
                        row_dict[mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD")+"Add"] = value[4]
                        row_dict[mapping_standard_to_bamara["PS"][0].get("Nom FREDD")+"Add"] = value[5]
                        row_dict[mapping_standard_to_bamara["agePS"][0].get("Nom FREDD")+"Add"] = value[6]
                        row_dict[mapping_standard_to_bamara["diaCli"][0].get("Nom FREDD")+"Add"] = value[7]
                        row_dict[mapping_standard_to_bamara["ageDC"][0].get("Nom FREDD")+"Add"] = value[8]
                        row_dict[mapping_standard_to_bamara["diaGen"][0].get("Nom FREDD")+"Add"] = value[9]
                        row_dict[mapping_standard_to_bamara["ageDG"][0].get("Nom FREDD")+"Add"] = value[10]
                        row_dict[mapping_standard_to_bamara["spor"][0].get("Nom FREDD")+"Add"] = value[11]  
                # We now populate the genetic variables associated with each disease
                row_dict[mapping_standard_to_bamara["gene_nom"][0].get("Nom FREDD")+"_"+ f"{gene_index}{malID}"] = gene
                for i, variation in enumerate(sorted(variations, key=str), start=1):
                    row_dict[mapping_standard_to_bamara["var_nom"][0].get("Nom FREDD")+f"_{gene_index}_{i}{malID}"] = variation
                    value = var_att.get((id_bamara, maladie, gene, variation), None)
                    if value is not None:
                        row_dict[mapping_standard_to_bamara["var_classe"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"] = str(value[0])
                        row_dict[mapping_standard_to_bamara["var_stat"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[1]
                        row_dict[mapping_standard_to_bamara["var_caus"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[2]
                        row_dict[mapping_standard_to_bamara["var_refseq"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[3]
                        row_dict[mapping_standard_to_bamara["var_trans"][0].get("Nom FREDD") + f"_{gene_index}{malID}"]  = value[4]
                        row_dict[mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{gene_index}_{i}{malID}"]  = value[5]
                gene_index += 1
        
        output_data.append(row_dict)
        
    df = pd.DataFrame(output_data)
    print(f"There are {patient_multiple_bloc} diagnostis blocks of the same disease for the same patient") 
    print(f"There are {patient_multiple_diag} patients with 2 rare eye diseases")
    print(f"✅ Process 2 finished.") 
    return df

def rename_columns(df, mapping_file):
    """
    Renames the columns of a DataFrame according to a mapping file between BaMaRa and FREDD.
    This function reads an Excel file containing two columns: "BaMaRa Name" and "FREDD Name", and then renames 
    the columns of the DataFrame df, but only for columns with a BaMaRa name.

    - df: Input DataFrame containing data with BaMaRa column names.
    - mapping_file: Path to the Excel mapping file BaMaRa ↔ FREDD.
    """

    df_mapping = pd.read_excel(mapping_file)

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

def handle_FREDD_columns(df_output, mapping_file, centre, numero_centre):
    """
    Function to process and transform a DataFrame specific to FREDD (hard-coded FREDD column names).
    This function performs several operations to adapt the DataFrame to FREDD dataset requirements. It applies specific 
    transformations to certain columns, fills missing columns, and performs calculations on the data to make it compatible with 
    the SKEZIA FREDD e-CRF.

    Arguments:
    - df_output: DataFrame containing the data to be processed.
    - mapping_file: Path to an Excel file containing mappings between BaMaRa and FREDD column names.
    - centre: Name of the treatment center.
    - numero_centre: Identifier of the FREDD data collection center.
    """
    def deploy_column_signs(df, column_name, max_elements, prefix=""):
        """
        Generic function to expand values contained in a column into multiple columns based on a maximum number of elements.
        Arguments:
        - df: DataFrame
        - column_name: Name of the column to process
        - max_elements: Maximum number of allowed elements (10 for clinical signs, 5 for unusual signs, etc.)
        - prefix: Prefix used to name the new columns
        """        
        if column_name+prefix in df.columns and df[column_name+prefix].notna().any():
            max_signes = df[column_name+prefix].dropna().str.split(',').map(len).max()
            if max_signes > max_elements:
                max_signes = max_elements
            for i in range(max_signes):
                if 'Add' in prefix:
                    col_name = column_name+str(i+1)+'Add' 
                else:
                    col_name = column_name+str(i+1)  
                df[col_name] = df[column_name+prefix].fillna('').str.split(',').str[i]  
            # Supress the original column
            df = df.drop(columns=[column_name+prefix])
        return df
        
    def compute_chained_counts(df, base_name, target_base, suffixes, chain_len=3, group_indices=None):
        """
        Calculates the number of non-empty chained columns and fills a target column. Used to compute the number of genes 
        and variants based on columns containing filled-in names.
        - df: DataFrame
        - base_name: column prefix (e.g. 'diaGen_var_hgcn' or 'diaGen_var_nom')
        - target_base: base name of the target column (e.g. 'diaGen_var_nbgene')
        - suffixes: list of suffixes to process (e.g. ['', 'Add'])
        - chain_len: chain length (default 3)
        - group_indices: list of indices for grouped variants (e.g. [1, 2, 3]) or None
        """
        for suffix in suffixes:
            if group_indices is None:
                # Cases diaGen_var_hgcn_1, _2, _3
                target_col = f"{target_base}{suffix}"
                #df[target_col] = 0

                col_names = [f"{base_name}_{i}{suffix}" for i in range(1, chain_len + 1)]
                if col_names[0] in df.columns:
                    mask = df[col_names[0]] != ""
                    df.loc[mask, target_col] = 1
                    for i in range(1, chain_len):
                        if col_names[i] in df.columns:
                            mask = mask & (df[col_names[i]] != "")
                            df.loc[mask, target_col] = i + 1
            else:
                # case for Cas diaGen_var_nom_1_1, _1_2, etc.
                for idx in group_indices:
                    target_col = f"{target_base}_{idx}{suffix}"
                    #df[target_col] = 0

                    col_names = [f"{base_name}_{idx}_{i}{suffix}" for i in range(1, chain_len + 1)]
                    if col_names[0] in df.columns:
                        mask = df[col_names[0]] != ""
                        df.loc[mask, target_col] = 1
                        for i in range(1, chain_len):
                            if col_names[i] in df.columns:
                                mask = mask & (df[col_names[i]] != "")
                                df.loc[mask, target_col] = i + 1
        return df

    df_mapping = pd.read_excel(mapping_file)
    
    # Building the renaming dictionary using the standardised names
    mapping_standard_to_bamara = (
        df_mapping[
            df_mapping["Nom standardisé"].notna()
            & df_mapping["Nom BaMaRa"].notna()
            & df_mapping["Nom FREDD"].notna()
        ]
        .groupby("Nom standardisé")[["Nom BaMaRa", "Nom FREDD"]]
        .apply(lambda df: df.to_dict(orient="records"))
        .to_dict()
    )
    # Notice FREDD OK, REDgistry KO for the moment
    df_output['leg_notice_info'] = 'true'
    df_output['leg_patient_REDgistryCons'] = '0'
    df_output['leg_date_incFREDD'] = date.today()

    df_output['leg_site_inc_nom'] = centre
    df_output['leg_site_inc'] = numero_centre

    df_output['adm_patient_MR'] = 'true'  #because of deletion of "Non malade" column in BaMaRa -> diseased by default

    # Filling of boolean variables related to clinical signs: set to "Yes" if at least one clinical sign boolean is "Yes"
    col_signesAss_base = mapping_standard_to_bamara["signesAss"][0].get("Nom FREDD")
    for suffix in ["", "Add"]:
        col_signesAss = col_signesAss_base + suffix 
        col_signesAss_boo = col_signesAss_base + "Boo" + suffix
        if col_signesAss in df_output.columns:
            non_vide = (df_output[col_signesAss].notna()) & (df_output[col_signesAss] != "")
            df_output[col_signesAss_boo] = non_vide.map({True: "true", False: "false"})

    # Expansion of columns containing multiple fields
    columns_to_deploy = [
        ("signesAss", 10), # max 10 clinical signs
        #("signesInh", 5),  #max 5 
        ("pro_name", 4)    # max 4 clincial trials
    ]
    for key, max_count in columns_to_deploy:
        col_base = mapping_standard_to_bamara[key][0].get("Nom FREDD")
        df_output = deploy_column_signs(df_output, col_base, max_count)
        if key != "pro_name":  # no "Add" for pro_name
            df_output = deploy_column_signs(df_output, col_base, max_count)    
            df_output = deploy_column_signs(df_output, col_base, max_count, "Add")    
        df_output = df_output.fillna('')
        
    # If the place of birth is provided, it is retained and the country of birth is set to France
    col_pays = mapping_standard_to_bamara["pays"][0].get("Nom FREDD")
    mask = df_output[col_pays].str.contains(r"\(", na=False)
    df_output.loc[mask, "adm_ville_naissance"] = df_output.loc[mask, col_pays].str.replace("|", " ", regex=False)
    df_output.loc[mask, col_pays] = "France"
    
    # Genetic characterization → analysis status set to "Completed"
    for suffix in ["", "Add"]:
        col_cara = mapping_standard_to_bamara["diaCara"][0].get("Nom FREDD") + suffix
        col_statut = "diaGen_statut_analyse" + suffix
        if col_cara in df_output.columns:
            df_output[col_statut] = df_output[col_cara].eq("Oui").map({True: "2", False: None})
                    
    # Expansion of the list of investigations
    mapping = {
        "clinique": "diaCli_invesCli",
        "biochimique": "diaCli_invesBio",
        "biologique": "diaCli_invesBiol",
        "imagerie": "diaCli_invesIma",
        "exploration fonctionnelle": "diaCli_invesExp",
        "anatomopathologie": "diaCli_invesAna",
        "test génétique": "diaCli_invesGen"
    }
    
    # Add "Boo" (for boolean) columns with default value "true".
    for suffix in ["", "Add"]:
        nom_fredd = mapping_standard_to_bamara["invest"][0].get("Nom FREDD")
        df_output[nom_fredd + "Boo" + suffix] = "true"
        col_check = nom_fredd + suffix
        if col_check in df_output.columns:
            # Select the column to be tested for each suffix
            valid_rows = df_output[col_check].notna() & df_output[col_check].str.contains("|".join(mapping.keys()), na=False)
            # Update the values of "Boo" based on the investigations found
            df_output.loc[valid_rows, nom_fredd + "Boo" + suffix] = "false"
            # Populate the corresponding investigation columns
            for investigation, target_col in mapping.items():
                valid_investigation_rows = valid_rows & df_output[col_check].str.contains(investigation, na=False)
                df_output.loc[valid_investigation_rows, target_col + suffix] = "1"
            df_output = df_output.drop(columns=[col_check])  # suppression of original column

    # Calculation of the number of genes from diaGen_var_hgcn
    df_output = compute_chained_counts(
        df_output,
        base_name=mapping_standard_to_bamara["gene_nom"][0].get("Nom FREDD"),
        target_base="diaGen_var_nbgene",
        suffixes=["", "Add"]    
    )
    # Calculation of the number of variants from diaGen_var_nom
    df_output = compute_chained_counts(
        df_output,
        base_name=mapping_standard_to_bamara["var_nom"][0].get("Nom FREDD"),
        target_base="diaGen_var_nbvar",
        suffixes=["", "Add"],
        group_indices=[1, 2, 3]
    )

    # Check whether the patient has two rare diseases
    nomFREDD = mapping_standard_to_bamara["maladie"][0].get("Nom FREDD")+"Add"
    if nomFREDD in df_output.columns:
        df_output["diaCli_nb_MR"] = df_output[nomFREDD].ne("").map({True: "true", False: "false"})
    else:
        df_output["diaCli_nb_MR"] = "false"
    
    # Variant origin = "de novo" → mode of inheritance = "de novo". Same for "unknown" → "undetermined"
    for suffix in ["", "Add"]:
        for i in range(1, 4):
            col_trans = mapping_standard_to_bamara["var_trans"][0].get("Nom FREDD") + f"_{i}{suffix}"
            if col_trans in df_output.columns:
                mask_denovo = pd.Series(False, index=df_output.index)
                mask_inconnue = pd.Series(False, index=df_output.index)

                for j in range(1, 4):
                    col_parent = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{i}_{j}{suffix}"
                    if col_parent in df_output.columns:
                        mask_denovo |= df_output[col_parent] == "de novo"
                        mask_inconnue |= df_output[col_parent] == "inconnue"

                df_output.loc[mask_denovo, col_trans] = "de novo"
                df_output.loc[mask_inconnue, col_trans] = "non déterminé"

                # Clear the source columns containing "de novo" or "unknown"
                for j in range(1, 4):
                    col_parent = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + f"_{i}_{j}{suffix}"
                    if col_parent in df_output.columns:
                        df_output.loc[mask_denovo & (df_output[col_parent] == "de novo"), col_parent] = ""
                        df_output.loc[mask_inconnue & (df_output[col_parent] == "inconnue"), col_parent] = ""


    # If a parent is indicated as carrying the same genetic variant, "Parent carries the variant?" is set to "true"
    for suffix in ["", "Add"]:
        for i in range(1, 4):
            for j in range(1, 4):
                base_name = mapping_standard_to_bamara["var_par"][0].get("Nom FREDD") + "_" + str(i) + "_" + str(j)
                col_boo = f"{base_name}Boo{suffix}"
                col_source = base_name + suffix
                if col_source in df_output.columns:
                    df_output[col_boo] = df_output[col_source].apply(lambda x: "1" if pd.notna(x) and x != "" else "")
                    
    # Enforcement of integer type for columns that must be integers
    cols_to_convert = [
        "diaGen_var_nbgene", "diaGen_var_nbgeneAdd", "diaCli_diagMR_code",
        "his_age_psignPrec", "his_age_diagMRPrec", "diaGen_agePrec",
        "diaCli_diagMR_codeAdd", "his_age_psignPrecAdd", "his_age_diagMRPrecAdd",
        "diaGen_agePrecAdd", "hisPer_sa"
    ]

    # Addition of dynamic columns: diaGen_var_nbvar_{i}[Add], diaGen_var_classe_{i}_{j}[Add]
    for i in range(1, 4):
        cols_to_convert.append(f"diaGen_var_nbvar_{i}")
        cols_to_convert.append(f"diaGen_var_nbvar_{i}Add")

    for col in cols_to_convert:
        if col in df_output.columns:
            df_output[col] = pd.to_numeric(df_output[col], errors='coerce').astype('Int64')

            
    print(f"✅ Process 3 finished.") 
    return df_output

def appliquer_remplacements(df, mapping_file):
    """
    Function that applies, for each value, the replacement of BaMaRa values to make them match the expected values in the FREDD 
    questionnaire.

    Arguments:
    - input df to be processed
    - mapping_file: BaMaRa/FREDD mapping file
    """
    def charger_remplacements_depuis_excel(fichier_excel):
        """
        Loads an Excel file containing mappings between BaMaRa and FREDD values and transforms it into a dictionary to facilitate 
        subsequent replacements during data processing.

        Arguments:
        - fichier_excel: path to the Excel file containing the mappings to load.
        Returns remplacements (dict): a dictionary where keys are FREDD field names, and values are dictionaries mapping BaMaRa values to their corresponding FREDD values.
        """
        df = pd.read_excel(fichier_excel)
        df = df.dropna(subset=['Nom BaMaRa', 'Valeurs BaMaRa', 'Nom FREDD', 'Valeurs FREDD'])
        remplacements = {}

        for _, row in df.iterrows():
            nom_fredd = row['Nom FREDD']
            valeur_bamara = ' '.join(str(row['Valeurs BaMaRa']).split())
            valeur_fredd = ' '.join(str(row['Valeurs FREDD']).split())
            if nom_fredd not in remplacements:
                remplacements[nom_fredd] = {}
            remplacements[nom_fredd][valeur_bamara] = valeur_fredd
        return remplacements
        
    def nettoyer_chaine(s):
        """
        Cleans a string by:
        - Replacing multiple spaces with a single space.
        - Removing non-printable characters.
        - Stripping leading and trailing spaces.
        """
        if pd.isna(s):
            return s
        s = str(s)
        s = re.sub(r'\s+', ' ', s)  # replace all spaces by a single one
        s = ''.join(c for c in s if c.isprintable())  # Removes non-printable characters
        return s.strip()

    remplacements_valeurs = charger_remplacements_depuis_excel(mapping_file)
    for nom_fredd, mapping in remplacements_valeurs.items():
        # Search for columns whose names start with the FREDD name prefix
        colonnes_cibles = [col for col in df.columns if col == nom_fredd or col.startswith(nom_fredd)]
        for col in colonnes_cibles:
            df[col] = df[col].apply(nettoyer_chaine)
            df[col] = df[col].replace(mapping)
            
    df_clean = df.replace([np.inf, -np.inf], np.nan).infer_objects().where(pd.notnull(df), None)
    
    print(f"✅ Process 4 finished.")
    return df_clean   

def create_patient_profiles(patients_info, token_manager, max_workers=5):
    """
    Creates patient profiles in SKEZIA in parallel, with automatic handling of expired tokens and parallel retrieval of 
    existing patients.
    This function is SKEZIA specific.
    """
    url = "https://api.skezi.eu/skezia/Patient"
    created_patients = []
    print_lock = threading.Lock()

    def normalize_name(name):
        """
        Normalizes a name by removing accents and non-letter characters, then returns it in uppercase.
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

    def get_headers():
        """
        Returns the HTTP headers required for API requests, including the authorization token and content type.
        """
        return {"Authorization": f"Bearer {token_manager.get_token()}",
                "Content-Type": "application/json"}
    
    # Full retrieval of existing patients (robust pagination handling)
    existing_patients = []
    page = 1

    while True:
        response = requests.get(
            url,
            headers=get_headers(),
            params={"_page": page, "_count": 50}
        )

        if response.status_code == 401:
            response = requests.get(
                url,
                headers=get_headers(),
                params={"_page": page, "_count": 50}
            )

        if response.status_code != 200:
            print(f"❌ Error page {page} : {response.status_code}")
            break

        data = response.json()
        entries = data.get("entry", [])

        if not entries:
            break

        existing_patients.extend(entries)

        if len(entries) < 50:
            break

        page += 1

    print(f"Number of retrieved existing patients : {len(existing_patients)}")

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

    def process_patient(patient):
        """
        Processes a patient by checking for existing records (with tolerant matching on names and birth date formats) and 
        creating a new patient in SKEZIA if no match is found. Includes parallel execution with thread pooling and automatic 
        handling of expired authentication tokens.
        """
        def generate_possible_dates(date_str):
            """
            Generates possible date formats (YYYY-MM-DD and YYYY-DD-MM) from a given input date string. 
            This function exists to handle a legacy bug where day and month were inverted during patient creation, 
            ensuring these patients can still be retrieved via the API.
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
            
        try:
            patient_id_bamara = str(patient["ID BaMaRa"])
            patient_given = normalize_name(patient["given"])
            patient_birth_date = normalize_birth_date(patient["birthDate"])
            family_nai = normalize_name(patient["family_nai"])
            family_us = normalize_name(patient["family_us"])

            if not patient_given:  # we don't handle patients with no first name
                with print_lock:
                    print(f"⚠️ Patient ignored (no first name) : ID BaMaRa {patient_id_bamara}")
                return None

            key1 = (patient_given, family_nai) # this allows to handle patient where the birth family name is indicated but not the used family name, and vice-versa
            key2 = (patient_given, family_us)

            possible_dates = generate_possible_dates(patient["birthDate"])  # related to a previous bug where day and month of the birth dates where inverted

            match_found = False

            for key in [key1, key2]:
                if key in existing_index:
                    existing_dates = existing_index[key]

                    for d in possible_dates:
                        if d in existing_dates:
                            match_found = True
                            break

                if match_found:
                    break

            if match_found:
                print(f"✅ Patient already in the database (birthday tolerant match) : {patient_id_bamara}")
                return None

            # Patient creation
            headers = get_headers()
            data = {
                "resourceType": "Patient",
                "identifier": [{"system": "BaMaRa", "value": patient_id_bamara}],
                "name": [{"family": patient["family_us"] or patient["family_nai"], "given": [patient["given"]]}],
                "gender": "male" if patient.get("gender") == "1" else "female",
                "birthDate": patient_birth_date
            }

            response_post = requests.post(url, headers=headers, json=data)
            if response_post.status_code == 401:  # expired token 
                response_post = requests.post(url, headers=get_headers(), json=data)

            if response_post.status_code == 201:
                patient_id_skezi = response_post.json()["identifier"][0]["value"]
                with print_lock:
                    print(f"✅ Profil créé : {patient['given']} {patient['family_us']}{ patient['family_nai']} {patient["birthDate"]} -> {patient_id_skezi}")
                return {
                    "family_us": patient["family_us"],
                    "family_nai": patient["family_nai"],
                    "given": patient["given"],
                    "id_barama": patient_id_bamara,
                    "id": patient_id_skezi
                }
            else:
                with print_lock:
                    print(f"❌ Error in patient creation for {patient['given']} {patient['family_us'] or patient['family_nai']}: "
                          f"{response_post.status_code} - {response_post.text}")
                return None

        except Exception as e:
            with print_lock:
                print(f"❌ Exception patient {patient.get('given')} {patient.get('family_us') or patient.get('family_nai')}: {e}")
                traceback.print_exc()
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_patient, patient) for _, patient in patients_info.iterrows()]
        for future in as_completed(futures):
            result = future.result()
            if result:
                created_patients.append(result)

    with print_lock:
        print("📌 Patients creation finished")

    return created_patients

def process_patient(patient, responses_data, question_types, token_manager, questionnaire_id, url):
    """
    Processes responses for a given patient and sends them to the SKEZIA API.

    Steps performed:
    1) Retrieves patient responses from the dataset.
    2) Formats responses according to their expected type (boolean, numeric, date, text, etc.).
    3) Creates a local JSON file containing the structured responses.
    4) Attempts to send the data to the SKEZIA API, retrying up to 3 times in case of temporary errors (503 or timeouts).
    5) Logs all errors and statuses in a thread-safe manner using a lock.

    Parameters:
    - patient: dictionary containing patient identifiers (BaMaRa and SKEZIA).
    - responses_data: pandas DataFrame containing all responses to process.
    - question_types: dictionary mapping each linkId to its expected type (e.g. "boolean", "number").
    - token_manager: handles retrieval and refresh of the authentication token.
    - questionnaire_id: SKEZIA questionnaire identifier.
    - url: full SKEZIA endpoint URL for sending QuestionnaireResponse.

    Returns:
    A dictionary containing patient and response identifiers if successful, otherwise None.
    """
    id_barama = patient["id_barama"]
    id_skezi = patient["id"]
    sent_response = None

    if id_skezi is None:
        with print_lock:
            print(f"Patient with ID BaMaRa {id_barama} has no ID SKEZIA. Ignored.")
        return None

    patient_responses = responses_data[responses_data['adm_identifiant_bamara'] == id_barama]
    if patient_responses.empty:
        with print_lock:
            print(f"No data found for patient with ID BaMaRa {id_barama}. Ignored.")
        return None

    items = []
    for linkId, response in patient_responses.iloc[0].items():
        if linkId == 'ID BaMaRa':
            continue
        if response is None or (isinstance(response, str) and response.strip() == ""):
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
                            print(f"Numerical value ignored : {response}")
                        continue
                else:
                    continue

            elif question_type == "date":
                # Check whether the response is not the string "UNK/UNK/UNK" and is not NaN.
                if not pd.isna(response) and response != "UNK/UNK/UNK":
                    try:
                        # if valid, conversion in date format
                        answer = {
                            "valueDate": pd.to_datetime(
                                response,
                                dayfirst=True,
                                errors="raise"
                            ).strftime("%Y-%m-%d")
                        }
                    except Exception as e:
                        answer = {"valueDate": None}
                else:
                    # If the response is "UNK/UNK/UNK" or NaN, ignore it and do not assign a date
                    answer = {"valueDate": None}

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
                print(f"Porcessing error for linkId={linkId}, response={response}: {e}")
            continue

    if not items:
        with print_lock:
            print(f"No valid data for patient with ID BaMaRa {id_barama}. Ignored.")
        return None
    
    data = {
        "resourceType": "QuestionnaireResponse",
        "questionnaire": f"https://api.skezi.eu/skezia/Questionnaire/{questionnaire_id}",
        "subject": {"reference": f"Patient/{id_skezi}"},
        "item": items
    }

    headers = {
        "Authorization": f"Bearer {token_manager.get_token()}",
        "Content-Type": "application/json"
    }

    for attempt in range(1, 4):  # Attempt 3 times max to send responses
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 201:
                response_id = response.json().get("id")
                with print_lock:
                    print(f"Responses successfully sent for the patient {id_barama} (attempt {attempt})")
                sent_response = {
                    "id_barama": id_barama,
                    "id_skezi": id_skezi,
                    "response_id": response_id
                }
                break
            elif response.status_code == 503:
                with print_lock:
                    print(f"503 for patient {id_barama} (attempt {attempt}), new attempt in 2 seconds...")
                time.sleep(2)
            else:
                with print_lock:
                    print(f"Error {response.status_code} for patient {id_barama} : {response.text}")
                break
        except requests.exceptions.Timeout:
            with print_lock:
                print(f"⏱️ Timeout at attempt {attempt}. New attempt in 1 second.")
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            with print_lock:
                print(f"❌ Ntework error at attempt {attempt}: {e}")
            break
        except Exception as e:
            with print_lock:
                print(f"Unexpected error while sending data for the patient.{id_barama} with data {data}, (attempt {attempt}): {e}")
            continue

    time.sleep(1)  # 1 second break after each patient
    return sent_response

def send_questionnaire_responses(patients_profiles, responses_data, question_types, token_manager, questionnaire_id):
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
    Returns:
    List of successfully sent responses, including BaMaRa ID, SKEZIA ID, and response ID.
    """
    url = "https://api.skezi.eu/skezia/QuestionnaireResponse"
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
                url
            )
            for patient in patients_profiles
        ]

        for future in as_completed(futures):
            result = future.result()
            if result:
                sent_responses.append(result)

    with print_lock:
        print("Responses sent.")

    return sent_responses

def def_questions_type(survey):
    """
    Function that defines the FREDD field format in SKEZIA based on the Survey.csv file. This file must be downloaded 
    from SKEZIA (by exporting the questionnaire data) in order to obtain the exact format of all fields.    
    """
    
    question_types = {}

    for _, row in survey.iterrows():
        variable = row["Variable / Field name"]
        field_type = row["Field type"]
        input_type = row["Field input type"]

        if input_type == "number":
            question_types[variable] = "number"
        elif input_type == "date" : 
            question_types[variable] = "date"
        elif input_type == "number" :
            question_types[variable] = "number"
        elif "boolean" in field_type:
            question_types[variable] = "boolean"
        elif field_type in ["radiogroup", "dropdown","checkbox","tagbox"]:
            question_types[variable] = "string"  # by default for multiple choices
        else:
            question_types[variable] = "string"  # by default
    return question_types

def create_patient_info(df):
    """
    Function that creates the information required to build a patient profile in SKEZIA.
    """          

    patients_info = df[["adm_identifiant_bamara","adm_nom_naissance", "adm_nom_usage", "adm_prenom", "adm_sexe", "adm_date_naissance"]].copy().dropna() 
    patients_info.columns = ["ID BaMaRa","family_nai","family_us", "given", "gender", "birthDate"]
    patients_info["birthDate"] = patients_info["birthDate"]
    
    return patients_info

def resource_path(relative_path):
    """
    Returns the absolute path to a resource, handling both standard Python execution and PyInstaller bundled execution.
    """
    try:
        base_path = Path(sys._MEIPASS)
    except AttributeError:
        base_path = Path(__file__).resolve().parent
    return base_path / relative_path

def lancer_interface_et_traitement(fonction_de_traitement):
    """
    Launches a simple graphical interface to run a long background processing function.
    This function creates a window with:
    - an indeterminate progress bar
    - a waiting message
    - an image
    - an “OK” button displayed at the end of processing
    - a scrollable message area if needed
    Parameter:
    fonction_de_traitement: function to execute, expected to return a message to display upon completion.
    """
    # Select input file
    root_temp = tk.Tk()
    root_temp.withdraw()
    root_temp.attributes("-topmost", True)
    fichier_selectionne = filedialog.askopenfilename(title="FREDDEX - Sélectionner le fichier BaMara")
    root_temp.destroy()

    if not fichier_selectionne:
        return # Cancelled by user, stop everything

    # Waiting interface
    root = tk.Tk()
    root.title("FREDDEX - Traitement en cours")
    root.geometry("600x400")
    
    canvas = tk.Canvas(root, borderwidth=0)
    scrollbar = ttk.Scrollbar(root, orient="vertical", command=canvas.yview)
    frame_contenu = ttk.Frame(canvas)

    canvas_frame = canvas.create_window((300, 20), window=frame_contenu, anchor="n")
    
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side="left", fill="both", expand=True)

    # Waiting messages
    label_message = ttk.Label(frame_contenu, text="Initialisation de FREDDEX...", wraplength=500, justify="center")
    label_message.pack(pady=20)
    try:
        image_path = resource_path("files/icone.png") # logo of the project
        
        if os.path.exists(image_path):
            photo = tk.PhotoImage(file=image_path, master=frame_contenu)
            label_img = tk.Label(frame_contenu, image=photo)
            label_img.image = photo 
            label_img.pack(pady=10)
        else:
            print(f"Image not found at : {image_path}")
    except Exception as e:
        print(f"Error while loading the image : {e}")

    # Fake progression bar (for visual effect)
    progress_bar = ttk.Progressbar(frame_contenu, orient="horizontal", length=400, mode="indeterminate")
    progress_bar.pack(pady=10)

    # OK button (hidden at the beginning)
    bouton_ok = ttk.Button(frame_contenu, text="OK", command=root.destroy)

    def update_scroll(event):
        """
        Automatic adjustment of the scrollable area. Center the content when the window is wide.
        """
        canvas.configure(scrollregion=canvas.bbox("all"))
        canvas.itemconfig(canvas_frame, width=canvas.winfo_width())

    canvas.bind('<Configure>', update_scroll)

    def execution_calcul():
        """
        Runs the processing function while displaying a progress indicator, then safely updates the UI on the main 
        thread once execution is completed or if an error occurs.
        """
        progress_bar.start()
        root.after(0, lambda: label_message.config(text="Data transfert in progress..."))
        
        try:
            resultat = fonction_de_traitement(fichier_selectionne)
        except Exception as e:
            resultat = f"❌ Erreur critique :\n{str(e)}"
        finally:
            root.after(0, lambda: finaliser_UI(resultat))

    def finaliser_UI(message):
        """
        Finalizes the UI after processing completion: stops and hides the progress bar if it exists, updates the message label, 
        displays the scrollbar and OK button, and brings the window to the foreground.
        """
        try:
            if progress_bar.winfo_exists():
                progress_bar.stop()
        except:
            pass
        progress_bar.pack_forget()
        label_message.config(text=message)
        scrollbar.pack(side="right", fill="y")
        bouton_ok.pack(pady=20)
        root.attributes("-topmost", True) 

    root.after(500, lambda: threading.Thread(target=execution_calcul, daemon=True).start())
    root.mainloop()
   
def traitement_complet(fichier_entree, centre=NOM_CENTRE): 
    """
    Main function:
    0) Opens a window to select the BaMaRa input file to be processed
    1) Processes the BaMaRa Excel file sheet by sheet
    2) Processes all retrieved data using the traiter_donnees function
    3) Handles diagnostic blocks using the diag_block_treatment function
    4) Applies column transformation rules using the handle_columns function
    5) Inserts properly formatted questionnaire responses using the appliquer_remplacements function
    6) Connects to the SKEZIA API
    7) Creates patient profiles and sends them to the API
    8) Creates patient questionnaires and sends them to the API
    """  

    def initialiser_ressources(centre):
        """
        Initializes the resources required for the import process:
        - ORPHA rare disease codes
        - available collecting centers
        - BaMaRa–FREDD mapping
        - FREDD questionnaire structure
        Args:
        centre (str): Name of the center for which the application is launched. Each center has its own FREDDEX.
        """
        def charger_codes_mr(fichier_path):
            """
            Loads rare disease codes from a file. The function reads a text file, extracts comma-separated values, c
            onverts valid entries to integers, and filters out empty or invalid values.
            """
            with open(fichier_path, 'r', encoding='utf-8') as f:
                contenu = f.read()
            codes = [int(code.strip()) for code in contenu.split(',') if code.strip().isdigit()]
            return codes
        try:
            # Load centers
            centres_file = resource_path("files/fichier_config.csv")
            if not os.path.exists(centres_file):
                return "❌ File fichier_config.csv not found."
            centres_dict = lire_centres(centres_file)

            if centre not in centres_dict:
                return f"❌ Center {centre} not found."

            infos_centre = centres_dict[centre]  

            numero_centre = infos_centre["Numero_centre"]
            questionnaire_centre = infos_centre["questionnaire_id"]
            codesMR_file = resource_path(f"files/codes_MR/{infos_centre['Ficher_codes_MR']}") # load ORPHA codes for the center

            print(f"Application launched for centre : {centre}")            
            
            if not os.path.exists(codesMR_file):
                return "❌ ORPHA codes file not found."
            liste_codes_MR = charger_codes_mr(codesMR_file)

            map_file = resource_path("files/map_BaMaRa_FREDD.xlsx")
            if not os.path.exists(map_file):
                return "❌ Mapping file map_BaMaRa_FREDD.xlsx not found."

            csv_file = resource_path("files/Survey.csv")
            if not os.path.exists(csv_file):
                return "❌ Structure file Survey.csv not found."
            survey = pd.read_csv(csv_file, sep=";")

            return liste_codes_MR, numero_centre, questionnaire_centre, map_file, survey

        except Exception as e:
            return f"❌ Unexpected error : {str(e)}"

    def lire_centres(fichier_csv):
        """
        Reads a CSV file containing center information and returns a structured dictionary indexed by center name.
        """
        df = pd.read_csv(fichier_csv, sep=';')
        
        df = df.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)
        
        colonnes_attendues = {"Nom_centre","Numero_centre","Ficher_codes_MR","questionnaire_id"}
        if not colonnes_attendues.issubset(df.columns):
            raise ValueError(
                f"Warning : your center file must contain the following columns : {', '.join(colonnes_attendues)}."
            )

        dict_centres = (
            df
            .set_index("Nom_centre")[["Numero_centre", "Ficher_codes_MR", "questionnaire_id"]]
            .to_dict(orient="index")
        )
        return dict_centres
        
    def charger_token_manager(numero_centre):
        """
        Loads encrypted credentials from center-specific files, decrypts them, and returns an initialized TokenManager instance.
        The function retrieves the encryption key and encrypted secrets from secure files, decrypts the payload using Fernet, 
        parses the JSON content, and extracts the API credentials (client ID and client secret) required to instantiate the TokenManager.
        """
        # Path to secured key files
        key_file = f"files/cles/secret_{numero_centre}.key"
        print(key_file)
        key_path = resource_path(key_file)
        enc_file = f"files/cles/secrets_{numero_centre}.enc"
        enc_path = resource_path(enc_file)

        # Loading the encryption key
        with open(key_path, "rb") as key_file:
            key = key_file.read()

        # Loading and decrypting secrets
        with open(enc_path, "rb") as f:
            encrypted_data = f.read()

        fernet = Fernet(key)
        decrypted = fernet.decrypt(encrypted_data)
        secrets = json.loads(decrypted.decode())

        client_id = secrets["appId"]
        client_secret = secrets["secretId"]
        return TokenManager(client_id, client_secret)        

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
            print(f"FREDDEX version of {DATE_VERSION} for center {NOM_CENTRE}")
            print(f"Input file selected  : {fichier_entree}")
            print("Processing of Données administratives sheet...")
            df_admin = pd.read_excel(fichier_entree, sheet_name="Données administratives")
            print("Processing of Prises en charge sheet...")
            df_prises = pd.read_excel(fichier_entree, sheet_name="Prises en charge")
            print("Processing of Diagnostics sheet...")
            df_diagnostic = pd.read_excel(fichier_entree, sheet_name="Diagnostics")
            print("Processing of Gènes sheet...")
            df_genes = pd.read_excel(fichier_entree, sheet_name="Gènes")
            print("Processing of Variations sheet...")
            df_var = pd.read_excel(fichier_entree, sheet_name="Variations")
            print("Processing of Anté-natal sheet...")
            df_neo = pd.read_excel(fichier_entree, sheet_name="Anté-néonatal")
            print("Processing of Recherche sheet...")
            df_recherche = pd.read_excel(fichier_entree, sheet_name="Recherche")
            print("Processing ongoing...")

            df_output = traiter_donnees(liste_codes_MR, map_file, df_admin, df_prises, df_diagnostic, df_genes, df_var, df_neo, df_recherche)
            if df_output.empty:
                print("No patients were selected based on the FREDDEX criteria. Processing stopped.")
                return "ℹ️ No patients were selected based on the FREDDEX criteria. Processing stopped."

            #df_output.to_excel("resultat_1.xlsx", index=False)  # uncomment to see what your output file look like at this point
            df_output = diag_block_treatment(df_output, map_file)
            #df_output.to_excel("resultat_2.xlsx", index=False) # uncomment to see what your output file look like at this point
            df_output = rename_columns(df_output, map_file)
            df_output = handle_FREDD_columns(df_output, map_file, centre, numero_centre) # this function is usefull only for FREDD data
            df_final = appliquer_remplacements(df_output, map_file)
            #df_final.to_excel("resultat_f.xlsx", index=False) # uncomment to see what your output file look like at this point
            
            # If you don't use the SKEZIA API comment from here.... (*)
            question_types = def_questions_type(survey)
            patients_info = create_patient_info(df_final)
            
            print("Connection to SKEZIA APU...") 
            token_manager = charger_token_manager(numero_centre)

            print("Sending patient profiles...")
            created_profiles = create_patient_profiles(patients_info, token_manager)

            if created_profiles:
                print("Sending patient questionnaires...")
                sent_responses = send_questionnaire_responses(
                    created_profiles,
                    df_final,
                    question_types,
                    token_manager,
                    questionnaire_id=questionnaire_centre
                )
                print(f"{len(created_profiles)} profile(s) created.")
                print(f"{len(sent_responses)} questionnary sent.")

                resume = ""
                for patient in created_profiles:
                    id_barama = str(patient.get("id_barama", "N/A"))
                    nom1 = str(patient.get("family_nai", "N/A"))
                    nom2 = str(patient.get("family_us", "N/A"))                 
                    prenom = str(patient.get("given", "N/A"))
                    resume += f"ID BaMaRa: {id_barama}, NOM: {nom1} {nom2}, Prénom: {prenom}\n"
                end_time = time.time()
                duration = (end_time - start_time)/60

                return f"✅ {len(created_profiles)} profiles created and {len(sent_responses)} questionnairy sent in {duration:.2f} minutes for :\n{resume}"
            else:
                print("All patients are already present in FREDD.")
                return "ℹ️ All patients are already present in FREDD. No profiles were created."
            
            #... (*) to here (if you don't use SKEZIA API)

        except Exception as e:
            with open(error_file, "a", encoding="utf-8") as f:  
                f.write(traceback.format_exc())
            return "❌ An error occurred. Please check error_log.txt and your report file."

    else:
        print("No file selected, script terminated.")
        return "❌ No file selected, script terminated."

def afficher_bienvenue_et_lancer():
    """
    Displays a lightweight welcome window and launches the main application workflow.
    The function creates a minimal Tkinter splash screen with a loading message and an indeterminate progress bar, 
    then waits briefly before closing the window and starting the full processing pipeline, including file selection 
    and execution of the main treatment function.
    """
    welcome = tk.Tk()
    welcome.title("FREDDEX")
    welcome.geometry("300x150")
    welcome.eval('tk::PlaceWindow . center') 
    
    label = tk.Label(welcome, text="🚀 FREDDEX \nChargement en cours...", font=("Arial", 12))
    label.pack(expand=True)
    
    # progression bar
    pb = ttk.Progressbar(welcome, mode='indeterminate', length=200)
    pb.pack(pady=10)
    pb.start()
    
    def charger_la_suite():
        """
        Closes the welcome window and starts the main processing workflow by launching the file selection interface and executing the full treatment pipeline.
        """
        welcome.destroy()
        lancer_interface_et_traitement(lambda f: traitement_complet(f, NOM_CENTRE))

    # wait 500ms to allow the window to be displayed before starting the loading process
    welcome.after(500, charger_la_suite)
    welcome.mainloop()

if __name__ == "__main__":
    setup_logging()
    afficher_bienvenue_et_lancer()
