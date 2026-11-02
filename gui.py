# -*- coding: utf-8 -*-
# FREDDEX - Tkinter GUI (file selection, waiting screen, welcome screen)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# This module is deliberately decoupled from the business logic: every function takes the
# processing pipeline as a parameter (fonction_de_traitement) instead of importing it
# directly, so the GUI can be reused, tested, or swapped independently of what it runs.

import inspect
import os
import re
import threading
import time
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from paths import resource_path
from progress_relay import run_with_relay
from extraction import identifier_export, SHEETS_PRINCIPAL, SHEETS_GENETIQUE

# Small, brand-neutral palette used throughout this module, so the window reads as one
# consistent piece rather than default-grey Tk widgets.
COLOR_BG = "#EEF2F7"          # app background (slightly cooler, more depth than before)
COLOR_CARD = "#FFFFFF"        # raised card surface for content blocks
COLOR_BORDER = "#D5DEE8"      # hairline separators / card borders
COLOR_HEADER_BG = "#17324F"   # deep navy header
COLOR_HEADER_ACCENT = "#3B82F6"  # accent rule under the header
COLOR_HEADER_TEXT = "#FFFFFF"
COLOR_TEXT = "#16202B"
COLOR_MUTED = "#5B6470"
COLOR_LOG_BG = "#0F172A"
COLOR_LOG_TEXT = "#D8E1EA"
COLOR_SUCCESS = "#15803D"
COLOR_SUCCESS_BG = "#E7F6EC"
COLOR_ERROR = "#B3261E"
COLOR_ERROR_BG = "#FBEAE8"
COLOR_INFO = "#8A5A00"
COLOR_INFO_BG = "#FBF3E2"
COLOR_ROW_ALT = "#F2F6FB"     # alternating row shade in the final patient table

# Progress lines are refreshed on the GUI thread this often (ms). Frequent enough to feel
# live, cheap enough not to matter against a background job that runs for minutes.
POLL_INTERVAL_MS = 150

# Max number of lines kept in the on-screen live log . The full history
# stays in the log file; this only bounds the in-memory Text widget for big imports.
MAX_LOG_LINES = 600

# Friendly labels for the structured "PROGRESS:<stage>:<current>/<total>" markers emitted by
# skezia_api.py (see progress_relay.py) -- these are the two slowest phases (one HTTP call
# per patient, plus the deliberate SKEZIA throttling pause), so they are the ones worth
# showing a real count for instead of a plain bouncing bar.
STAGE_LABELS = {
    "patients_crees": "Création des profils patients",
    "questionnaires_envoyes": "Envoi des questionnaires",
}

# Where the last folder used for file selection is remembered between runs (see
# _lire_dernier_dossier / _memoriser_dernier_dossier). Deliberately its own small file, not
# shared with logging_utils.log_dir or the offline-mode "output" folder -- those are
# pipeline/business concerns, and this module is meant to stay usable without importing
# anything pipeline-specific (see module docstring). A plain relative path (not
# paths.resource_path): resource_path resolves inside the PyInstaller bundle's extraction
# directory when running as a built .exe, which is recreated fresh on every launch --
# writing there would silently defeat the whole point of remembering anything across runs.
_DERNIER_DOSSIER_FILE = os.path.join("gui_state", "dernier_dossier.txt")


def _lire_dernier_dossier():
    """
    Returns the folder used the last time a file was picked, or None if there isn't one yet,
    or it can no longer be used (deleted, moved to another drive...). Never raises -- this is
    a convenience for pre-filling the file dialog, not something that should ever prevent
    file selection from working.
    """
    try:
        with open(_DERNIER_DOSSIER_FILE, "r", encoding="utf-8") as f:
            dossier = f.read().strip()
    except OSError:
        return None
    return dossier if dossier and os.path.isdir(dossier) else None


def _memoriser_dernier_dossier(chemin_fichier):
    """Persists the folder containing chemin_fichier, best-effort (see _lire_dernier_dossier
    for why a failure here must never propagate -- remembering it is a nicety, not a
    requirement for FREDDEX to work)."""
    try:
        os.makedirs(os.path.dirname(_DERNIER_DOSSIER_FILE) or ".", exist_ok=True)
        with open(_DERNIER_DOSSIER_FILE, "w", encoding="utf-8") as f:
            f.write(os.path.dirname(chemin_fichier))
    except OSError:
        pass


def _configure_style():
    """
    Configures a ttk.Style shared by every window this module opens, so FREDDEX has one
    consistent look instead of the platform-default Tk widget styling.
    """
    style = ttk.Style()
    try:
        style.theme_use("clam")  # a theme that actually honours custom colors cross-platform
    except tk.TclError:
        pass  # fall back to whatever the default theme is if "clam" is unavailable
    style.configure("Freddex.TFrame", background=COLOR_BG)
    style.configure("FreddexHeader.TFrame", background=COLOR_HEADER_BG)
    style.configure("FreddexHeader.TLabel", background=COLOR_HEADER_BG, foreground=COLOR_HEADER_TEXT,
                     font=("Segoe UI", 14, "bold"))
    style.configure("FreddexStatus.TLabel", background=COLOR_BG, foreground=COLOR_TEXT,
                     font=("Segoe UI", 11), wraplength=560, justify="left")
    style.configure("FreddexMuted.TLabel", background=COLOR_BG, foreground=COLOR_MUTED,
                     font=("Segoe UI", 9), wraplength=520, justify="left")
    style.configure("Freddex.Horizontal.TProgressbar", troughcolor=COLOR_BG, background=COLOR_HEADER_BG)
    style.configure("Freddex.TButton", font=("Segoe UI", 10, "bold"), padding=6)
    style.configure("Freddex.TCheckbutton", background=COLOR_CARD, foreground=COLOR_TEXT,
                     font=("Segoe UI", 10), wraplength=460)
    style.map("Freddex.TCheckbutton", background=[("active", COLOR_CARD)])

    # Card surface (raised white block that holds the actual content, sitting on COLOR_BG).
    style.configure("FreddexCard.TFrame", background=COLOR_CARD)
    # Thin accent rule drawn just under the header for a more finished look.
    style.configure("FreddexAccent.TFrame", background=COLOR_HEADER_ACCENT)
    # Labels living on the white card (as opposed to the app background).
    style.configure("FreddexCard.TLabel", background=COLOR_CARD, foreground=COLOR_TEXT,
                     font=("Segoe UI", 11))
    style.configure("FreddexCardMuted.TLabel", background=COLOR_CARD, foreground=COLOR_MUTED,
                     font=("Segoe UI", 9), wraplength=560, justify="left")
    style.configure("FreddexTitle.TLabel", background=COLOR_CARD, foreground=COLOR_TEXT,
                     font=("Segoe UI", 13, "bold"), wraplength=600, justify="left")
    # Primary (accent) action button, used for the main call-to-action on each screen.
    style.configure("FreddexPrimary.TButton", font=("Segoe UI", 10, "bold"), padding=(14, 7),
                     background=COLOR_HEADER_ACCENT, foreground="#FFFFFF", borderwidth=0)
    style.map("FreddexPrimary.TButton",
              background=[("active", "#2f6fd0"), ("disabled", "#A9C4EC")],
              foreground=[("disabled", "#EEF2F7")])
    # Secondary (quiet) button.
    style.configure("FreddexGhost.TButton", font=("Segoe UI", 10), padding=(12, 7))
    # Final per-patient summary table.
    style.configure("Freddex.Treeview", background=COLOR_CARD, fieldbackground=COLOR_CARD,
                     foreground=COLOR_TEXT, rowheight=26, font=("Segoe UI", 9), borderwidth=0)
    style.configure("Freddex.Treeview.Heading", background=COLOR_HEADER_BG,
                     foreground=COLOR_HEADER_TEXT, font=("Segoe UI", 9, "bold"),
                     relief="flat", padding=6)
    style.map("Freddex.Treeview.Heading", background=[("active", COLOR_HEADER_BG)])
    style.map("Freddex.Treeview",
              background=[("selected", "#DCE7F5")], foreground=[("selected", COLOR_TEXT)])
    return style


def creer_root():
    """
    Creates THE single tk.Tk() instance for the whole application, hidden, with the shared
    style applied. Every screen in FREDDEX -- the first-run key setup (key_setup.py), the
    welcome window, file selection, confirmation, and the processing window -- is a
    tk.Toplevel of this one root (see afficher_bienvenue_et_lancer's architecture note).
    """
    root = tk.Tk()
    root.title("FREDDEX")
    root.configure(bg=COLOR_HEADER_BG)
    _configure_style()
    root.withdraw()
    return root


def _dimensionner_fenetre(root, marge=28, largeur_min=0, hauteur_min=0):
    """
    Sizes `root` to fit the content actually packed inside it, instead of a fixed "WxH" pixel
    guess -- call this once, right before root.mainloop(), after every widget has been packed.
    """
    deja_affichee = root.winfo_ismapped()
    root.update_idletasks()
    largeur_actuelle = root.winfo_width() if deja_affichee else 0
    hauteur_actuelle = root.winfo_height() if deja_affichee else 0
    largeur = max(root.winfo_reqwidth() + marge, largeur_min, largeur_actuelle)
    hauteur = max(root.winfo_reqheight() + marge, hauteur_min, hauteur_actuelle)
    root.geometry(f"{largeur}x{hauteur}")
    root.minsize(largeur, hauteur)
    if not deja_affichee:
        root.tk.eval(f'tk::PlaceWindow {root} center')

_icon_warned = {"done": False}


def _load_icon_photo(master):
    """
    Loads the FREDDEX icon as a Tk PhotoImage if the file exists, returning None otherwise
    (the caller simply skips displaying it -- this must never prevent the window from
    opening).
    """
    try:
        image_path = resource_path("files/icone.png")
        if os.path.exists(image_path):
            return tk.PhotoImage(file=image_path, master=master)
        if not _icon_warned["done"]:
            print(f"Image introuvable : {image_path}")
            _icon_warned["done"] = True
    except Exception as e:
        print(f"Erreur lors du chargement de l'image : {e}")
    return None


def _entete(root, titre):
    """
    Builds the standard FREDDEX window header (navy bar + optional icon + title) followed by a
    thin accent rule, for a consistent, more finished look across every screen. Returns the
    header frame.
    """
    header = ttk.Frame(root, style="FreddexHeader.TFrame")
    header.pack(side="top", fill="x")
    icon_photo = _load_icon_photo(header)
    if icon_photo is not None:
        icon_label = tk.Label(header, image=icon_photo, bg=COLOR_HEADER_BG)
        icon_label.image = icon_photo  # keep a reference, otherwise Tk garbage-collects it
        icon_label.pack(side="left", padx=(18, 10), pady=12)
    ttk.Label(header, text=titre, style="FreddexHeader.TLabel").pack(side="left", pady=12)
    ttk.Frame(root, style="FreddexAccent.TFrame", height=3).pack(side="top", fill="x")
    return header


def _bandeau_mode(root, texte, hors_ligne):
    """
    Bandeau coloré sous l'en-tête, qui dit EN PERMANENCE dans quel mode on est : vert pour
    l'import normal (envoi à SKEZIA), ambre pour le mode hors-ligne (aucun envoi). Sur chaque
    fenêtre du parcours, pour qu'on ne confonde jamais les deux.
    """
    fond, texte_couleur = (COLOR_INFO_BG, COLOR_INFO) if hors_ligne else (COLOR_SUCCESS_BG, COLOR_SUCCESS)
    tk.Label(root, text=texte, bg=fond, fg=texte_couleur, font=("Segoe UI", 9, "bold"),
             anchor="w", padx=20, pady=6).pack(side="top", fill="x")


ACTION_NOUVEL_IMPORT = "nouvel_import"   # retour de _executer_un_import
ACTION_ETAPE2 = "etape2"
ACTION_QUITTER = "quitter"

MODE_NORMAL = "normal"
MODE_HORS_LIGNE = "hors_ligne"          # écran d'accueil : ouvre le sous-menu hors-ligne
MODE_HORS_LIGNE_1 = "hors_ligne_1"
MODE_HORS_LIGNE_2 = "hors_ligne_2"


def _fenetre_de_choix(parent, titre_entete, blocs, texte_bas, avec_bandeau=None):
    """
    Fenêtre modale de choix : une liste de blocs colorés (titre, explication, bouton) et un
    bouton en bas à droite. blocs : [(hors_ligne, titre, explication, valeur, texte_bouton, style)].
    Renvoie la valeur du bloc choisi, ou None si le bouton du bas (ou la fermeture) est utilisé.
    """
    choix = {"valeur": None}
    root = tk.Toplevel(parent)
    root.title("FREDDEX")
    root.configure(bg=COLOR_BG)
    root.attributes("-topmost", True)
    root.after(200, lambda: root.attributes("-topmost", False))
    _configure_style()
    _entete(root, titre_entete)
    if avec_bandeau:
        _bandeau_mode(root, avec_bandeau, True)

    body = ttk.Frame(root, style="Freddex.TFrame", padding=(20, 16))
    body.pack(side="top", fill="both", expand=True)

    def choisir(valeur):
        choix["valeur"] = valeur
        root.destroy()

    for hors_ligne, titre, explication, valeur, texte_bouton, style_bouton in blocs:
        fond, couleur = (COLOR_INFO_BG, COLOR_INFO) if hors_ligne else (COLOR_SUCCESS_BG, COLOR_SUCCESS)
        cadre = tk.Frame(body, bg=fond, padx=14, pady=10)
        cadre.pack(fill="x", pady=(0, 10))
        tk.Label(cadre, text=titre, bg=fond, fg=couleur, font=("Segoe UI", 10, "bold"), anchor="w").pack(fill="x")
        tk.Label(cadre, text=explication, bg=fond, fg=COLOR_TEXT, font=("Segoe UI", 9), anchor="w",
                 justify="left", wraplength=470).pack(fill="x", pady=(2, 8))
        ttk.Button(cadre, text=texte_bouton, command=lambda v=valeur: choisir(v), style=style_bouton).pack(anchor="w")

    ttk.Button(body, text=texte_bas, command=root.destroy, style="FreddexGhost.TButton").pack(anchor="e")

    _dimensionner_fenetre(root, largeur_min=560)
    root.grab_set()
    parent.wait_window(root)
    return choix["valeur"]


def _choisir_mode(parent, avec_etape2=True):
    """
    Écran d'accueil : DEUX parcours seulement, tout ce qui concerne le hors-ligne restant caché
    tant qu'on n'a pas cliqué sur « Hors ligne ». Renvoie MODE_NORMAL, MODE_HORS_LIGNE (le
    sous-menu s'ouvre ensuite, voir _menu_hors_ligne) ou None si l'utilisateur quitte.
    avec_etape2 n'est plus utilisé ici (conservé pour compatibilité des appels).
    """
    return _fenetre_de_choix(parent, "FREDDEX — Que voulez-vous faire ?", [
        (False, "IMPORT NORMAL",
         "Transforme les exports BaMaRa et les envoie à SKEZIA. Nécessite l'accès à Internet.",
         MODE_NORMAL, "Import normal", "FreddexPrimary.TButton"),
        (True, "HORS LIGNE",
         "Prépare les fichiers sans aucune connexion à SKEZIA : étape 1/2 (fichiers à importer dans "
         "la plateforme), puis étape 2/2 (ajout des ID SKEZIA).",
         MODE_HORS_LIGNE, "Hors ligne", "Freddex.TButton"),
    ], "Quitter")


def _menu_hors_ligne(parent):
    """
    Sous-menu du mode hors-ligne (affiché seulement après un clic sur « Hors ligne »).
    Renvoie MODE_HORS_LIGNE_1, MODE_HORS_LIGNE_2, ou None pour revenir à l'accueil. L'étape 2/2
    est aussi proposée automatiquement à la fin de l'étape 1/2 ; cette entrée sert à la reprendre
    plus tard (après l'import des participants dans la plateforme, parfois un autre jour).
    """
    return _fenetre_de_choix(parent, "FREDDEX — Mode hors ligne", [
        (True, "ÉTAPE 1/2 — Préparer les fichiers",
         "Transforme les exports BaMaRa sans rien envoyer. Produit un fichier de participants "
         "(à importer d'abord dans la plateforme) et un fichier de données.",
         MODE_HORS_LIGNE_1, "Étape 1/2", "Freddex.TButton"),
        (True, "ÉTAPE 2/2 — Ajouter les ID SKEZIA",
         "Une fois les participants importés : ajoute leur ID SKEZIA au fichier de données.",
         MODE_HORS_LIGNE_2, "Étape 2/2", "Freddex.TButton"),
    ], "Retour", avec_bandeau="MODE HORS LIGNE · aucun envoi à SKEZIA")


_RESUME_LINE = re.compile(r"^ID BaMaRa:\s*(.*?),\s*NOM:\s*(.*?),\s*Prénom:\s*(.*)$")


def _decouper_message_final(message):
    """
    Splits a final status message into (titre, patients, autres_lignes):
    - titre: the headline (first line), with a trailing "pour :"/":" trimmed so it reads as a
      clean sentence above the table.
    - patients: list of (id_bamara, nom, prenom) parsed from the per-patient summary lines.
    - autres_lignes: any remaining non-empty lines that are not per-patient rows (e.g. the
      offline-mode "aucune donnée envoyée" note), shown as muted text.
    """
    lignes = [ligne for ligne in message.split("\n")]
    titre = lignes[0].rstrip() if lignes else message
    for suffixe in (" pour :", " pour:", ":"):
        if titre.endswith(suffixe):
            titre = titre[: -len(suffixe)].rstrip()
            break
    patients, autres = [], []
    for ligne in lignes[1:]:
        ligne = ligne.strip()
        if not ligne:
            continue
        m = _RESUME_LINE.match(ligne)
        if m:
            ident, nom, prenom = (part.strip() for part in m.groups())
            patients.append((ident, nom, prenom))
        else:
            autres.append(ligne)
    return titre, patients, autres


def _ouvrir_dossier(chemin):
    """
    Opens `chemin` in the OS file explorer. os.startfile is Windows-only (this GUI only ever
    ships as a Windows .exe for FREDDEX's clinical centers, see the project's own scope), so
    this falls back to just showing the path in a message box on any other platform (this
    codebase's own Linux dev/test environment included) -- never crashes either way.
    """
    if hasattr(os, "startfile"):
        try:
            os.startfile(chemin)
            return
        except OSError as e:
            messagebox.showerror("FREDDEX", f"Impossible d'ouvrir le dossier :\n{chemin}\n\n{e}")
            return
    messagebox.showinfo("FREDDEX", f"Dossier : {chemin}")


LOG_TAG_COLORS = {
    "❌": "#FF6B6B",
    "🛑": "#FF6B6B",
    "⚠️": "#FBBF24",
    "✅": "#4ADE80",
    "ℹ️": "#60A5FA",
}


def _status_color(message):
    """Picks a text color for the final status message based on its leading emoji marker."""
    if message.startswith("✅"):
        return COLOR_SUCCESS
    if message.startswith("❌"):
        return COLOR_ERROR
    if message.startswith("ℹ️"):
        return COLOR_INFO
    return COLOR_TEXT


def _libelle_fichiers(fichier_selectionne):
    """Libellé court d'une sélection : un chemin, ou le tuple (principal, génétique)."""
    if isinstance(fichier_selectionne, (tuple, list)):
        return " + ".join(os.path.basename(f) for f in fichier_selectionne)
    return os.path.basename(fichier_selectionne)


def _dialogue_liaison_ids(parent, fonction_liaison, donnees_initiales=""):
    """
    Fenêtre de l'ÉTAPE 2 du mode hors-ligne : ajouter à l'export de données FREDDEX une colonne
    portant l'ID SKEZIA de chaque participant, à partir de l'export de la plateforme (aucun accès
    à l'API, aucune ligne de commande : deux fichiers à choisir, un bouton).

    fonction_liaison(export_donnees, export_participants) -> (message, chemin_sortie) : fournie
    par FREDDEX-base (ce module reste découplé de la logique métier). chemin_sortie vaut None si
    la liaison a échoué ; sinon c'est le fichier créé (le dossier peut alors être ouvert).
    donnees_initiales : chemin de l'export de données à présélectionner (celui qui vient d'être
    produit par l'étape 1/2), ou "" ; ignoré s'il n'existe plus.

    Le traitement est rapide (lecture de deux fichiers) : il s'exécute dans le thread de
    l'interface, avec un message d'attente, plutôt que dans un thread dédié.
    """
    choix = {"donnees": "", "participants": "", "sortie": None}

    fen = tk.Toplevel(parent)
    fen.title("FREDDEX - Mode hors-ligne, étape 2/2")
    fen.configure(bg=COLOR_BG)
    fen.attributes("-topmost", True)
    fen.after(200, lambda: fen.attributes("-topmost", False))
    _configure_style()
    _entete(fen, "FREDDEX — Ajouter les ID SKEZIA")
    _bandeau_mode(fen, "MODE HORS-LIGNE — ÉTAPE 2/2 · aucun envoi à SKEZIA", True)

    body = ttk.Frame(fen, style="Freddex.TFrame", padding=(20, 18))
    body.pack(side="top", fill="both", expand=True)

    ttk.Label(
        body, style="FreddexMuted.TLabel", justify="left", wraplength=560,
        text="Ajoute l'ID SKEZIA de chaque patient et produit le fichier CSV prêt à importer dans SKEZIA (l'export d'origine n'est pas modifié).",
    ).pack(anchor="w", pady=(0, 10))

    labels = {}

    def ligne_fichier(cle, titre_etape, titre_dialogue, types):
        ttk.Label(body, text=titre_etape, style="FreddexMuted.TLabel").pack(anchor="w")
        labels[cle] = ttk.Label(body, text="aucun fichier sélectionné.", style="FreddexStatus.TLabel")
        labels[cle].pack(anchor="w", pady=(2, 0), fill="x")

        def choisir():
            chemin = filedialog.askopenfilename(
                title=titre_dialogue, parent=fen,
                initialdir=_lire_dernier_dossier() or os.getcwd(), filetypes=types)
            if chemin:
                choix[cle] = chemin
                _memoriser_dernier_dossier(chemin)
                labels[cle].config(text=os.path.basename(chemin), foreground=COLOR_TEXT)
                bouton_lier.config(state="normal" if choix["donnees"] and choix["participants"] else "disabled")

        ttk.Button(body, text="Parcourir...", command=choisir, style="Freddex.TButton").pack(
            anchor="w", pady=(6, 12))

    ligne_fichier("participants", "1. Export de la plateforme SKEZIA (Answers.csv, colonne « id »)",
                  "FREDDEX - Sélectionner l'export de la plateforme",
                  [("Fichiers CSV ou Excel", "*.csv *.xlsx"), ("Tous les fichiers", "*.*")])
    ligne_fichier("donnees", "2. Export de données FREDDEX (FREDD_export_….xlsx)",
                  "FREDDEX - Sélectionner l'export de données",
                  [("Fichiers Excel", "*.xlsx"), ("Tous les fichiers", "*.*")])

    cadre_texte = ttk.Frame(body, style="Freddex.TFrame")
    zone = tk.Text(cadre_texte, wrap="word", state="disabled", height=9, bg=COLOR_LOG_BG, fg=COLOR_LOG_TEXT,
                   font=("Consolas", 9), relief="flat", padx=8, pady=6)
    barre = ttk.Scrollbar(cadre_texte, orient="vertical", command=zone.yview)
    zone.configure(yscrollcommand=barre.set)
    barre.pack(side="right", fill="y")
    zone.pack(side="left", fill="both", expand=True)
    for emoji, couleur in LOG_TAG_COLORS.items():
        zone.tag_configure(emoji, foreground=couleur)

    def afficher(message):
        if not cadre_texte.winfo_ismapped():
            cadre_texte.pack(fill="both", expand=True, pady=(0, 10))
        zone.config(state="normal")
        zone.delete("1.0", "end")
        for ligne in message.split("\n"):
            tag = next((e for e in LOG_TAG_COLORS if ligne.startswith(e)), None)
            zone.insert("end", ligne + "\n", (tag,) if tag else ())
        zone.config(state="disabled")
        _dimensionner_fenetre(fen, largeur_min=620)

    def lier():
        bouton_lier.config(state="disabled")
        afficher("ℹ️ Liaison en cours...")
        fen.update_idletasks()
        try:
            message, sortie = fonction_liaison(choix["donnees"], choix["participants"])
        except Exception as e:  # filet : la fonction fournie gère déjà ses erreurs attendues
            message, sortie = f"❌ Échec inattendu : {type(e).__name__}: {e}", None
        choix["sortie"] = sortie
        afficher(message)
        if sortie:
            bouton_dossier.pack(side="left")
        # Une liaison réussie ne se relance pas à l'identique (le fichier produit existe déjà) :
        # pour refaire, changer de fichier ou supprimer la copie.
        bouton_lier.config(state="disabled" if sortie else "normal")

    boutons = ttk.Frame(body, style="Freddex.TFrame")
    boutons.pack(side="bottom", fill="x", pady=(8, 0))
    bouton_dossier = ttk.Button(boutons, text="Ouvrir le dossier",
                                command=lambda: _ouvrir_dossier(os.path.dirname(os.path.abspath(choix["sortie"]))),
                                style="FreddexGhost.TButton")
    bouton_lier = ttk.Button(boutons, text="Ajouter les ID SKEZIA", command=lier,
                             style="FreddexPrimary.TButton", state="disabled")
    bouton_lier.pack(side="right")
    if donnees_initiales and os.path.isfile(donnees_initiales):
        choix["donnees"] = donnees_initiales
        labels["donnees"].config(text=os.path.basename(donnees_initiales), foreground=COLOR_TEXT)
    ttk.Button(boutons, text="Fermer", command=fen.destroy, style="FreddexGhost.TButton").pack(
        side="right", padx=(0, 8))

    _dimensionner_fenetre(fen, largeur_min=620)
    fen.grab_set()
    parent.wait_window(fen)
    return choix["sortie"]


def _demander_fichier_et_options(parent, mode_hors_ligne=False):
    """
    Shows a single window to pick the BaMaRa input file and choose whether to run in offline
    mode (transformation only, no SKEZIA sending) -- see the "Mode hors-ligne" checkbox.

    A single visible window hosts the whole file-selection step (file field and options),
    rather than a throw-away window used only for the file dialog.

    parent: the application's one persistent tk.Tk() root (see afficher_bienvenue_et_lancer's
    architecture note) -- this screen is a tk.Toplevel(parent), never its own tk.Tk() (see
    that note for why: repeatedly creating/destroying separate Tk() instances is what caused
    a crash on a second import).

    The file dialog also opens on whichever folder a file was last picked from (persisted
    between runs -- see _lire_dernier_dossier/_memoriser_dernier_dossier), instead of always
    starting from the same default location: BaMaRa exports for a given center are typically
    downloaded to the same place each time, so re-navigating there by hand on every single
    import added friction for no benefit.

    mode_hors_ligne : le mode est choisi AVANT, sur l'écran _choisir_mode (plus de case à
    cocher ici) : la fenêtre, son titre et son bandeau coloré en dépendent, et ce même booléen
    est renvoyé tel quel.

    Returns (fichier_selectionne, mode_hors_ligne). fichier_selectionne est le tuple
    (export principal, export génétique), ou "" si
    l'utilisateur a annulé -- même contrat que lancer_interface_et_traitement vérifie avec
    "if not fichier_selectionne: return". Un seul bouton « Parcourir » : les deux fichiers
    peuvent être choisis ensemble ou l'un après l'autre ; chacun est reconnu par ses onglets
    (extraction.identifier_export), et un fichier non reconnu est signalé immédiatement, au
    lieu d'échouer en plein import.
    """
    selection = {"principal": "", "genetique": "", "confirmed": False}

    root = tk.Toplevel(parent)
    root.title("FREDDEX - Mode hors-ligne, étape 1/2" if mode_hors_ligne else "FREDDEX - Import normal")
    root.configure(bg=COLOR_BG)
    root.attributes("-topmost", True)
    root.after(200, lambda: root.attributes("-topmost", False))  # come to front once, then behave normally
    _configure_style()

    if mode_hors_ligne:
        _entete(root, "FREDDEX — Préparer les fichiers")
        _bandeau_mode(root, "MODE HORS-LIGNE — ÉTAPE 1/2 · aucun envoi à SKEZIA", True)
    else:
        _entete(root, "FREDDEX — Import normal")
        _bandeau_mode(root, "IMPORT NORMAL · envoi des données à SKEZIA", False)

    body = ttk.Frame(root, style="Freddex.TFrame", padding=(20, 18))
    body.pack(side="top", fill="both", expand=True)

    libelles = {"principal": "Export principal", "genetique": "Export génétique"}
    ttk.Label(body, text="Sélectionner les deux exports BaMaRa (principal + génétique) :",
              style="FreddexMuted.TLabel").pack(anchor="w")
    labels_fichier = {}
    for type_export in ("principal", "genetique"):
        labels_fichier[type_export] = ttk.Label(
            body, text=f"{libelles[type_export]} : aucun fichier sélectionné.", style="FreddexStatus.TLabel")
        labels_fichier[type_export].pack(anchor="w", pady=(2, 0), fill="x")

    def choisir_fichiers():
        # Un seul bouton : on peut sélectionner les deux fichiers d'un coup (Ctrl+clic) ou un
        # par un. Chaque fichier est reconnu par ses onglets (extraction.identifier_export) et
        # rangé à sa place ; un fichier non reconnu est signalé avec les onglets attendus.
        chemins = filedialog.askopenfilenames(
            title="FREDDEX - Sélectionner les exports BaMaRa (principal et génétique)", parent=root,
            initialdir=_lire_dernier_dossier() or os.getcwd(),
            filetypes=[("Fichiers Excel", "*.xlsx *.xls"), ("Tous les fichiers", "*.*")],
        )
        non_reconnus = []
        for chemin in chemins or ():
            try:
                type_reel = identifier_export(chemin)
            except Exception as e:
                non_reconnus.append(f"{os.path.basename(chemin)} (illisible : {e})")
                continue
            if type_reel is None:
                non_reconnus.append(os.path.basename(chemin))
                continue
            selection[type_reel] = chemin
            _memoriser_dernier_dossier(chemin)
            labels_fichier[type_reel].config(
                text=f"{libelles[type_reel]} : {os.path.basename(chemin)}", foreground=COLOR_TEXT)
        if non_reconnus:
            messagebox.showerror(
                "FREDDEX", "Fichier(s) non reconnu(s) comme export BaMaRa :\n- " + "\n- ".join(non_reconnus)
                + "\n\nL'export principal doit contenir les onglets : " + ", ".join(SHEETS_PRINCIPAL.values())
                + ".\nL'export génétique doit contenir les onglets : " + ", ".join(SHEETS_GENETIQUE.values()) + ".",
                parent=root)
        if selection["principal"] and selection["genetique"]:
            bouton_continuer.config(state="normal")

    ttk.Button(body, text="Parcourir...", command=choisir_fichiers, style="Freddex.TButton").pack(
        anchor="w", pady=(8, 16))

    def continuer():
        selection["confirmed"] = True
        root.destroy()

    boutons = ttk.Frame(body, style="Freddex.TFrame")
    boutons.pack(side="bottom", fill="x", pady=(18, 0))
    bouton_continuer = ttk.Button(boutons, text="Continuer", command=continuer,
                                  style="FreddexPrimary.TButton", state="disabled")
    bouton_continuer.pack(side="right")
    ttk.Button(boutons, text="Retour", command=root.destroy, style="FreddexGhost.TButton").pack(
        side="right", padx=(0, 8))

    # UX : Enter validates once a file has been chosen.
    root.bind("<Return>", lambda e: continuer() if selection["principal"] and selection["genetique"] else None)

    _dimensionner_fenetre(root, largeur_min=580)
    root.grab_set()
    parent.wait_window(root)  # blocks here until this Toplevel closes, without a nested mainloop

    if not selection["confirmed"] or not (selection["principal"] and selection["genetique"]):
        return "", False
    return (selection["principal"], selection["genetique"]), mode_hors_ligne


def _demander_confirmation(parent, fichier_selectionne, mode_hors_ligne, fonction_apercu):
    """
    Shows a confirmation screen between file selection and the actual launch: a summary of
    what is about to happen (file, mode) and, if fonction_apercu is given, a quick preview of
    the file's content (e.g. a raw patient count -- see FREDDEX-base.apercu_fichier).

    parent: the application's one persistent tk.Tk() root, see _demander_fichier_et_options.

    fonction_apercu is called on a background thread, since even a "quick" preview means
    reading the file, and this window must stay responsive while that happens. The "Lancer
    l'import" button starts disabled until the preview finishes (or immediately, if no
    fonction_apercu was given at all -- see lancer_interface_et_traitement's docstring for
    why this stays optional).

    Returns True if the user confirmed, False otherwise (clicked "Annuler" or closed the
    window) -- lancer_interface_et_traitement treats False exactly like cancelling file
    selection: stop, nothing was launched.
    """
    confirmed = {"value": False}

    root = tk.Toplevel(parent)
    root.title("FREDDEX - Confirmation")
    root.configure(bg=COLOR_BG)
    root.attributes("-topmost", True)
    root.after(200, lambda: root.attributes("-topmost", False))
    _configure_style()

    _entete(root, "FREDDEX — Confirmer l'import")
    _bandeau_mode(root, "MODE HORS-LIGNE — ÉTAPE 1/2 · aucun envoi à SKEZIA" if mode_hors_ligne
                  else "IMPORT NORMAL · envoi des données à SKEZIA", mode_hors_ligne)

    body = ttk.Frame(root, style="Freddex.TFrame", padding=(20, 18))
    body.pack(side="top", fill="both", expand=True)

    ttk.Label(body, text="Vérifiez avant de lancer", style="FreddexStatus.TLabel").pack(
        anchor="w", pady=(0, 10), fill="x")
    ttk.Label(body, text=f"📄  Fichiers : {_libelle_fichiers(fichier_selectionne)}",
              style="FreddexStatus.TLabel").pack(anchor="w", pady=(0, 4), fill="x")
    mode_text = "Mode hors-ligne (aucun envoi à SKEZIA)" if mode_hors_ligne else "Mode normal (envoi à SKEZIA)"
    ttk.Label(body, text=f"⚙️  Mode : {mode_text}", style="FreddexStatus.TLabel").pack(
        anchor="w", pady=(0, 12), fill="x")

    label_apercu = ttk.Label(body, text="", style="FreddexMuted.TLabel")
    label_apercu.pack(anchor="w", pady=(0, 4), fill="x")
    apercu_bar = ttk.Progressbar(body, orient="horizontal", mode="indeterminate",
                                  style="Freddex.Horizontal.TProgressbar")

    def confirmer():
        confirmed["value"] = True
        root.destroy()

    boutons = ttk.Frame(body, style="Freddex.TFrame")
    boutons.pack(side="bottom", fill="x", pady=(18, 0))
    bouton_lancer = ttk.Button(boutons, text="Lancer l'import", command=confirmer,
                               style="FreddexPrimary.TButton",
                               state="normal" if fonction_apercu is None else "disabled")
    bouton_lancer.pack(side="right")
    ttk.Button(boutons, text="Annuler", command=root.destroy, style="FreddexGhost.TButton").pack(
        side="right", padx=(0, 8))

    # UX : Enter launches the import once the button is enabled.
    root.bind("<Return>", lambda e: confirmer() if str(bouton_lancer["state"]) == "normal" else None)

    if fonction_apercu is not None:
        label_apercu.config(text="Calcul de l'aperçu...")
        apercu_bar.pack(fill="x", pady=(0, 4))
        apercu_bar.start()

        def afficher_apercu(texte):
            # The window may already be gone if the user clicked "Annuler" while the preview
            # was still loading -- updating destroyed widgets raises TclError, harmlessly
            # ignored here (there is nothing left to show it to).
            try:
                apercu_bar.stop()
                apercu_bar.pack_forget()
                label_apercu.config(text=texte)
                bouton_lancer.config(state="normal")
                # The real preview text can wrap onto more lines than the placeholder
                # ("Calcul de l'aperçu...") -- re-fit the window to its new content so a long
                # preview can never push the "Lancer l'import" button out of view
                # (see _dimensionner_fenetre's docstring).
                _dimensionner_fenetre(root, largeur_min=560)
            except tk.TclError:
                pass

        def calculer_apercu():
            try:
                texte = fonction_apercu(fichier_selectionne)
            except Exception as e:
                # fonction_apercu is caller-supplied: FREDDEX-base.apercu_fichier already
                # catches its own errors and returns a message instead of raising, but this
                # window must not crash even if a different fonction_apercu doesn't.
                texte = f"⚠️ Aperçu indisponible : {e}"
            try:
                root.after(0, lambda: afficher_apercu(texte))
            except tk.TclError:
                pass  # window already closed

        threading.Thread(target=calculer_apercu, daemon=True).start()

    _dimensionner_fenetre(root, largeur_min=560)
    root.grab_set()
    parent.wait_window(root)
    return confirmed["value"]


def lancer_interface_et_traitement(root, fonction_de_traitement, fonction_apercu=None, dossier_logs=None,
                                   fonction_liaison=None):
    """
    Launches a graphical interface to run a long background processing function, with a
    LIVE view of its progress rather than a plain spinning bar, and loops to let the user run
    another import right away instead of having to relaunch the whole application.

    root: the application's one persistent tk.Tk() instance, created once by
    afficher_bienvenue_et_lancer and never destroyed until the loop below actually ends
    (user chose "Quitter", or cancelled at file selection). Every screen this loop opens
    (_demander_fichier_et_options, _demander_confirmation, _executer_un_import) is a
    tk.Toplevel(root), not its own tk.Tk() -- see afficher_bienvenue_et_lancer's docstring
    for why: repeatedly creating and destroying separate Tk() instances (one per screen, one
    per loop iteration) is what caused a crash on a second "Nouvel import".

    The window shows:
    - a header with the FREDDEX icon/title
    - a short status line, updated to the latest progress message as it happens
    - a scrollable log panel mirroring every progress line the pipeline reports (the same
      messages that would otherwise only be visible in the log file), each colored by its
      leading emoji marker (❌/🛑 red, ⚠️ amber, ✅ green, ℹ️ blue -- see LOG_TAG_COLORS) so a
      warning or error is visible at a glance instead of blending into a wall of same-colored
      monospace text.
    - a progress bar that switches from indeterminate (bouncing) to determinate (filling up)
      with a "n/total" counter label as soon as the pipeline reaches its two slowest phases
      (patient profile creation, questionnaire sending) -- see STAGE_LABELS.
    - an "Annuler" button, visible while processing runs, that requests cooperative
      cancellation (see cancel_event below) -- with a stronger warning if SKEZIA sending has
      already started, since cancelling then leaves a partial import (some patients
      created/sent, others not) rather than a clean stop. The window's native close button
      (the OS titlebar's ✕) triggers this exact same confirmation while processing is running,
      instead of silently killing the window out from under an in-flight SKEZIA send -- see
      the WM_DELETE_WINDOW handler below.
    - a color-coded final status (green/red/neutral), an optional "Ouvrir le dossier des
      journaux" button (only shown if dossier_logs was given -- see below), and "Nouvel
      import" / "Quitter" buttons once processing ends.

    Before this window even opens, on every loop iteration: _demander_fichier_et_options()
    shows a small selection window with the file picker and a "Mode hors-ligne" checkbox
    (transformation only, no SKEZIA sending) -- see mode_hors_ligne below -- then
    _demander_confirmation() shows a second screen summarizing the file/mode and, if
    fonction_apercu was given, a preview of the file's content, before the actual (potentially
    long) processing starts.


    Parameters:
    fonction_de_traitement: function to execute, expected to return a message to display upon
    completion. Called as fonction_de_traitement(fichier_selectionne). If its signature also
    accepts a "cancel_event" keyword parameter (a threading.Event), it is passed one: the
    pipeline can check cancel_event.is_set() at safe points to stop early and cooperatively
    (see FREDDEX-base.traitement_complet for the reference implementation). Likewise, if its
    signature accepts a "mode_hors_ligne" keyword parameter (a bool), it is passed the
    checkbox's value. Both are optional and detected via introspection, so functions that
    don't support them keep working unchanged -- this module stays decoupled from any
    specific pipeline (see module docstring).

    fonction_apercu: optional function called as fonction_apercu(fichier_selectionne) on the
    confirmation screen, expected to return a short string describing what will be imported
    (e.g. a patient count -- see FREDDEX-base.apercu_fichier). If omitted, the confirmation
    screen still appears (file + mode summary, "Lancer l'import" / "Annuler") but without a
    preview line.

    dossier_logs: optional absolute path to the folder holding FREDDEX's log files. If given,
    the final screen shows an "Ouvrir le dossier des journaux" button that opens it directly
    in the OS file explorer (see _ouvrir_dossier) -- every error message already tells the
    user to check a log file there, but a relative path like "logs/error_log.txt" means
    nothing to someone who isn't a developer and doesn't know where the .exe itself lives.
    Passed in rather than imported directly (this module stays decoupled from FREDDEX's own
    logging_utils, see module docstring) -- FREDDEX-base.py supplies logging_utils.log_dir.

    fonction_liaison: optionnelle, fonction_liaison(export_donnees, export_participants) ->
    (message, chemin_sortie) : étape 2 du mode hors-ligne (ajout des ID SKEZIA), voir
    _dialogue_liaison_ids. Sans elle, le bouton correspondant n'apparaît pas.
    """
    while True:
        mode = _choisir_mode(root)
        if mode is None:
            return  # « Quitter » sur l'accueil : fin de l'application.
        if mode == MODE_HORS_LIGNE:
            if fonction_liaison is None:
                mode = MODE_HORS_LIGNE_1   # pas d'étape 2 disponible : directement l'étape 1/2
            else:
                mode = _menu_hors_ligne(root)
                if mode is None:
                    continue               # « Retour » : retour à l'accueil
        if mode == MODE_HORS_LIGNE_2:
            _dialogue_liaison_ids(root, fonction_liaison)
            continue

        mode_hors_ligne = (mode == MODE_HORS_LIGNE_1)
        fichier_selectionne, mode_hors_ligne = _demander_fichier_et_options(root, mode_hors_ligne=mode_hors_ligne)

        if not fichier_selectionne:
            continue  # « Retour » à la sélection : on revient à l'accueil, rien n'est lancé.

        if not _demander_confirmation(root, fichier_selectionne, mode_hors_ligne, fonction_apercu):
            continue  # Cancelled at confirmation: back to the home screen, not a full quit.

        action, message = _executer_un_import(
            root, fonction_de_traitement, fichier_selectionne, mode_hors_ligne, dossier_logs,
            proposer_etape2=mode_hors_ligne and fonction_liaison is not None)
        if action == ACTION_ETAPE2:
            # L'étape 2/2 s'enchaîne d'office, avec l'export de données qui vient d'être produit.
            trouve = re.search(r"dans (.+?FREDD_export_\d{8}_\d{6}\.xlsx)", message or "")
            _dialogue_liaison_ids(root, fonction_liaison, donnees_initiales=trouve.group(1) if trouve else "")
            continue
        if action == ACTION_NOUVEL_IMPORT:
            continue  # retour à l'écran d'accueil
        return  # "Quitter" (or the window was closed once processing had ended).


def _executer_un_import(parent, fonction_de_traitement, fichier_selectionne, mode_hors_ligne, dossier_logs,
                        proposer_etape2=False):
    """
    Runs one processing window from open to close (the actual "waiting / progress" screen) --
    split out of lancer_interface_et_traitement so its loop body (a new import per iteration)
    doesn't have to re-indent this entire block. Returns True if the user chose "Nouvel
    import" on the final screen (the caller should loop back to file selection), False if they
    chose "Quitter" or closed the window once processing had ended.

    parent: the application's one persistent tk.Tk() root, see _demander_fichier_et_options.
    """
    root = tk.Toplevel(parent)
    root.title("FREDDEX - Traitement en cours")
    root.configure(bg=COLOR_BG)
    root.attributes("-topmost", True)
    root.after(200, lambda: root.attributes("-topmost", False))  # come to front once, like every other FREDDEX window
    _configure_style()

    # --- Header -----------------------------------------------------------------
    titre_fenetre = "FREDDEX — Import en cours (mode hors-ligne)" if mode_hors_ligne else "FREDDEX — Import en cours"
    _entete(root, titre_fenetre)
    _bandeau_mode(root, "MODE HORS-LIGNE — ÉTAPE 1/2 · aucun envoi à SKEZIA" if mode_hors_ligne
                  else "IMPORT NORMAL · envoi des données à SKEZIA", mode_hors_ligne)

    # --- Body ---------------------------------------------------------------------
    body = ttk.Frame(root, style="Freddex.TFrame", padding=(20, 14))
    body.pack(side="top", fill="both", expand=True)

    # UX : show the file being imported, so someone who steps away
    # and comes back can tell what is running (it was only visible on the now-closed
    # confirmation screen before).
    ttk.Label(body, text=f"📄  {_libelle_fichiers(fichier_selectionne)}",
              style="FreddexMuted.TLabel").pack(anchor="w", pady=(0, 6))

    if mode_hors_ligne:
        # Visible reminder throughout the run that nothing will reach SKEZIA -- without this,
        # a user could easily forget which mode they picked on the previous window by the
        # time processing is halfway done.
        pass  # rappel déjà donné en permanence par le bandeau coloré ci-dessus

    label_message = ttk.Label(body, text="Initialisation de FREDDEX...", style="FreddexStatus.TLabel")
    label_message.pack(anchor="w", pady=(0, 2), fill="x")

    debut_traitement = time.time()
    label_temps = ttk.Label(body, text="Temps écoulé : 0:00", style="FreddexMuted.TLabel")
    label_temps.pack(anchor="w", pady=(0, 8))

    progress_bar = ttk.Progressbar(body, orient="horizontal", mode="indeterminate",
                                    style="Freddex.Horizontal.TProgressbar")
    progress_bar.pack(fill="x", pady=(0, 10))

    label_counter = ttk.Label(body, text="", style="FreddexMuted.TLabel")
    # (packed later, only once the first PROGRESS update arrives -- most imports never need
    # it for the fast stages, and an empty label reserving space would look like a glitch.)

    label_journal = ttk.Label(body, text="Journal de traitement :", style="FreddexMuted.TLabel")
    label_journal.pack(anchor="w")

    log_frame = ttk.Frame(body, style="Freddex.TFrame")
    log_frame.pack(fill="both", expand=True, pady=(4, 10))
    log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical")
    log_text = tk.Text(
        log_frame, wrap="word", state="disabled", height=10,
        bg=COLOR_LOG_BG, fg=COLOR_LOG_TEXT, insertbackground=COLOR_LOG_TEXT,
        font=("Consolas", 9), relief="flat", padx=8, pady=6,
        yscrollcommand=log_scrollbar.set,
    )
    log_scrollbar.configure(command=log_text.yview)
    log_scrollbar.pack(side="right", fill="y")
    log_text.pack(side="left", fill="both", expand=True)
    for emoji, couleur in LOG_TAG_COLORS.items():
        log_text.tag_configure(emoji, foreground=couleur)

    # Result of the final screen, read by the caller once root.mainloop() below returns.
    resultat_ecran = {"action": ACTION_QUITTER, "message": ""}

    def demander_nouvel_import():
        resultat_ecran["action"] = ACTION_NOUVEL_IMPORT
        root.destroy()

    def demander_etape2():
        resultat_ecran["action"] = ACTION_ETAPE2
        root.destroy()

    def quitter_apres_traitement():
        resultat_ecran["action"] = ACTION_QUITTER
        root.destroy()

    boutons_fin = ttk.Frame(body, style="Freddex.TFrame")
    # (packed later, once processing has finished -- see finaliser_UI)
    if dossier_logs is not None:
        ttk.Button(boutons_fin, text="Ouvrir le dossier des journaux",
                   command=lambda: _ouvrir_dossier(dossier_logs), style="FreddexGhost.TButton").pack(side="left")
    bouton_etape2 = ttk.Button(boutons_fin, text="Étape 2/2 : ajouter les ID SKEZIA  →",
                               command=demander_etape2, style="FreddexPrimary.TButton")
    # (affiché par finaliser_UI, seulement si l'étape 1/2 a réussi)
    bouton_retour = ttk.Button(boutons_fin, text="Retour à l'accueil", command=demander_nouvel_import,
                               style="Freddex.TButton")
    bouton_retour.pack(side="right")
    ttk.Button(boutons_fin, text="Quitter", command=quitter_apres_traitement,
               style="FreddexGhost.TButton").pack(side="right", padx=(0, 8))

    cancel_event = threading.Event()
    sending_started = {"value": False}  # True once SKEZIA sending is known to have begun

    def demander_annulation():
        """
        Confirms and requests cancellation. The warning wording differs depending on whether
        SKEZIA sending has already started (sending_started), because cancelling then cannot
        undo work already done: in-flight and already-completed patients stay
        created/sent -- only patients not yet started are skipped -- leaving a partial import
        rather than a clean stop. See skezia_api.create_patient_profiles /
        send_questionnaire_responses for how cancel_event is checked cooperatively.
        """
        if sending_started["value"]:
            message = (
                "FREDDEX est en train d'envoyer des données à SKEZIA.\n\n"
                "Annuler maintenant n'interrompt pas les envois déjà en cours : les patients "
                "déjà créés ou déjà envoyés le resteront. L'import sera donc PARTIEL "
                "(certains patients traités, d'autres non).\n\n"
                "Voulez-vous vraiment annuler ?"
            )
        else:
            message = (
                "Voulez-vous vraiment annuler le traitement en cours ?\n\n"
                "Aucune donnée n'a encore été envoyée à SKEZIA à ce stade : l'annulation sera "
                "propre, rien ne sera transmis."
            )
        if messagebox.askyesno("FREDDEX — Confirmer l'annulation", message, parent=root):
            cancel_event.set()
            bouton_annuler.config(state="disabled", text="Annulation demandée...")
            label_message.config(text="⏳ Annulation demandée — finalisation en cours...")

    bouton_annuler = ttk.Button(body, text="Annuler", command=demander_annulation, style="FreddexGhost.TButton")
    bouton_annuler.pack(pady=(0, 4))

    def append_log_lines(lines):
        if not lines:
            return
        log_text.configure(state="normal")
        for line in lines:
            tag = next((emoji for emoji in LOG_TAG_COLORS if line.startswith(emoji)), None)
            log_text.insert("end", line + "\n", tag if tag else ())

        nb_lignes = int(log_text.index("end-1c").split(".")[0])
        if nb_lignes > MAX_LOG_LINES:
            log_text.delete("1.0", f"{nb_lignes - MAX_LOG_LINES}.0")
        log_text.configure(state="disabled")
        log_text.see("end")  # auto-scroll to the latest line

    relay_holder = {}  # populated once the background thread installs its ProgressRelay
    polling_state = {"active": True}  # cleared by finaliser_UI to stop the poll loop

    def apply_progress_update(stage, current, total):
        """
        Switches the progress bar from its default bouncing/indeterminate mode to a real,
        determinate one showing current/total, for the phases that report it (patient
        creation, questionnaire sending -- see STAGE_LABELS). Earlier phases have no such
        marker and simply keep bouncing, which is fine: they take seconds, not minutes.
        """
        sending_started["value"] = True  # these markers only ever fire during SKEZIA sending
        if str(progress_bar["mode"]) != "determinate":
            progress_bar.stop()
            progress_bar.config(mode="determinate")
            if not label_counter.winfo_ismapped():
                label_counter.pack(anchor="w", pady=(0, 4), before=progress_bar)

        progress_bar.config(maximum=max(total, 1))
        progress_bar["value"] = current
        stage_label = STAGE_LABELS.get(stage, stage)
        label_counter.config(text=f"{stage_label} : {current} / {total}")

    def poll_progress():
        relay = relay_holder.get("relay")
        if relay is not None:
            new_lines = relay.drain()
            append_log_lines(new_lines)
            if new_lines:
                label_message.config(text=new_lines[-1])
            progress_updates = relay.drain_progress()
            if progress_updates:
                apply_progress_update(*progress_updates[-1])  # only the latest matters
        elapsed = int(time.time() - debut_traitement)
        label_temps.config(text=f"Temps écoulé : {elapsed // 60}:{elapsed % 60:02d}")
        if polling_state["active"]:
            root.after(POLL_INTERVAL_MS, poll_progress)

    def execution_calcul():
        """
        Runs the processing function on a background thread while a ProgressRelay captures
        its print() output for the polling loop above to display live, then safely updates
        the UI on the main thread once execution is completed or if an error occurs.
        """
        try:
            # Only pass cancel_event / mode_hors_ligne if fonction_de_traitement's signature
            # actually accepts them (see the docstring above) -- keeps this module usable
            # with any pipeline function, aware of these options or not, without raising a
            # TypeError here.
            extra_kwargs = {}
            try:
                accepted_params = inspect.signature(fonction_de_traitement).parameters
            except (TypeError, ValueError):
                accepted_params = {}
            if "cancel_event" in accepted_params:
                extra_kwargs["cancel_event"] = cancel_event
            if "mode_hors_ligne" in accepted_params:
                extra_kwargs["mode_hors_ligne"] = mode_hors_ligne

            resultat, _ = run_with_relay(
                fonction_de_traitement, fichier_selectionne,
                # Populate relay_holder the instant the relay exists, before
                # fonction_de_traitement starts running -- not after it returns. Without
                # this, poll_progress() finds relay_holder["relay"] still None for the
                # entire run and the log panel only fills in retroactively at the end.
                on_relay_ready=lambda relay: relay_holder.__setitem__("relay", relay),
                **extra_kwargs,
            )
        except Exception as e:
            # This is a defensive catch-all around the threading/relay plumbing itself, not
            # the pipeline's own logic -- traitement_complet() already catches its own
            # errors and returns a friendly message (see its "except Exception:" block), so
            # anything landing here is by definition unexpected (an error in this harness, or
            # in a fonction_de_traitement that doesn't follow that pattern). The raw
            # exception text (e.g. "KeyError: 'fr_pays'") means nothing to someone who isn't
            # reading the code, so the GUI shows a plain message instead -- the technical
            # detail still goes to the log file, exactly like every other print() in the
            # pipeline (see setup_logging()), since run_with_relay's own "finally" has
            # already restored stdout to what it was before processing started.
            print(f"❌ Erreur inattendue dans le thread de traitement : {e}")
            traceback.print_exc()
            resultat = ("❌ Une erreur inattendue s'est produite pendant le traitement.\n"
                        "Veuillez consulter le fichier de log pour plus de détails.")
        finally:
            root.after(0, lambda: finaliser_UI(resultat))

    def finaliser_UI(message):
        """
        Finalizes the UI after processing completion: drains any remaining progress lines,
        stops the progress bar, shows the color-coded final message, and displays the
        "Nouvel import" / "Quitter" buttons (plus "Ouvrir le dossier des journaux" if
        dossier_logs was given).
        """
        polling_state["active"] = False
        # One last precise update: poll_progress() only ticks every POLL_INTERVAL_MS, so the
        # elapsed time shown could otherwise lag the real completion time by up to that much.
        try:
            if label_temps.winfo_exists():
                elapsed = int(time.time() - debut_traitement)
                label_temps.config(text=f"Temps écoulé : {elapsed // 60}:{elapsed % 60:02d}")
        except tk.TclError:
            pass
        relay = relay_holder.get("relay")
        if relay is not None:
            append_log_lines(relay.drain())
            progress_updates = relay.drain_progress()
            if progress_updates:
                apply_progress_update(*progress_updates[-1])  # final tally, e.g. 50/50
        try:
            if progress_bar.winfo_exists():
                progress_bar.stop()
        except tk.TclError:
            pass
        progress_bar.pack_forget()
        bouton_annuler.pack_forget()
        try:
            if label_counter.winfo_ismapped():
                label_counter.pack_forget()  # the running "n/total" counter is redundant now
        except tk.TclError:
            pass

        resultat_ecran["message"] = message
        suite_a_afficher = []
        if proposer_etape2 and message.startswith("✅"):
            # Étape suivante proposée d'office : bouton principal tout à droite + rappel de la marche à suivre.
            bouton_etape2.pack(side="right", padx=(8, 0), before=bouton_retour)
            suite_a_afficher.append(
                "Suite : importez d'abord le fichier de participants dans la plateforme, exportez la "
                "liste des participants (Answers.csv), puis cliquez sur « Étape 2/2 ».")

        titre, patients, autres = _decouper_message_final(message)

        if patients:

            label_journal.pack_forget()
            log_frame.pack_forget()
            label_message.config(text=titre, foreground=_status_color(message))

            resume_frame = ttk.Frame(body, style="Freddex.TFrame")
            resume_frame.pack(side="top", fill="both", expand=True, pady=(6, 8))

            legende = f"{len(patients)} patient(s) traité(s)"
            if autres:
                legende += " — " + " ".join(autres)
            ttk.Label(resume_frame, text=legende, style="FreddexMuted.TLabel").pack(
                anchor="w", pady=(0, 6))

            table_frame = ttk.Frame(resume_frame, style="FreddexCard.TFrame")
            table_frame.pack(fill="both", expand=True)
            tree = ttk.Treeview(table_frame, columns=("id", "nom", "prenom"), show="headings",
                                style="Freddex.Treeview", height=8)
            tree.heading("id", text="ID BaMaRa")
            tree.heading("nom", text="Nom")
            tree.heading("prenom", text="Prénom")
            tree.column("id", width=120, anchor="w", stretch=False)
            tree.column("nom", width=240, anchor="w")
            tree.column("prenom", width=170, anchor="w")
            tree.tag_configure("pair", background=COLOR_CARD)
            tree.tag_configure("impair", background=COLOR_ROW_ALT)
            for i, (ident, nom, prenom) in enumerate(patients):
                tree.insert("", "end", values=(ident, nom, prenom),
                            tags=("impair" if i % 2 else "pair",))
            table_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=table_scroll.set)
            table_scroll.pack(side="right", fill="y")
            tree.pack(side="left", fill="both", expand=True)
        else:
            # No per-patient list (error, "already present", offline note, cancellation before
            # sending...): keep the journal visible for context and show the whole message.
            label_message.config(text=message, foreground=_status_color(message))

        boutons_fin.pack(side="bottom", fill="x", pady=(8, 0))
        for texte_suite in suite_a_afficher:
            ttk.Label(body, text=texte_suite, style="FreddexMuted.TLabel", justify="left",
                      wraplength=560).pack(side="bottom", anchor="w", pady=(4, 0), after=boutons_fin)
        try:
            # Re-fit so the final buttons stay visible. With the summary now in a fixed-height
            # scrollable table (not an unbounded label), the window height stays sane no matter
            # how many patients were imported. Already mapped: this only grows, never shrinks.
            _dimensionner_fenetre(root)
        except tk.TclError:
            pass
        root.attributes("-topmost", True)
        root.attributes("-topmost", False)  # bring to front once, then behave like a normal window

    progress_bar.start()
    relay_holder["relay"] = None
    # largeur_min/hauteur_min keep this window at its previous comfortable default size (the
    # log panel benefits from real estate even when today's content would technically fit in
    # less) -- see _dimensionner_fenetre's docstring: it still guarantees the action buttons
    # stay visible if content ever needed more than that.
    _dimensionner_fenetre(root, largeur_min=640, hauteur_min=480)

    def fermeture_native():
        """
        Handles the OS titlebar's native close button (✕). Tk's default handling would destroy
        the window outright; while processing is running (polling_state still active) that
        would kill the GUI without ever checking cancel_event, so an in-flight SKEZIA send
        would keep going in the background with no warning to the user and none of the
        "import PARTIEL" messaging that demander_annulation provides. Routing it through the exact same confirmation as the
        "Annuler" button closes that gap. Once processing has already finished (the buttons
        below are showing), nothing is in flight any more, so closing directly is safe and
        equivalent to "Quitter".
        """
        if polling_state["active"]:
            demander_annulation()
        else:
            quitter_apres_traitement()

    root.protocol("WM_DELETE_WINDOW", fermeture_native)

    root.after(300, lambda: threading.Thread(target=execution_calcul, daemon=True).start())
    root.after(POLL_INTERVAL_MS, poll_progress)
    root.grab_set()
    parent.wait_window(root)
    return resultat_ecran["action"], resultat_ecran["message"]


def afficher_bienvenue_et_lancer(fonction_de_traitement, fonction_apercu=None, dossier_logs=None, root=None,
                                 fonction_liaison=None):
    """
    Displays a lightweight welcome window and launches the main application workflow.
    The function creates a minimal Tkinter splash screen with a loading message and an indeterminate progress bar,
    then waits briefly before clearing the window and starting the full processing pipeline, including file selection
    and execution of the main treatment function.

    Parameters:
    fonction_de_traitement: function to execute after file selection (same contract as in
    lancer_interface_et_traitement). Passed in rather than hard-coded, so this module has no
    dependency on the specific FREDDEX business pipeline (modularity improvement).
    fonction_apercu: optional preview function, forwarded as-is to
    lancer_interface_et_traitement (see its docstring) -- shown on the confirmation screen
    before the real processing starts.
    dossier_logs: optional absolute path to the log folder, forwarded as-is to
    lancer_interface_et_traitement (see its docstring) -- lets the final screen offer an
    "Ouvrir le dossier des journaux" button.
    """
    if root is None:
        # Standalone/backward-compatible path: no shared root supplied, create our own. When
        # FREDDEX-base.py runs the first-run key setup, it instead creates the single root via
        # creer_root() and passes it in, so key setup and this welcome share ONE tk.Tk() (see
        # the architecture note above and ).
        root = tk.Tk()
        _configure_style()
    root.title("FREDDEX")
    root.configure(bg=COLOR_HEADER_BG)
    root.deiconify()  # creer_root() leaves the shared root hidden; show it for the welcome

    tk.Label(root, text="🚀 FREDDEX", font=("Segoe UI", 16, "bold"),
             bg=COLOR_HEADER_BG, fg=COLOR_HEADER_TEXT).pack(expand=True, pady=(24, 4))
    tk.Label(root, text="Chargement en cours...", font=("Segoe UI", 10),
             bg=COLOR_HEADER_BG, fg=COLOR_HEADER_TEXT).pack()

    pb = ttk.Progressbar(root, mode="indeterminate", length=220,
                          style="Freddex.Horizontal.TProgressbar")
    pb.pack(pady=16)
    pb.start()

    def charger_la_suite():
        """
        Clears the welcome window's content and hides it (it becomes the invisible anchor for
        every later Toplevel screen, not a visible window of its own any more), then starts
        the main processing workflow. root.destroy() only happens once that whole workflow
        returns (the user chose "Quitter", or cancelled at file selection) -- i.e. once, at
        the true end of the application, not once per screen or per import as before.
        """
        for widget in root.winfo_children():
            widget.destroy()
        root.withdraw()
        lancer_interface_et_traitement(root, fonction_de_traitement, fonction_apercu=fonction_apercu,
                                        dossier_logs=dossier_logs, fonction_liaison=fonction_liaison)
        root.destroy()

    _dimensionner_fenetre(root, largeur_min=320, hauteur_min=180)  # also centers it, see docstring

    # wait 500ms to allow the window to be displayed before starting the loading process
    root.after(500, charger_la_suite)
    root.mainloop()
