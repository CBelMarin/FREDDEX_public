# -*- coding: utf-8 -*-
# FREDDEX - first-run SKEZIA key setup (Windows registry persistence)
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Why this module exists: skezia_api.charger_token_manager() reads the Fernet decryption key
# from the FREDDEX_FERNET_KEY_<numero_centre> environment variable, and that variable must
# never be bundled inside the .exe together with the encrypted secrets it protects (see the
# security note in skezia_api.py). In practice this meant every clinical center receiving a
# FREDDEX .exe had to manually create a Windows environment variable before first use -- fine
# for a technical user, but centers here have no IT contact, only clinical/administrative
# staff. This module removes that requirement: on first launch, if no key is found, a small
# window asks the person to paste the key they were given ONCE, then FREDDEX persists it
# itself (same mechanism as the `setx` command line tool) so every later launch just works.

import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox

from cryptography.fernet import Fernet

from gui import (
    COLOR_BG, COLOR_ERROR,
    _configure_style, _entete, _dimensionner_fenetre,
)
from paths import resource_path


def _env_var_name(numero_centre):
    """Same naming convention as skezia_api.charger_token_manager -- kept in one place here
    so both modules can never drift apart."""
    return f"FREDDEX_FERNET_KEY_{numero_centre}"


def cle_disponible(numero_centre):
    """
    True if a (non-empty) Fernet key is already available for numero_centre in the CURRENT
    process' environment -- i.e. charger_token_manager() would find one without falling back
    to the local files/cles/secret_<n>.key test file. Pure check, no GUI: safe to call from
    any thread, including the background processing thread (see FREDDEX-base.traitement_complet).
    """
    return bool(os.environ.get(_env_var_name(numero_centre)))


def _cle_dechiffre_secrets(numero_centre, cle_str):
    """
    True only if cle_str is BOTH a well-formed Fernet key AND actually the correct key to
    decrypt THIS center's files/cles/secrets_<numero_centre>.enc.

    Returns False for a badly-formed key, or a well-formed key that fails to decrypt
    secrets_<numero_centre>.enc (wrong key, or a key meant for a different center). Returns
    True only once decryption genuinely succeeds -- or, as a lenient fallback, if
    secrets_<numero_centre>.enc itself cannot be found/read (nothing real to test against in
    that case; format alone is accepted rather than blocking setup entirely for what is then
    a separate, missing-resource problem -- see config.initialiser_ressources, which already
    reports that clearly on the next import attempt). Never raises.
    """
    try:
        fernet = Fernet(cle_str.encode())
    except Exception:
        return False

    enc_path = resource_path(f"files/cles/secrets_{numero_centre}.enc")
    try:
        with open(enc_path, "rb") as f:
            encrypted_data = f.read()
    except OSError:
        return True  # nothing to test the key against -- see docstring

    try:
        fernet.decrypt(encrypted_data)
        return True
    except Exception:
        return False


def enregistrer_cle_windows(numero_centre, cle):
    """
    Persists the Fernet key for numero_centre so every future launch of this .exe finds it
    automatically, with NO environment-variable knowledge required from the person running it.

    Three steps:
    1) Write it to HKCU\\Environment (the per-user registry key) -- exactly what the `setx`
       command line tool does under the hood. Survives reboots and future launches, and does
       NOT require Administrator rights (HKCU is per-user, unlike HKLM).
    2) Set it immediately in this process' own os.environ. Registry/setx changes are only
       picked up by processes started AFTER the change is made -- without this step, the very first launch would still fail to find the
       key it just "saved", which would defeat the entire point of this screen.
    3) Best-effort: broadcast WM_SETTINGCHANGE so other already-running Windows programs
       (e.g. a terminal window left open) can pick up the change too. Never critical -- FREDDEX
       itself already has the key via step 2 regardless of whether this succeeds.

    Raises RuntimeError on any non-Windows platform (the `winreg` module used here does not
    exist there). FREDDEX only ships as a Windows .exe for its clinical centers, so this is
    not expected to run anywhere else -- but it fails loudly instead of silently doing nothing
    if it somehow does.
    """
    if sys.platform != "win32":
        raise RuntimeError(
            "L'enregistrement automatique de la clé n'est possible que sous Windows."
        )

    import winreg  # Windows-only stdlib module -- imported here, not at module top, so this
    # file can still be imported (though not fully used) if anything ever inspects it on a
    # non-Windows machine, e.g. this project's Linux-based test/CI environment.

    env_var = _env_var_name(numero_centre)

    # Step 1 -- persist to the registry (HKCU\Environment), same effect as `setx VAR value`.
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment", 0, winreg.KEY_SET_VALUE) as key:
        winreg.SetValueEx(key, env_var, 0, winreg.REG_SZ, cle)

    # Step 2 -- make it usable immediately in the current process, no restart needed.
    os.environ[env_var] = cle

    # Step 3 -- best-effort notification to other already-running processes.
    try:
        import ctypes
        HWND_BROADCAST = 0xFFFF
        WM_SETTINGCHANGE = 0x001A
        SMTO_ABORTIFHUNG = 0x0002
        result = ctypes.c_long()
        ctypes.windll.user32.SendMessageTimeoutW(
            HWND_BROADCAST, WM_SETTINGCHANGE, 0, "Environment",
            SMTO_ABORTIFHUNG, 5000, ctypes.byref(result),
        )
    except Exception:
        pass  # purely a convenience for other already-open programs, never critical


def _afficher_ecran_saisie_cle(numero_centre, root=None):
    """
    Shows the first-run "paste your key" screen and returns True once a valid key has been
    validated AND saved, False if the user closed/cancelled the window instead.

    Deliberately minimal: one masked input field, one "Enregistrer" button. The person using
    this is clinical/administrative staff with no IT background and no IT contact at their
    center -- they only need to paste a string Camille sent them once, not understand what a
    Fernet key or an environment variable is.

    Single-Tk() architecture : when a shared application root is
    supplied (FREDDEX-base.py creates it via gui.creer_root() before this screen), this window
    is a tk.Toplevel of it, shown modally with wait_window() -- so the whole application uses a
    single tk.Tk(). Only when called standalone with no root (e.g. a unit test) does it fall
    back to its own tk.Tk() + mainloop(). This avoids creating a tk.Tk() here that is then
    destroyed before gui.py creates ANOTHER one for the welcome -- exactly the repeated
    create/destroy-Tcl-interpreter pattern that caused the second-import crash elsewhere,
    here on the first-launch path this module exists for.

    Must run on the MAIN thread (see FREDDEX-base.py's __main__, which calls
    assurer_cle_disponible() before the background processing thread is ever started).
    """
    resultat = {"success": False}

    proprietaire_root = root is None
    if proprietaire_root:
        win = tk.Tk()
        _configure_style()
    else:
        win = tk.Toplevel(root)
    win.title("FREDDEX - Configuration initiale")
    win.configure(bg=COLOR_BG)
    win.attributes("-topmost", True)
    win.after(200, lambda: win.attributes("-topmost", False))

    _entete(win, "FREDDEX — Configuration initiale")

    body = ttk.Frame(win, style="Freddex.TFrame", padding=(20, 18))
    body.pack(side="top", fill="both", expand=True)

    ttk.Label(
        body,
        text="Première utilisation de FREDDEX sur cet ordinateur.\n\n"
             "Collez ci-dessous la clé de sécurité qui vous a été transmise pour votre centre. "
             "Elle ne vous sera demandée qu'une seule fois : FREDDEX l'enregistrera "
             "automatiquement pour tous les prochains lancements.",
        style="FreddexStatus.TLabel",
    ).pack(anchor="w", pady=(0, 12), fill="x")

    entree_cle = ttk.Entry(body, show="*", width=60)
    entree_cle.pack(anchor="w", pady=(0, 4), fill="x")
    entree_cle.focus_set()

    label_erreur = ttk.Label(body, text="", style="FreddexMuted.TLabel")
    label_erreur.pack(anchor="w", pady=(4, 0), fill="x")

    def enregistrer():
        cle = entree_cle.get().strip()
        if not cle:
            label_erreur.config(text="⚠️ Merci de coller la clé avant de continuer.",
                                 foreground=COLOR_ERROR)
            _dimensionner_fenetre(win)
            return
        if not _cle_dechiffre_secrets(numero_centre, cle):
            label_erreur.config(
                text="⚠️ Cette clé ne permet pas de déchiffrer les identifiants de ce centre. "
                     "Vérifiez qu'il s'agit bien de la clé transmise pour CE centre "
                     "(numéro de centre correct), et qu'elle a été copiée en entier, sans "
                     "espace ni retour à la ligne supplémentaire.",
                foreground=COLOR_ERROR,
            )
            # This message wraps onto several lines -- re-fit so it doesn't push the
            # "Enregistrer"/"Quitter" buttons below the bottom edge (see
            # gui._dimensionner_fenetre's docstring). Window already mapped: grows only.
            _dimensionner_fenetre(win)
            return
        try:
            enregistrer_cle_windows(numero_centre, cle)
        except Exception as e:
            messagebox.showerror(
                "FREDDEX — Erreur",
                f"Impossible d'enregistrer la clé : {e}\n\n"
                "Contactez l'équipe FREDD si le problème persiste.",
                parent=win,
            )
            return
        resultat["success"] = True
        win.destroy()

    def quitter():
        win.destroy()

    boutons = ttk.Frame(body, style="Freddex.TFrame")
    boutons.pack(side="bottom", fill="x", pady=(16, 0))
    ttk.Button(boutons, text="Enregistrer", command=enregistrer, style="FreddexPrimary.TButton").pack(side="right")
    ttk.Button(boutons, text="Quitter", command=quitter, style="FreddexGhost.TButton").pack(side="right", padx=(0, 8))

    win.bind("<Return>", lambda event: enregistrer())

    _dimensionner_fenetre(win, largeur_min=560)
    if proprietaire_root:
        win.mainloop()
    else:
        # Shared root: show this screen modally on top of it, without a nested mainloop
        # (the single mainloop lives in afficher_bienvenue_et_lancer, run later).
        win.grab_set()
        root.wait_window(win)
    return resultat["success"]


def assurer_cle_disponible(numero_centre, root=None):
    """
    Ensures a SKEZIA Fernet key is available for numero_centre before FREDDEX needs it,
    showing the first-run setup screen if it is currently missing.

    root : the shared application tk.Tk() (from gui.creer_root()),
    forwarded to the setup screen so it opens as a tk.Toplevel of it instead of a separate
    tk.Tk(). Optional and backward-compatible: called without it (e.g. a unit test), the
    screen falls back to its own tk.Tk().

    Returns True if a key is now available (it already was, or the user just entered and
    saved one), False if the user closed the setup screen without providing one -- in which
    case FREDDEX still starts normally (see FREDDEX-base.py's __main__), but any later attempt
    at a real (non offline-mode) import will fail with a clear message instead of a cryptic
    one (see the cle_disponible() check inside traitement_complet).
    """
    if cle_disponible(numero_centre):
        return True
    return _afficher_ecran_saisie_cle(numero_centre, root=root)
