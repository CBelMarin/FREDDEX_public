# -*- coding: utf-8 -*-
# FREDDEX - logging setup
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License

import os
import sys
from datetime import datetime
from threading import Lock

# Shared lock used by every module that prints from multiple threads (SKEZIA API calls),
# so that concurrent print() calls do not interleave inside the log file.
print_lock = Lock()


log_dir = os.path.abspath("logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"rapport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
error_file = os.path.join(log_dir, "error_log.txt")

MAX_RAPPORTS_CONSERVES = 30


def _purger_anciens_rapports(dossier=None, max_fichiers=None):
    """
    Deletes the oldest "rapport_*.txt" files in dossier (defaults to log_dir), keeping only
    the max_fichiers most recent ones (defaults to MAX_RAPPORTS_CONSERVES). Filenames embed a
    YYYYMMDD_HHMMSS timestamp (see log_file above), so plain lexicographic sort is already
    chronological order -- no need to read file metadata.

    Both defaults are resolved from the module globals INSIDE the function body rather than as
    ordinary default parameter values -- ordinary defaults are bound once, at import time, so
    a default of "dossier=log_dir" or "max_fichiers=MAX_RAPPORTS_CONSERVES" would silently keep
    using whatever log_dir/MAX_RAPPORTS_CONSERVES were at import time even if something (a
    test, a future feature) legitimately changes them afterward.

    Best-effort and silent: any failure (permissions, a file already removed by something
    else, an unreadable folder...) is ignored rather than raised. This is routine housekeeping,
    called automatically at the start of every run (see setup_logging) -- it must never be the
    reason FREDDEX fails to start.
    """
    dossier = dossier if dossier is not None else log_dir
    max_fichiers = max_fichiers if max_fichiers is not None else MAX_RAPPORTS_CONSERVES
    try:
        rapports = sorted(
            nom for nom in os.listdir(dossier)
            if nom.startswith("rapport_") and nom.endswith(".txt")
        )
    except OSError:
        return

    a_supprimer = rapports[:-max_fichiers] if max_fichiers > 0 else rapports
    for nom_fichier in a_supprimer:
        try:
            os.remove(os.path.join(dossier, nom_fichier))
        except OSError:
            pass


def setup_logging():
    """
    Redirects stdout/stderr to the log files defined above, so that every print() call
    (progress messages, warnings, errors) ends up in a persistent, dated report file.

    Also purges old rapport_*.txt files beyond MAX_RAPPORTS_CONSERVES (see
    _purger_anciens_rapports) -- done here, at the one point that marks "a real run is
    starting" (as opposed to logging_utils simply being imported, e.g. by tests), and before
    opening this run's own new log file so that file is never at risk of being purged.
    """
    _purger_anciens_rapports()
    sys.stdout = open(log_file, "w", encoding="utf-8")
    sys.stderr = open(error_file, "w", encoding="utf-8")
