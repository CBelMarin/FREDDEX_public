# -*- coding: utf-8 -*-
# FREDDEX - resource path resolution
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License

import sys
from pathlib import Path


def resource_path(relative_path):
    """
    Returns the absolute path to a resource, handling both standard Python execution and
    PyInstaller bundled execution (where bundled files are extracted to a temporary
    directory referenced by sys._MEIPASS).
    """
    try:
        base_path = Path(sys._MEIPASS)
    except AttributeError:
        base_path = Path(__file__).resolve().parent
    return base_path / relative_path
