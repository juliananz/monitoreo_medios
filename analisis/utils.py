"""
Shared utilities for analysis modules.
"""

import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from config.settings import DB_PATH


@contextmanager
def get_db_connection():
    """Context manager for SQLite database connections.

    Opens in read-only mode when the DB directory is not writable (e.g.
    Streamlit Community Cloud mounts the repo as read-only).  Falls back to
    normal read-write mode for the pipeline and local development.
    """
    db_path = Path(DB_PATH)
    if not os.access(db_path.parent, os.W_OK):
        # Read-only filesystem (Streamlit Cloud) — use URI read-only mode so
        # SQLite does not attempt to create journal or lock files.
        uri = db_path.as_posix()
        if not uri.startswith("/"):
            uri = "/" + uri
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(str(db_path))
    try:
        yield conn
    finally:
        conn.close()


def normalizar_texto(texto: str) -> str:
    """Normalize text for keyword matching (strips punctuation and accents)."""
    if not texto:
        return ""
    texto = texto.lower()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^\w\s]", " ", texto)
    return texto


def normalizar_entidad(texto: str) -> str:
    """Normalize text for entity lookups (strips accents, case-insensitive)."""
    if not texto:
        return ""
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def clasificar_tipo(riesgo: int, oportunidad: int) -> str:
    """
    Classify news type based on risk/opportunity flags.

    Returns: MIXTO, RIESGO, OPORTUNIDAD, or NEUTRO
    """
    if riesgo == 1 and oportunidad == 1:
        return "MIXTO"
    if riesgo == 1:
        return "RIESGO"
    if oportunidad == 1:
        return "OPORTUNIDAD"
    return "NEUTRO"
