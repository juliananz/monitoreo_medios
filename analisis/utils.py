"""
Shared utilities for analysis modules.
"""

import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from config.settings import DB_PATH


@contextmanager
def get_db_connection():
    """Context manager for SQLite database connections."""
    conn = sqlite3.connect(DB_PATH)
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
