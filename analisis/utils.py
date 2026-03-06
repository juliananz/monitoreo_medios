"""
Shared utilities for analysis modules.
"""

import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from config.settings import DB_PATH


@contextmanager
def get_db_connection():
    """Context manager for SQLite database connections.

    Tries a normal read-write connection first.  If SQLite fails (e.g. on
    Streamlit Community Cloud where /mount/src/ is read-only and SQLite
    cannot create journal/lock files), automatically retries with
    immutable=1 URI mode which bypasses all locking entirely.
    """
    db_path = Path(DB_PATH)
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")  # smoke test — fails fast on read-only FS
    except (sqlite3.DatabaseError, sqlite3.OperationalError):
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        # Fallback: immutable read-only mode — no locking, no journal files.
        # Required on Streamlit Community Cloud where the repo is mounted
        # read-only and SQLite cannot acquire file locks.
        conn = sqlite3.connect(f"file://{db_path.as_posix()}?immutable=1", uri=True)
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
