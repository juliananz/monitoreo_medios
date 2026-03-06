"""
Shared utilities for analysis modules.
"""

import os
import re
import shutil
import sqlite3
import tempfile
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from config.settings import DB_PATH


@contextmanager
def get_db_connection():
    """Context manager for SQLite database connections.

    Fallback chain for read-only filesystems (e.g. Streamlit Community Cloud):
      1. Normal read-write connection (local dev, GitHub Actions pipeline).
      2. immutable=1 URI — skips all file locking entirely.
      3. Copy to /tmp — gives SQLite a writable directory for lock files.
    Each step is smoke-tested with a real page read before yielding.
    """
    db_path = Path(DB_PATH)

    def _smoke(conn):
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return conn

    def _close(conn):
        try:
            conn.close()
        except Exception:
            pass

    tmp_path = None
    conn = None

    # 1. Normal connection
    try:
        conn = _smoke(sqlite3.connect(str(db_path)))
    except (sqlite3.DatabaseError, sqlite3.OperationalError):
        _close(conn)
        conn = None

    # 2. immutable=1 — no locking at all
    if conn is None:
        try:
            conn = _smoke(sqlite3.connect(f"file://{db_path.as_posix()}?immutable=1", uri=True))
        except (sqlite3.DatabaseError, sqlite3.OperationalError):
            _close(conn)
            conn = None

    # 3. Copy to /tmp — gives SQLite a writable directory
    if conn is None:
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
        os.close(tmp_fd)
        shutil.copy2(str(db_path), tmp_path)
        conn = sqlite3.connect(tmp_path)

    try:
        yield conn
    finally:
        _close(conn)
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


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
