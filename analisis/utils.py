"""
Shared utilities for analysis modules.
"""

import sqlite3
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
