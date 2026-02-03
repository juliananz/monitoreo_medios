"""
Migration 001: Add NER-related columns to noticias table.

This migration adds columns for:
- personas: Detected person names
- organizaciones: Detected organization names
- lugares: Detected location names
- nivel_geografico: Inferred geographic level
- requiere_analisis_profundo: Flag for deep analysis

Note: Run this only once on existing databases created before NER support.
New databases created with crear_base_datos() already include these columns.
"""

import sqlite3
import logging
from pathlib import Path

# Use relative path resolution (portable)
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

logger = logging.getLogger(__name__)


def run_migration():
    """Add NER columns to existing database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    columns_to_add = [
        ("personas", "TEXT"),
        ("organizaciones", "TEXT"),
        ("lugares", "TEXT"),
        ("nivel_geografico", "TEXT"),
        ("requiere_analisis_profundo", "INTEGER DEFAULT 0"),
    ]

    for col_name, col_type in columns_to_add:
        try:
            cursor.execute(f"ALTER TABLE noticias ADD COLUMN {col_name} {col_type};")
            logger.info(f"Added column: {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                logger.info(f"Column already exists: {col_name}")
            else:
                raise

    conn.commit()
    conn.close()

    logger.info("Migration 001 completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migration()
