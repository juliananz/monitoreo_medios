"""
Migration 002: Analytical schema - normalized tables for analytics.

Creates:
- temas: Topic catalog
- regiones: Geographic hierarchy
- entidades: Normalized entity catalog
- noticia_tema: Many-to-many news ↔ topics
- noticia_entidad: Many-to-many news ↔ entities
- entidad_alias: For entity normalization lookups

Also adds indexes for temporal queries.
"""

import sqlite3
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

logger = logging.getLogger(__name__)


def run_migration():
    """Create analytical schema tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # =========================================================================
    # REFERENCE TABLES
    # =========================================================================

    # TEMAS: Topic catalog
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT UNIQUE NOT NULL,
            descripcion TEXT,
            palabras_clave TEXT,
            activo INTEGER DEFAULT 1,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Created table: temas")

    # REGIONES: Geographic hierarchy
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS regiones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            nombre_normalizado TEXT NOT NULL,
            tipo TEXT NOT NULL CHECK(tipo IN ('pais', 'estado', 'ciudad')),
            pais TEXT DEFAULT 'Mexico',
            codigo TEXT,
            es_objetivo INTEGER DEFAULT 0,
            UNIQUE(nombre_normalizado, tipo)
        );
    """)
    logger.info("Created table: regiones")

    # ENTIDADES: Normalized entity catalog
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entidades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_canonico TEXT UNIQUE NOT NULL,
            tipo TEXT NOT NULL CHECK(tipo IN ('PER', 'ORG', 'LOC', 'MISC')),
            es_clave INTEGER DEFAULT 0,
            categoria TEXT,
            metadata TEXT,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            ultima_mencion DATE
        );
    """)
    logger.info("Created table: entidades")

    # ENTIDAD_ALIAS: For entity normalization
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entidad_alias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entidad_id INTEGER NOT NULL REFERENCES entidades(id) ON DELETE CASCADE,
            alias TEXT NOT NULL,
            es_principal INTEGER DEFAULT 0,
            UNIQUE(alias)
        );
    """)
    logger.info("Created table: entidad_alias")

    # =========================================================================
    # JUNCTION TABLES
    # =========================================================================

    # NOTICIA_TEMA: Many-to-many news ↔ topics
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS noticia_tema (
            noticia_id INTEGER NOT NULL REFERENCES noticias(id) ON DELETE CASCADE,
            tema_id INTEGER NOT NULL REFERENCES temas(id) ON DELETE CASCADE,
            score INTEGER DEFAULT 1,
            fecha_asignacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (noticia_id, tema_id)
        );
    """)
    logger.info("Created table: noticia_tema")

    # NOTICIA_ENTIDAD: Many-to-many news ↔ entities
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS noticia_entidad (
            noticia_id INTEGER NOT NULL REFERENCES noticias(id) ON DELETE CASCADE,
            entidad_id INTEGER NOT NULL REFERENCES entidades(id) ON DELETE CASCADE,
            rol TEXT DEFAULT 'mencionado',
            frecuencia INTEGER DEFAULT 1,
            fecha_asignacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (noticia_id, entidad_id)
        );
    """)
    logger.info("Created table: noticia_entidad")

    # =========================================================================
    # ADD COLUMNS TO NOTICIAS
    # =========================================================================

    columns_to_add = [
        ("fecha_scraping", "DATETIME"),
        ("region_id", "INTEGER REFERENCES regiones(id)"),
        ("procesado_temas", "INTEGER DEFAULT 0"),
        ("procesado_ner", "INTEGER DEFAULT 0"),
        ("procesado_riesgo", "INTEGER DEFAULT 0"),
    ]

    for col_name, col_type in columns_to_add:
        try:
            cursor.execute(f"ALTER TABLE noticias ADD COLUMN {col_name} {col_type};")
            logger.info(f"Added column to noticias: {col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                logger.info(f"Column already exists: {col_name}")
            else:
                raise

    # =========================================================================
    # INDEXES FOR PERFORMANCE
    # =========================================================================

    indexes = [
        ("idx_noticias_fecha", "noticias", "fecha"),
        ("idx_noticias_region", "noticias", "region_id"),
        ("idx_noticias_relevante_fecha", "noticias", "relevante, fecha"),
        ("idx_noticia_tema_tema", "noticia_tema", "tema_id"),
        ("idx_noticia_entidad_entidad", "noticia_entidad", "entidad_id"),
        ("idx_entidad_alias_alias", "entidad_alias", "alias"),
        ("idx_regiones_nombre", "regiones", "nombre_normalizado"),
    ]

    for idx_name, table, columns in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns});")
            logger.info(f"Created index: {idx_name}")
        except sqlite3.OperationalError as e:
            logger.warning(f"Index {idx_name}: {e}")

    conn.commit()
    conn.close()

    logger.info("Migration 002 completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migration()
