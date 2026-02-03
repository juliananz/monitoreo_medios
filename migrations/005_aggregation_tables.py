"""
Migration 005: Aggregation tables for pre-computed analytics.

Creates:
- agregacion_diaria: Global daily metrics
- agregacion_tema_diaria: Per-topic daily metrics
- agregacion_region_diaria: Per-region daily metrics
- agregacion_entidad_diaria: Per-entity daily metrics
- agregacion_medio_diaria: Per-media-source daily metrics

All tables use INSERT OR REPLACE pattern for idempotent updates.
"""

import sqlite3
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

logger = logging.getLogger(__name__)


def run_migration():
    """Create aggregation tables and indexes."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # =========================================================================
    # AGGREGATION TABLES
    # =========================================================================

    # AGREGACION_DIARIA: Global daily metrics
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregacion_diaria (
            fecha DATE PRIMARY KEY,
            total_noticias INTEGER DEFAULT 0,
            total_relevantes INTEGER DEFAULT 0,
            total_riesgo INTEGER DEFAULT 0,
            total_oportunidad INTEGER DEFAULT 0,
            total_mixto INTEGER DEFAULT 0,
            medios_activos INTEGER DEFAULT 0,
            requieren_analisis INTEGER DEFAULT 0,
            fecha_calculo DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Created table: agregacion_diaria")

    # AGREGACION_TEMA_DIARIA: Per-topic daily metrics
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregacion_tema_diaria (
            fecha DATE NOT NULL,
            tema_id INTEGER NOT NULL REFERENCES temas(id) ON DELETE CASCADE,
            total_noticias INTEGER DEFAULT 0,
            total_riesgo INTEGER DEFAULT 0,
            total_oportunidad INTEGER DEFAULT 0,
            score_promedio REAL DEFAULT 0.0,
            PRIMARY KEY (fecha, tema_id)
        );
    """)
    logger.info("Created table: agregacion_tema_diaria")

    # AGREGACION_REGION_DIARIA: Per-region daily metrics
    # Note: region_id can be NULL for news without region assignment
    # Use -1 as sentinel value for NULL region_id to enable proper primary key
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregacion_region_diaria (
            fecha DATE NOT NULL,
            region_id INTEGER DEFAULT -1,
            nivel_geografico TEXT NOT NULL,
            total_noticias INTEGER DEFAULT 0,
            total_riesgo INTEGER DEFAULT 0,
            total_oportunidad INTEGER DEFAULT 0,
            PRIMARY KEY (fecha, region_id, nivel_geografico)
        );
    """)
    logger.info("Created table: agregacion_region_diaria")

    # AGREGACION_ENTIDAD_DIARIA: Per-entity daily metrics
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregacion_entidad_diaria (
            fecha DATE NOT NULL,
            entidad_id INTEGER NOT NULL REFERENCES entidades(id) ON DELETE CASCADE,
            menciones INTEGER DEFAULT 0,
            noticias_riesgo INTEGER DEFAULT 0,
            noticias_oportunidad INTEGER DEFAULT 0,
            frecuencia_total INTEGER DEFAULT 0,
            PRIMARY KEY (fecha, entidad_id)
        );
    """)
    logger.info("Created table: agregacion_entidad_diaria")

    # AGREGACION_MEDIO_DIARIA: Per-media-source daily metrics
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agregacion_medio_diaria (
            fecha DATE NOT NULL,
            medio TEXT NOT NULL,
            total_noticias INTEGER DEFAULT 0,
            total_relevantes INTEGER DEFAULT 0,
            total_riesgo INTEGER DEFAULT 0,
            total_oportunidad INTEGER DEFAULT 0,
            PRIMARY KEY (fecha, medio)
        );
    """)
    logger.info("Created table: agregacion_medio_diaria")

    # =========================================================================
    # INDEXES FOR PERFORMANCE
    # =========================================================================

    indexes = [
        # Global daily
        ("idx_agregacion_diaria_fecha", "agregacion_diaria", "fecha"),

        # Topic daily
        ("idx_agregacion_tema_fecha", "agregacion_tema_diaria", "fecha"),
        ("idx_agregacion_tema_tema", "agregacion_tema_diaria", "tema_id"),

        # Region daily
        ("idx_agregacion_region_fecha", "agregacion_region_diaria", "fecha"),
        ("idx_agregacion_region_nivel", "agregacion_region_diaria", "nivel_geografico"),

        # Entity daily
        ("idx_agregacion_entidad_fecha", "agregacion_entidad_diaria", "fecha"),
        ("idx_agregacion_entidad_entidad", "agregacion_entidad_diaria", "entidad_id"),

        # Media daily
        ("idx_agregacion_medio_fecha", "agregacion_medio_diaria", "fecha"),
    ]

    for idx_name, table, columns in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns});")
            logger.info(f"Created index: {idx_name}")
        except sqlite3.OperationalError as e:
            logger.warning(f"Index {idx_name}: {e}")

    conn.commit()
    conn.close()

    logger.info("Migration 005 completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migration()
