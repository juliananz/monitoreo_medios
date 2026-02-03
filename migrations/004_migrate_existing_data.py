"""
Migration 004: Migrate existing data to normalized schema.

Migrates:
- noticias.temas (comma-separated) → noticia_tema
- noticias.personas/organizaciones/lugares → entidades + noticia_entidad
- noticias.nivel_geografico → regiones lookup

This is a data migration, not a schema migration.
"""

import sqlite3
import logging
import unicodedata
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

logger = logging.getLogger(__name__)


def normalizar_texto(texto: str) -> str:
    """Normalize text for lookups."""
    if not texto:
        return ""
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def run_migration():
    """Migrate existing data to normalized tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # =========================================================================
    # MIGRATE TEMAS: noticias.temas → noticia_tema
    # =========================================================================

    logger.info("Migrating temas to noticia_tema...")

    # Get tema name → id mapping
    cursor.execute("SELECT id, nombre FROM temas")
    tema_map = {row[1]: row[0] for row in cursor.fetchall()}

    # Get all noticias with temas
    cursor.execute("""
        SELECT id, temas
        FROM noticias
        WHERE temas IS NOT NULL AND temas != ''
    """)
    noticias_temas = cursor.fetchall()

    temas_migrated = 0
    for noticia_id, temas_str in noticias_temas:
        if not temas_str:
            continue

        temas_list = [t.strip() for t in temas_str.split(",") if t.strip()]

        for tema_nombre in temas_list:
            tema_id = tema_map.get(tema_nombre)
            if tema_id:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO noticia_tema (noticia_id, tema_id)
                        VALUES (?, ?)
                    """, (noticia_id, tema_id))
                    temas_migrated += cursor.rowcount
                except sqlite3.Error:
                    pass

    logger.info(f"Migrated {temas_migrated} tema assignments")

    # =========================================================================
    # MIGRATE ENTITIES: personas/organizaciones/lugares → entidades + noticia_entidad
    # =========================================================================

    logger.info("Migrating entities to normalized tables...")

    # Get existing entity alias → id mapping
    cursor.execute("""
        SELECT alias, entidad_id FROM entidad_alias
    """)
    alias_map = {row[0]: row[1] for row in cursor.fetchall()}

    # Get all noticias with entities
    cursor.execute("""
        SELECT id, personas, organizaciones, lugares
        FROM noticias
        WHERE personas IS NOT NULL
           OR organizaciones IS NOT NULL
           OR lugares IS NOT NULL
    """)
    noticias_entidades = cursor.fetchall()

    entities_created = 0
    links_created = 0

    for noticia_id, personas_str, orgs_str, lugares_str in noticias_entidades:

        # Process each entity type
        entity_groups = [
            (personas_str, "PER"),
            (orgs_str, "ORG"),
            (lugares_str, "LOC"),
        ]

        for entities_str, tipo in entity_groups:
            if not entities_str:
                continue

            entities_list = [e.strip() for e in entities_str.split(",") if e.strip()]

            for entity_name in entities_list:
                entity_norm = normalizar_texto(entity_name)

                if not entity_norm:
                    continue

                # Check if alias exists
                entidad_id = alias_map.get(entity_norm)

                if not entidad_id:
                    # Create new entity
                    try:
                        cursor.execute("""
                            INSERT INTO entidades (nombre_canonico, tipo)
                            VALUES (?, ?)
                        """, (entity_name, tipo))
                        entidad_id = cursor.lastrowid
                        entities_created += 1

                        # Add alias for the canonical name
                        cursor.execute("""
                            INSERT OR IGNORE INTO entidad_alias (entidad_id, alias, es_principal)
                            VALUES (?, ?, 1)
                        """, (entidad_id, entity_norm))

                        # Update local map
                        alias_map[entity_norm] = entidad_id

                    except sqlite3.IntegrityError:
                        # Entity might exist with different casing
                        cursor.execute("""
                            SELECT id FROM entidades WHERE nombre_canonico = ?
                        """, (entity_name,))
                        row = cursor.fetchone()
                        if row:
                            entidad_id = row[0]
                        else:
                            continue

                # Create link
                if entidad_id:
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO noticia_entidad (noticia_id, entidad_id, rol)
                            VALUES (?, ?, 'mencionado')
                        """, (noticia_id, entidad_id))
                        links_created += cursor.rowcount
                    except sqlite3.Error:
                        pass

    logger.info(f"Created {entities_created} new entities")
    logger.info(f"Created {links_created} entity-news links")

    # =========================================================================
    # MIGRATE REGIONS: nivel_geografico → region_id
    # =========================================================================

    logger.info("Migrating geographic regions...")

    # For now, we'll link based on lugares detected
    # Get region name → id mapping
    cursor.execute("SELECT id, nombre_normalizado FROM regiones")
    region_map = {row[1]: row[0] for row in cursor.fetchall()}

    # Get noticias with lugares
    cursor.execute("""
        SELECT id, lugares, nivel_geografico
        FROM noticias
        WHERE lugares IS NOT NULL AND lugares != ''
    """)
    noticias_lugares = cursor.fetchall()

    regions_linked = 0
    for noticia_id, lugares_str, nivel_geo in noticias_lugares:
        if not lugares_str:
            continue

        lugares_list = [l.strip() for l in lugares_str.split(",") if l.strip()]

        # Find first matching region
        region_id = None
        for lugar in lugares_list:
            lugar_norm = normalizar_texto(lugar)
            if lugar_norm in region_map:
                region_id = region_map[lugar_norm]
                break

        if region_id:
            try:
                cursor.execute("""
                    UPDATE noticias SET region_id = ? WHERE id = ?
                """, (region_id, noticia_id))
                regions_linked += cursor.rowcount
            except sqlite3.Error:
                pass

    logger.info(f"Linked {regions_linked} noticias to regions")

    # =========================================================================
    # SET PROCESSING FLAGS
    # =========================================================================

    logger.info("Setting processing flags for existing data...")

    # Mark existing data as processed
    cursor.execute("""
        UPDATE noticias
        SET procesado_temas = 1
        WHERE temas IS NOT NULL AND temas != ''
    """)

    cursor.execute("""
        UPDATE noticias
        SET procesado_ner = 1
        WHERE personas IS NOT NULL
    """)

    cursor.execute("""
        UPDATE noticias
        SET procesado_riesgo = 1
        WHERE relevante IS NOT NULL
    """)

    conn.commit()
    conn.close()

    logger.info("Migration 004 completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migration()
