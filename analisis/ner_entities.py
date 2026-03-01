"""
Named Entity Recognition (NER) for news articles.
Uses Spanish BERT model for entity extraction.
Now uses normalized entidades and noticia_entidad tables.
"""

import logging
import unicodedata
import yaml
from pathlib import Path
from transformers import pipeline

from config.settings import DB_PATH, KEYWORDS_PATH
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)

# NER Model (loaded once at module level)
ner_pipeline = pipeline(
    "ner",
    model="mrm8488/bert-spanish-cased-finetuned-ner",
    aggregation_strategy="simple",
)


def normalizar_texto(texto: str) -> str:
    """Normalize text for lookups."""
    if not texto:
        return ""
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def cargar_config_geografia():
    """Load geographic classification config from YAML."""
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    geo = config.get("geografia", {})
    return {
        "estado_objetivo": geo.get("estado_objetivo", "coahuila").lower(),
        "estados_mexico": [normalizar_texto(e) for e in geo.get("estados_mexico", [])],
        "paises_clave": [normalizar_texto(p) for p in geo.get("paises_clave", [])],
        "empresas_clave": [normalizar_texto(c) for c in geo.get("empresas_clave", [])],
    }


def get_or_create_entidad(cursor, nombre: str, tipo: str, alias_map: dict) -> int:
    """
    Get entity ID from alias map, or create new entity if not found.
    Returns entity ID.
    """
    nombre_norm = normalizar_texto(nombre)

    # Check alias map first
    if nombre_norm in alias_map:
        return alias_map[nombre_norm]

    # Create new entity
    try:
        cursor.execute("""
            INSERT INTO entidades (nombre_canonico, tipo)
            VALUES (?, ?)
        """, (nombre, tipo))
        entidad_id = cursor.lastrowid

        # Add alias
        cursor.execute("""
            INSERT OR IGNORE INTO entidad_alias (entidad_id, alias, es_principal)
            VALUES (?, ?, 1)
        """, (entidad_id, nombre_norm))

        # Update local cache
        alias_map[nombre_norm] = entidad_id

        return entidad_id

    except Exception as e:
        # Try to get existing
        cursor.execute("""
            SELECT id FROM entidades WHERE nombre_canonico = ?
        """, (nombre,))
        row = cursor.fetchone()
        if row:
            alias_map[nombre_norm] = row[0]
            return row[0]

        logger.debug(f"Could not create entity {nombre}: {e}")
        return None


def inferir_nivel_geografico(lugares: set, config: dict) -> str:
    """Infer geographic level from detected locations."""
    lugares_norm = [normalizar_texto(l) for l in lugares]

    estado_objetivo = config["estado_objetivo"]
    estados_mexico = config["estados_mexico"]
    paises_clave = config["paises_clave"]

    # Check for international (non-Mexico countries)
    paises_no_mexico = [p for p in paises_clave if p not in ("mexico", "méxico")]
    if any(p in lugares_norm for p in paises_no_mexico):
        return "internacional"

    # Check for target state
    if estado_objetivo in lugares_norm:
        return "estatal"

    # Check for other Mexican states
    if any(e in lugares_norm for e in estados_mexico):
        return "nacional"

    return "indeterminado"


def get_region_id(cursor, lugares: set, region_map: dict) -> int:
    """Get region ID for the most relevant location."""
    for lugar in lugares:
        lugar_norm = normalizar_texto(lugar)
        if lugar_norm in region_map:
            return region_map[lugar_norm]
    return None


def requiere_analisis(lugares: set, organizaciones: set, config: dict) -> int:
    """Determine if news requires deep analysis."""
    org_norm = [normalizar_texto(o) for o in organizaciones]
    lug_norm = [normalizar_texto(l) for l in lugares]

    empresas_clave = config["empresas_clave"]
    paises_clave = config["paises_clave"]

    # Key company mentioned
    if any(e in org_norm for e in empresas_clave):
        return 1

    # International country mentioned
    paises_no_mexico = [p for p in paises_clave if p not in ("mexico", "méxico")]
    if any(p in lug_norm for p in paises_no_mexico):
        return 1

    return 0


def ejecutar_ner():
    """Run NER on all unprocessed relevant news."""
    # Load geography config
    geo_config = cargar_config_geografia()
    logger.info(f"Target state: {geo_config['estado_objetivo']}")
    logger.info(f"Tracking {len(geo_config['empresas_clave'])} key companies")

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Load alias map for entity resolution
        cursor.execute("SELECT alias, entidad_id FROM entidad_alias")
        alias_map = {row[0]: row[1] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(alias_map)} entity aliases")

        # Load region map
        cursor.execute("SELECT nombre_normalizado, id FROM regiones")
        region_map = {row[0]: row[1] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(region_map)} regions")

        # Get unprocessed relevant news
        cursor.execute("""
            SELECT id, titulo, descripcion
            FROM noticias
            WHERE relevante = 1
              AND (procesado_ner = 0 OR procesado_ner IS NULL)
        """)
        noticias = cursor.fetchall()

        logger.info(f"News to process with NER: {len(noticias)}")

        entities_created = 0
        links_created = 0

        for noticia_id, titulo, descripcion in noticias:
            # Use both title and description for NER
            texto = f"{titulo} {descripcion or ''}"

            # Truncate to BERT's 512 token limit using the pipeline's tokenizer
            encoded = ner_pipeline.tokenizer(texto, truncation=True, max_length=512)
            texto = ner_pipeline.tokenizer.decode(encoded["input_ids"], skip_special_tokens=True)

            entidades = ner_pipeline(texto)

            personas = set()
            organizaciones = set()
            lugares = set()

            for ent in entidades:
                etiqueta = ent["entity_group"]
                valor = ent["word"].strip()

                if len(valor) < 2:  # Skip single chars
                    continue

                if etiqueta == "PER":
                    personas.add(valor)
                elif etiqueta == "ORG":
                    organizaciones.add(valor)
                elif etiqueta == "LOC":
                    lugares.add(valor)

            # Legacy columns (keep for backward compatibility)
            personas_str = ",".join(personas)
            organizaciones_str = ",".join(organizaciones)
            lugares_str = ",".join(lugares)

            nivel_geo = inferir_nivel_geografico(lugares, geo_config)
            region_id = get_region_id(cursor, lugares, region_map)
            flag_analisis = requiere_analisis(lugares, organizaciones, geo_config)

            # Update noticias table
            cursor.execute("""
                UPDATE noticias
                SET personas = ?,
                    organizaciones = ?,
                    lugares = ?,
                    nivel_geografico = ?,
                    region_id = ?,
                    requiere_analisis_profundo = ?,
                    procesado_ner = 1
                WHERE id = ?
            """, (
                personas_str,
                organizaciones_str,
                lugares_str,
                nivel_geo,
                region_id,
                flag_analisis,
                noticia_id
            ))

            # Insert into normalized tables
            all_entities = [
                (personas, "PER"),
                (organizaciones, "ORG"),
                (lugares, "LOC"),
            ]

            for entity_set, tipo in all_entities:
                for entity_name in entity_set:
                    entidad_id = get_or_create_entidad(cursor, entity_name, tipo, alias_map)

                    if entidad_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO noticia_entidad (noticia_id, entidad_id, rol)
                                VALUES (?, ?, 'mencionado')
                            """, (noticia_id, entidad_id))
                            if cursor.rowcount > 0:
                                links_created += 1
                        except Exception:
                            pass

            # Commit every 100 records
            if noticia_id % 100 == 0:
                conn.commit()

        conn.commit()

    logger.info(f"NER completed. Created {links_created} entity links.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ejecutar_ner()
