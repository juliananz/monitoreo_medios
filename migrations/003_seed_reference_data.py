"""
Migration 003: Seed reference tables with initial data.

Populates:
- temas: From keywords.yaml topics
- regiones: Mexican states, key countries
- entidades: Key companies from config
"""

import sqlite3
import logging
import yaml
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"
KEYWORDS_PATH = BASE_DIR / "config" / "keywords.yaml"

logger = logging.getLogger(__name__)


def normalizar_texto(texto: str) -> str:
    """Normalize text for lookups."""
    import unicodedata
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def run_migration():
    """Seed reference tables with initial data."""

    # Load config
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # =========================================================================
    # SEED TEMAS
    # =========================================================================

    temas_config = config.get("temas", {})

    tema_descriptions = {
        "inversion": "Inversiones, capital, expansión empresarial",
        "empleo": "Empleo, contrataciones, despidos, mercado laboral",
        "industria": "Industria, manufactura, sector automotriz",
        "comercio_exterior": "Exportaciones, importaciones, aranceles, T-MEC",
    }

    for tema_nombre, palabras in temas_config.items():
        palabras_str = ",".join(palabras) if palabras else ""
        descripcion = tema_descriptions.get(tema_nombre, "")

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO temas (nombre, descripcion, palabras_clave)
                VALUES (?, ?, ?)
            """, (tema_nombre, descripcion, palabras_str))
        except sqlite3.Error as e:
            logger.warning(f"Error inserting tema {tema_nombre}: {e}")

    logger.info(f"Seeded {len(temas_config)} temas")

    # =========================================================================
    # SEED REGIONES - Mexican States
    # =========================================================================

    geografia = config.get("geografia", {})
    estado_objetivo = geografia.get("estado_objetivo", "coahuila")
    estados = geografia.get("estados_mexico", [])
    paises = geografia.get("paises_clave", [])

    # Mexican states
    for estado in estados:
        es_objetivo = 1 if normalizar_texto(estado) == normalizar_texto(estado_objetivo) else 0
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO regiones (nombre, nombre_normalizado, tipo, pais, es_objetivo)
                VALUES (?, ?, 'estado', 'Mexico', ?)
            """, (estado.title(), normalizar_texto(estado), es_objetivo))
        except sqlite3.Error as e:
            logger.warning(f"Error inserting estado {estado}: {e}")

    logger.info(f"Seeded {len(estados)} Mexican states")

    # Key countries
    pais_nombres = {
        "mexico": "México",
        "estados unidos": "Estados Unidos",
        "usa": "Estados Unidos",
        "eeuu": "Estados Unidos",
        "china": "China",
        "canada": "Canadá",
        "alemania": "Alemania",
        "japon": "Japón",
        "corea del sur": "Corea del Sur",
        "india": "India",
        "brasil": "Brasil",
        "espana": "España",
        "francia": "Francia",
        "italia": "Italia",
        "reino unido": "Reino Unido",
    }

    paises_insertados = set()
    for pais in paises:
        pais_norm = normalizar_texto(pais)
        nombre_display = pais_nombres.get(pais_norm, pais.title())

        # Skip duplicates (usa, eeuu -> Estados Unidos)
        if nombre_display in paises_insertados:
            continue
        paises_insertados.add(nombre_display)

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO regiones (nombre, nombre_normalizado, tipo, pais)
                VALUES (?, ?, 'pais', ?)
            """, (nombre_display, normalizar_texto(nombre_display), nombre_display))
        except sqlite3.Error as e:
            logger.warning(f"Error inserting pais {pais}: {e}")

    logger.info(f"Seeded {len(paises_insertados)} countries")

    # =========================================================================
    # SEED ENTIDADES - Key Companies
    # =========================================================================

    empresas = geografia.get("empresas_clave", [])

    # Canonical names and aliases for key companies
    empresas_canonicas = {
        "general motors": {
            "canonical": "General Motors",
            "aliases": ["gm", "general motors", "g.m."],
            "categoria": "automotriz"
        },
        "ford": {
            "canonical": "Ford Motor Company",
            "aliases": ["ford", "ford motor", "ford motors"],
            "categoria": "automotriz"
        },
        "stellantis": {
            "canonical": "Stellantis",
            "aliases": ["stellantis", "chrysler", "fiat chrysler"],
            "categoria": "automotriz"
        },
        "tesla": {
            "canonical": "Tesla",
            "aliases": ["tesla", "tesla motors", "tesla inc"],
            "categoria": "automotriz"
        },
        "amazon": {
            "canonical": "Amazon",
            "aliases": ["amazon", "amazon.com", "aws"],
            "categoria": "tecnologia"
        },
        "walmart": {
            "canonical": "Walmart",
            "aliases": ["walmart", "wal-mart", "walmex"],
            "categoria": "retail"
        },
        "bmw": {
            "canonical": "BMW",
            "aliases": ["bmw", "bayerische motoren werke"],
            "categoria": "automotriz"
        },
        "toyota": {
            "canonical": "Toyota",
            "aliases": ["toyota", "toyota motor"],
            "categoria": "automotriz"
        },
        "nissan": {
            "canonical": "Nissan",
            "aliases": ["nissan", "nissan motor"],
            "categoria": "automotriz"
        },
        "honda": {
            "canonical": "Honda",
            "aliases": ["honda", "honda motor"],
            "categoria": "automotriz"
        },
        "volkswagen": {
            "canonical": "Volkswagen",
            "aliases": ["volkswagen", "vw", "volks"],
            "categoria": "automotriz"
        },
        "kia": {
            "canonical": "Kia",
            "aliases": ["kia", "kia motors"],
            "categoria": "automotriz"
        },
        "hyundai": {
            "canonical": "Hyundai",
            "aliases": ["hyundai", "hyundai motor"],
            "categoria": "automotriz"
        },
        "caterpillar": {
            "canonical": "Caterpillar",
            "aliases": ["caterpillar", "cat"],
            "categoria": "maquinaria"
        },
        "john deere": {
            "canonical": "John Deere",
            "aliases": ["john deere", "deere", "deere & company"],
            "categoria": "maquinaria"
        },
        "honeywell": {
            "canonical": "Honeywell",
            "aliases": ["honeywell", "honeywell international"],
            "categoria": "industrial"
        },
    }

    for empresa_key, empresa_data in empresas_canonicas.items():
        try:
            # Insert canonical entity
            cursor.execute("""
                INSERT OR IGNORE INTO entidades (nombre_canonico, tipo, es_clave, categoria)
                VALUES (?, 'ORG', 1, ?)
            """, (empresa_data["canonical"], empresa_data["categoria"]))

            # Get the entity ID
            cursor.execute(
                "SELECT id FROM entidades WHERE nombre_canonico = ?",
                (empresa_data["canonical"],)
            )
            row = cursor.fetchone()
            if row:
                entidad_id = row[0]

                # Insert aliases
                for i, alias in enumerate(empresa_data["aliases"]):
                    es_principal = 1 if i == 0 else 0
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO entidad_alias (entidad_id, alias, es_principal)
                            VALUES (?, ?, ?)
                        """, (entidad_id, normalizar_texto(alias), es_principal))
                    except sqlite3.Error:
                        pass  # Alias might already exist

        except sqlite3.Error as e:
            logger.warning(f"Error inserting empresa {empresa_key}: {e}")

    logger.info(f"Seeded {len(empresas_canonicas)} key companies with aliases")

    conn.commit()
    conn.close()

    logger.info("Migration 003 completed successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migration()
