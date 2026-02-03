"""
Database initialization and schema management.
This defines the CANONICAL schema for the project.
"""

import logging
from config.settings import DB_PATH, DATA_DIR
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


def crear_base_datos():
    """
    Create the database and all tables.
    This is the CANONICAL schema for the project.
    """
    DATA_DIR.mkdir(exist_ok=True)

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # =====================================================================
        # CORE TABLE: noticias
        # =====================================================================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS noticias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT NOT NULL,
            descripcion TEXT,
            url TEXT UNIQUE,
            fecha TEXT,
            fecha_scraping DATETIME,
            medio TEXT,

            -- Classification
            temas TEXT,
            score INTEGER,
            relevante INTEGER,

            -- Risk/Opportunity
            riesgo INTEGER DEFAULT 0,
            oportunidad INTEGER DEFAULT 0,

            -- NER results (legacy columns for backward compatibility)
            personas TEXT,
            organizaciones TEXT,
            lugares TEXT,

            -- Geographic
            nivel_geografico TEXT,
            region_id INTEGER REFERENCES regiones(id),
            requiere_analisis_profundo INTEGER DEFAULT 0,

            -- Processing flags
            procesado_temas INTEGER DEFAULT 0,
            procesado_ner INTEGER DEFAULT 0,
            procesado_riesgo INTEGER DEFAULT 0
        );
        """)

        # =====================================================================
        # REFERENCE TABLES
        # =====================================================================

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

        # =====================================================================
        # JUNCTION TABLES
        # =====================================================================

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

        # =====================================================================
        # MIGRATION TRACKING
        # =====================================================================

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS migrations_applied (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_name TEXT UNIQUE NOT NULL,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # =====================================================================
        # INDEXES
        # =====================================================================

        indexes = [
            ("idx_noticias_fecha", "noticias", "fecha"),
            ("idx_noticias_region", "noticias", "region_id"),
            ("idx_noticias_relevante_fecha", "noticias", "relevante, fecha"),
            ("idx_noticias_procesado", "noticias", "procesado_temas, procesado_ner, procesado_riesgo"),
            ("idx_noticia_tema_tema", "noticia_tema", "tema_id"),
            ("idx_noticia_entidad_entidad", "noticia_entidad", "entidad_id"),
            ("idx_entidad_alias_alias", "entidad_alias", "alias"),
            ("idx_regiones_nombre", "regiones", "nombre_normalizado"),
            ("idx_entidades_tipo", "entidades", "tipo"),
        ]

        for idx_name, table, columns in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns});")
            except Exception:
                pass  # Index might already exist

        conn.commit()

    logger.info(f"Database ready at: {DB_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    crear_base_datos()
