"""
Database operations for thematic classification.
Now uses normalized noticia_tema junction table.
"""

import logging
from analisis.clasificador_temas import clasificar_noticia
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


def get_tema_map(cursor) -> dict:
    """Get tema name → id mapping."""
    cursor.execute("SELECT id, nombre FROM temas")
    return {row[1]: row[0] for row in cursor.fetchall()}


def clasificar_noticias():
    """Classify all unprocessed news articles by topic."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get tema mapping
        tema_map = get_tema_map(cursor)

        if not tema_map:
            logger.warning("No temas found in database. Run migrations first.")
            return

        # Get unprocessed news
        cursor.execute("""
            SELECT id, titulo, descripcion
            FROM noticias
            WHERE procesado_temas = 0 OR procesado_temas IS NULL
        """)

        noticias = cursor.fetchall()
        logger.info(f"News to classify: {len(noticias)}")

        total_tema_links = 0

        for noticia_id, titulo, descripcion in noticias:
            resultado = clasificar_noticia(titulo, descripcion or "")

            temas_list = resultado["temas"]
            score = resultado["score"]
            relevante = resultado["relevante"]

            # Update noticias table (keep legacy columns for backward compatibility)
            temas_str = ",".join(temas_list)
            cursor.execute("""
                UPDATE noticias
                SET temas = ?, score = ?, relevante = ?, procesado_temas = 1
                WHERE id = ?
            """, (temas_str, score, relevante, noticia_id))

            # Insert into junction table
            for tema_nombre in temas_list:
                tema_id = tema_map.get(tema_nombre)
                if tema_id:
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO noticia_tema (noticia_id, tema_id, score)
                            VALUES (?, ?, 1)
                        """, (noticia_id, tema_id))
                        total_tema_links += cursor.rowcount
                    except Exception as e:
                        logger.debug(f"Error linking tema: {e}")

        conn.commit()

    logger.info(f"Thematic classification completed. Created {total_tema_links} tema links.")


def reclasificar_todo():
    """
    Full reclassification of ALL articles:
    1. Ensure every topic in keywords.yaml exists in the temas table.
    2. Delete all existing noticia_tema links.
    3. Reset procesado_temas=0 for every article.
    4. Run clasificar_noticias() to classify from scratch.
    """
    import yaml
    from config.settings import KEYWORDS_PATH

    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    temas_yaml = config.get("temas", {})

    tema_descriptions = {
        "inversion": "Inversiones, capital, expansión empresarial",
        "empleo": "Empleo, contrataciones, despidos, mercado laboral",
        "industria": "Industria, manufactura, sector automotriz",
        "comercio_exterior": "Exportaciones, importaciones, aranceles, T-MEC",
        "nearshoring": "Nearshoring, relocalización, cadena de suministro",
        "aranceles": "Aranceles, tarifas, medidas comerciales proteccionistas",
        "energia": "Energía, petróleo, CFE, Pemex, renovables",
        "infraestructura": "Infraestructura, carreteras, puertos, parques industriales",
    }

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # 1. Ensure all temas from YAML exist in the DB
        for nombre in temas_yaml:
            palabras_str = ",".join(temas_yaml[nombre]) if temas_yaml[nombre] else ""
            desc = tema_descriptions.get(nombre, "")
            cursor.execute("""
                INSERT OR IGNORE INTO temas (nombre, descripcion, palabras_clave)
                VALUES (?, ?, ?)
            """, (nombre, desc, palabras_str))
        conn.commit()
        logger.info(f"Ensured {len(temas_yaml)} temas in DB")

        # 2. Delete all existing topic links
        cursor.execute("DELETE FROM noticia_tema")
        deleted = cursor.rowcount
        conn.commit()
        logger.info(f"Deleted {deleted} existing noticia_tema rows")

        # 3. Reset procesado_temas for all articles
        cursor.execute("UPDATE noticias SET procesado_temas = 0, temas = NULL, relevante = 0, score = 0")
        conn.commit()
        logger.info("Reset procesado_temas=0 for all articles")

    # 4. Reclassify everything
    clasificar_noticias()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clasificar_noticias()
