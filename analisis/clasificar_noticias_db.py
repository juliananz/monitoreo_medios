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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clasificar_noticias()
