"""
Database operations for risk/opportunity classification.
Uses procesado_riesgo flag for tracking.
"""

import logging
from analisis.clasificador_riesgo_oportunidad import clasificar_riesgo_oportunidad
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


def clasificar_riesgo_oportunidad_db():
    """Classify all unprocessed relevant news as risk/opportunity."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Only relevant news not yet evaluated
        cursor.execute("""
            SELECT id, titulo, descripcion
            FROM noticias
            WHERE relevante = 1
              AND (procesado_riesgo = 0 OR procesado_riesgo IS NULL)
        """)
        filas = cursor.fetchall()
        logger.info(f"News to evaluate: {len(filas)}")

        riesgos_count = 0
        oportunidades_count = 0

        for nid, titulo, descripcion in filas:
            texto = f"{titulo} {descripcion or ''}"
            riesgo, oportunidad = clasificar_riesgo_oportunidad(texto)

            cursor.execute("""
                UPDATE noticias
                SET riesgo = ?, oportunidad = ?, procesado_riesgo = 1
                WHERE id = ?
            """, (riesgo, oportunidad, nid))

            if riesgo:
                riesgos_count += 1
            if oportunidad:
                oportunidades_count += 1

        conn.commit()

    logger.info(f"Risk/opportunity classification completed.")
    logger.info(f"  Risks detected: {riesgos_count}")
    logger.info(f"  Opportunities detected: {oportunidades_count}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clasificar_riesgo_oportunidad_db()
