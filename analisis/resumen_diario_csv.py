"""
Daily CSV summary generator for executive reports.
"""

import logging
import pandas as pd
from datetime import datetime

from config.settings import DB_PATH, OUTPUT_DIR
from analisis.utils import get_db_connection, clasificar_tipo

logger = logging.getLogger(__name__)


def generar_resumen_diario():
    """Generate daily CSV summary of risk/opportunity news."""
    with get_db_connection() as conn:
        query = """
            SELECT
                fecha,
                medio,
                titulo,
                temas,
                score,
                riesgo,
                oportunidad
            FROM noticias
            WHERE riesgo = 1 OR oportunidad = 1
            ORDER BY fecha DESC
        """

        df = pd.read_sql_query(query, conn)

    if df.empty:
        logger.warning("No risk/opportunity news to export.")
        return

    # Apply classification
    df["tipo"] = df.apply(
        lambda row: clasificar_tipo(row["riesgo"], row["oportunidad"]),
        axis=1
    )

    df = df[["fecha", "medio", "titulo", "temas", "tipo", "score"]]

    # Export
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    output_file = OUTPUT_DIR / f"resumen_diario_{fecha_hoy}.csv"

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    logger.info(f"Daily summary generated: {output_file}")
    logger.info(f"Total news exported: {len(df)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generar_resumen_diario()
