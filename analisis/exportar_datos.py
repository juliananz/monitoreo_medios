"""
Export dashboard data to a static CSV file.

The dashboard on Streamlit Community Cloud cannot use SQLite directly
(read-only filesystem).  This module exports all needed data to
data/salidas/dashboard_noticias.csv so the dashboard reads CSV only.
"""

import logging

import pandas as pd

from analisis.utils import get_db_connection
from config.settings import OUTPUT_DIR
from pathlib import Path

logger = logging.getLogger(__name__)

DASHBOARD_CSV = Path(OUTPUT_DIR) / "dashboard_noticias.csv"


def exportar_dashboard_data() -> Path:
    """Export noticias relevantes (with topics) to CSV for the dashboard."""
    with get_db_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                n.id,
                n.fecha,
                n.medio,
                n.titulo,
                n.url,
                n.personas,
                n.organizaciones,
                n.lugares,
                n.nivel_geografico,
                n.requiere_analisis_profundo,
                n.riesgo,
                n.oportunidad,
                GROUP_CONCAT(t.nombre, '|') AS temas
            FROM noticias n
            LEFT JOIN noticia_tema nt ON n.id = nt.noticia_id
            LEFT JOIN temas t ON nt.tema_id = t.id
            WHERE n.relevante = 1
            GROUP BY n.id
            ORDER BY n.fecha DESC
            """,
            conn,
        )

    DASHBOARD_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DASHBOARD_CSV, index=False)
    logger.info(f"Dashboard CSV exported: {len(df)} rows -> {DASHBOARD_CSV}")
    return DASHBOARD_CSV
