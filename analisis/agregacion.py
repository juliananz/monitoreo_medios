"""
Aggregation module for computing pre-aggregated analytics.

Calculates daily aggregations for:
- Global metrics (total news, risks, opportunities)
- Per-topic metrics
- Per-region metrics
- Per-entity metrics
- Per-media-source metrics

All functions use INSERT OR REPLACE for idempotent updates.
"""

import logging
from datetime import date, datetime
from typing import List, Optional

from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_fechas_con_datos() -> List[date]:
    """Get distinct dates that have news data."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT DATE(fecha) as fecha
            FROM noticias
            WHERE fecha IS NOT NULL
            ORDER BY fecha
        """)
        return [date.fromisoformat(row[0]) for row in cursor.fetchall() if row[0]]


# =============================================================================
# DAILY AGGREGATION FUNCTIONS
# =============================================================================

def calcular_agregacion_diaria(fecha: date) -> None:
    """
    Calculate global metrics for a single day.

    Metrics: total news, relevant, risks, opportunities, mixed,
    active media sources, requiring deep analysis.
    """
    fecha_str = fecha.isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO agregacion_diaria (
                fecha,
                total_noticias,
                total_relevantes,
                total_riesgo,
                total_oportunidad,
                total_mixto,
                medios_activos,
                requieren_analisis,
                fecha_calculo
            )
            SELECT
                DATE(fecha) as fecha,
                COUNT(*) as total_noticias,
                SUM(CASE WHEN relevante = 1 THEN 1 ELSE 0 END) as total_relevantes,
                SUM(CASE WHEN riesgo = 1 THEN 1 ELSE 0 END) as total_riesgo,
                SUM(CASE WHEN oportunidad = 1 THEN 1 ELSE 0 END) as total_oportunidad,
                SUM(CASE WHEN riesgo = 1 AND oportunidad = 1 THEN 1 ELSE 0 END) as total_mixto,
                COUNT(DISTINCT medio) as medios_activos,
                SUM(CASE WHEN requiere_analisis_profundo = 1 THEN 1 ELSE 0 END) as requieren_analisis,
                CURRENT_TIMESTAMP as fecha_calculo
            FROM noticias
            WHERE DATE(fecha) = ?
            GROUP BY DATE(fecha)
        """, (fecha_str,))

        conn.commit()


def calcular_agregacion_tema_diaria(fecha: date) -> None:
    """
    Calculate per-topic metrics for a single day.

    Uses the noticia_tema junction table for accurate topic assignments.
    """
    fecha_str = fecha.isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Delete existing records for this date (to handle removed topics)
        cursor.execute(
            "DELETE FROM agregacion_tema_diaria WHERE fecha = ?",
            (fecha_str,)
        )

        cursor.execute("""
            INSERT INTO agregacion_tema_diaria (
                fecha,
                tema_id,
                total_noticias,
                total_riesgo,
                total_oportunidad,
                score_promedio
            )
            SELECT
                DATE(n.fecha) as fecha,
                nt.tema_id,
                COUNT(*) as total_noticias,
                SUM(CASE WHEN n.riesgo = 1 THEN 1 ELSE 0 END) as total_riesgo,
                SUM(CASE WHEN n.oportunidad = 1 THEN 1 ELSE 0 END) as total_oportunidad,
                AVG(nt.score) as score_promedio
            FROM noticias n
            JOIN noticia_tema nt ON n.id = nt.noticia_id
            WHERE DATE(n.fecha) = ?
            GROUP BY DATE(n.fecha), nt.tema_id
        """, (fecha_str,))

        conn.commit()


def calcular_agregacion_region_diaria(fecha: date) -> None:
    """
    Calculate per-region metrics for a single day.

    Groups by region_id and nivel_geografico.
    Uses -1 as sentinel value for NULL region_id.
    """
    fecha_str = fecha.isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Delete existing records for this date
        cursor.execute(
            "DELETE FROM agregacion_region_diaria WHERE fecha = ?",
            (fecha_str,)
        )

        cursor.execute("""
            INSERT INTO agregacion_region_diaria (
                fecha,
                region_id,
                nivel_geografico,
                total_noticias,
                total_riesgo,
                total_oportunidad
            )
            SELECT
                DATE(fecha) as fecha,
                COALESCE(region_id, -1) as region_id,
                COALESCE(nivel_geografico, 'indeterminado') as nivel_geografico,
                COUNT(*) as total_noticias,
                SUM(CASE WHEN riesgo = 1 THEN 1 ELSE 0 END) as total_riesgo,
                SUM(CASE WHEN oportunidad = 1 THEN 1 ELSE 0 END) as total_oportunidad
            FROM noticias
            WHERE DATE(fecha) = ?
              AND relevante = 1
            GROUP BY DATE(fecha), COALESCE(region_id, -1), nivel_geografico
        """, (fecha_str,))

        conn.commit()


def calcular_agregacion_entidad_diaria(fecha: date) -> None:
    """
    Calculate per-entity metrics for a single day.

    Uses the noticia_entidad junction table.
    Counts mentions and tracks risk/opportunity context.
    """
    fecha_str = fecha.isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Delete existing records for this date
        cursor.execute(
            "DELETE FROM agregacion_entidad_diaria WHERE fecha = ?",
            (fecha_str,)
        )

        cursor.execute("""
            INSERT INTO agregacion_entidad_diaria (
                fecha,
                entidad_id,
                menciones,
                noticias_riesgo,
                noticias_oportunidad,
                frecuencia_total
            )
            SELECT
                DATE(n.fecha) as fecha,
                ne.entidad_id,
                COUNT(*) as menciones,
                SUM(CASE WHEN n.riesgo = 1 THEN 1 ELSE 0 END) as noticias_riesgo,
                SUM(CASE WHEN n.oportunidad = 1 THEN 1 ELSE 0 END) as noticias_oportunidad,
                SUM(ne.frecuencia) as frecuencia_total
            FROM noticias n
            JOIN noticia_entidad ne ON n.id = ne.noticia_id
            WHERE DATE(n.fecha) = ?
            GROUP BY DATE(n.fecha), ne.entidad_id
        """, (fecha_str,))

        conn.commit()


def calcular_agregacion_medio_diaria(fecha: date) -> None:
    """
    Calculate per-media-source metrics for a single day.

    Tracks total, relevant, risk, and opportunity news per source.
    """
    fecha_str = fecha.isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Delete existing records for this date
        cursor.execute(
            "DELETE FROM agregacion_medio_diaria WHERE fecha = ?",
            (fecha_str,)
        )

        cursor.execute("""
            INSERT INTO agregacion_medio_diaria (
                fecha,
                medio,
                total_noticias,
                total_relevantes,
                total_riesgo,
                total_oportunidad
            )
            SELECT
                DATE(fecha) as fecha,
                medio,
                COUNT(*) as total_noticias,
                SUM(CASE WHEN relevante = 1 THEN 1 ELSE 0 END) as total_relevantes,
                SUM(CASE WHEN riesgo = 1 THEN 1 ELSE 0 END) as total_riesgo,
                SUM(CASE WHEN oportunidad = 1 THEN 1 ELSE 0 END) as total_oportunidad
            FROM noticias
            WHERE DATE(fecha) = ?
            GROUP BY DATE(fecha), medio
        """, (fecha_str,))

        conn.commit()


# =============================================================================
# ORCHESTRATION FUNCTIONS
# =============================================================================

def ejecutar_agregaciones(fecha: Optional[date] = None) -> None:
    """
    Run all aggregation calculations for a given date.

    Args:
        fecha: Date to aggregate. Defaults to today.
    """
    if fecha is None:
        fecha = date.today()

    logger.info(f"Computing aggregations for {fecha.isoformat()}...")

    calcular_agregacion_diaria(fecha)
    logger.debug(f"  - Global aggregation done")

    calcular_agregacion_tema_diaria(fecha)
    logger.debug(f"  - Topic aggregation done")

    calcular_agregacion_region_diaria(fecha)
    logger.debug(f"  - Region aggregation done")

    calcular_agregacion_entidad_diaria(fecha)
    logger.debug(f"  - Entity aggregation done")

    calcular_agregacion_medio_diaria(fecha)
    logger.debug(f"  - Media aggregation done")

    logger.info(f"Aggregations completed for {fecha.isoformat()}")


def backfill_agregaciones(
    fecha_inicio: Optional[date] = None,
    fecha_fin: Optional[date] = None
) -> None:
    """
    Backfill aggregations for a date range.

    If no dates provided, processes all historical data.

    Args:
        fecha_inicio: Start date (inclusive). If None, uses earliest data date.
        fecha_fin: End date (inclusive). If None, uses today.
    """
    logger.info("Starting aggregation backfill...")

    # Get all dates with data
    fechas_con_datos = get_fechas_con_datos()

    if not fechas_con_datos:
        logger.warning("No data found to backfill.")
        return

    # Apply date range filters
    if fecha_inicio:
        fechas_con_datos = [f for f in fechas_con_datos if f >= fecha_inicio]
    if fecha_fin:
        fechas_con_datos = [f for f in fechas_con_datos if f <= fecha_fin]

    total = len(fechas_con_datos)
    logger.info(f"Found {total} dates to process")

    for i, fecha in enumerate(fechas_con_datos, 1):
        if i % 10 == 0 or i == total:
            logger.info(f"Progress: {i}/{total} ({100*i//total}%)")

        ejecutar_agregaciones(fecha)

    logger.info(f"Backfill completed: {total} dates processed")


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if len(sys.argv) > 1:
        if sys.argv[1] == "--backfill":
            backfill_agregaciones()
        elif sys.argv[1] == "--today":
            ejecutar_agregaciones()
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: python -m analisis.agregacion [--backfill | --today]")
            sys.exit(1)
    else:
        # Default: run for today
        ejecutar_agregaciones()
