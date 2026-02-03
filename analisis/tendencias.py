"""
Trend detection and analysis module.

Provides functions for:
- Time-series trend queries (daily, weekly, monthly)
- Period comparisons
- Anomaly detection

All functions read from pre-computed aggregation tables for fast performance.
"""

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


# =============================================================================
# DAILY TREND QUERIES
# =============================================================================

def get_tendencia_diaria(dias: int = 30) -> pd.DataFrame:
    """
    Get daily global metrics for last N days.

    Returns DataFrame with columns:
    fecha, total_noticias, total_relevantes, total_riesgo, total_oportunidad, total_mixto
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                fecha,
                total_noticias,
                total_relevantes,
                total_riesgo,
                total_oportunidad,
                total_mixto,
                medios_activos,
                requieren_analisis
            FROM agregacion_diaria
            WHERE fecha >= ?
            ORDER BY fecha
        """, conn, params=(fecha_inicio,))

    df['fecha'] = pd.to_datetime(df['fecha'])
    return df


def get_tendencia_temas(dias: int = 30) -> pd.DataFrame:
    """
    Get daily topic metrics for last N days.

    Returns DataFrame with columns:
    fecha, tema, total_noticias, total_riesgo, total_oportunidad
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                atd.fecha,
                t.nombre as tema,
                atd.total_noticias,
                atd.total_riesgo,
                atd.total_oportunidad,
                atd.score_promedio
            FROM agregacion_tema_diaria atd
            JOIN temas t ON atd.tema_id = t.id
            WHERE atd.fecha >= ?
            ORDER BY atd.fecha, t.nombre
        """, conn, params=(fecha_inicio,))

    df['fecha'] = pd.to_datetime(df['fecha'])
    return df


def get_tendencia_regiones(dias: int = 30) -> pd.DataFrame:
    """
    Get daily region metrics for last N days.

    Returns DataFrame with columns:
    fecha, region, nivel_geografico, total_noticias, total_riesgo, total_oportunidad
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                ard.fecha,
                CASE WHEN ard.region_id = -1 THEN 'Sin region' ELSE COALESCE(r.nombre, 'Sin region') END as region,
                ard.nivel_geografico,
                ard.total_noticias,
                ard.total_riesgo,
                ard.total_oportunidad
            FROM agregacion_region_diaria ard
            LEFT JOIN regiones r ON ard.region_id = r.id AND ard.region_id != -1
            WHERE ard.fecha >= ?
            ORDER BY ard.fecha, ard.nivel_geografico
        """, conn, params=(fecha_inicio,))

    df['fecha'] = pd.to_datetime(df['fecha'])
    return df


def get_tendencia_entidades(top_n: int = 10, dias: int = 30) -> pd.DataFrame:
    """
    Get daily metrics for top N entities by total mentions.

    Returns DataFrame with columns:
    fecha, entidad, tipo, menciones, noticias_riesgo, noticias_oportunidad
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        # First, get top N entities by total mentions in period
        top_entities = pd.read_sql_query("""
            SELECT entidad_id
            FROM agregacion_entidad_diaria
            WHERE fecha >= ?
            GROUP BY entidad_id
            ORDER BY SUM(menciones) DESC
            LIMIT ?
        """, conn, params=(fecha_inicio, top_n))

        if top_entities.empty:
            return pd.DataFrame()

        entity_ids = top_entities['entidad_id'].tolist()
        placeholders = ','.join(['?'] * len(entity_ids))

        # Get daily data for these entities
        df = pd.read_sql_query(f"""
            SELECT
                aed.fecha,
                e.nombre_canonico as entidad,
                e.tipo,
                aed.menciones,
                aed.noticias_riesgo,
                aed.noticias_oportunidad,
                aed.frecuencia_total
            FROM agregacion_entidad_diaria aed
            JOIN entidades e ON aed.entidad_id = e.id
            WHERE aed.fecha >= ?
              AND aed.entidad_id IN ({placeholders})
            ORDER BY aed.fecha, e.nombre_canonico
        """, conn, params=[fecha_inicio] + entity_ids)

    df['fecha'] = pd.to_datetime(df['fecha'])
    return df


def get_tendencia_medios(dias: int = 30) -> pd.DataFrame:
    """
    Get daily metrics by media source for last N days.

    Returns DataFrame with columns:
    fecha, medio, total_noticias, total_relevantes, total_riesgo, total_oportunidad
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                fecha,
                medio,
                total_noticias,
                total_relevantes,
                total_riesgo,
                total_oportunidad
            FROM agregacion_medio_diaria
            WHERE fecha >= ?
            ORDER BY fecha, medio
        """, conn, params=(fecha_inicio,))

    df['fecha'] = pd.to_datetime(df['fecha'])
    return df


# =============================================================================
# WEEKLY/MONTHLY ROLLUPS
# =============================================================================

def get_resumen_semanal(semanas: int = 12) -> pd.DataFrame:
    """
    Aggregate daily data into weekly buckets.

    Uses ISO week numbering (YYYY-WNN format).

    Returns DataFrame with columns:
    semana, total_noticias, total_relevantes, total_riesgo, total_oportunidad, total_mixto
    """
    dias = semanas * 7
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                strftime('%Y-W%W', fecha) as semana,
                SUM(total_noticias) as total_noticias,
                SUM(total_relevantes) as total_relevantes,
                SUM(total_riesgo) as total_riesgo,
                SUM(total_oportunidad) as total_oportunidad,
                SUM(total_mixto) as total_mixto,
                AVG(medios_activos) as medios_activos_promedio
            FROM agregacion_diaria
            WHERE fecha >= ?
            GROUP BY strftime('%Y-W%W', fecha)
            ORDER BY semana
        """, conn, params=(fecha_inicio,))

    return df


def get_resumen_mensual(meses: int = 12) -> pd.DataFrame:
    """
    Aggregate daily data into monthly buckets.

    Uses YYYY-MM format.

    Returns DataFrame with columns:
    mes, total_noticias, total_relevantes, total_riesgo, total_oportunidad, total_mixto
    """
    # Approximate days for N months
    dias = meses * 31
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                strftime('%Y-%m', fecha) as mes,
                SUM(total_noticias) as total_noticias,
                SUM(total_relevantes) as total_relevantes,
                SUM(total_riesgo) as total_riesgo,
                SUM(total_oportunidad) as total_oportunidad,
                SUM(total_mixto) as total_mixto,
                AVG(medios_activos) as medios_activos_promedio,
                COUNT(*) as dias_con_datos
            FROM agregacion_diaria
            WHERE fecha >= ?
            GROUP BY strftime('%Y-%m', fecha)
            ORDER BY mes
        """, conn, params=(fecha_inicio,))

    return df


def get_resumen_temas_semanal(semanas: int = 12) -> pd.DataFrame:
    """
    Get weekly topic summary.

    Returns DataFrame with columns:
    semana, tema, total_noticias, total_riesgo, total_oportunidad
    """
    dias = semanas * 7
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                strftime('%Y-W%W', atd.fecha) as semana,
                t.nombre as tema,
                SUM(atd.total_noticias) as total_noticias,
                SUM(atd.total_riesgo) as total_riesgo,
                SUM(atd.total_oportunidad) as total_oportunidad
            FROM agregacion_tema_diaria atd
            JOIN temas t ON atd.tema_id = t.id
            WHERE atd.fecha >= ?
            GROUP BY strftime('%Y-W%W', atd.fecha), t.nombre
            ORDER BY semana, tema
        """, conn, params=(fecha_inicio,))

    return df


# =============================================================================
# PERIOD COMPARISONS
# =============================================================================

def comparar_periodos(
    periodo_actual: Tuple[date, date],
    periodo_anterior: Tuple[date, date]
) -> Dict:
    """
    Compare two periods and return differences.

    Args:
        periodo_actual: Tuple (start_date, end_date) for current period
        periodo_anterior: Tuple (start_date, end_date) for previous period

    Returns:
        Dict with metrics for both periods and percentage changes
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Current period
        cursor.execute("""
            SELECT
                SUM(total_noticias) as noticias,
                SUM(total_relevantes) as relevantes,
                SUM(total_riesgo) as riesgos,
                SUM(total_oportunidad) as oportunidades
            FROM agregacion_diaria
            WHERE fecha >= ? AND fecha <= ?
        """, (periodo_actual[0].isoformat(), periodo_actual[1].isoformat()))
        actual = cursor.fetchone()

        # Previous period
        cursor.execute("""
            SELECT
                SUM(total_noticias) as noticias,
                SUM(total_relevantes) as relevantes,
                SUM(total_riesgo) as riesgos,
                SUM(total_oportunidad) as oportunidades
            FROM agregacion_diaria
            WHERE fecha >= ? AND fecha <= ?
        """, (periodo_anterior[0].isoformat(), periodo_anterior[1].isoformat()))
        anterior = cursor.fetchone()

    def calc_change(current, previous):
        if previous and previous > 0:
            return round((current - previous) / previous * 100, 1)
        return None

    result = {
        "periodo_actual": {
            "inicio": periodo_actual[0].isoformat(),
            "fin": periodo_actual[1].isoformat(),
            "noticias": actual[0] or 0,
            "relevantes": actual[1] or 0,
            "riesgos": actual[2] or 0,
            "oportunidades": actual[3] or 0,
        },
        "periodo_anterior": {
            "inicio": periodo_anterior[0].isoformat(),
            "fin": periodo_anterior[1].isoformat(),
            "noticias": anterior[0] or 0,
            "relevantes": anterior[1] or 0,
            "riesgos": anterior[2] or 0,
            "oportunidades": anterior[3] or 0,
        },
        "cambios_pct": {
            "noticias": calc_change(actual[0] or 0, anterior[0] or 0),
            "relevantes": calc_change(actual[1] or 0, anterior[1] or 0),
            "riesgos": calc_change(actual[2] or 0, anterior[2] or 0),
            "oportunidades": calc_change(actual[3] or 0, anterior[3] or 0),
        }
    }

    return result


def comparar_con_periodo_anterior(dias: int = 7) -> Dict:
    """
    Compare last N days with previous N days.

    Convenience wrapper for comparar_periodos.

    Args:
        dias: Number of days in each period

    Returns:
        Same structure as comparar_periodos
    """
    hoy = date.today()

    periodo_actual = (hoy - timedelta(days=dias - 1), hoy)
    periodo_anterior = (hoy - timedelta(days=2 * dias - 1), hoy - timedelta(days=dias))

    return comparar_periodos(periodo_actual, periodo_anterior)


# =============================================================================
# ANOMALY DETECTION
# =============================================================================

def detectar_anomalias(dias: int = 30, umbral_sigma: float = 2.0) -> List[Dict]:
    """
    Detect days with metrics outside N standard deviations.

    Simple statistical anomaly detection:
    - Calculates mean and std dev for each metric over the period
    - Flags days where any metric exceeds mean + (umbral_sigma * std_dev)

    Args:
        dias: Number of days to analyze
        umbral_sigma: Threshold in standard deviations (default: 2.0)

    Returns:
        List of dicts with anomalous days and the metrics that triggered
    """
    df = get_tendencia_diaria(dias)

    if df.empty:
        return []

    anomalias = []
    metricas = ['total_noticias', 'total_riesgo', 'total_oportunidad']

    for metrica in metricas:
        if metrica not in df.columns:
            continue

        media = df[metrica].mean()
        std = df[metrica].std()

        if std == 0:
            continue

        umbral_alto = media + (umbral_sigma * std)
        umbral_bajo = media - (umbral_sigma * std)

        # Find anomalous days
        anomalos_altos = df[df[metrica] > umbral_alto]
        anomalos_bajos = df[df[metrica] < umbral_bajo]

        for _, row in anomalos_altos.iterrows():
            anomalias.append({
                "fecha": row['fecha'].strftime('%Y-%m-%d'),
                "metrica": metrica,
                "valor": int(row[metrica]),
                "media": round(media, 1),
                "desviacion": round(std, 1),
                "tipo": "alto",
                "sigma": round((row[metrica] - media) / std, 2)
            })

        for _, row in anomalos_bajos.iterrows():
            anomalias.append({
                "fecha": row['fecha'].strftime('%Y-%m-%d'),
                "metrica": metrica,
                "valor": int(row[metrica]),
                "media": round(media, 1),
                "desviacion": round(std, 1),
                "tipo": "bajo",
                "sigma": round((row[metrica] - media) / std, 2)
            })

    # Sort by date
    anomalias.sort(key=lambda x: x['fecha'], reverse=True)

    return anomalias


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("\n=== Daily Trend (last 7 days) ===")
    df = get_tendencia_diaria(7)
    print(df.to_string(index=False))

    print("\n=== Weekly Summary (last 4 weeks) ===")
    df = get_resumen_semanal(4)
    print(df.to_string(index=False))

    print("\n=== Period Comparison (7 days) ===")
    comp = comparar_con_periodo_anterior(7)
    print(f"Current period: {comp['periodo_actual']['noticias']} news")
    print(f"Previous period: {comp['periodo_anterior']['noticias']} news")
    print(f"Change: {comp['cambios_pct']['noticias']}%")

    print("\n=== Anomalies (last 30 days) ===")
    anomalies = detectar_anomalias(30)
    for a in anomalies[:5]:
        print(f"  {a['fecha']}: {a['metrica']} = {a['valor']} ({a['tipo']}, {a['sigma']}σ)")
