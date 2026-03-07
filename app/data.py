"""
Data access layer for the Dash dashboard.

All query functions accept fecha_inicio / fecha_fin (str or date) and return DataFrames.
No pipeline dependencies — only analisis.utils and config.settings are imported.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import date, timedelta

import pandas as pd

from analisis.utils import clasificar_tipo, get_db_connection
from config.settings import OUTPUT_DIR


def _s(d) -> str:
    """Convert date / datetime / str to ISO-8601 date string."""
    return d if isinstance(d, str) else d.isoformat()


# ---------------------------------------------------------------------------
# KPI summary
# ---------------------------------------------------------------------------


def get_kpis(fecha_inicio, fecha_fin) -> dict:
    """
    Aggregate KPI totals for the given date range from agregacion_diaria.

    Also computes week-over-week delta (last 7 days vs prior 7, fixed window).
    Returns dict with keys: total, relevantes, riesgo, oportunidad, analisis,
    delta_total, delta_riesgo, delta_oportunidad (None when no prior data exists).
    """
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        row = pd.read_sql_query(
            """SELECT COALESCE(SUM(total_noticias), 0) AS total,
                      COALESCE(SUM(total_relevantes), 0) AS relevantes,
                      COALESCE(SUM(total_riesgo), 0) AS riesgo,
                      COALESCE(SUM(total_oportunidad), 0) AS oportunidad,
                      COALESCE(SUM(requieren_analisis), 0) AS analisis
               FROM agregacion_diaria WHERE fecha BETWEEN ? AND ?""",
            conn,
            params=(fi, ff),
        ).iloc[0]

    today = date.today()
    w_cur = ((today - timedelta(days=7)).isoformat(), today.isoformat())
    w_prev = ((today - timedelta(days=14)).isoformat(), (today - timedelta(days=8)).isoformat())

    with get_db_connection() as conn:
        cur = pd.read_sql_query(
            "SELECT COALESCE(SUM(total_noticias),0) t, COALESCE(SUM(total_riesgo),0) r, "
            "COALESCE(SUM(total_oportunidad),0) o FROM agregacion_diaria WHERE fecha BETWEEN ? AND ?",
            conn,
            params=w_cur,
        ).iloc[0]
        prev = pd.read_sql_query(
            "SELECT COALESCE(SUM(total_noticias),0) t, COALESCE(SUM(total_riesgo),0) r, "
            "COALESCE(SUM(total_oportunidad),0) o FROM agregacion_diaria WHERE fecha BETWEEN ? AND ?",
            conn,
            params=w_prev,
        ).iloc[0]

    def _pct(c, p):
        return round((c - p) / p * 100) if p else None

    return {
        "total": int(row["total"]),
        "relevantes": int(row["relevantes"]),
        "riesgo": int(row["riesgo"]),
        "oportunidad": int(row["oportunidad"]),
        "analisis": int(row["analisis"]),
        "delta_total": _pct(cur["t"], prev["t"]),
        "delta_riesgo": _pct(cur["r"], prev["r"]),
        "delta_oportunidad": _pct(cur["o"], prev["o"]),
    }


# ---------------------------------------------------------------------------
# Daily volume
# ---------------------------------------------------------------------------


def get_daily_volume(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return daily global metrics from agregacion_diaria ordered by date."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        return pd.read_sql_query(
            """SELECT fecha, total_noticias, total_relevantes, total_riesgo,
                      total_oportunidad, total_mixto, requieren_analisis
               FROM agregacion_diaria WHERE fecha BETWEEN ? AND ? ORDER BY fecha""",
            conn,
            params=(fi, ff),
        )


# ---------------------------------------------------------------------------
# Topic trends
# ---------------------------------------------------------------------------


def get_topic_trends(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return per-topic daily rows from agregacion_tema_diaria joined with temas.nombre."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        return pd.read_sql_query(
            """SELECT atd.fecha, t.nombre AS tema,
                      atd.total_noticias, atd.total_riesgo, atd.total_oportunidad
               FROM agregacion_tema_diaria atd
               JOIN temas t ON t.id = atd.tema_id
               WHERE atd.fecha BETWEEN ? AND ? ORDER BY atd.fecha""",
            conn,
            params=(fi, ff),
        )


# ---------------------------------------------------------------------------
# Media source volume
# ---------------------------------------------------------------------------


def get_medio_volume(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return aggregated per-media-source totals from agregacion_medio_diaria."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        return pd.read_sql_query(
            """SELECT medio,
                      SUM(total_noticias) AS total_noticias,
                      SUM(total_relevantes) AS total_relevantes,
                      SUM(total_riesgo) AS total_riesgo,
                      SUM(total_oportunidad) AS total_oportunidad
               FROM agregacion_medio_diaria WHERE fecha BETWEEN ? AND ?
               GROUP BY medio ORDER BY total_noticias DESC""",
            conn,
            params=(fi, ff),
        )


# ---------------------------------------------------------------------------
# Region distribution
# ---------------------------------------------------------------------------


def get_region_dist(fecha_inicio, fecha_fin, nivel_geografico: str = "all") -> pd.DataFrame:
    """Return per-nivel-geografico totals. Pass nivel_geografico='all' to skip geo filter."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    extra = "AND nivel_geografico = ?" if nivel_geografico and nivel_geografico != "all" else ""
    params: list = [fi, ff] + ([nivel_geografico] if extra else [])
    with get_db_connection() as conn:
        return pd.read_sql_query(
            f"""SELECT nivel_geografico,
                       SUM(total_noticias) AS total_noticias,
                       SUM(total_riesgo) AS total_riesgo,
                       SUM(total_oportunidad) AS total_oportunidad
                FROM agregacion_region_diaria WHERE fecha BETWEEN ? AND ? {extra}
                GROUP BY nivel_geografico ORDER BY total_noticias DESC""",
            conn,
            params=params,
        )


# ---------------------------------------------------------------------------
# Entity trends
# ---------------------------------------------------------------------------


def get_entity_trends(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return top-20 entities by mentions from agregacion_entidad_diaria joined with entidades."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        return pd.read_sql_query(
            """SELECT e.nombre_canonico AS entidad, e.tipo,
                      SUM(aed.menciones) AS menciones,
                      SUM(aed.noticias_riesgo) AS noticias_riesgo,
                      SUM(aed.noticias_oportunidad) AS noticias_oportunidad
               FROM agregacion_entidad_diaria aed
               JOIN entidades e ON e.id = aed.entidad_id
               WHERE aed.fecha BETWEEN ? AND ?
               GROUP BY aed.entidad_id, e.nombre_canonico, e.tipo
               ORDER BY menciones DESC LIMIT 20""",
            conn,
            params=(fi, ff),
        )


def get_entity_sparkline(entidad_nombre: str, fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return daily mention counts for one entity identified by its canonical name."""
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    with get_db_connection() as conn:
        return pd.read_sql_query(
            """SELECT aed.fecha, SUM(aed.menciones) AS menciones
               FROM agregacion_entidad_diaria aed
               JOIN entidades e ON e.id = aed.entidad_id
               WHERE e.nombre_canonico = ? AND aed.fecha BETWEEN ? AND ?
               GROUP BY aed.fecha ORDER BY aed.fecha""",
            conn,
            params=(entidad_nombre, fi, ff),
        )


# ---------------------------------------------------------------------------
# News direct query
# ---------------------------------------------------------------------------


def get_noticias(fecha_inicio, fecha_fin, nivel_geografico: str = "all") -> pd.DataFrame:
    """
    Return latest 500 news rows in the date range from the noticias table.
    Adds a computed 'tipo' column (RIESGO / OPORTUNIDAD / MIXTO / NEUTRO).
    """
    fi, ff = _s(fecha_inicio), _s(fecha_fin)
    extra = "AND nivel_geografico = ?" if nivel_geografico and nivel_geografico != "all" else ""
    params: list = [fi, ff] + ([nivel_geografico] if extra else [])
    with get_db_connection() as conn:
        df = pd.read_sql_query(
            f"""SELECT id, titulo, url, DATE(fecha) AS fecha, medio,
                       nivel_geografico, riesgo, oportunidad,
                       requiere_analisis_profundo, personas, organizaciones, lugares, temas
                FROM noticias WHERE DATE(fecha) BETWEEN ? AND ? {extra}
                ORDER BY fecha DESC LIMIT 500""",
            conn,
            params=params,
        )
    if not df.empty:
        df["riesgo"] = pd.to_numeric(df["riesgo"], errors="coerce").fillna(0).astype(int)
        df["oportunidad"] = pd.to_numeric(df["oportunidad"], errors="coerce").fillna(0).astype(int)
        df["tipo"] = df.apply(lambda r: clasificar_tipo(r["riesgo"], r["oportunidad"]), axis=1)
    return df


# ---------------------------------------------------------------------------
# LLM summary
# ---------------------------------------------------------------------------


def cargar_resumen_llm() -> tuple:
    """Return (fecha_str, texto) of the most recent LLM summary .txt file, or ('', '')."""
    today = date.today()
    for delta in range(4):
        dia = today - timedelta(days=delta)
        path = Path(OUTPUT_DIR) / f"resumen_llm_{dia.isoformat()}.txt"
        if path.exists():
            return dia.isoformat(), path.read_text(encoding="utf-8")
    return "", ""
