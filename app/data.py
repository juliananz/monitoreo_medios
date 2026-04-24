"""
Data access layer for the Dash dashboard — BigQuery edition.

All query functions accept fecha_inicio / fecha_fin (str or date) and return
DataFrames. Reads exclusively from dbt-built marts plus the stg_entidades view
for the entity name/tipo join. Function signatures match the previous SQLite
implementation so app_dash.py is unchanged.

Auth: uses Application Default Credentials. Run `gcloud auth application-default
login` locally or set GOOGLE_APPLICATION_CREDENTIALS to a service-account key.
"""

import re
import sys
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from google.cloud import bigquery

from analisis.utils import clasificar_tipo
from config.settings import BASE_DIR, BQ_DATASET, BQ_PROJECT, OUTPUT_DIR

# ---------------------------------------------------------------------------
# BigQuery client + table refs
# ---------------------------------------------------------------------------

_DS = f"{BQ_PROJECT}.{BQ_DATASET}"
_T_AGG_DIARIA = f"`{_DS}.mart_agg_diaria`"
_T_AGG_TEMA = f"`{_DS}.mart_agg_tema_diaria`"
_T_AGG_MEDIO = f"`{_DS}.mart_agg_medio_diaria`"
_T_AGG_ENTIDAD = f"`{_DS}.mart_agg_entidad_diaria`"
_T_ENTIDADES = f"`{_DS}.stg_entidades`"
_T_NOTICIAS = f"`{_DS}.mart_noticias`"

_client: bigquery.Client | None = None


def _bq() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=BQ_PROJECT)
    return _client


def _to_date(d) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.fromisoformat(str(d)[:10]).date()


def _q(sql: str, **params) -> pd.DataFrame:
    """Run a parametrised BigQuery SQL and return a DataFrame."""
    query_params = []
    for key, val in params.items():
        if isinstance(val, date) and not isinstance(val, datetime):
            query_params.append(bigquery.ScalarQueryParameter(key, "DATE", val))
        elif isinstance(val, int):
            query_params.append(bigquery.ScalarQueryParameter(key, "INT64", val))
        else:
            query_params.append(bigquery.ScalarQueryParameter(key, "STRING", val))
    cfg = bigquery.QueryJobConfig(query_parameters=query_params)
    return _bq().query(sql, job_config=cfg).result().to_dataframe()


# ---------------------------------------------------------------------------
# Municipality / Region helpers (regiones_coahuila.xlsx) — unchanged
# ---------------------------------------------------------------------------

_MUNICIPIO_MAP: dict | None = None


def _norm_str(s: str) -> str:
    """Lowercase + strip diacritics for fuzzy matching."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower()) if unicodedata.category(c) != "Mn"
    )


def _build_municipio_map() -> dict:
    global _MUNICIPIO_MAP
    if _MUNICIPIO_MAP is not None:
        return _MUNICIPIO_MAP

    xlsx_path = BASE_DIR / "regiones_coahuila.xlsx"
    df = pd.read_excel(xlsx_path)
    df.columns = ["id", "municipio", "region"]

    mapping: dict = {}
    for _, row in df.iterrows():
        municipio = row["municipio"].strip()
        region = row["region"].strip()
        mapping[_norm_str(municipio)] = (municipio, region)

    _MUNICIPIO_MAP = mapping
    return mapping


def _medio_to_municipio(medio) -> tuple:
    if not isinstance(medio, str) or "Google News - " not in medio:
        return None, None

    part = medio.split("Google News - ", 1)[1].strip()
    part = re.sub(r"\s+coahuila$", "", part.lower()).strip()
    part_norm = _norm_str(part)

    mapa = _build_municipio_map()

    if part_norm in mapa:
        return mapa[part_norm]

    for key, val in mapa.items():
        if key in part_norm or part_norm in key:
            return val

    return None, None


def _build_titulo_keywords() -> list:
    mapa = _build_municipio_map()
    no_rss = {
        _norm_str("Saltillo"),
        _norm_str("Torreón"),
        _norm_str("Matamoros"),
        _norm_str("Francisco I. Madero"),
        _norm_str("Ramos Arizpe"),
        _norm_str("San Pedro"),
        _norm_str("Viesca"),
    }
    entries = [(k, v[0], v[1]) for k, v in mapa.items() if k in no_rss]
    return sorted(entries, key=lambda x: len(x[0]), reverse=True)


_TITULO_KEYWORDS: list | None = None


def _titulo_to_municipio(titulo: str) -> tuple:
    global _TITULO_KEYWORDS
    if _TITULO_KEYWORDS is None:
        _TITULO_KEYWORDS = _build_titulo_keywords()

    titulo_norm = _norm_str(titulo if isinstance(titulo, str) else "")
    for keyword, municipio, region in _TITULO_KEYWORDS:
        if keyword in titulo_norm:
            return municipio, region
    return None, None


# ---------------------------------------------------------------------------
# KPI summary
# ---------------------------------------------------------------------------


def get_kpis(fecha_inicio, fecha_fin) -> dict:
    """Aggregate KPI totals for the date range + week-over-week delta."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)

    row = _q(
        f"""SELECT COALESCE(SUM(total_noticias), 0) AS total,
                   COALESCE(SUM(total_relevantes), 0) AS relevantes,
                   COALESCE(SUM(total_riesgo), 0) AS riesgo,
                   COALESCE(SUM(total_oportunidad), 0) AS oportunidad,
                   COALESCE(SUM(requieren_analisis), 0) AS analisis
            FROM {_T_AGG_DIARIA}
            WHERE fecha BETWEEN @fi AND @ff""",
        fi=fi, ff=ff,
    ).iloc[0]

    today = date.today()
    w_cur = (today - timedelta(days=7), today)
    w_prev = (today - timedelta(days=14), today - timedelta(days=8))

    wow_sql = f"""SELECT COALESCE(SUM(total_noticias),0) AS t,
                         COALESCE(SUM(total_riesgo),0) AS r,
                         COALESCE(SUM(total_oportunidad),0) AS o
                  FROM {_T_AGG_DIARIA}
                  WHERE fecha BETWEEN @fi AND @ff"""
    cur = _q(wow_sql, fi=w_cur[0], ff=w_cur[1]).iloc[0]
    prev = _q(wow_sql, fi=w_prev[0], ff=w_prev[1]).iloc[0]

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
    """Return daily global metrics from mart_agg_diaria ordered by date."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    return _q(
        f"""SELECT fecha, total_noticias, total_relevantes, total_riesgo,
                   total_oportunidad, total_mixto, requieren_analisis
            FROM {_T_AGG_DIARIA}
            WHERE fecha BETWEEN @fi AND @ff
            ORDER BY fecha""",
        fi=fi, ff=ff,
    )


# ---------------------------------------------------------------------------
# Topic trends
# ---------------------------------------------------------------------------


def get_topic_trends(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return per-topic daily rows from mart_agg_tema_diaria (tema name inline)."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    return _q(
        f"""SELECT fecha, nombre_tema AS tema,
                   total_noticias, total_riesgo, total_oportunidad
            FROM {_T_AGG_TEMA}
            WHERE fecha BETWEEN @fi AND @ff
            ORDER BY fecha""",
        fi=fi, ff=ff,
    )


# ---------------------------------------------------------------------------
# Media source volume
# ---------------------------------------------------------------------------


def get_medio_volume(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return aggregated per-media-source totals from mart_agg_medio_diaria."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    return _q(
        f"""SELECT medio,
                   SUM(total_noticias) AS total_noticias,
                   SUM(total_relevantes) AS total_relevantes,
                   SUM(total_riesgo) AS total_riesgo,
                   SUM(total_oportunidad) AS total_oportunidad
            FROM {_T_AGG_MEDIO}
            WHERE fecha BETWEEN @fi AND @ff
            GROUP BY medio
            ORDER BY total_noticias DESC""",
        fi=fi, ff=ff,
    )


# ---------------------------------------------------------------------------
# Region distribution — no mart; derived from mart_noticias for signature parity
# ---------------------------------------------------------------------------


def get_region_dist(fecha_inicio, fecha_fin, nivel_geografico: str = "all") -> pd.DataFrame:
    """Return per-nivel-geografico totals. Pass nivel_geografico='all' to skip geo filter."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    if nivel_geografico and nivel_geografico != "all":
        return _q(
            f"""SELECT nivel_geografico,
                       COUNT(*) AS total_noticias,
                       SUM(riesgo) AS total_riesgo,
                       SUM(oportunidad) AS total_oportunidad
                FROM {_T_NOTICIAS}
                WHERE fecha BETWEEN @fi AND @ff AND nivel_geografico = @nivel
                GROUP BY nivel_geografico
                ORDER BY total_noticias DESC""",
            fi=fi, ff=ff, nivel=nivel_geografico,
        )
    return _q(
        f"""SELECT nivel_geografico,
                   COUNT(*) AS total_noticias,
                   SUM(riesgo) AS total_riesgo,
                   SUM(oportunidad) AS total_oportunidad
            FROM {_T_NOTICIAS}
            WHERE fecha BETWEEN @fi AND @ff
            GROUP BY nivel_geografico
            ORDER BY total_noticias DESC""",
        fi=fi, ff=ff,
    )


# ---------------------------------------------------------------------------
# Entity trends — join mart_agg_entidad_diaria with stg_entidades (name + tipo)
# ---------------------------------------------------------------------------


def get_entity_trends(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return top-20 entities by mentions joined with stg_entidades for name/tipo."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    return _q(
        f"""SELECT e.nombre_canonico AS entidad, e.tipo,
                   SUM(aed.menciones) AS menciones,
                   SUM(aed.noticias_riesgo) AS noticias_riesgo,
                   SUM(aed.noticias_oportunidad) AS noticias_oportunidad
            FROM {_T_AGG_ENTIDAD} aed
            JOIN {_T_ENTIDADES} e ON e.id = aed.entidad_id
            WHERE aed.fecha BETWEEN @fi AND @ff
            GROUP BY aed.entidad_id, e.nombre_canonico, e.tipo
            ORDER BY menciones DESC
            LIMIT 20""",
        fi=fi, ff=ff,
    )


def get_entity_sparkline(entidad_nombre: str, fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Return daily mention counts for one entity identified by its canonical name."""
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    return _q(
        f"""SELECT aed.fecha, SUM(aed.menciones) AS menciones
            FROM {_T_AGG_ENTIDAD} aed
            JOIN {_T_ENTIDADES} e ON e.id = aed.entidad_id
            WHERE e.nombre_canonico = @name
              AND aed.fecha BETWEEN @fi AND @ff
            GROUP BY aed.fecha
            ORDER BY aed.fecha""",
        name=entidad_nombre, fi=fi, ff=ff,
    )


# ---------------------------------------------------------------------------
# News row-level queries
# ---------------------------------------------------------------------------


def get_noticias(fecha_inicio, fecha_fin, nivel_geografico: str = "all") -> pd.DataFrame:
    """
    Return latest 500 news rows in the date range from mart_noticias.
    Adds a computed 'tipo' column (RIESGO / OPORTUNIDAD / MIXTO / NEUTRO).
    """
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    extra = "AND nivel_geografico = @nivel" if nivel_geografico and nivel_geografico != "all" else ""
    params = {"fi": fi, "ff": ff}
    if extra:
        params["nivel"] = nivel_geografico

    df = _q(
        f"""SELECT id, titulo, url,
                   FORMAT_DATE('%Y-%m-%d', fecha) AS fecha,
                   medio, nivel_geografico, riesgo, oportunidad,
                   requiere_analisis_profundo, personas, organizaciones, lugares, temas
            FROM {_T_NOTICIAS}
            WHERE fecha BETWEEN @fi AND @ff {extra}
            ORDER BY fecha DESC
            LIMIT 500""",
        **params,
    )
    if not df.empty:
        df["riesgo"] = pd.to_numeric(df["riesgo"], errors="coerce").fillna(0).astype(int)
        df["oportunidad"] = pd.to_numeric(df["oportunidad"], errors="coerce").fillna(0).astype(int)
        df["tipo"] = df.apply(lambda r: clasificar_tipo(r["riesgo"], r["oportunidad"]), axis=1)
    return df


# ---------------------------------------------------------------------------
# Municipios Coahuila (regiones_coahuila.xlsx mapping)
# ---------------------------------------------------------------------------


def get_noticias_municipios(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """
    Return news attributed to a Coahuila municipality via two strategies:
    1. Google News RSS medio  → exact municipality from feed name.
    2. Non-RSS sources        → first municipality name found in the titulo.
    """
    fi, ff = _to_date(fecha_inicio), _to_date(fecha_fin)
    df = _q(
        f"""SELECT id, titulo, url,
                   FORMAT_DATE('%Y-%m-%d', fecha) AS fecha,
                   medio, riesgo, oportunidad, requiere_analisis_profundo
            FROM {_T_NOTICIAS}
            WHERE fecha BETWEEN @fi AND @ff
            ORDER BY fecha DESC""",
        fi=fi, ff=ff,
    )
    if df.empty:
        return df

    df["riesgo"] = pd.to_numeric(df["riesgo"], errors="coerce").fillna(0).astype(int)
    df["oportunidad"] = pd.to_numeric(df["oportunidad"], errors="coerce").fillna(0).astype(int)
    df["tipo"] = df.apply(lambda r: clasificar_tipo(r["riesgo"], r["oportunidad"]), axis=1)

    def _assign(row):
        mun, reg = _medio_to_municipio(row["medio"])
        if reg is None:
            mun, reg = _titulo_to_municipio(row["titulo"])
        return pd.Series({"municipio": mun, "region": reg})

    parsed = df.apply(_assign, axis=1)
    df = pd.concat([df, parsed], axis=1)
    return df[df["region"].notna()].copy()


def get_municipio_counts(fecha_inicio, fecha_fin) -> pd.DataFrame:
    """Aggregated news counts per (region, municipio): total, riesgos, oportunidades."""
    df = get_noticias_municipios(fecha_inicio, fecha_fin)
    if df.empty:
        return df

    return (
        df.groupby(["region", "municipio"])
        .agg(
            total=("id", "count"),
            riesgos=("riesgo", lambda x: (x > 0).sum()),
            oportunidades=("oportunidad", lambda x: (x > 0).sum()),
        )
        .reset_index()
        .sort_values(["region", "total"], ascending=[True, False])
    )


# ---------------------------------------------------------------------------
# LLM summary (filesystem, unchanged)
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
