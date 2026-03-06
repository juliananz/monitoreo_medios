"""
Dashboard de Monitoreo de Medios — Secretaria de Economia de Coahuila.

Layout:
  - Sidebar: filtros de periodo, tipo, nivel geografico
  - Resumen ejecutivo LLM del dia
  - KPIs con variacion vs semana anterior
  - Tabs: Noticias | Riesgos & Oportunidades | Tendencias | Entidades | Regiones
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
import hashlib
import sqlite3
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from analisis.utils import get_db_connection, clasificar_tipo
from analisis.tendencias import (
    get_tendencia_diaria,
    get_tendencia_temas,
    comparar_con_periodo_anterior,
)
from analisis.queries import (
    get_top_entidades_periodo,
    get_conteo_por_region,
    get_conteo_diario,
)
from config.settings import DB_PATH, OUTPUT_DIR

# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="Monitoreo de Medios | SEC Coahuila",
    page_icon="\U0001f4f0",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# AUTHENTICATION
# =============================================================================


def check_password() -> bool:
    correct_password = os.getenv("DASHBOARD_PASSWORD", "")

    if not correct_password:
        st.sidebar.warning("Modo desarrollo: sin contrasena.")
        return True

    if st.session_state.get("authenticated", False):
        return True

    st.title("Monitoreo de Medios")
    st.subheader("Secretaria de Economia de Coahuila")
    password = st.text_input("Contrasena", type="password")
    if st.button("Entrar"):
        if hashlib.sha256(password.encode()).hexdigest() == hashlib.sha256(correct_password.encode()).hexdigest():
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Contrasena incorrecta")
    return False


if not check_password():
    st.stop()

if os.getenv("DASHBOARD_PASSWORD") and st.sidebar.button("Cerrar sesion"):
    st.session_state["authenticated"] = False
    st.rerun()


# =============================================================================
# DATA LOADING  (ttl=3600 → auto-refresh every hour)
# =============================================================================


@st.cache_data(ttl=3600)
def _db_debug_info() -> str:
    """Return one-line debug string shown in sidebar to diagnose connection issues."""
    import shutil, tempfile
    db_path = Path(DB_PATH)
    lines = [
        f"sqlite3 version: {sqlite3.sqlite_version}",
        f"DB size: {db_path.stat().st_size // 1024} KB" if db_path.exists() else "DB not found",
        f"DB dir writable: {os.access(str(db_path.parent), os.W_OK)}",
        f"DB file writable: {os.access(str(db_path), os.W_OK)}",
    ]
    for label, uri in [
        ("normal", str(db_path)),
        ("immutable", f"file://{db_path.as_posix()}?immutable=1"),
    ]:
        try:
            c = sqlite3.connect(uri, uri=(uri.startswith("file:")))
            n = c.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]
            c.close()
            lines.append(f"{label} OK (schema rows: {n})")
        except Exception as e:
            lines.append(f"{label} FAIL: {e}")
    return " | ".join(lines)


@st.cache_data(ttl=3600)
def cargar_noticias(dias: int = 30) -> pd.DataFrame:
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                """
                SELECT
                    fecha, medio, titulo, url,
                    personas, organizaciones, lugares,
                    nivel_geografico,
                    requiere_analisis_profundo,
                    riesgo, oportunidad
                FROM noticias
                WHERE relevante = 1
                  AND fecha >= ?
                ORDER BY fecha DESC
                """,
                conn,
                params=[fecha_inicio],
            )
        return df
    except Exception as e:
        st.error(f"Error cargando datos: {e}\nDB_PATH: {DB_PATH}\nExiste: {Path(DB_PATH).exists()}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def cargar_tendencia_diaria(dias: int = 30) -> pd.DataFrame:
    return get_tendencia_diaria(dias)


@st.cache_data(ttl=3600)
def cargar_tendencia_temas(dias: int = 30) -> pd.DataFrame:
    return get_tendencia_temas(dias)


@st.cache_data(ttl=3600)
def cargar_top_entidades(dias: int = 30, limit: int = 20):
    return get_top_entidades_periodo(dias, limit)


@st.cache_data(ttl=3600)
def cargar_conteo_regiones():
    return get_conteo_por_region()


@st.cache_data(ttl=3600)
def cargar_comparacion(dias: int = 7):
    return comparar_con_periodo_anterior(dias)


@st.cache_data(ttl=3600)
def cargar_conteo_diario(dias: int = 30):
    return get_conteo_diario(dias)


def cargar_resumen_llm() -> tuple[str, str]:
    """Return (fecha_str, texto) of the most recent LLM summary, or ('', '')."""
    for delta in range(0, 4):
        dia = date.today() - timedelta(days=delta)
        path = Path(OUTPUT_DIR) / f"resumen_llm_{dia.isoformat()}.txt"
        if path.exists():
            return dia.isoformat(), path.read_text(encoding="utf-8")
    return "", ""


# =============================================================================
# SIDEBAR
# =============================================================================

st.sidebar.title("Filtros")

PERIODOS = {
    "Hoy": 1,
    "Ultimos 7 dias": 7,
    "Ultimos 14 dias": 14,
    "Ultimos 30 dias": 30,
    "Ultimos 60 dias": 60,
}
periodo_label = st.sidebar.selectbox("Periodo", list(PERIODOS.keys()), index=2)
dias_sel = PERIODOS[periodo_label]

# DB diagnostics — visible even when data fails so we can debug remotely
with st.sidebar.expander("Diagnostico DB"):
    for line in _db_debug_info().split(" | "):
        st.caption(line)

# Load raw data for this period
df_raw = cargar_noticias(dias_sel)

if df_raw.empty:
    st.warning("No hay datos disponibles en la base de datos.")
    st.stop()

df = df_raw.copy()
df = df.fillna("")
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
df["tipo"] = df.apply(lambda r: clasificar_tipo(r["riesgo"], r["oportunidad"]), axis=1)

# Type filter
tipos_disp = sorted(df["tipo"].unique())
tipos_sel = st.sidebar.multiselect("Tipo de noticia", tipos_disp, default=tipos_disp)

# Geo filter
niveles_disp = [n for n in sorted(df["nivel_geografico"].unique()) if n]
niveles_sel = st.sidebar.multiselect("Nivel geografico", niveles_disp, default=niveles_disp)

solo_profundo = st.sidebar.checkbox("Solo requieren analisis profundo")

# Apply filters
mask = df["tipo"].isin(tipos_sel)
if niveles_sel:
    mask &= df["nivel_geografico"].isin(niveles_sel)
if solo_profundo:
    mask &= df["requiere_analisis_profundo"] == 1
df_f = df[mask]

st.sidebar.divider()
if st.sidebar.button("Actualizar datos"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(f"DB: {Path(DB_PATH).name}")
st.sidebar.caption(f"Noticias cargadas: {len(df_raw)}")
st.sidebar.caption(f"Filtradas: {len(df_f)}")

# =============================================================================
# HEADER
# =============================================================================

st.title("Monitoreo de Medios")
st.caption("Secretaria de Economia — Gobierno de Coahuila")

# =============================================================================
# LLM EXECUTIVE SUMMARY
# =============================================================================

fecha_resumen, texto_resumen = cargar_resumen_llm()
if texto_resumen:
    label = "Resumen Ejecutivo del dia" if fecha_resumen == date.today().isoformat() else f"Resumen Ejecutivo — {fecha_resumen}"
    with st.expander(label, expanded=True):
        st.markdown(texto_resumen)
else:
    st.info("Sin resumen ejecutivo disponible. El pipeline genera uno diariamente.")

st.divider()

# =============================================================================
# KPIs
# =============================================================================

try:
    comp = cargar_comparacion(7)
    cambios = comp.get("cambios_pct", {})
except Exception:
    cambios = {}

total = len(df_f)
riesgos = int(df_f["tipo"].isin(["RIESGO", "MIXTO"]).sum())
oportunidades = int(df_f["tipo"].isin(["OPORTUNIDAD", "MIXTO"]).sum())
analisis = int(df_f["requiere_analisis_profundo"].sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total noticias", total)
c2.metric(
    "Riesgos",
    riesgos,
    delta=f"{cambios.get('riesgos', 0)}% vs semana anterior" if cambios.get("riesgos") else None,
    delta_color="inverse",
)
c3.metric(
    "Oportunidades",
    oportunidades,
    delta=f"{cambios.get('oportunidades', 0)}% vs semana anterior" if cambios.get("oportunidades") else None,
)
c4.metric("Requieren analisis profundo", analisis)

st.divider()

# =============================================================================
# TABS
# =============================================================================

tab_noticias, tab_ry_o, tab_tendencias, tab_entidades, tab_regiones = st.tabs([
    "Noticias",
    "Riesgos y Oportunidades",
    "Tendencias",
    "Entidades",
    "Regiones",
])


# ---------------------------------------------------------------------------
# TAB: NOTICIAS
# ---------------------------------------------------------------------------

def _df_display(df_src: pd.DataFrame) -> pd.DataFrame:
    """Prepare a dataframe slice for display with formatted date."""
    cols = ["fecha", "tipo", "medio", "titulo", "url", "nivel_geografico", "organizaciones", "lugares"]
    out = df_src[cols].copy()
    out["fecha"] = out["fecha"].dt.strftime("%Y-%m-%d")
    return out.sort_values("fecha", ascending=False)


_link_col = st.column_config.LinkColumn("Enlace", display_text="Ver nota")
_col_cfg_base = {
    "url": _link_col,
    "fecha": st.column_config.TextColumn("Fecha", width="small"),
    "tipo": st.column_config.TextColumn("Tipo", width="small"),
    "medio": st.column_config.TextColumn("Medio", width="medium"),
    "titulo": st.column_config.TextColumn("Titulo", width="large"),
    "nivel_geografico": st.column_config.TextColumn("Nivel Geo.", width="small"),
    "organizaciones": st.column_config.TextColumn("Organizaciones", width="medium"),
    "lugares": st.column_config.TextColumn("Lugares", width="medium"),
}

with tab_noticias:
    st.subheader(f"Noticias relevantes — {periodo_label}")
    if df_f.empty:
        st.info("No hay noticias para los filtros seleccionados.")
    else:
        # Quick summary bar
        tipo_counts = df_f["tipo"].value_counts()
        col_sum = st.columns(len(tipo_counts))
        for i, (tipo, cnt) in enumerate(tipo_counts.items()):
            col_sum[i].metric(tipo, cnt)

        st.dataframe(
            _df_display(df_f),
            column_config=_col_cfg_base,
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# TAB: RIESGOS Y OPORTUNIDADES
# ---------------------------------------------------------------------------

with tab_ry_o:
    df_riesgo = df_f[df_f["tipo"].isin(["RIESGO", "MIXTO"])]
    df_oport = df_f[df_f["tipo"].isin(["OPORTUNIDAD", "MIXTO"])]

    col_r, col_o = st.columns(2)

    with col_r:
        st.markdown(f"### Riesgos ({len(df_riesgo)})")
        if df_riesgo.empty:
            st.success("Sin riesgos en el periodo seleccionado.")
        else:
            st.dataframe(
                _df_display(df_riesgo),
                column_config=_col_cfg_base,
                use_container_width=True,
                hide_index=True,
            )

    with col_o:
        st.markdown(f"### Oportunidades ({len(df_oport)})")
        if df_oport.empty:
            st.info("Sin oportunidades en el periodo seleccionado.")
        else:
            st.dataframe(
                _df_display(df_oport),
                column_config=_col_cfg_base,
                use_container_width=True,
                hide_index=True,
            )

    # Noticias que requieren atencion inmediata
    df_profundo = df_f[df_f["requiere_analisis_profundo"] == 1]
    if not df_profundo.empty:
        st.divider()
        st.markdown(f"### Requieren analisis profundo ({len(df_profundo)})")
        st.dataframe(
            _df_display(df_profundo),
            column_config=_col_cfg_base,
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# TAB: TENDENCIAS
# ---------------------------------------------------------------------------

with tab_tendencias:
    periodo_t = st.selectbox(
        "Periodo de analisis",
        [7, 14, 30, 60],
        index=2,
        format_func=lambda x: f"Ultimos {x} dias",
        key="periodo_t",
    )

    # --- Volumen diario ---
    st.markdown("#### Volumen diario de noticias")
    try:
        df_tend = cargar_tendencia_diaria(periodo_t)
        if not df_tend.empty:
            chart_data = df_tend.set_index("fecha")[
                ["total_noticias", "total_riesgo", "total_oportunidad"]
            ].rename(columns={
                "total_noticias": "Total",
                "total_riesgo": "Riesgos",
                "total_oportunidad": "Oportunidades",
            })
            st.line_chart(chart_data)
        else:
            # Fallback: compute directly from noticias table
            daily = cargar_conteo_diario(periodo_t)
            if daily:
                df_daily = pd.DataFrame(daily).set_index("fecha")
                st.line_chart(df_daily[["total", "riesgos", "oportunidades"]].rename(columns={
                    "total": "Total", "riesgos": "Riesgos", "oportunidades": "Oportunidades"
                }))
            else:
                st.info("Sin datos de tendencias diarias. Ejecuta el pipeline para generar agregaciones.")
    except Exception as e:
        st.warning(f"Tendencias diarias no disponibles: {e}")

    st.divider()

    # --- Por tema ---
    st.markdown("#### Tendencias por tema")
    try:
        df_temas = cargar_tendencia_temas(periodo_t)
        if not df_temas.empty:
            df_pivot = df_temas.pivot_table(
                index="fecha", columns="tema", values="total_noticias", aggfunc="sum"
            ).fillna(0)
            st.line_chart(df_pivot)
        else:
            st.info("Sin datos de tendencias por tema.")
    except Exception as e:
        st.warning(f"Tendencias por tema no disponibles: {e}")

    st.divider()

    # --- Por medio ---
    st.markdown("#### Noticias por medio")
    if not df_f.empty:
        medio_data = (
            df_f.groupby("medio")
            .agg(Total=("titulo", "count"), Riesgos=("riesgo", "sum"), Oportunidades=("oportunidad", "sum"))
            .sort_values("Total", ascending=False)
        )
        col_chart_m, col_table_m = st.columns([1, 1])
        with col_chart_m:
            st.bar_chart(medio_data["Total"])
        with col_table_m:
            st.dataframe(medio_data, use_container_width=True)


# ---------------------------------------------------------------------------
# TAB: ENTIDADES
# ---------------------------------------------------------------------------

with tab_entidades:
    periodo_e = st.selectbox(
        "Periodo",
        [7, 14, 30, 60],
        index=2,
        format_func=lambda x: f"Ultimos {x} dias",
        key="periodo_e",
    )

    try:
        top_ents = cargar_top_entidades(periodo_e, 20)
        if top_ents:
            df_ents = pd.DataFrame(top_ents)

            col_chart_e, col_table_e = st.columns([1, 1])
            with col_chart_e:
                st.markdown("#### Top 10 entidades")
                st.bar_chart(df_ents.set_index("entidad")["total_menciones"].head(10))
            with col_table_e:
                st.markdown("#### Detalle completo")
                st.dataframe(
                    df_ents[["entidad", "tipo", "total_menciones", "total_riesgo", "total_oportunidad", "dias_activos"]],
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info("No hay datos de entidades disponibles para este periodo.")
    except Exception as e:
        st.warning(f"Entidades no disponibles: {e}")


# ---------------------------------------------------------------------------
# TAB: REGIONES
# ---------------------------------------------------------------------------

with tab_regiones:
    st.markdown("#### Distribucion por nivel geografico")
    if not df_f.empty:
        geo_counts = df_f["nivel_geografico"].value_counts()
        if not geo_counts.empty:
            st.bar_chart(geo_counts)

        st.divider()
        st.markdown("#### Riesgos y oportunidades por nivel geografico")
        geo_summary = (
            df_f.groupby("nivel_geografico")
            .agg(Total=("titulo", "count"), Riesgos=("riesgo", "sum"), Oportunidades=("oportunidad", "sum"))
            .sort_values("Total", ascending=False)
        )
        st.dataframe(geo_summary, use_container_width=True)

    st.divider()
    st.markdown("#### Por region (municipio / zona)")
    try:
        regiones = cargar_conteo_regiones()
        if regiones:
            df_reg = pd.DataFrame(regiones)
            col_chart_reg, col_table_reg = st.columns([1, 1])
            with col_chart_reg:
                st.bar_chart(df_reg.set_index("region")["total"].head(15))
            with col_table_reg:
                st.dataframe(
                    df_reg[["region", "tipo_region", "total", "riesgos", "oportunidades"]],
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info("No hay datos de regiones disponibles.")
    except Exception as e:
        st.info(f"Datos de regiones no disponibles: {e}")
