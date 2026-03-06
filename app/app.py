"""
Dashboard de Monitoreo de Medios — Secretaria de Economia de Coahuila.

Reads from data/salidas/dashboard_noticias.csv (exported by the pipeline).
No SQLite access needed — works on Streamlit Community Cloud read-only FS.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
import hashlib
from datetime import date, timedelta
from collections import Counter

import pandas as pd
from dateutil import parser as dateutil_parse
import streamlit as st

from analisis.utils import clasificar_tipo
from config.settings import OUTPUT_DIR

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
# PATHS
# =============================================================================

DASHBOARD_CSV = Path(OUTPUT_DIR) / "dashboard_noticias.csv"

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


def _parse_fecha(val):
    try:
        return dateutil_parse.parse(str(val)).date()
    except Exception:
        return None


@st.cache_data(ttl=3600)
def cargar_datos() -> pd.DataFrame:
    """Load all relevant news from dashboard CSV."""
    if not DASHBOARD_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(DASHBOARD_CSV)
    df["fecha"] = df["fecha"].apply(_parse_fecha)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.fillna("")
    df["riesgo"] = pd.to_numeric(df["riesgo"], errors="coerce").fillna(0).astype(int)
    df["oportunidad"] = pd.to_numeric(df["oportunidad"], errors="coerce").fillna(0).astype(int)
    df["requiere_analisis_profundo"] = pd.to_numeric(df["requiere_analisis_profundo"], errors="coerce").fillna(0).astype(int)
    df["tipo"] = df.apply(lambda r: clasificar_tipo(r["riesgo"], r["oportunidad"]), axis=1)
    return df


def cargar_resumen_llm() -> tuple:
    """Return (fecha_str, texto) of the most recent LLM summary."""
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
    "Todo": 3650,
}
periodo_label = st.sidebar.selectbox("Periodo", list(PERIODOS.keys()), index=2)
dias_sel = PERIODOS[periodo_label]

# Load full dataset
df_all = cargar_datos()

if df_all.empty:
    st.error(
        f"No hay datos disponibles.\n\n"
        f"CSV esperado: `{DASHBOARD_CSV}`\n\n"
        f"Existe: `{DASHBOARD_CSV.exists()}`\n\n"
        "Ejecuta el pipeline para generar los datos."
    )
    st.stop()

# Apply date filter
fecha_inicio = pd.Timestamp(date.today() - timedelta(days=dias_sel))
df = df_all[df_all["fecha"] >= fecha_inicio].copy()

# Sidebar filters on filtered data
tipos_disp = sorted(df["tipo"].unique())
tipos_sel = st.sidebar.multiselect("Tipo de noticia", tipos_disp, default=tipos_disp)

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
st.sidebar.caption(f"Total historico: {len(df_all)} noticias")
st.sidebar.caption(f"En periodo: {len(df)} | Filtradas: {len(df_f)}")
st.sidebar.caption(f"Datos: {DASHBOARD_CSV.name}")

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

total = len(df_f)
riesgos = int(df_f["tipo"].isin(["RIESGO", "MIXTO"]).sum())
oportunidades = int(df_f["tipo"].isin(["OPORTUNIDAD", "MIXTO"]).sum())
analisis = int(df_f["requiere_analisis_profundo"].sum())

# Week-over-week comparison
try:
    hoy = pd.Timestamp(date.today())
    semana_actual = df_all[(df_all["fecha"] >= hoy - timedelta(days=7)) & (df_all["fecha"] <= hoy)]
    semana_anterior = df_all[(df_all["fecha"] >= hoy - timedelta(days=14)) & (df_all["fecha"] < hoy - timedelta(days=7))]
    if len(semana_anterior) > 0:
        delta_noticias = round((len(semana_actual) - len(semana_anterior)) / len(semana_anterior) * 100)
        delta_riesgos = round((semana_actual["riesgo"].sum() - semana_anterior["riesgo"].sum()) / max(semana_anterior["riesgo"].sum(), 1) * 100)
        delta_oport = round((semana_actual["oportunidad"].sum() - semana_anterior["oportunidad"].sum()) / max(semana_anterior["oportunidad"].sum(), 1) * 100)
    else:
        delta_noticias = delta_riesgos = delta_oport = None
except Exception:
    delta_noticias = delta_riesgos = delta_oport = None

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total noticias", total, delta=f"{delta_noticias}% vs sem. ant." if delta_noticias is not None else None)
c2.metric("Riesgos", riesgos, delta=f"{delta_riesgos}% vs sem. ant." if delta_riesgos is not None else None, delta_color="inverse")
c3.metric("Oportunidades", oportunidades, delta=f"{delta_oport}% vs sem. ant." if delta_oport is not None else None)
c4.metric("Requieren analisis", analisis)

st.divider()

# =============================================================================
# TABS
# =============================================================================

tab_noticias, tab_ryo, tab_tendencias, tab_entidades, tab_regiones = st.tabs([
    "Noticias",
    "Riesgos y Oportunidades",
    "Tendencias",
    "Entidades",
    "Regiones",
])

# Shared column config for news tables
_link_col = st.column_config.LinkColumn("Enlace", display_text="Ver nota")
_col_cfg = {
    "url": _link_col,
    "fecha": st.column_config.TextColumn("Fecha", width="small"),
    "tipo": st.column_config.TextColumn("Tipo", width="small"),
    "medio": st.column_config.TextColumn("Medio", width="medium"),
    "titulo": st.column_config.TextColumn("Titulo", width="large"),
    "nivel_geografico": st.column_config.TextColumn("Nivel Geo.", width="small"),
    "organizaciones": st.column_config.TextColumn("Organizaciones", width="medium"),
    "lugares": st.column_config.TextColumn("Lugares", width="medium"),
}


def _prep_table(src: pd.DataFrame) -> pd.DataFrame:
    cols = ["fecha", "tipo", "medio", "titulo", "url", "nivel_geografico", "organizaciones", "lugares"]
    out = src[cols].copy()
    out["fecha"] = out["fecha"].dt.strftime("%Y-%m-%d")
    return out.sort_values("fecha", ascending=False)


# ---------------------------------------------------------------------------
# TAB: NOTICIAS
# ---------------------------------------------------------------------------

with tab_noticias:
    st.subheader(f"Noticias relevantes — {periodo_label}")
    if df_f.empty:
        st.info("No hay noticias para los filtros seleccionados.")
    else:
        tipo_counts = df_f["tipo"].value_counts()
        cols_t = st.columns(len(tipo_counts))
        for i, (tipo, cnt) in enumerate(tipo_counts.items()):
            cols_t[i].metric(tipo, cnt)
        st.dataframe(_prep_table(df_f), column_config=_col_cfg, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# TAB: RIESGOS Y OPORTUNIDADES
# ---------------------------------------------------------------------------

with tab_ryo:
    df_riesgo = df_f[df_f["tipo"].isin(["RIESGO", "MIXTO"])]
    df_oport = df_f[df_f["tipo"].isin(["OPORTUNIDAD", "MIXTO"])]

    col_r, col_o = st.columns(2)
    with col_r:
        st.markdown(f"### Riesgos ({len(df_riesgo)})")
        if df_riesgo.empty:
            st.success("Sin riesgos en el periodo.")
        else:
            st.dataframe(_prep_table(df_riesgo), column_config=_col_cfg, use_container_width=True, hide_index=True)

    with col_o:
        st.markdown(f"### Oportunidades ({len(df_oport)})")
        if df_oport.empty:
            st.info("Sin oportunidades en el periodo.")
        else:
            st.dataframe(_prep_table(df_oport), column_config=_col_cfg, use_container_width=True, hide_index=True)

    df_profundo = df_f[df_f["requiere_analisis_profundo"] == 1]
    if not df_profundo.empty:
        st.divider()
        st.markdown(f"### Requieren analisis profundo ({len(df_profundo)})")
        st.dataframe(_prep_table(df_profundo), column_config=_col_cfg, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# TAB: TENDENCIAS
# ---------------------------------------------------------------------------

with tab_tendencias:
    periodo_t = st.selectbox(
        "Periodo",
        [7, 14, 30, 60],
        index=2,
        format_func=lambda x: f"Ultimos {x} dias",
        key="periodo_t",
    )
    fecha_t = pd.Timestamp(date.today() - timedelta(days=periodo_t))
    df_t = df_all[df_all["fecha"] >= fecha_t]

    # Daily volume
    st.markdown("#### Volumen diario")
    if not df_t.empty:
        daily = (
            df_t.assign(fecha_str=df_t["fecha"].dt.strftime("%Y-%m-%d"))
            .groupby("fecha_str")
            .agg(Total=("id", "count"), Riesgos=("riesgo", "sum"), Oportunidades=("oportunidad", "sum"))
        )
        st.line_chart(daily)
    else:
        st.info("Sin datos para este periodo.")

    st.divider()

    # By topic
    st.markdown("#### Tendencias por tema")
    df_temas_raw = df_t[df_t["temas"] != ""].copy()
    if not df_temas_raw.empty:
        df_temas_raw = df_temas_raw.assign(tema=df_temas_raw["temas"].str.split("|"))
        df_temas_exp = df_temas_raw.explode("tema")
        df_temas_exp = df_temas_exp[df_temas_exp["tema"].str.strip() != ""]
        df_temas_exp["fecha_str"] = df_temas_exp["fecha"].dt.strftime("%Y-%m-%d")
        pivot = (
            df_temas_exp.groupby(["fecha_str", "tema"])
            .size()
            .reset_index(name="n")
            .pivot(index="fecha_str", columns="tema", values="n")
            .fillna(0)
        )
        st.line_chart(pivot)
    else:
        st.info("Sin datos de temas.")

    st.divider()

    # By media source
    st.markdown("#### Por medio")
    if not df_t.empty:
        medio_data = (
            df_t.groupby("medio")
            .agg(Total=("id", "count"), Riesgos=("riesgo", "sum"), Oportunidades=("oportunidad", "sum"))
            .sort_values("Total", ascending=False)
        )
        col_mc, col_mt = st.columns([1, 1])
        with col_mc:
            st.bar_chart(medio_data["Total"])
        with col_mt:
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
    fecha_e = pd.Timestamp(date.today() - timedelta(days=periodo_e))
    df_e = df_all[df_all["fecha"] >= fecha_e]

    # Parse personas + organizaciones into entity counts
    ent_counter: Counter = Counter()
    ent_riesgo: Counter = Counter()
    ent_oport: Counter = Counter()

    for _, row in df_e.iterrows():
        names = []
        for col in ("personas", "organizaciones"):
            val = str(row.get(col, "")).strip()
            if val:
                names += [n.strip() for n in val.split(",") if n.strip()]
        for name in names:
            ent_counter[name] += 1
            if row["riesgo"] == 1:
                ent_riesgo[name] += 1
            if row["oportunidad"] == 1:
                ent_oport[name] += 1

    if ent_counter:
        top_ents = pd.DataFrame([
            {"entidad": k, "menciones": v, "riesgo": ent_riesgo[k], "oportunidad": ent_oport[k]}
            for k, v in ent_counter.most_common(20)
        ])

        col_ec, col_et = st.columns([1, 1])
        with col_ec:
            st.markdown("#### Top 10 entidades")
            st.bar_chart(top_ents.set_index("entidad")["menciones"].head(10))
        with col_et:
            st.markdown("#### Detalle")
            st.dataframe(top_ents, use_container_width=True, hide_index=True)
    else:
        st.info("Sin datos de entidades para este periodo.")


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
        geo_sum = (
            df_f.groupby("nivel_geografico")
            .agg(Total=("id", "count"), Riesgos=("riesgo", "sum"), Oportunidades=("oportunidad", "sum"))
            .sort_values("Total", ascending=False)
        )
        st.dataframe(geo_sum, use_container_width=True)

    st.divider()
    st.markdown("#### Lugares mas mencionados")
    if not df_f.empty:
        lugar_counter: Counter = Counter()
        for val in df_f["lugares"]:
            for lugar in str(val).split(","):
                lugar = lugar.strip()
                if lugar:
                    lugar_counter[lugar] += 1
        if lugar_counter:
            df_lugares = pd.DataFrame(
                [{"lugar": k, "menciones": v} for k, v in lugar_counter.most_common(20)]
            )
            col_lc, col_lt = st.columns([1, 1])
            with col_lc:
                st.bar_chart(df_lugares.set_index("lugar")["menciones"].head(15))
            with col_lt:
                st.dataframe(df_lugares, use_container_width=True, hide_index=True)
