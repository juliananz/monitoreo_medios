import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

st.set_page_config(
    page_title="Monitoreo de Medios",
    layout="wide"
)

st.title("📊 Monitoreo de Medios – Riesgos y Oportunidades")

@st.cache_data
def cargar_datos():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            fecha,
            medio,
            titulo,
            personas,
            organizaciones,
            lugares,
            nivel_geografico,
            requiere_analisis_profundo,
            riesgo,
            oportunidad
        FROM noticias
        WHERE relevante = 1
        ORDER BY fecha DESC
    """, conn)
    conn.close()
    return df

df = cargar_datos()

if df.empty:
    st.warning("No hay datos disponibles.")
    st.stop()

# --- Limpieza básica ---
df = df.fillna("")
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

# --- Tipo de noticia ---
def tipo_noticia(row):
    if row["riesgo"] == 1 and row["oportunidad"] == 1:
        return "MIXTO"
    if row["riesgo"] == 1:
        return "RIESGO"
    if row["oportunidad"] == 1:
        return "OPORTUNIDAD"
    return "NEUTRO"

df["tipo"] = df.apply(tipo_noticia, axis=1)

# --- Sidebar filtros ---
st.sidebar.header("Filtros")

tipos = st.sidebar.multiselect(
    "Tipo de noticia",
    options=sorted(df["tipo"].unique()),
    default=sorted(df["tipo"].unique())
)

medios = st.sidebar.multiselect(
    "Medio",
    options=sorted(df["medio"].unique()),
    default=sorted(df["medio"].unique())
)

niveles_geo = st.sidebar.multiselect(
    "Nivel geográfico",
    options=sorted(df["nivel_geografico"].unique()),
    default=sorted(df["nivel_geografico"].unique())
)

solo_profundo = st.sidebar.checkbox(
    "Solo noticias que requieren análisis profundo"
)

# --- Aplicar filtros ---
df_filtrado = df[
    (df["tipo"].isin(tipos)) &
    (df["medio"].isin(medios)) &
    (df["nivel_geografico"].isin(niveles_geo))
]

if solo_profundo:
    df_filtrado = df_filtrado[df_filtrado["requiere_analisis_profundo"] == 1]

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total noticias", len(df_filtrado))
col2.metric("Riesgos", (df_filtrado["tipo"] == "RIESGO").sum())
col3.metric("Oportunidades", (df_filtrado["tipo"] == "OPORTUNIDAD").sum())
col4.metric(
    "Requieren análisis profundo",
    df_filtrado["requiere_analisis_profundo"].sum()
)

st.divider()

# --- Tabla principal ---
st.subheader("📰 Noticias relevantes con entidades detectadas")

st.dataframe(
    df_filtrado[
        [
            "fecha",
            "medio",
            "titulo",
            "nivel_geografico",
            "personas",
            "organizaciones",
            "lugares",
            "tipo",
            "requiere_analisis_profundo"
        ]
    ].sort_values("fecha", ascending=False),
    width="stretch"
)
