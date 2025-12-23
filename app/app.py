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
            temas,
            score,
            riesgo,
            oportunidad
        FROM noticias
    """, conn)
    conn.close()
    return df

df = cargar_datos()

if df.empty:
    st.warning("No hay datos disponibles.")
    st.stop()

# --- Transformaciones ---
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

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
    options=sorted(df["medio"].dropna().unique()),
    default=sorted(df["medio"].dropna().unique())
)

df_filtrado = df[
    (df["tipo"].isin(tipos)) &
    (df["medio"].isin(medios))
]

# --- KPIs ---
col1, col2, col3 = st.columns(3)

col1.metric("Total noticias", len(df_filtrado))
col2.metric("Riesgos", (df_filtrado["tipo"] == "RIESGO").sum())
col3.metric("Oportunidades", (df_filtrado["tipo"] == "OPORTUNIDAD").sum())

st.divider()

# --- Tabla ---
st.subheader("📰 Noticias")
st.dataframe(
    df_filtrado[["fecha", "medio", "titulo", "temas", "tipo", "score"]]
    .sort_values("fecha", ascending=False),
    width="stretch"
)
