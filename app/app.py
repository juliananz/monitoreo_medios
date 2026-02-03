"""
Streamlit dashboard for media monitoring visualization.
Includes basic password authentication and trend analysis.
"""

import os
import hashlib
import pandas as pd
import streamlit as st

from config.settings import DB_PATH
from analisis.utils import get_db_connection, clasificar_tipo
from analisis.tendencias import (
    get_tendencia_diaria,
    get_tendencia_temas,
    get_tendencia_entidades,
    get_resumen_semanal,
    get_resumen_mensual,
    comparar_con_periodo_anterior,
    detectar_anomalias
)
from analisis.queries import (
    get_top_entidades_periodo,
    get_entidades_clave_en_riesgo,
    get_conteo_por_region,
    get_temas_activos
)

# =============================================================================
# AUTHENTICATION
# =============================================================================

def check_password() -> bool:
    """
    Simple password authentication for the dashboard.

    Set DASHBOARD_PASSWORD environment variable or use default for development.
    For production, always set a strong password via environment variable.
    """
    # Get password from environment (default for dev only)
    correct_password = os.getenv("DASHBOARD_PASSWORD", "")

    # If no password is set, show warning but allow access (dev mode)
    if not correct_password:
        st.sidebar.warning("No password set. Set DASHBOARD_PASSWORD for production.")
        return True

    # Check if already authenticated
    if st.session_state.get("authenticated", False):
        return True

    # Show login form
    st.title("Monitoreo de Medios")
    st.subheader("Iniciar sesion")

    password = st.text_input("Contrasena", type="password", key="password_input")

    if st.button("Entrar"):
        # Compare hashed passwords
        if hashlib.sha256(password.encode()).hexdigest() == hashlib.sha256(correct_password.encode()).hexdigest():
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Contrasena incorrecta")

    return False


def logout():
    """Logout the user."""
    st.session_state["authenticated"] = False
    st.rerun()


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

@st.cache_data
def cargar_datos():
    """Load relevant news from database."""
    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                fecha,
                medio,
                titulo,
                url,
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
    return df


@st.cache_data
def cargar_tendencia_diaria(dias: int = 30):
    """Load daily trend data."""
    return get_tendencia_diaria(dias)


@st.cache_data
def cargar_tendencia_temas(dias: int = 30):
    """Load topic trend data."""
    return get_tendencia_temas(dias)


@st.cache_data
def cargar_tendencia_entidades(top_n: int = 10, dias: int = 30):
    """Load entity trend data."""
    return get_tendencia_entidades(top_n, dias)


@st.cache_data
def cargar_resumen_semanal(semanas: int = 12):
    """Load weekly summary."""
    return get_resumen_semanal(semanas)


@st.cache_data
def cargar_resumen_mensual(meses: int = 12):
    """Load monthly summary."""
    return get_resumen_mensual(meses)


@st.cache_data
def cargar_comparacion_periodo(dias: int = 7):
    """Load period comparison."""
    return comparar_con_periodo_anterior(dias)


@st.cache_data
def cargar_top_entidades(dias: int = 30, limit: int = 20):
    """Load top entities."""
    return get_top_entidades_periodo(dias, limit)


@st.cache_data
def cargar_entidades_riesgo():
    """Load key entities in risk context."""
    return get_entidades_clave_en_riesgo()


@st.cache_data
def cargar_conteo_regiones():
    """Load region counts."""
    return get_conteo_por_region()


@st.cache_data
def cargar_anomalias(dias: int = 30):
    """Load detected anomalies."""
    return detectar_anomalias(dias)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def formato_cambio(valor, cambio_pct):
    """Format a value with change indicator."""
    if cambio_pct is None:
        return str(valor), None
    elif cambio_pct > 0:
        return str(valor), f"+{cambio_pct}%"
    elif cambio_pct < 0:
        return str(valor), f"{cambio_pct}%"
    else:
        return str(valor), "0%"


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

st.set_page_config(
    page_title="Monitoreo de Medios",
    layout="wide"
)

# Check authentication
if not check_password():
    st.stop()

# Show logout button in sidebar if authenticated
if os.getenv("DASHBOARD_PASSWORD"):
    if st.sidebar.button("Cerrar sesion"):
        logout()

st.title("Monitoreo de Medios - Riesgos y Oportunidades")


# Load data
df = cargar_datos()

if df.empty:
    st.warning("No hay datos disponibles.")
    st.stop()

# Basic cleanup
df = df.fillna("")
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

# News type classification
df["tipo"] = df.apply(
    lambda row: clasificar_tipo(row["riesgo"], row["oportunidad"]),
    axis=1
)

# =============================================================================
# SIDEBAR FILTERS
# =============================================================================

st.sidebar.header("Filtros")

# Date range filter
if df["fecha"].notna().any():
    min_date = df["fecha"].min().date()
    max_date = df["fecha"].max().date()

    date_range = st.sidebar.date_input(
        "Rango de fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        df = df[
            (df["fecha"].dt.date >= start_date) &
            (df["fecha"].dt.date <= end_date)
        ]

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
    "Nivel geografico",
    options=sorted(df["nivel_geografico"].unique()),
    default=sorted(df["nivel_geografico"].unique())
)

solo_profundo = st.sidebar.checkbox(
    "Solo noticias que requieren analisis profundo"
)

# Apply filters
df_filtrado = df[
    (df["tipo"].isin(tipos)) &
    (df["medio"].isin(medios)) &
    (df["nivel_geografico"].isin(niveles_geo))
]

if solo_profundo:
    df_filtrado = df_filtrado[df_filtrado["requiere_analisis_profundo"] == 1]

# =============================================================================
# KPIs WITH TREND INDICATORS
# =============================================================================

# Load comparison data
try:
    comparacion = cargar_comparacion_periodo(7)
    cambios = comparacion.get("cambios_pct", {})
except Exception:
    cambios = {}

col1, col2, col3, col4 = st.columns(4)

total_noticias = len(df_filtrado)
total_riesgos = (df_filtrado["tipo"] == "RIESGO").sum()
total_oportunidades = (df_filtrado["tipo"] == "OPORTUNIDAD").sum()
total_analisis = int(df_filtrado["requiere_analisis_profundo"].sum())

col1.metric(
    "Total noticias",
    total_noticias,
    delta=f"{cambios.get('noticias', 0)}% vs semana anterior" if cambios.get('noticias') else None
)
col2.metric(
    "Riesgos",
    total_riesgos,
    delta=f"{cambios.get('riesgos', 0)}% vs semana anterior" if cambios.get('riesgos') else None,
    delta_color="inverse"
)
col3.metric(
    "Oportunidades",
    total_oportunidades,
    delta=f"{cambios.get('oportunidades', 0)}% vs semana anterior" if cambios.get('oportunidades') else None
)
col4.metric(
    "Requieren analisis profundo",
    total_analisis
)

st.divider()

# =============================================================================
# TABS FOR DIFFERENT VIEWS
# =============================================================================

tab_noticias, tab_riesgos, tab_oportunidades, tab_tendencias, tab_entidades, tab_regiones = st.tabs([
    "Todas las noticias",
    "Solo Riesgos",
    "Solo Oportunidades",
    "Tendencias",
    "Entidades",
    "Regiones"
])

# -----------------------------------------------------------------------------
# TAB: Todas las noticias
# -----------------------------------------------------------------------------
with tab_noticias:
    st.subheader("Noticias relevantes con entidades detectadas")
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
        use_container_width=True,
        hide_index=True
    )

# -----------------------------------------------------------------------------
# TAB: Solo Riesgos
# -----------------------------------------------------------------------------
with tab_riesgos:
    st.subheader("Noticias de Riesgo")
    df_riesgos = df_filtrado[df_filtrado["tipo"].isin(["RIESGO", "MIXTO"])]
    st.dataframe(
        df_riesgos[
            ["fecha", "medio", "titulo", "nivel_geografico", "organizaciones", "lugares"]
        ].sort_values("fecha", ascending=False),
        use_container_width=True,
        hide_index=True
    )

# -----------------------------------------------------------------------------
# TAB: Solo Oportunidades
# -----------------------------------------------------------------------------
with tab_oportunidades:
    st.subheader("Noticias de Oportunidad")
    df_oportunidades = df_filtrado[df_filtrado["tipo"].isin(["OPORTUNIDAD", "MIXTO"])]
    st.dataframe(
        df_oportunidades[
            ["fecha", "medio", "titulo", "nivel_geografico", "organizaciones", "lugares"]
        ].sort_values("fecha", ascending=False),
        use_container_width=True,
        hide_index=True
    )

# -----------------------------------------------------------------------------
# TAB: Tendencias
# -----------------------------------------------------------------------------
with tab_tendencias:
    st.subheader("Tendencias de Noticias")

    # Period selector
    periodo_tendencia = st.selectbox(
        "Periodo de analisis",
        options=[7, 14, 30, 60, 90],
        index=2,
        format_func=lambda x: f"Ultimos {x} dias"
    )

    # Daily trend chart
    try:
        df_tendencia = cargar_tendencia_diaria(periodo_tendencia)
        if not df_tendencia.empty:
            st.markdown("#### Volumen diario de noticias")

            # Prepare data for chart
            chart_data = df_tendencia.set_index('fecha')[
                ['total_noticias', 'total_riesgo', 'total_oportunidad']
            ].rename(columns={
                'total_noticias': 'Total',
                'total_riesgo': 'Riesgos',
                'total_oportunidad': 'Oportunidades'
            })

            st.line_chart(chart_data)
        else:
            st.info("No hay datos de tendencias disponibles. Ejecute el backfill de agregaciones.")
    except Exception as e:
        st.warning(f"No se pudieron cargar las tendencias diarias: {e}")

    st.divider()

    # Topic trends
    try:
        df_temas = cargar_tendencia_temas(periodo_tendencia)
        if not df_temas.empty:
            st.markdown("#### Tendencias por Tema")

            # Pivot for chart
            df_pivot = df_temas.pivot_table(
                index='fecha',
                columns='tema',
                values='total_noticias',
                aggfunc='sum'
            ).fillna(0)

            st.line_chart(df_pivot)
    except Exception as e:
        st.warning(f"No se pudieron cargar las tendencias por tema: {e}")

    st.divider()

    # Weekly/Monthly comparison
    col_sem, col_mes = st.columns(2)

    with col_sem:
        st.markdown("#### Resumen Semanal")
        try:
            df_semanal = cargar_resumen_semanal(8)
            if not df_semanal.empty:
                st.bar_chart(
                    df_semanal.set_index('semana')[['total_noticias', 'total_riesgo', 'total_oportunidad']].rename(columns={
                        'total_noticias': 'Total',
                        'total_riesgo': 'Riesgos',
                        'total_oportunidad': 'Oportunidades'
                    })
                )
        except Exception as e:
            st.info("Datos semanales no disponibles")

    with col_mes:
        st.markdown("#### Resumen Mensual")
        try:
            df_mensual = cargar_resumen_mensual(6)
            if not df_mensual.empty:
                st.bar_chart(
                    df_mensual.set_index('mes')[['total_noticias', 'total_riesgo', 'total_oportunidad']].rename(columns={
                        'total_noticias': 'Total',
                        'total_riesgo': 'Riesgos',
                        'total_oportunidad': 'Oportunidades'
                    })
                )
        except Exception as e:
            st.info("Datos mensuales no disponibles")

    st.divider()

    # Anomalies
    st.markdown("#### Anomalias Detectadas")
    try:
        anomalias = cargar_anomalias(30)
        if anomalias:
            df_anomalias = pd.DataFrame(anomalias)
            st.dataframe(
                df_anomalias[['fecha', 'metrica', 'valor', 'media', 'tipo', 'sigma']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No se detectaron anomalias en los ultimos 30 dias")
    except Exception as e:
        st.info("Deteccion de anomalias no disponible")

# -----------------------------------------------------------------------------
# TAB: Entidades
# -----------------------------------------------------------------------------
with tab_entidades:
    st.subheader("Analisis de Entidades")

    # Top entities
    st.markdown("#### Top Entidades Mencionadas")
    try:
        top_entidades = cargar_top_entidades(30, 15)
        if top_entidades:
            df_top = pd.DataFrame(top_entidades)

            # Bar chart
            st.bar_chart(
                df_top.set_index('entidad')['total_menciones'].head(10)
            )

            # Full table
            st.dataframe(
                df_top[['entidad', 'tipo', 'total_menciones', 'total_riesgo', 'total_oportunidad', 'dias_activos']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No hay datos de entidades disponibles")
    except Exception as e:
        st.warning(f"No se pudieron cargar las entidades: {e}")

    st.divider()

    # Entity timeline
    st.markdown("#### Tendencia de Entidad Seleccionada")
    try:
        df_entidad_trend = cargar_tendencia_entidades(10, 30)
        if not df_entidad_trend.empty:
            entidades_disponibles = df_entidad_trend['entidad'].unique().tolist()

            if entidades_disponibles:
                entidad_seleccionada = st.selectbox(
                    "Seleccionar entidad",
                    options=entidades_disponibles
                )

                df_entidad_filtrada = df_entidad_trend[
                    df_entidad_trend['entidad'] == entidad_seleccionada
                ].set_index('fecha')['menciones']

                st.line_chart(df_entidad_filtrada)
    except Exception as e:
        st.info("Tendencias de entidades no disponibles")

    st.divider()

    # Key entities in risk context
    st.markdown("#### Entidades Clave en Contexto de Riesgo")
    try:
        entidades_riesgo = cargar_entidades_riesgo()
        if entidades_riesgo:
            df_riesgo_ent = pd.DataFrame(entidades_riesgo)
            st.dataframe(
                df_riesgo_ent,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No hay entidades clave en contexto de riesgo")
    except Exception as e:
        st.info("Datos de entidades en riesgo no disponibles")

# -----------------------------------------------------------------------------
# TAB: Regiones
# -----------------------------------------------------------------------------
with tab_regiones:
    st.subheader("Analisis Geografico")

    # Geographic level distribution
    st.markdown("#### Distribucion por Nivel Geografico")
    if not df_filtrado.empty:
        nivel_counts = df_filtrado['nivel_geografico'].value_counts()
        st.bar_chart(nivel_counts)

    st.divider()

    # Region breakdown
    st.markdown("#### Desglose por Region")
    try:
        regiones = cargar_conteo_regiones()
        if regiones:
            df_regiones = pd.DataFrame(regiones)
            st.dataframe(
                df_regiones[['region', 'tipo_region', 'total', 'riesgos', 'oportunidades']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No hay datos de regiones disponibles")
    except Exception as e:
        st.info("Datos de regiones no disponibles")

    st.divider()

    # Risk/Opportunity by geographic level
    st.markdown("#### Riesgos y Oportunidades por Nivel Geografico")
    if not df_filtrado.empty:
        geo_summary = df_filtrado.groupby('nivel_geografico').agg(
            total=('titulo', 'count'),
            riesgos=('riesgo', 'sum'),
            oportunidades=('oportunidad', 'sum')
        ).reset_index()

        st.dataframe(
            geo_summary.sort_values('total', ascending=False),
            use_container_width=True,
            hide_index=True
        )

# =============================================================================
# SUMMARY STATS
# =============================================================================

st.divider()
st.subheader("Resumen por Medio")

if not df_filtrado.empty:
    summary = df_filtrado.groupby("medio").agg(
        total=("titulo", "count"),
        riesgos=("riesgo", "sum"),
        oportunidades=("oportunidad", "sum")
    ).reset_index()

    st.dataframe(
        summary.sort_values("total", ascending=False),
        use_container_width=True,
        hide_index=True
    )

# =============================================================================
# REFRESH BUTTON
# =============================================================================

st.sidebar.divider()
if st.sidebar.button("Actualizar datos"):
    st.cache_data.clear()
    st.rerun()
