"""
Dash dashboard — Monitoreo de Medios | Secretaria de Economia de Coahuila.

Reads exclusively from SQLite aggregation tables via app.data.
Run:  python app/app_dash.py
"""

import hashlib
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback_context, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate

from app.data import (
    cargar_resumen_llm,
    get_daily_volume,
    get_entity_sparkline,
    get_entity_trends,
    get_kpis,
    get_medio_volume,
    get_municipio_counts,
    get_noticias,
    get_noticias_municipios,
    get_region_dist,
    get_topic_trends,
)

# =============================================================================
# CONSTANTS
# =============================================================================

_PWD = os.getenv("DASHBOARD_PASSWORD", "")
_TODAY = date.today()
DEFAULT_START = (_TODAY - timedelta(days=30)).isoformat()
DEFAULT_END = _TODAY.isoformat()
DARK_TEMPLATE = "plotly_dark"

TIPO_COLORS = {
    "RIESGO": "#e15759",
    "OPORTUNIDAD": "#59a14f",
    "MIXTO": "#f28e2b",
    "NEUTRO": "#76b7b2",
}

# =============================================================================
# APP INIT
# =============================================================================

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="Monitoreo de Medios | SEC Coahuila",
)

# =============================================================================
# HELPERS
# =============================================================================


def make_kpi_card(title: str, value: int, delta: int | None = None, delta_inverse: bool = False) -> dbc.Card:
    """Render a Bootstrap KPI card with optional week-over-week delta badge."""
    delta_el = html.Div()
    if delta is not None:
        positive_is_good = not delta_inverse
        is_good = (delta > 0) == positive_is_good
        color = "success" if is_good else "danger"
        arrow = "▲" if delta > 0 else "▼"
        delta_el = html.Div(
            dbc.Badge(f"{arrow} {abs(delta)}% vs sem. ant.", color=color, className="mt-1"),
        )
    return dbc.Card(
        dbc.CardBody([
            html.P(title, className="text-muted small mb-1"),
            html.H3(f"{value:,}", className="mb-0 fw-bold"),
            delta_el,
        ]),
        className="h-100 text-center",
    )


def empty_fig(msg: str = "Sin datos para este periodo") -> go.Figure:
    """Return a blank dark plotly figure with a centred annotation."""
    return go.Figure().update_layout(
        template=DARK_TEMPLATE,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{"text": msg, "showarrow": False, "font": {"size": 15}, "xref": "paper", "yref": "paper",
                       "x": 0.5, "y": 0.5}],
        margin=dict(t=20, b=20),
    )


# =============================================================================
# LAYOUT COMPONENTS
# =============================================================================

_login_modal = dbc.Modal(
    [
        dbc.ModalHeader(dbc.ModalTitle("Monitoreo de Medios")),
        dbc.ModalBody([
            html.P("Secretaria de Economia — Gobierno de Coahuila", className="text-muted mb-3"),
            dbc.Label("Contrasena"),
            dbc.Input(id="login-password", type="password", placeholder="Ingresa la contrasena", debounce=True),
            html.Div(id="login-error", className="text-danger mt-2 small"),
        ]),
        dbc.ModalFooter(dbc.Button("Entrar", id="login-btn", color="primary", n_clicks=0)),
    ],
    id="login-modal",
    is_open=bool(_PWD),
    backdrop="static",
    keyboard=False,
    centered=True,
)

_header = dbc.Row(
    [
        dbc.Col([
            html.H4("Monitoreo de Medios", className="mb-0 fw-bold"),
            html.Small("Secretaria de Economia — Gobierno de Coahuila", className="text-muted"),
        ], width="auto"),
        dbc.Col(
            html.Div(id="logout-container", children=dbc.Button(
                "Cerrar sesion", id="logout-btn", color="outline-secondary", size="sm", n_clicks=0,
            )),
            width="auto",
            className="ms-auto d-flex align-items-center",
        ),
    ],
    align="center",
    className="py-3 border-bottom mb-3",
)

_filter_bar = dbc.Row(
    [
        dbc.Col([
            dbc.Label("Periodo", size="sm", className="mb-1"),
            dcc.DatePickerRange(
                id="date-picker",
                start_date=DEFAULT_START,
                end_date=DEFAULT_END,
                display_format="YYYY-MM-DD",
                min_date_allowed="2020-01-01",
                max_date_allowed=DEFAULT_END,
                className="d-block",
            ),
        ], md=6, lg=5),
        dbc.Col([
            dbc.Label("Nivel geografico", size="sm", className="mb-1"),
            dcc.Dropdown(
                id="nivel-geo-dropdown",
                options=[
                    {"label": "Todos los niveles", "value": "all"},
                    {"label": "Local", "value": "local"},
                    {"label": "Estatal", "value": "estatal"},
                    {"label": "Nacional", "value": "nacional"},
                    {"label": "Internacional", "value": "internacional"},
                ],
                value="all",
                clearable=False,
            ),
        ], md=4, lg=4),
        dbc.Col(
            dbc.Button("↺ Actualizar", id="refresh-btn", color="secondary", size="sm", n_clicks=0,
                       className="w-100 mt-3"),
            md=2, lg=3,
        ),
    ],
    className="mb-3 p-3 rounded",
    style={"backgroundColor": "rgba(255,255,255,0.04)"},
)

# ---------------------------------------------------------------------------
# Tab 1: Resumen
# ---------------------------------------------------------------------------

_tab_resumen = dbc.Tab(
    label="Resumen",
    tab_id="resumen",
    children=[
        html.Div(id="kpi-row", className="mb-3"),
        dcc.Graph(id="daily-volume-chart", config={"displayModeBar": False}),
        html.Div(id="llm-summary-div", className="mt-3"),
    ],
)

# ---------------------------------------------------------------------------
# Tab 2: Noticias
# ---------------------------------------------------------------------------

_noticias_table = dash_table.DataTable(
    id="noticias-table",
    columns=[],
    data=[],
    sort_action="native",
    filter_action="native",
    page_size=20,
    page_action="native",
    markdown_options={"link_target": "_blank"},
    style_table={"overflowX": "auto"},
    style_cell={
        "textAlign": "left",
        "padding": "8px 12px",
        "backgroundColor": "#2b2b2b",
        "color": "#e0e0e0",
        "border": "1px solid #404040",
        "whiteSpace": "normal",
        "height": "auto",
        "minWidth": "80px",
        "maxWidth": "400px",
        "overflow": "hidden",
        "textOverflow": "ellipsis",
    },
    style_header={
        "backgroundColor": "#1a1a2e",
        "fontWeight": "bold",
        "color": "#ffffff",
        "border": "1px solid #404040",
    },
    style_data_conditional=[
        {"if": {"filter_query": '{tipo} = "RIESGO"'}, "backgroundColor": "#3d1515"},
        {"if": {"filter_query": '{tipo} = "OPORTUNIDAD"'}, "backgroundColor": "#153d15"},
        {"if": {"filter_query": '{tipo} = "MIXTO"'}, "backgroundColor": "#3d2a10"},
    ],
)

_tab_noticias = dbc.Tab(
    label="Noticias",
    tab_id="noticias",
    children=[
        dbc.Row(
            [
                dbc.Col([
                    dbc.Label("Filtrar por tipo", size="sm"),
                    dcc.Dropdown(
                        id="tipo-filter",
                        options=[{"label": t, "value": t} for t in ("RIESGO", "OPORTUNIDAD", "MIXTO", "NEUTRO")],
                        multi=True,
                        value=["RIESGO", "OPORTUNIDAD", "MIXTO", "NEUTRO"],
                        placeholder="Seleccionar tipos...",
                    ),
                ], md=6),
                dbc.Col([
                    dbc.Label("", size="sm"),
                    dbc.Checklist(
                        id="profundo-filter",
                        options=[{"label": "Solo requieren analisis profundo", "value": 1}],
                        value=[],
                        switch=True,
                        className="mt-2",
                    ),
                ], md=6),
            ],
            className="mb-3 mt-2",
        ),
        html.Div(id="noticias-counts", className="mb-2"),
        _noticias_table,
    ],
)

# ---------------------------------------------------------------------------
# Tab 3: Riesgos y Oportunidades
# ---------------------------------------------------------------------------

_tab_ryo = dbc.Tab(
    label="Riesgos y Oportunidades",
    tab_id="ryo",
    children=[
        dcc.Graph(id="ryo-chart", config={"displayModeBar": False}),
        html.Hr(),
        dcc.Graph(id="topic-ryo-chart", config={"displayModeBar": False}),
    ],
)

# ---------------------------------------------------------------------------
# Tab 4: Tendencias
# ---------------------------------------------------------------------------

_tab_tendencias = dbc.Tab(
    label="Tendencias",
    tab_id="tendencias",
    children=[
        html.H6("Volumen diario por tema", className="mt-3 text-muted"),
        dcc.Graph(id="tema-trend-chart", config={"displayModeBar": False}),
        html.Hr(),
        html.H6("Volumen por medio de comunicacion", className="text-muted"),
        dcc.Graph(id="medio-chart", config={"displayModeBar": False}),
    ],
)

# ---------------------------------------------------------------------------
# Tab 5: Entidades
# ---------------------------------------------------------------------------

_tab_entidades = dbc.Tab(
    label="Entidades",
    tab_id="entidades",
    children=[
        dcc.Graph(id="entity-chart", config={"displayModeBar": False}),
        html.Hr(),
        dbc.Row(
            dbc.Col([
                dbc.Label("Tendencia para entidad", size="sm"),
                dcc.Dropdown(id="entity-selector", placeholder="Selecciona una entidad...", clearable=True),
            ], md=6),
            className="mb-2",
        ),
        dcc.Graph(id="entity-sparkline-chart", config={"displayModeBar": False}),
    ],
)

# ---------------------------------------------------------------------------
# Tab 6: Municipios Coahuila
# ---------------------------------------------------------------------------

_REGIONES_COAHUILA = ["Carbonífera", "Centro", "Laguna", "Norte", "Sureste"]

_tab_municipios = dbc.Tab(
    label="Municipios",
    tab_id="municipios",
    children=[
        dbc.Row(
            [
                dbc.Col([
                    dbc.Label("Filtrar por región", size="sm"),
                    dcc.Dropdown(
                        id="region-filter",
                        options=[{"label": r, "value": r} for r in _REGIONES_COAHUILA],
                        multi=True,
                        value=[],
                        placeholder="Todas las regiones...",
                    ),
                ], md=6),
                dbc.Col([
                    dbc.Label("Filtrar por tipo", size="sm"),
                    dcc.Dropdown(
                        id="municipio-tipo-filter",
                        options=[{"label": t, "value": t} for t in ("RIESGO", "OPORTUNIDAD", "MIXTO", "NEUTRO")],
                        multi=True,
                        value=[],
                        placeholder="Todos los tipos...",
                    ),
                ], md=6),
            ],
            className="mb-3 mt-2",
        ),
        dcc.Graph(id="municipio-bar-chart", config={"displayModeBar": False}),
        html.Hr(),
        html.Div(id="municipio-noticias-counts", className="mb-2"),
        dash_table.DataTable(
            id="municipio-noticias-table",
            columns=[],
            data=[],
            sort_action="native",
            filter_action="native",
            page_size=20,
            page_action="native",
            markdown_options={"link_target": "_blank"},
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "8px 12px",
                "backgroundColor": "#2b2b2b",
                "color": "#e0e0e0",
                "border": "1px solid #404040",
                "whiteSpace": "normal",
                "height": "auto",
                "minWidth": "80px",
                "maxWidth": "400px",
                "overflow": "hidden",
                "textOverflow": "ellipsis",
            },
            style_header={
                "backgroundColor": "#1a1a2e",
                "fontWeight": "bold",
                "color": "#ffffff",
                "border": "1px solid #404040",
            },
            style_data_conditional=[
                {"if": {"filter_query": '{tipo} = "RIESGO"'}, "backgroundColor": "#3d1515"},
                {"if": {"filter_query": '{tipo} = "OPORTUNIDAD"'}, "backgroundColor": "#153d15"},
                {"if": {"filter_query": '{tipo} = "MIXTO"'}, "backgroundColor": "#3d2a10"},
            ],
        ),
    ],
)


# =============================================================================
# MAIN LAYOUT
# =============================================================================

app.layout = dbc.Container(
    [
        dcc.Store(id="store-filters", storage_type="memory"),
        dcc.Store(id="auth-store", storage_type="session"),
        _login_modal,
        _header,
        _filter_bar,
        dbc.Tabs(
            [_tab_resumen, _tab_noticias, _tab_ryo, _tab_tendencias, _tab_entidades, _tab_municipios],
            id="main-tabs",
            active_tab="resumen",
        ),
    ],
    fluid=True,
    className="px-4",
)

# =============================================================================
# CALLBACKS — AUTHENTICATION
# =============================================================================


@app.callback(
    Output("auth-store", "data"),
    Output("login-modal", "is_open"),
    Output("login-error", "children"),
    Input("login-btn", "n_clicks"),
    Input("logout-btn", "n_clicks"),
    State("auth-store", "data"),
    State("login-password", "value"),
    prevent_initial_call=False,
)
def handle_auth(login_clicks, logout_clicks, auth_data, password):
    if not _PWD:
        return {"authenticated": True}, False, ""

    triggered_id = callback_context.triggered_id if callback_context.triggered else None

    if triggered_id == "logout-btn":
        return {"authenticated": False}, True, ""

    if triggered_id == "login-btn":
        pwd = password or ""
        expected = hashlib.sha256(_PWD.encode()).hexdigest()
        if hashlib.sha256(pwd.encode()).hexdigest() == expected:
            return {"authenticated": True}, False, ""
        return no_update, True, "Contrasena incorrecta"

    # Initial page load — honour existing session
    is_auth = bool(auth_data and auth_data.get("authenticated"))
    return no_update, not is_auth, ""


@app.callback(
    Output("logout-container", "style"),
    Input("auth-store", "data"),
)
def toggle_logout_btn(auth_data):
    if _PWD and auth_data and auth_data.get("authenticated"):
        return {"display": "inline-block"}
    return {"display": "none"}


# =============================================================================
# CALLBACKS — GLOBAL FILTER STORE
# =============================================================================


@app.callback(
    Output("store-filters", "data"),
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date"),
    Input("nivel-geo-dropdown", "value"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=False,
)
def update_store(start_date, end_date, nivel_geo, _):
    return {
        "start": start_date or DEFAULT_START,
        "end": end_date or DEFAULT_END,
        "nivel_geo": nivel_geo or "all",
    }


# =============================================================================
# CALLBACKS — TAB 1: RESUMEN
# =============================================================================


@app.callback(
    Output("kpi-row", "children"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_kpis(filters):
    if not filters:
        return []
    try:
        k = get_kpis(filters["start"], filters["end"])
    except Exception:
        return dbc.Alert("Error al cargar KPIs.", color="danger")

    return dbc.Row(
        [
            dbc.Col(make_kpi_card("Total noticias", k["total"], k["delta_total"]), md=3, sm=6, className="mb-2"),
            dbc.Col(make_kpi_card("Relevantes", k["relevantes"]), md=3, sm=6, className="mb-2"),
            dbc.Col(make_kpi_card("Riesgos", k["riesgo"], k["delta_riesgo"], delta_inverse=True),
                    md=3, sm=6, className="mb-2"),
            dbc.Col(make_kpi_card("Oportunidades", k["oportunidad"], k["delta_oportunidad"]),
                    md=3, sm=6, className="mb-2"),
        ],
        className="g-3",
    )


@app.callback(
    Output("daily-volume-chart", "figure"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_daily_volume(filters):
    if not filters:
        return empty_fig()
    try:
        df = get_daily_volume(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos de volumen diario")
    if df.empty:
        return empty_fig()
    fig = px.bar(
        df,
        x="fecha",
        y="total_noticias",
        title="Volumen diario de noticias",
        template=DARK_TEMPLATE,
        color_discrete_sequence=["#4e79a7"],
        labels={"fecha": "Fecha", "total_noticias": "Noticias"},
    )
    fig.update_layout(margin=dict(t=40, b=20), showlegend=False)
    return fig


@app.callback(
    Output("llm-summary-div", "children"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_llm_summary(_filters):
    fecha, texto = cargar_resumen_llm()
    if not texto:
        return dbc.Alert("Sin resumen ejecutivo disponible. El pipeline genera uno diariamente.", color="info")
    label = "Resumen Ejecutivo del dia" if fecha == date.today().isoformat() else f"Resumen Ejecutivo — {fecha}"
    return dbc.Card(
        [dbc.CardHeader(html.Strong(label)), dbc.CardBody(dcc.Markdown(texto, link_target="_blank"))],
        className="mt-2",
    )


# =============================================================================
# CALLBACKS — TAB 2: NOTICIAS
# =============================================================================


@app.callback(
    Output("noticias-table", "data"),
    Output("noticias-table", "columns"),
    Output("noticias-counts", "children"),
    Input("store-filters", "data"),
    Input("tipo-filter", "value"),
    Input("profundo-filter", "value"),
    prevent_initial_call=True,
)
def update_noticias(filters, tipos, profundo):
    if not filters:
        return [], [], ""
    try:
        df = get_noticias(filters["start"], filters["end"], filters.get("nivel_geo", "all"))
    except Exception:
        return [], [], dbc.Alert("Error al cargar noticias.", color="danger")

    if df.empty:
        return [], [], dbc.Alert("Sin noticias para los filtros seleccionados.", color="info")

    if tipos:
        df = df[df["tipo"].isin(tipos)]
    if profundo and 1 in profundo:
        df = df[df["requiere_analisis_profundo"] == 1]

    tipo_counts = df["tipo"].value_counts()
    counts_row = dbc.Row(
        [dbc.Col(dbc.Badge(f"{tipo}: {cnt}", color="secondary", className="me-1"), width="auto")
         for tipo, cnt in tipo_counts.items()],
        className="mb-2 g-1",
    )

    display = df[["fecha", "tipo", "medio", "titulo", "url", "nivel_geografico"]].copy()
    display["url"] = display["url"].apply(lambda u: f"[Ver]({u})" if u else "")

    columns = [
        {"name": "Fecha", "id": "fecha", "type": "text"},
        {"name": "Tipo", "id": "tipo", "type": "text"},
        {"name": "Medio", "id": "medio", "type": "text"},
        {"name": "Titulo", "id": "titulo", "type": "text"},
        {"name": "Enlace", "id": "url", "type": "text", "presentation": "markdown"},
        {"name": "Nivel Geo.", "id": "nivel_geografico", "type": "text"},
    ]
    return display.to_dict("records"), columns, counts_row


# =============================================================================
# CALLBACKS — TAB 3: RIESGOS Y OPORTUNIDADES
# =============================================================================


@app.callback(
    Output("ryo-chart", "figure"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_ryo_chart(filters):
    if not filters:
        return empty_fig()
    try:
        df = get_daily_volume(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos")
    if df.empty:
        return empty_fig()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["fecha"], y=df["total_riesgo"], name="Riesgo", marker_color="#e15759"))
    fig.add_trace(go.Bar(x=df["fecha"], y=df["total_oportunidad"], name="Oportunidad", marker_color="#59a14f"))
    fig.add_trace(go.Bar(x=df["fecha"], y=df["total_mixto"], name="Mixto", marker_color="#f28e2b"))
    fig.update_layout(
        barmode="stack",
        title="Riesgos y Oportunidades por dia",
        template=DARK_TEMPLATE,
        margin=dict(t=40, b=20),
        legend=dict(orientation="h", y=1.05),
    )
    return fig


@app.callback(
    Output("topic-ryo-chart", "figure"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_topic_ryo(filters):
    if not filters:
        return empty_fig()
    try:
        df = get_topic_trends(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos de temas")
    if df.empty:
        return empty_fig("Sin datos de temas para este periodo")

    summary = (
        df.groupby("tema")
        .agg(total_noticias=("total_noticias", "sum"),
             total_riesgo=("total_riesgo", "sum"),
             total_oportunidad=("total_oportunidad", "sum"))
        .reset_index()
        .sort_values("total_noticias", ascending=True)
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(y=summary["tema"], x=summary["total_riesgo"],
                         name="Riesgo", orientation="h", marker_color="#e15759"))
    fig.add_trace(go.Bar(y=summary["tema"], x=summary["total_oportunidad"],
                         name="Oportunidad", orientation="h", marker_color="#59a14f"))
    fig.update_layout(
        barmode="stack",
        title="Riesgos y Oportunidades por tema",
        template=DARK_TEMPLATE,
        height=max(350, len(summary) * 30),
        margin=dict(t=40, b=20, l=160),
        legend=dict(orientation="h", y=1.05),
    )
    return fig


# =============================================================================
# CALLBACKS — TAB 4: TENDENCIAS
# =============================================================================


@app.callback(
    Output("tema-trend-chart", "figure"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_tema_trends(filters):
    if not filters:
        return empty_fig()
    try:
        df = get_topic_trends(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar tendencias de temas")
    if df.empty:
        return empty_fig("Sin datos de temas para este periodo")

    fig = px.line(
        df,
        x="fecha",
        y="total_noticias",
        color="tema",
        title="Volumen diario por tema",
        template=DARK_TEMPLATE,
        markers=True,
        labels={"fecha": "Fecha", "total_noticias": "Noticias", "tema": "Tema"},
    )
    fig.update_layout(margin=dict(t=40, b=20), legend=dict(orientation="h", y=-0.2))
    return fig


@app.callback(
    Output("medio-chart", "figure"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_medio_chart(filters):
    if not filters:
        return empty_fig()
    try:
        df = get_medio_volume(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos de medios")
    if df.empty:
        return empty_fig("Sin datos de medios para este periodo")

    df_top = df.head(20).sort_values("total_noticias", ascending=True)
    fig = go.Figure(go.Bar(
        y=df_top["medio"],
        x=df_top["total_noticias"],
        orientation="h",
        marker_color="#4e79a7",
        text=df_top["total_noticias"],
        textposition="outside",
    ))
    fig.update_layout(
        title="Volumen por medio (top 20)",
        template=DARK_TEMPLATE,
        height=max(350, len(df_top) * 28),
        margin=dict(t=40, b=20, l=180),
    )
    return fig


# =============================================================================
# CALLBACKS — TAB 5: ENTIDADES
# =============================================================================


@app.callback(
    Output("entity-chart", "figure"),
    Output("entity-selector", "options"),
    Output("entity-selector", "value"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_entity_chart(filters):
    if not filters:
        return empty_fig(), [], None
    try:
        df = get_entity_trends(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos de entidades"), [], None
    if df.empty:
        return empty_fig("Sin datos de entidades para este periodo"), [], None

    df_sorted = df.sort_values("menciones", ascending=True)
    colors = ["#e15759" if t == "PER" else "#4e79a7" for t in df_sorted["tipo"]]

    fig = go.Figure(go.Bar(
        y=df_sorted["entidad"],
        x=df_sorted["menciones"],
        orientation="h",
        marker_color=colors,
        text=df_sorted["menciones"],
        textposition="outside",
    ))
    fig.update_layout(
        title="Top 20 entidades por menciones (rojo=persona, azul=org/otro)",
        template=DARK_TEMPLATE,
        height=max(350, len(df_sorted) * 28),
        margin=dict(t=50, b=20, l=200),
    )

    options = [{"label": row["entidad"], "value": row["entidad"]} for _, row in df.iterrows()]
    first = df.iloc[0]["entidad"] if not df.empty else None
    return fig, options, first


@app.callback(
    Output("entity-sparkline-chart", "figure"),
    Input("entity-selector", "value"),
    Input("store-filters", "data"),
    prevent_initial_call=True,
)
def update_entity_sparkline(entidad, filters):
    if not entidad or not filters:
        return empty_fig("Selecciona una entidad para ver su tendencia diaria")
    try:
        df = get_entity_sparkline(entidad, filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar tendencia de entidad")
    if df.empty:
        return empty_fig(f"Sin datos de tendencia para: {entidad}")

    fig = px.line(
        df,
        x="fecha",
        y="menciones",
        title=f"Tendencia diaria: {entidad}",
        template=DARK_TEMPLATE,
        markers=True,
        labels={"fecha": "Fecha", "menciones": "Menciones"},
    )
    fig.update_traces(line_color="#4e79a7", marker_color="#4e79a7")
    fig.update_layout(margin=dict(t=40, b=20))
    return fig


# =============================================================================
# CALLBACKS — TAB 6: MUNICIPIOS COAHUILA
# =============================================================================

_REGION_COLORS = {
    "Carbonífera": "#f28e2b",
    "Centro": "#4e79a7",
    "Laguna": "#76b7b2",
    "Norte": "#59a14f",
    "Sureste": "#e15759",
}


@app.callback(
    Output("municipio-bar-chart", "figure"),
    Output("municipio-noticias-table", "data"),
    Output("municipio-noticias-table", "columns"),
    Output("municipio-noticias-counts", "children"),
    Input("store-filters", "data"),
    Input("region-filter", "value"),
    Input("municipio-tipo-filter", "value"),
    prevent_initial_call=True,
)
def update_municipios(filters, regiones_sel, tipos_sel):
    if not filters:
        return empty_fig(), [], [], ""

    try:
        counts = get_municipio_counts(filters["start"], filters["end"])
        df_news = get_noticias_municipios(filters["start"], filters["end"])
    except Exception:
        return empty_fig("Error al cargar datos de municipios"), [], [], ""

    if counts.empty:
        return empty_fig("Sin noticias municipales para este periodo"), [], [], ""

    # Apply region filter
    if regiones_sel:
        counts = counts[counts["region"].isin(regiones_sel)]
        df_news = df_news[df_news["region"].isin(regiones_sel)]

    # Apply tipo filter (only affects the table and counts, not the bar chart)
    df_news_filtered = df_news[df_news["tipo"].isin(tipos_sel)] if tipos_sel else df_news

    if counts.empty:
        return empty_fig("Sin datos para las regiones seleccionadas"), [], [], ""

    # Bar chart: grouped by municipio, coloured by region
    counts_sorted = counts.sort_values("total", ascending=True)
    bar_colors = [_REGION_COLORS.get(r, "#aaaaaa") for r in counts_sorted["region"]]

    fig = go.Figure(go.Bar(
        y=counts_sorted["municipio"],
        x=counts_sorted["total"],
        orientation="h",
        marker_color=bar_colors,
        text=counts_sorted["total"],
        textposition="outside",
        customdata=counts_sorted[["region", "riesgos", "oportunidades"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>Región: %{customdata[0]}<br>"
            "Total: %{x}<br>Riesgos: %{customdata[1]}<br>"
            "Oportunidades: %{customdata[2]}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title="Noticias por municipio (color = región)",
        template=DARK_TEMPLATE,
        height=max(350, len(counts_sorted) * 28),
        margin=dict(t=50, b=20, l=200),
        showlegend=False,
    )

    # Legend annotation listing region colours
    legend_items = " | ".join(
        f'<span style="color:{_REGION_COLORS.get(r, "#aaa")}">■</span> {r}'
        for r in _REGIONES_COAHUILA
    )

    # News table
    if df_news_filtered.empty:
        return fig, [], [], ""

    display = df_news_filtered[["fecha", "tipo", "region", "municipio", "medio", "titulo", "url"]].copy()
    display["url"] = display["url"].apply(lambda u: f"[Ver]({u})" if u else "")

    columns = [
        {"name": "Fecha", "id": "fecha", "type": "text"},
        {"name": "Tipo", "id": "tipo", "type": "text"},
        {"name": "Región", "id": "region", "type": "text"},
        {"name": "Municipio", "id": "municipio", "type": "text"},
        {"name": "Medio", "id": "medio", "type": "text"},
        {"name": "Título", "id": "titulo", "type": "text"},
        {"name": "Enlace", "id": "url", "type": "text", "presentation": "markdown"},
    ]

    tipo_counts = df_news_filtered["tipo"].value_counts()
    counts_row = dbc.Row(
        [dbc.Col(dbc.Badge(f"{tipo}: {cnt}", color="secondary", className="me-1"), width="auto")
         for tipo, cnt in tipo_counts.items()],
        className="mb-2 g-1",
    )

    return fig, display.to_dict("records"), columns, counts_row


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port)
