"""
FastAPI REST API for Monitoreo Medios.

Endpoints:
  GET /health              - DB record count and last processed date
  GET /noticias            - Paginated news list with filters
  GET /resumen/{fecha}     - Daily executive summary text
  GET /entidades/top       - Top entities by mention count
  GET /tendencias/diaria   - Daily aggregation time series

Swagger UI: http://localhost:8000/docs
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from config.settings import DB_PATH, OUTPUT_DIR

app = FastAPI(
    title="Monitoreo Medios API",
    description="API de inteligencia de medios — Secretaría de Economía de Coahuila",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    total_noticias: int
    ultima_fecha_procesada: Optional[str]


class NoticiaItem(BaseModel):
    id: int
    titulo: str
    fecha: Optional[str]
    medio: Optional[str]
    url: Optional[str]
    temas: Optional[str]
    riesgo: int
    oportunidad: int
    nivel_geografico: Optional[str]


class NoticiasResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[NoticiaItem]


class ResumenResponse(BaseModel):
    fecha: str
    texto: str


class EntidadTop(BaseModel):
    entidad: str
    tipo: str
    menciones: int


class EntidadesTopResponse(BaseModel):
    dias: int
    limit: int
    items: List[EntidadTop]


class TendenciaDiaria(BaseModel):
    fecha: str
    total_noticias: int
    total_relevantes: int
    total_riesgo: int
    total_oportunidad: int


class TendenciasResponse(BaseModel):
    dias: int
    series: List[TendenciaDiaria]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, summary="Estado del sistema")
def health():
    """Retorna el conteo de registros en la DB y la última fecha procesada."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM noticias")
        total = cur.fetchone()[0]
        cur.execute("SELECT MAX(fecha) FROM noticias")
        ultima = cur.fetchone()[0]
    return HealthResponse(status="ok", total_noticias=total, ultima_fecha_procesada=ultima)


@app.get("/noticias", response_model=NoticiasResponse, summary="Lista paginada de noticias")
def get_noticias(
    fecha: Optional[str] = Query(None, description="Filtrar por fecha (YYYY-MM-DD)"),
    medio: Optional[str] = Query(None, description="Filtrar por nombre de medio (parcial)"),
    tipo: Optional[str] = Query(
        None, description="Filtrar por tipo: riesgo | oportunidad | relevante"
    ),
    limit: int = Query(20, ge=1, le=200, description="Noticias por página"),
    offset: int = Query(0, ge=0, description="Desplazamiento para paginación"),
):
    """Lista noticias con filtros opcionales por fecha, medio y tipo."""
    filters = ["1=1"]
    params: list = []

    if fecha:
        filters.append("fecha = ?")
        params.append(fecha)
    if medio:
        filters.append("medio LIKE ?")
        params.append(f"%{medio}%")
    if tipo == "riesgo":
        filters.append("riesgo = 1")
    elif tipo == "oportunidad":
        filters.append("oportunidad = 1")
    elif tipo == "relevante":
        filters.append("relevante = 1")

    where = " AND ".join(filters)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM noticias WHERE {where}", params)
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT id, titulo, fecha, medio, url, temas,
                   COALESCE(riesgo, 0) AS riesgo,
                   COALESCE(oportunidad, 0) AS oportunidad,
                   nivel_geografico
            FROM noticias
            WHERE {where}
            ORDER BY fecha DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()

    items = [
        NoticiaItem(
            id=r["id"],
            titulo=r["titulo"],
            fecha=r["fecha"],
            medio=r["medio"],
            url=r["url"],
            temas=r["temas"],
            riesgo=r["riesgo"],
            oportunidad=r["oportunidad"],
            nivel_geografico=r["nivel_geografico"],
        )
        for r in rows
    ]

    return NoticiasResponse(total=total, limit=limit, offset=offset, items=items)


@app.get(
    "/resumen/{fecha}",
    response_model=ResumenResponse,
    summary="Resumen ejecutivo diario",
)
def get_resumen(fecha: str):
    """Retorna el texto del resumen ejecutivo LLM generado para la fecha dada."""
    filepath = Path(OUTPUT_DIR) / f"resumen_llm_{fecha}.txt"
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"No hay resumen para {fecha}")
    texto = filepath.read_text(encoding="utf-8")
    return ResumenResponse(fecha=fecha, texto=texto)


@app.get(
    "/entidades/top",
    response_model=EntidadesTopResponse,
    summary="Entidades más mencionadas",
)
def get_entidades_top(
    dias: int = Query(30, ge=1, le=365, description="Ventana de tiempo en días"),
    limit: int = Query(20, ge=1, le=100, description="Número máximo de entidades"),
):
    """Retorna las entidades con más menciones acumuladas en el período."""
    desde = (date.today() - timedelta(days=dias)).isoformat()

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.nombre_canonico, COALESCE(e.tipo, '') AS tipo,
                   SUM(aed.menciones) AS total
            FROM agregacion_entidad_diaria aed
            JOIN entidades e ON aed.entidad_id = e.id
            WHERE aed.fecha >= ?
            GROUP BY e.id
            ORDER BY total DESC
            LIMIT ?
            """,
            (desde, limit),
        )
        rows = cur.fetchall()

    items = [
        EntidadTop(entidad=r["nombre_canonico"], tipo=r["tipo"], menciones=r["total"] or 0)
        for r in rows
    ]
    return EntidadesTopResponse(dias=dias, limit=limit, items=items)


@app.get(
    "/tendencias/diaria",
    response_model=TendenciasResponse,
    summary="Serie de tiempo diaria",
)
def get_tendencias_diaria(
    dias: int = Query(30, ge=1, le=365, description="Número de días hacia atrás"),
):
    """Retorna la agregación diaria de noticias para graficar tendencias."""
    desde = (date.today() - timedelta(days=dias)).isoformat()

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT fecha,
                   COALESCE(total_noticias, 0)   AS total_noticias,
                   COALESCE(total_relevantes, 0)  AS total_relevantes,
                   COALESCE(total_riesgo, 0)      AS total_riesgo,
                   COALESCE(total_oportunidad, 0) AS total_oportunidad
            FROM agregacion_diaria
            WHERE fecha >= ?
            ORDER BY fecha ASC
            """,
            (desde,),
        )
        rows = cur.fetchall()

    series = [
        TendenciaDiaria(
            fecha=r["fecha"],
            total_noticias=r["total_noticias"],
            total_relevantes=r["total_relevantes"],
            total_riesgo=r["total_riesgo"],
            total_oportunidad=r["total_oportunidad"],
        )
        for r in rows
    ]
    return TendenciasResponse(dias=dias, series=series)
