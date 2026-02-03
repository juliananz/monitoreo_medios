"""
Query helpers for normalized analytical tables.
Provides convenient functions for common analytical queries.
"""

import logging
from datetime import date, timedelta
from typing import List, Dict, Optional
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


# =============================================================================
# TOPIC QUERIES
# =============================================================================

def get_noticias_por_tema(tema_nombre: str, fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get news articles for a specific topic."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT n.id, n.titulo, n.fecha, n.medio, n.riesgo, n.oportunidad
            FROM noticias n
            JOIN noticia_tema nt ON n.id = nt.noticia_id
            JOIN temas t ON nt.tema_id = t.id
            WHERE t.nombre = ?
        """
        params = [tema_nombre]

        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " ORDER BY n.fecha DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_conteo_por_tema(fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get news count per topic."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT t.nombre as tema,
                   COUNT(*) as total,
                   SUM(n.riesgo) as riesgos,
                   SUM(n.oportunidad) as oportunidades
            FROM noticias n
            JOIN noticia_tema nt ON n.id = nt.noticia_id
            JOIN temas t ON nt.tema_id = t.id
            WHERE 1=1
        """
        params = []

        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY t.nombre ORDER BY total DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# ENTITY QUERIES
# =============================================================================

def get_top_entidades(tipo: str = None, limit: int = 20,
                      fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get most mentioned entities."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT e.nombre_canonico as entidad,
                   e.tipo,
                   COUNT(*) as menciones,
                   COUNT(DISTINCT ne.noticia_id) as noticias
            FROM entidades e
            JOIN noticia_entidad ne ON e.id = ne.entidad_id
            JOIN noticias n ON ne.noticia_id = n.id
            WHERE 1=1
        """
        params = []

        if tipo:
            query += " AND e.tipo = ?"
            params.append(tipo)
        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY e.id ORDER BY menciones DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_entidad_timeline(entidad_nombre: str, dias: int = 30) -> List[Dict]:
    """Get daily mention count for an entity over time."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

        cursor.execute("""
            SELECT n.fecha, COUNT(*) as menciones
            FROM noticias n
            JOIN noticia_entidad ne ON n.id = ne.noticia_id
            JOIN entidades e ON ne.entidad_id = e.id
            WHERE e.nombre_canonico = ?
              AND n.fecha >= ?
            GROUP BY n.fecha
            ORDER BY n.fecha
        """, (entidad_nombre, fecha_inicio))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_entidades_clave_en_riesgo(fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get key entities mentioned in risk context."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT e.nombre_canonico as entidad,
                   e.categoria,
                   COUNT(*) as menciones_riesgo
            FROM entidades e
            JOIN noticia_entidad ne ON e.id = ne.entidad_id
            JOIN noticias n ON ne.noticia_id = n.id
            WHERE e.es_clave = 1
              AND n.riesgo = 1
        """
        params = []

        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY e.id ORDER BY menciones_riesgo DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# REGION QUERIES
# =============================================================================

def get_conteo_por_region(fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get news count per region."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT r.nombre as region,
                   r.tipo as tipo_region,
                   COUNT(*) as total,
                   SUM(n.riesgo) as riesgos,
                   SUM(n.oportunidad) as oportunidades
            FROM noticias n
            JOIN regiones r ON n.region_id = r.id
            WHERE n.region_id IS NOT NULL
        """
        params = []

        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY r.id ORDER BY total DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_tema_region_crosstab(fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get topic × region cross-tabulation."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT t.nombre as tema,
                   r.nombre as region,
                   COUNT(*) as total
            FROM noticias n
            JOIN noticia_tema nt ON n.id = nt.noticia_id
            JOIN temas t ON nt.tema_id = t.id
            JOIN regiones r ON n.region_id = r.id
            WHERE n.region_id IS NOT NULL
        """
        params = []

        if fecha_inicio:
            query += " AND n.fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND n.fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY t.id, r.id ORDER BY total DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# TIME SERIES QUERIES
# =============================================================================

def get_conteo_diario(dias: int = 30) -> List[Dict]:
    """Get daily news count for recent period."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

        cursor.execute("""
            SELECT fecha,
                   COUNT(*) as total,
                   SUM(CASE WHEN relevante = 1 THEN 1 ELSE 0 END) as relevantes,
                   SUM(riesgo) as riesgos,
                   SUM(oportunidad) as oportunidades
            FROM noticias
            WHERE fecha >= ?
            GROUP BY fecha
            ORDER BY fecha
        """, (fecha_inicio,))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_conteo_por_medio(fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
    """Get news count per media source."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT medio,
                   COUNT(*) as total,
                   SUM(CASE WHEN relevante = 1 THEN 1 ELSE 0 END) as relevantes,
                   SUM(riesgo) as riesgos,
                   SUM(oportunidad) as oportunidades
            FROM noticias
            WHERE 1=1
        """
        params = []

        if fecha_inicio:
            query += " AND fecha >= ?"
            params.append(fecha_inicio.isoformat())
        if fecha_fin:
            query += " AND fecha <= ?"
            params.append(fecha_fin.isoformat())

        query += " GROUP BY medio ORDER BY total DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


# =============================================================================
# SUMMARY QUERIES
# =============================================================================

def get_resumen_periodo(fecha_inicio: date, fecha_fin: date) -> Dict:
    """Get summary statistics for a period."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as total_noticias,
                SUM(CASE WHEN relevante = 1 THEN 1 ELSE 0 END) as total_relevantes,
                SUM(riesgo) as total_riesgos,
                SUM(oportunidad) as total_oportunidades,
                SUM(CASE WHEN riesgo = 1 AND oportunidad = 1 THEN 1 ELSE 0 END) as total_mixtos,
                COUNT(DISTINCT medio) as medios_activos,
                SUM(requiere_analisis_profundo) as requieren_analisis
            FROM noticias
            WHERE fecha >= ? AND fecha <= ?
        """, (fecha_inicio.isoformat(), fecha_fin.isoformat()))

        row = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row)) if row else {}


# =============================================================================
# AGGREGATION-BASED QUERIES (reads from pre-computed tables)
# =============================================================================

def get_agregacion_diaria(fecha_inicio: date, fecha_fin: date) -> List[Dict]:
    """
    Get pre-computed daily aggregates for date range.

    Faster than computing on-the-fly from noticias table.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
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
            WHERE fecha >= ? AND fecha <= ?
            ORDER BY fecha
        """, (fecha_inicio.isoformat(), fecha_fin.isoformat()))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agregacion_tema_periodo(tema_id: int, dias: int = 30) -> List[Dict]:
    """
    Get daily aggregates for a specific topic.

    Args:
        tema_id: ID of the topic
        dias: Number of days to look back

    Returns:
        List of daily records with fecha, total_noticias, total_riesgo, etc.
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                atd.fecha,
                t.nombre as tema,
                atd.total_noticias,
                atd.total_riesgo,
                atd.total_oportunidad,
                atd.score_promedio
            FROM agregacion_tema_diaria atd
            JOIN temas t ON atd.tema_id = t.id
            WHERE atd.tema_id = ?
              AND atd.fecha >= ?
            ORDER BY atd.fecha
        """, (tema_id, fecha_inicio))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_agregacion_entidad_periodo(entidad_id: int, dias: int = 30) -> List[Dict]:
    """
    Get daily aggregates for a specific entity.

    Args:
        entidad_id: ID of the entity
        dias: Number of days to look back

    Returns:
        List of daily records with fecha, menciones, noticias_riesgo, etc.
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
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
            WHERE aed.entidad_id = ?
              AND aed.fecha >= ?
            ORDER BY aed.fecha
        """, (entidad_id, fecha_inicio))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_top_entidades_periodo(dias: int = 30, limit: int = 20,
                               tipo: Optional[str] = None) -> List[Dict]:
    """
    Get top entities by total mentions from aggregation table.

    Faster than computing on-the-fly for large date ranges.

    Args:
        dias: Number of days to look back
        limit: Max entities to return
        tipo: Filter by entity type (PER, ORG, LOC, MISC)

    Returns:
        List of entities with total mentions and risk/opportunity counts.
    """
    fecha_inicio = (date.today() - timedelta(days=dias)).isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = """
            SELECT
                e.nombre_canonico as entidad,
                e.tipo,
                e.es_clave,
                e.categoria,
                SUM(aed.menciones) as total_menciones,
                SUM(aed.noticias_riesgo) as total_riesgo,
                SUM(aed.noticias_oportunidad) as total_oportunidad,
                COUNT(DISTINCT aed.fecha) as dias_activos
            FROM agregacion_entidad_diaria aed
            JOIN entidades e ON aed.entidad_id = e.id
            WHERE aed.fecha >= ?
        """
        params = [fecha_inicio]

        if tipo:
            query += " AND e.tipo = ?"
            params.append(tipo)

        query += """
            GROUP BY aed.entidad_id
            ORDER BY total_menciones DESC
            LIMIT ?
        """
        params.append(limit)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_temas_activos() -> List[Dict]:
    """Get list of active topics with their IDs."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, nombre, descripcion
            FROM temas
            WHERE activo = 1
            ORDER BY nombre
        """)

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_entidades_disponibles(limit: int = 100) -> List[Dict]:
    """Get list of entities available for selection in UI."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT e.id, e.nombre_canonico as nombre, e.tipo, e.es_clave, e.categoria
            FROM entidades e
            ORDER BY e.es_clave DESC, e.nombre_canonico
            LIMIT ?
        """, (limit,))

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


if __name__ == "__main__":
    # Test queries
    logging.basicConfig(level=logging.INFO)

    print("\n=== Top Entities ===")
    for e in get_top_entidades(limit=10):
        print(f"  {e['entidad']} ({e['tipo']}): {e['menciones']} mentions")

    print("\n=== News by Topic ===")
    for t in get_conteo_por_tema():
        print(f"  {t['tema']}: {t['total']} news ({t['riesgos']} risks, {t['oportunidades']} opps)")

    print("\n=== Daily Counts (last 7 days) ===")
    for d in get_conteo_diario(dias=7):
        print(f"  {d['fecha']}: {d['total']} total, {d['riesgos']} risks")
