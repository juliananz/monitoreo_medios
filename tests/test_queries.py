"""
Unit tests for analytical query helpers.
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from datetime import date, timedelta
from unittest.mock import patch


@pytest.fixture
def test_db():
    """Create a test database with sample data."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Create tables
    cursor.executescript("""
        CREATE TABLE noticias (
            id INTEGER PRIMARY KEY,
            titulo TEXT,
            descripcion TEXT,
            url TEXT UNIQUE,
            fecha TEXT,
            medio TEXT,
            relevante INTEGER DEFAULT 1,
            riesgo INTEGER DEFAULT 0,
            oportunidad INTEGER DEFAULT 0,
            region_id INTEGER,
            requiere_analisis_profundo INTEGER DEFAULT 0
        );

        CREATE TABLE temas (
            id INTEGER PRIMARY KEY,
            nombre TEXT UNIQUE
        );

        CREATE TABLE regiones (
            id INTEGER PRIMARY KEY,
            nombre TEXT,
            nombre_normalizado TEXT,
            tipo TEXT
        );

        CREATE TABLE entidades (
            id INTEGER PRIMARY KEY,
            nombre_canonico TEXT UNIQUE,
            tipo TEXT,
            es_clave INTEGER DEFAULT 0
        );

        CREATE TABLE noticia_tema (
            noticia_id INTEGER,
            tema_id INTEGER,
            PRIMARY KEY (noticia_id, tema_id)
        );

        CREATE TABLE noticia_entidad (
            noticia_id INTEGER,
            entidad_id INTEGER,
            rol TEXT DEFAULT 'mencionado',
            PRIMARY KEY (noticia_id, entidad_id)
        );
    """)

    # Insert sample data
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    # Temas
    cursor.execute("INSERT INTO temas (id, nombre) VALUES (1, 'inversion')")
    cursor.execute("INSERT INTO temas (id, nombre) VALUES (2, 'empleo')")

    # Regiones
    cursor.execute("INSERT INTO regiones (id, nombre, nombre_normalizado, tipo) VALUES (1, 'Coahuila', 'coahuila', 'estado')")
    cursor.execute("INSERT INTO regiones (id, nombre, nombre_normalizado, tipo) VALUES (2, 'Nuevo León', 'nuevo leon', 'estado')")

    # Entidades
    cursor.execute("INSERT INTO entidades (id, nombre_canonico, tipo, es_clave) VALUES (1, 'General Motors', 'ORG', 1)")
    cursor.execute("INSERT INTO entidades (id, nombre_canonico, tipo, es_clave) VALUES (2, 'Tesla', 'ORG', 1)")

    # Noticias
    cursor.execute("""
        INSERT INTO noticias (id, titulo, fecha, medio, relevante, riesgo, oportunidad, region_id)
        VALUES (1, 'Inversión millonaria en Coahuila', ?, 'Vanguardia', 1, 0, 1, 1)
    """, (today,))
    cursor.execute("""
        INSERT INTO noticias (id, titulo, fecha, medio, relevante, riesgo, oportunidad, region_id)
        VALUES (2, 'Crisis en sector automotriz', ?, 'El Economista', 1, 1, 0, 2)
    """, (today,))
    cursor.execute("""
        INSERT INTO noticias (id, titulo, fecha, medio, relevante, riesgo, oportunidad, region_id)
        VALUES (3, 'GM anuncia expansión', ?, 'Reforma', 1, 0, 1, 1)
    """, (yesterday,))

    # Junction tables
    cursor.execute("INSERT INTO noticia_tema (noticia_id, tema_id) VALUES (1, 1)")
    cursor.execute("INSERT INTO noticia_tema (noticia_id, tema_id) VALUES (2, 2)")
    cursor.execute("INSERT INTO noticia_tema (noticia_id, tema_id) VALUES (3, 1)")
    cursor.execute("INSERT INTO noticia_entidad (noticia_id, entidad_id) VALUES (3, 1)")

    conn.commit()
    conn.close()

    yield path

    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_db_path(test_db):
    """Patch DB_PATH to use test database."""
    with patch("analisis.utils.DB_PATH", Path(test_db)):
        with patch("analisis.queries.get_db_connection") as mock_conn:
            # Create a context manager that returns the test DB connection
            import sqlite3
            from contextlib import contextmanager

            @contextmanager
            def get_test_conn():
                conn = sqlite3.connect(test_db)
                try:
                    yield conn
                finally:
                    conn.close()

            mock_conn.side_effect = get_test_conn
            yield test_db


class TestConteoQueries:
    """Tests for count/aggregation queries."""

    def test_get_conteo_por_tema(self, mock_db_path):
        from analisis.queries import get_conteo_por_tema

        results = get_conteo_por_tema()

        assert len(results) == 2
        # inversion has 2 news
        inversion = next((r for r in results if r['tema'] == 'inversion'), None)
        assert inversion is not None
        assert inversion['total'] == 2

    def test_get_conteo_diario(self, mock_db_path):
        from analisis.queries import get_conteo_diario

        results = get_conteo_diario(dias=7)

        assert len(results) >= 1
        # Should have today's entries
        today = date.today().isoformat()
        today_data = next((r for r in results if r['fecha'] == today), None)
        assert today_data is not None
        assert today_data['total'] >= 2


class TestEntidadQueries:
    """Tests for entity queries."""

    def test_get_top_entidades(self, mock_db_path):
        from analisis.queries import get_top_entidades

        results = get_top_entidades(limit=10)

        assert len(results) >= 1
        # GM should be in top entities
        gm = next((r for r in results if 'General Motors' in r['entidad']), None)
        assert gm is not None


class TestRegionQueries:
    """Tests for region queries."""

    def test_get_conteo_por_region(self, mock_db_path):
        from analisis.queries import get_conteo_por_region

        results = get_conteo_por_region()

        assert len(results) >= 1
        # Coahuila should have 2 news
        coahuila = next((r for r in results if r['region'] == 'Coahuila'), None)
        assert coahuila is not None
        assert coahuila['total'] == 2
