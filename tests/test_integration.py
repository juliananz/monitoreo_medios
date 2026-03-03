"""
Integration tests for the media monitoring pipeline.

These tests verify the end-to-end flow using a temporary database.
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_db_path(temp_db):
    """Patch DB_PATH to use temporary database."""
    with patch("config.settings.DB_PATH", Path(temp_db)):
        with patch("analisis.utils.DB_PATH", Path(temp_db)):
            yield temp_db


class TestDatabaseIntegration:
    """Tests for database operations."""

    def test_create_database_schema(self, mock_db_path):
        """Test that database schema is created correctly."""
        from storage.database import crear_base_datos

        crear_base_datos()

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()

        # Check table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='noticias'"
        )
        assert cursor.fetchone() is not None

        # Check all columns exist
        cursor.execute("PRAGMA table_info(noticias)")
        columns = {row[1] for row in cursor.fetchall()}

        expected_columns = {
            "id", "titulo", "descripcion", "url", "fecha", "medio",
            "temas", "score", "relevante", "riesgo", "oportunidad",
            "personas", "organizaciones", "lugares",
            "nivel_geografico", "requiere_analisis_profundo"
        }

        assert expected_columns.issubset(columns)
        conn.close()


class TestClassificationIntegration:
    """Tests for classification pipeline."""

    def test_full_classification_flow(self, mock_db_path):
        """Test news insertion and classification."""
        from storage.database import crear_base_datos
        from analisis.clasificar_noticias_db import clasificar_noticias
        from analisis.clasificar_riesgo_oportunidad_db import clasificar_riesgo_oportunidad_db

        # Setup database
        crear_base_datos()

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()

        # Insert test news
        test_news = [
            ("Inversión millonaria en nueva planta", "Generará 500 empleos", "http://test1.com", "2024-01-15", "Test Media"),
            ("Crisis económica afecta al sector", "Anuncian despidos", "http://test2.com", "2024-01-15", "Test Media"),
            ("El clima será soleado mañana", "Temperaturas agradables", "http://test3.com", "2024-01-15", "Test Media"),
        ]

        for titulo, desc, url, fecha, medio in test_news:
            cursor.execute(
                "INSERT INTO noticias (titulo, descripcion, url, fecha, medio) VALUES (?, ?, ?, ?, ?)",
                (titulo, desc, url, fecha, medio)
            )
        conn.commit()

        # Run thematic classification
        clasificar_noticias()

        # Verify thematic classification
        cursor.execute("SELECT titulo, relevante, temas FROM noticias ORDER BY id")
        results = cursor.fetchall()

        # First news should be relevant (inversion, empleo)
        assert results[0][1] == 1  # relevante
        assert "inversion" in results[0][2] or "empleo" in results[0][2]

        # Second news should be relevant (empleo - despidos keyword)
        assert results[1][1] == 1

        # Third news should NOT be relevant
        assert results[2][1] == 0

        # Run risk/opportunity classification
        clasificar_riesgo_oportunidad_db()

        # Verify risk/opportunity classification
        cursor.execute("SELECT titulo, riesgo, oportunidad FROM noticias WHERE relevante = 1")
        results = cursor.fetchall()

        # First news should be opportunity
        assert results[0][2] == 1  # oportunidad

        # Second news should be risk
        assert results[1][1] == 1  # riesgo

        conn.close()

    def test_duplicate_url_ignored(self, mock_db_path):
        """Test that duplicate URLs are ignored on insert."""
        from storage.database import crear_base_datos

        crear_base_datos()

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()

        # Insert first news
        cursor.execute(
            "INSERT INTO noticias (titulo, descripcion, url, fecha, medio) VALUES (?, ?, ?, ?, ?)",
            ("Title 1", "Desc 1", "http://same-url.com", "2024-01-15", "Media 1")
        )
        conn.commit()

        # Try to insert duplicate URL
        cursor.execute(
            "INSERT OR IGNORE INTO noticias (titulo, descripcion, url, fecha, medio) VALUES (?, ?, ?, ?, ?)",
            ("Title 2", "Desc 2", "http://same-url.com", "2024-01-16", "Media 2")
        )
        conn.commit()

        # Should only have one record
        cursor.execute("SELECT COUNT(*) FROM noticias")
        count = cursor.fetchone()[0]
        assert count == 1

        # Should be the first one
        cursor.execute("SELECT titulo FROM noticias")
        titulo = cursor.fetchone()[0]
        assert titulo == "Title 1"

        conn.close()


class TestMigrationIntegration:
    """Tests for migration system."""

    def test_migration_tracking(self, mock_db_path):
        """Test that migrations are tracked correctly."""
        from migrations.manager import (
            init_migrations_table,
            mark_migration_applied,
            get_applied_migrations
        )

        with patch("migrations.manager.DB_PATH", Path(mock_db_path)):
            # Initialize
            init_migrations_table()

            # Should start empty
            applied = get_applied_migrations()
            assert len(applied) == 0

            # Mark a migration
            mark_migration_applied("001_test_migration")

            # Should now have one
            applied = get_applied_migrations()
            assert "001_test_migration" in applied

            # Marking again should not duplicate
            mark_migration_applied("001_test_migration")
            applied = get_applied_migrations()
            assert len(applied) == 1
