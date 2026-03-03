"""
Unit tests for thematic classifier.
"""

import pytest
from analisis.clasificador_temas import clasificar_noticia, normalizar_texto


class TestNormalizarTexto:
    """Tests for text normalization."""

    def test_lowercase(self):
        assert normalizar_texto("INVERSIÓN") == "inversión"

    def test_removes_punctuation(self):
        result = normalizar_texto("¡Hola, mundo!")
        assert "," not in result
        assert "!" not in result
        assert "¡" not in result

    def test_empty_string(self):
        assert normalizar_texto("") == ""

    def test_none_returns_empty(self):
        assert normalizar_texto(None) == ""


class TestClasificarNoticia:
    """Tests for news classification."""

    def test_detects_inversion_tema(self):
        resultado = clasificar_noticia(
            "Nueva inversión millonaria en la región",
            "La empresa anunció capital para expansión"
        )
        assert "inversion" in resultado["temas"]
        assert resultado["relevante"] == 1

    def test_detects_empleo_tema(self):
        resultado = clasificar_noticia(
            "Se generarán 500 nuevos empleos",
            "La contratación iniciará el próximo mes"
        )
        assert "empleo" in resultado["temas"]
        assert resultado["relevante"] == 1

    def test_detects_industria_tema(self):
        resultado = clasificar_noticia(
            "Planta automotriz aumenta producción",
            "La manufactura crece en el estado"
        )
        assert "industria" in resultado["temas"]
        assert resultado["relevante"] == 1

    def test_detects_comercio_exterior_tema(self):
        resultado = clasificar_noticia(
            "Nuevos aranceles afectan exportación",
            "El T-MEC establece nuevas reglas"
        )
        assert "comercio_exterior" in resultado["temas"]
        assert resultado["relevante"] == 1

    def test_multiple_temas(self):
        resultado = clasificar_noticia(
            "Inversión genera empleos en industria automotriz",
            "Nueva planta de manufactura"
        )
        assert len(resultado["temas"]) >= 2
        assert resultado["score"] >= 2

    def test_no_relevante_news(self):
        resultado = clasificar_noticia(
            "El clima será soleado mañana",
            "Temperaturas agradables en la región"
        )
        assert resultado["temas"] == []
        assert resultado["score"] == 0
        assert resultado["relevante"] == 0

    def test_case_insensitive(self):
        resultado = clasificar_noticia("INVERSIÓN MILLONARIA", "")
        assert "inversion" in resultado["temas"]

    def test_empty_description(self):
        resultado = clasificar_noticia("Nueva inversión anunciada", "")
        assert resultado["relevante"] == 1
