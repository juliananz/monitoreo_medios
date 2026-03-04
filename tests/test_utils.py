"""
Unit tests for shared utilities.
"""

from analisis.utils import clasificar_tipo


class TestClasificarTipo:
    """Tests for news type classification."""

    def test_riesgo_only(self):
        assert clasificar_tipo(riesgo=1, oportunidad=0) == "RIESGO"

    def test_oportunidad_only(self):
        assert clasificar_tipo(riesgo=0, oportunidad=1) == "OPORTUNIDAD"

    def test_mixto(self):
        assert clasificar_tipo(riesgo=1, oportunidad=1) == "MIXTO"

    def test_neutro(self):
        assert clasificar_tipo(riesgo=0, oportunidad=0) == "NEUTRO"

    def test_with_integer_values(self):
        # Ensure it works with actual DB integer values
        assert clasificar_tipo(1, 0) == "RIESGO"
        assert clasificar_tipo(0, 1) == "OPORTUNIDAD"
        assert clasificar_tipo(1, 1) == "MIXTO"
        assert clasificar_tipo(0, 0) == "NEUTRO"
