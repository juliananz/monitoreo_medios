"""
Unit tests for risk/opportunity classifier.
"""

from analisis.clasificador_riesgo_oportunidad import (
    clasificar_riesgo_oportunidad,
    normalizar
)


class TestNormalizar:
    """Tests for text normalization with accent removal."""

    def test_lowercase(self):
        assert "inversion" in normalizar("INVERSIÓN")

    def test_removes_accents(self):
        result = normalizar("inversión expansión")
        assert "ó" not in result
        assert "inversion" in result
        assert "expansion" in result

    def test_handles_spanish_chars(self):
        result = normalizar("año señor niño")
        # ñ is kept (not an accent mark)
        assert "ano" in result or "año" in result


class TestClasificarRiesgoOportunidad:
    """Tests for risk/opportunity classification."""

    # Risk detection tests
    def test_detects_cierre_as_risk(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Anuncian cierre de planta industrial"
        )
        assert riesgo == 1

    def test_detects_despidos_as_risk(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Empresa anuncia despidos masivos"
        )
        assert riesgo == 1

    def test_detects_huelga_as_risk(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Trabajadores inician huelga indefinida"
        )
        assert riesgo == 1

    def test_detects_crisis_as_risk(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Crisis económica afecta al sector"
        )
        assert riesgo == 1

    def test_detects_arancel_as_risk(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Nuevos aranceles impactan comercio"
        )
        assert riesgo == 1

    # Opportunity detection tests
    def test_detects_inversion_as_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Nueva inversión millonaria en la región"
        )
        assert oportunidad == 1

    def test_detects_expansion_as_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Empresa anuncia expansión de operaciones"
        )
        assert oportunidad == 1

    def test_detects_nearshoring_as_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "México se beneficia del nearshoring"
        )
        assert oportunidad == 1

    def test_detects_nueva_planta_as_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Construirán nueva planta en Coahuila"
        )
        assert oportunidad == 1

    def test_detects_empleos_as_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "Se crearán 1000 nuevos empleos"
        )
        assert oportunidad == 1

    # Mixed detection tests
    def test_detects_mixed_risk_and_opportunity(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "A pesar de los despidos, la empresa anuncia nueva inversión"
        )
        assert riesgo == 1
        assert oportunidad == 1

    # Neutral tests
    def test_neutral_news(self):
        riesgo, oportunidad = clasificar_riesgo_oportunidad(
            "El gobernador visitó las instalaciones"
        )
        assert riesgo == 0
        assert oportunidad == 0

    # Case sensitivity
    def test_case_insensitive_risk(self):
        riesgo, _ = clasificar_riesgo_oportunidad("CRISIS ECONÓMICA")
        assert riesgo == 1

    def test_case_insensitive_opportunity(self):
        _, oportunidad = clasificar_riesgo_oportunidad("INVERSIÓN MILLONARIA")
        assert oportunidad == 1

    # Accent handling
    def test_handles_accents_in_keywords(self):
        _, oportunidad = clasificar_riesgo_oportunidad("inversión con tilde")
        assert oportunidad == 1
