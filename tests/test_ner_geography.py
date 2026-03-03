"""
Unit tests for NER geographic classification.
"""

import pytest
from analisis.ner_entities import (
    inferir_nivel_geografico,
    requiere_analisis,
    cargar_config_geografia
)


@pytest.fixture
def geo_config():
    """Load actual geography config for tests."""
    return cargar_config_geografia()


class TestInferirNivelGeografico:
    """Tests for geographic level inference."""

    def test_internacional_usa(self, geo_config):
        lugares = {"Estados Unidos", "Texas"}
        assert inferir_nivel_geografico(lugares, geo_config) == "internacional"

    def test_internacional_china(self, geo_config):
        lugares = {"China", "Beijing"}
        assert inferir_nivel_geografico(lugares, geo_config) == "internacional"

    def test_estatal_coahuila(self, geo_config):
        lugares = {"Coahuila", "Saltillo"}
        assert inferir_nivel_geografico(lugares, geo_config) == "estatal"

    def test_nacional_other_state(self, geo_config):
        lugares = {"Nuevo Leon", "Monterrey"}
        assert inferir_nivel_geografico(lugares, geo_config) == "nacional"

    def test_nacional_jalisco(self, geo_config):
        lugares = {"Jalisco", "Guadalajara"}
        assert inferir_nivel_geografico(lugares, geo_config) == "nacional"

    def test_indeterminado_unknown_location(self, geo_config):
        lugares = {"Ciudad desconocida"}
        assert inferir_nivel_geografico(lugares, geo_config) == "indeterminado"

    def test_empty_places(self, geo_config):
        lugares = set()
        assert inferir_nivel_geografico(lugares, geo_config) == "indeterminado"

    def test_case_insensitive(self, geo_config):
        lugares = {"COAHUILA"}
        assert inferir_nivel_geografico(lugares, geo_config) == "estatal"

    def test_internacional_priority_over_nacional(self, geo_config):
        # If both international and national locations are present,
        # international should take priority
        lugares = {"Estados Unidos", "Coahuila"}
        assert inferir_nivel_geografico(lugares, geo_config) == "internacional"


class TestRequiereAnalisis:
    """Tests for deep analysis flag."""

    def test_key_company_gm(self, geo_config):
        lugares = set()
        organizaciones = {"General Motors", "GM"}
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1

    def test_key_company_tesla(self, geo_config):
        lugares = set()
        organizaciones = {"Tesla"}
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1

    def test_key_company_ford(self, geo_config):
        lugares = set()
        organizaciones = {"Ford Motor Company"}
        # Should match "ford" substring
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1

    def test_international_country(self, geo_config):
        lugares = {"Estados Unidos"}
        organizaciones = set()
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1

    def test_international_china(self, geo_config):
        lugares = {"China"}
        organizaciones = set()
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1

    def test_no_analysis_needed(self, geo_config):
        lugares = {"Saltillo"}
        organizaciones = {"Empresa Local SA"}
        assert requiere_analisis(lugares, organizaciones, geo_config) == 0

    def test_mexico_alone_no_analysis(self, geo_config):
        # Mexico alone shouldn't trigger deep analysis
        lugares = {"Mexico"}
        organizaciones = set()
        assert requiere_analisis(lugares, organizaciones, geo_config) == 0

    def test_case_insensitive_companies(self, geo_config):
        lugares = set()
        organizaciones = {"TESLA", "AMAZON"}
        assert requiere_analisis(lugares, organizaciones, geo_config) == 1
