"""
Risk vs Opportunity classifier based on keyword matching.
"""

import unicodedata
import yaml
from config.settings import KEYWORDS_PATH

# Load keywords at module level
with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
    KEYWORDS = yaml.safe_load(f)

RIESGO = [k.lower() for k in KEYWORDS.get("riesgo", [])]
OPORTUNIDAD = [k.lower() for k in KEYWORDS.get("oportunidad", [])]


def normalizar(texto: str) -> str:
    """Normalize text: lowercase and remove accents."""
    texto = texto.lower()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def clasificar_riesgo_oportunidad(texto: str) -> tuple[int, int]:
    """
    Classify text as risk and/or opportunity.

    Returns: (riesgo, oportunidad) as integers (0 or 1)
    """
    texto = normalizar(texto)

    riesgo = any(p in texto for p in RIESGO)
    oportunidad = any(p in texto for p in OPORTUNIDAD)

    return int(riesgo), int(oportunidad)
