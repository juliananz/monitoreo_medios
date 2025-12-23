import yaml
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
KEYWORDS_PATH = BASE_DIR / "config" / "keywords.yaml"

with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
    KEYWORDS = yaml.safe_load(f)

RIESGO = [k.lower() for k in KEYWORDS.get("riesgo", [])]
OPORTUNIDAD = [k.lower() for k in KEYWORDS.get("oportunidad", [])]


def clasificar_riesgo_oportunidad(texto: str):
    texto = (texto or "").lower()

    riesgo = any(p in texto for p in RIESGO)
    oportunidad = any(p in texto for p in OPORTUNIDAD)

    return int(riesgo), int(oportunidad)
