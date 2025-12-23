import yaml
from pathlib import Path
import unicodedata

BASE_DIR = Path(__file__).resolve().parents[1]
KEYWORDS_PATH = BASE_DIR / "config" / "keywords.yaml"

with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
    KEYWORDS = yaml.safe_load(f)

RIESGO = [k.lower() for k in KEYWORDS.get("riesgo", [])]
OPORTUNIDAD = [k.lower() for k in KEYWORDS.get("oportunidad", [])]

def normalizar(texto):
    texto = texto.lower()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto



def clasificar_riesgo_oportunidad(texto: str):
    texto = normalizar(texto)

    riesgo = any(p in texto for p in RIESGO)
    oportunidad = any(p in texto for p in OPORTUNIDAD)

    return int(riesgo), int(oportunidad)
