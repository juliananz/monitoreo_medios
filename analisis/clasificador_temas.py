"""
Keyword-based thematic classifier for news articles.
"""

import re
import yaml
from config.settings import KEYWORDS_PATH


def cargar_keywords():
    """Load topic keywords from YAML config."""
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("temas", {})


def normalizar_texto(texto: str) -> str:
    """Normalize text for keyword matching."""
    if not texto:
        return ""
    texto = texto.lower()
    texto = re.sub(r"[^\w\s]", " ", texto)
    return texto


def clasificar_noticia(titulo: str, descripcion: str = "") -> dict:
    """
    Classify a news article by topic based on keyword matching.

    Returns dict with:
        - temas: list of detected topics
        - score: number of topics matched
        - relevante: 1 if any topic matched, 0 otherwise
    """
    temas_keywords = cargar_keywords()
    texto = normalizar_texto(f"{titulo} {descripcion}")

    temas_detectados = []
    score = 0

    for tema, palabras in temas_keywords.items():
        for palabra in palabras:
            if palabra in texto:
                temas_detectados.append(tema)
                score += 1
                break

    relevante = 1 if score > 0 else 0

    return {
        "temas": temas_detectados,
        "score": score,
        "relevante": relevante
    }


if __name__ == "__main__":
    ejemplo = clasificar_noticia(
        "Empresa automotriz anuncia inversión",
        "La planta generará nuevos empleos"
    )
    print(ejemplo)
