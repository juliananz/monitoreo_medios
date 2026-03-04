"""
Named Entity Recognition (NER) for news articles.

Primary:  Google Gemini API (batched, 5 articles per request).
Fallback: Spanish BERT model (mrm8488/bert-spanish-cased-finetuned-ner).

Falls back to BERT per batch if Gemini is unavailable or fails.
"""

import json
import logging
import re
import time
import yaml
from pathlib import Path
from transformers import pipeline

from config.settings import DB_PATH, KEYWORDS_PATH, GEMINI_API_KEY, GEMINI_MODEL
from analisis.utils import get_db_connection, normalizar_entidad

logger = logging.getLogger(__name__)

GEMINI_BATCH_SIZE = 5

# ---------------------------------------------------------------------------
# Model initialization (lazy)
# ---------------------------------------------------------------------------

_ner_pipeline = None
_gemini_model = None


def _get_bert_pipeline():
    global _ner_pipeline
    if _ner_pipeline is None:
        _ner_pipeline = pipeline(
            "ner",
            model="mrm8488/bert-spanish-cased-finetuned-ner",
            aggregation_strategy="simple",
        )
    return _ner_pipeline


def _get_gemini_model():
    global _gemini_model
    if _gemini_model is None:
        if not GEMINI_API_KEY:
            return None
        try:
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            _gemini_model = genai.GenerativeModel(GEMINI_MODEL)
        except Exception as e:
            logger.warning(f"Could not initialize Gemini: {e}")
            return None
    return _gemini_model


# ---------------------------------------------------------------------------
# Gemini NER (batched)
# ---------------------------------------------------------------------------

_PROMPT_NER = """Eres un extractor de entidades para noticias de Coahuila, México.
Para cada noticia numerada, extrae entidades nombradas.
Devuelve ÚNICAMENTE un array JSON sin texto ni markdown adicional.

Guía de tipos:
- personas: funcionarios, empresarios, políticos (ej: "Manolo Jiménez Salinas", "secretario de Economía")
- organizaciones: empresas, dependencias, sindicatos, partidos (ej: "AHMSA", "Secretaría de Economía de Coahuila", "SEDECO", "IMMEX")
- lugares: municipios, regiones, estados, países (ej: "Saltillo", "Zona Laguna", "Coahuila", "Estados Unidos", "Carbonífera")

Formato esperado:
[
  {{"id": 1, "personas": [], "organizaciones": [], "lugares": []}},
  {{"id": 2, "personas": [], "organizaciones": [], "lugares": []}}
]

NOTICIAS:
{noticias_text}

JSON:"""


def _extraer_con_gemini(batch: list) -> dict:
    """
    Extract entities for a batch of (id, titulo, descripcion) tuples via Gemini.
    Returns dict {noticia_id: {personas, organizaciones, lugares}} or empty on failure.
    """
    model = _get_gemini_model()
    if model is None:
        return {}

    noticias_text = "\n\n".join(
        f"{i+1}. {titulo} {descripcion or ''}".strip()
        for i, (nid, titulo, descripcion) in enumerate(batch)
    )

    prompt = _PROMPT_NER.format(noticias_text=noticias_text)

    try:
        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.0, "max_output_tokens": 1024},
        )
        raw = response.text.strip()

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        parsed = json.loads(raw)

        result = {}
        for i, item in enumerate(parsed):
            if i >= len(batch):
                break
            noticia_id = batch[i][0]
            result[noticia_id] = {
                "personas": set(item.get("personas", [])),
                "organizaciones": set(item.get("organizaciones", [])),
                "lugares": set(item.get("lugares", [])),
            }
        return result

    except Exception as e:
        logger.warning(f"Gemini NER failed for batch: {e}")
        return {}


# ---------------------------------------------------------------------------
# BERT NER (fallback)
# ---------------------------------------------------------------------------

def _extraer_con_bert(batch: list) -> dict:
    """
    Extract entities for a batch using Spanish BERT NER.
    Returns dict {noticia_id: {personas, organizaciones, lugares}}.
    """
    nlp = _get_bert_pipeline()
    result = {}

    for noticia_id, titulo, descripcion in batch:
        texto = f"{titulo} {descripcion or ''}"
        encoded = nlp.tokenizer(texto, truncation=True, max_length=512)
        texto = nlp.tokenizer.decode(encoded["input_ids"], skip_special_tokens=True)

        entidades = nlp(texto)

        personas, organizaciones, lugares = set(), set(), set()
        for ent in entidades:
            valor = ent["word"].strip()
            if len(valor) < 2:
                continue
            etiqueta = ent["entity_group"]
            if etiqueta == "PER":
                personas.add(valor)
            elif etiqueta == "ORG":
                organizaciones.add(valor)
            elif etiqueta == "LOC":
                lugares.add(valor)

        result[noticia_id] = {
            "personas": personas,
            "organizaciones": organizaciones,
            "lugares": lugares,
        }

    return result


# ---------------------------------------------------------------------------
# Geography & analysis helpers (unchanged)
# ---------------------------------------------------------------------------

def cargar_config_geografia():
    with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    geo = config.get("geografia", {})
    return {
        "estado_objetivo": geo.get("estado_objetivo", "coahuila").lower(),
        "estados_mexico": [normalizar_entidad(e) for e in geo.get("estados_mexico", [])],
        "paises_clave": [normalizar_entidad(p) for p in geo.get("paises_clave", [])],
        "empresas_clave": [normalizar_entidad(c) for c in geo.get("empresas_clave", [])],
    }


def get_or_create_entidad(cursor, nombre: str, tipo: str, alias_map: dict) -> int:
    nombre_norm = normalizar_entidad(nombre)
    if nombre_norm in alias_map:
        return alias_map[nombre_norm]
    try:
        cursor.execute(
            "INSERT INTO entidades (nombre_canonico, tipo) VALUES (?, ?)",
            (nombre, tipo),
        )
        entidad_id = cursor.lastrowid
        cursor.execute(
            "INSERT OR IGNORE INTO entidad_alias (entidad_id, alias, es_principal) VALUES (?, ?, 1)",
            (entidad_id, nombre_norm),
        )
        alias_map[nombre_norm] = entidad_id
        return entidad_id
    except Exception as e:
        cursor.execute(
            "SELECT id FROM entidades WHERE nombre_canonico = ?", (nombre,)
        )
        row = cursor.fetchone()
        if row:
            alias_map[nombre_norm] = row[0]
            return row[0]
        logger.debug(f"Could not create entity {nombre}: {e}")
        return None


def inferir_nivel_geografico(lugares: set, config: dict) -> str:
    lugares_norm = [normalizar_entidad(l) for l in lugares]
    estado_objetivo = config["estado_objetivo"]
    estados_mexico = config["estados_mexico"]
    paises_clave = config["paises_clave"]
    paises_no_mexico = [p for p in paises_clave if p not in ("mexico", "méxico")]
    if any(p in lugares_norm for p in paises_no_mexico):
        return "internacional"
    if estado_objetivo in lugares_norm:
        return "estatal"
    if any(e in lugares_norm for e in estados_mexico):
        return "nacional"
    return "indeterminado"


def get_region_id(cursor, lugares: set, region_map: dict) -> int:
    for lugar in lugares:
        lugar_norm = normalizar_entidad(lugar)
        if lugar_norm in region_map:
            return region_map[lugar_norm]
    return None


def requiere_analisis(lugares: set, organizaciones: set, config: dict) -> int:
    org_norm = [normalizar_entidad(o) for o in organizaciones]
    lug_norm = [normalizar_entidad(l) for l in lugares]
    if any(e in org_norm for e in config["empresas_clave"]):
        return 1
    paises_no_mexico = [p for p in config["paises_clave"] if p not in ("mexico", "méxico")]
    if any(p in lug_norm for p in paises_no_mexico):
        return 1
    return 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ejecutar_ner():
    """Run NER on all unprocessed relevant news. Uses Gemini if API key is set, else BERT."""
    geo_config = cargar_config_geografia()
    use_gemini = bool(GEMINI_API_KEY)

    if use_gemini:
        logger.info(f"NER mode: Gemini ({GEMINI_MODEL}) with BERT fallback")
    else:
        logger.info("NER mode: BERT only (set GEMINI_API_KEY to enable Gemini)")

    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT alias, entidad_id FROM entidad_alias")
        alias_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT nombre_normalizado, id FROM regiones")
        region_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT id, titulo, descripcion
            FROM noticias
            WHERE relevante = 1
              AND (procesado_ner = 0 OR procesado_ner IS NULL)
        """)
        noticias = cursor.fetchall()

        logger.info(f"Articles to process with NER: {len(noticias)}")

        links_created = 0

        # Process in batches
        for batch_start in range(0, len(noticias), GEMINI_BATCH_SIZE):
            batch = noticias[batch_start: batch_start + GEMINI_BATCH_SIZE]

            # Try Gemini first, fall back to BERT if it fails
            if use_gemini:
                entities_by_id = _extraer_con_gemini(batch)
                if len(entities_by_id) < len(batch):
                    # Gemini missed some articles — process missing ones with BERT
                    missing = [row for row in batch if row[0] not in entities_by_id]
                    if missing:
                        logger.debug(f"BERT fallback for {len(missing)} articles in batch")
                        entities_by_id.update(_extraer_con_bert(missing))
                # Rate limit: stay safely under 15 RPM
                time.sleep(4)
            else:
                entities_by_id = _extraer_con_bert(batch)

            # Persist results
            for noticia_id, titulo, descripcion in batch:
                ents = entities_by_id.get(noticia_id, {"personas": set(), "organizaciones": set(), "lugares": set()})
                personas = ents["personas"]
                organizaciones = ents["organizaciones"]
                lugares = ents["lugares"]

                nivel_geo = inferir_nivel_geografico(lugares, geo_config)
                region_id = get_region_id(cursor, lugares, region_map)
                flag_analisis = requiere_analisis(lugares, organizaciones, geo_config)

                cursor.execute("""
                    UPDATE noticias
                    SET personas = ?,
                        organizaciones = ?,
                        lugares = ?,
                        nivel_geografico = ?,
                        region_id = ?,
                        requiere_analisis_profundo = ?,
                        procesado_ner = 1
                    WHERE id = ?
                """, (
                    ",".join(personas),
                    ",".join(organizaciones),
                    ",".join(lugares),
                    nivel_geo,
                    region_id,
                    flag_analisis,
                    noticia_id,
                ))

                for entity_set, tipo in [(personas, "PER"), (organizaciones, "ORG"), (lugares, "LOC")]:
                    for entity_name in entity_set:
                        entidad_id = get_or_create_entidad(cursor, entity_name, tipo, alias_map)
                        if entidad_id:
                            try:
                                cursor.execute("""
                                    INSERT OR IGNORE INTO noticia_entidad (noticia_id, entidad_id, rol)
                                    VALUES (?, ?, 'mencionado')
                                """, (noticia_id, entidad_id))
                                if cursor.rowcount > 0:
                                    links_created += 1
                            except Exception:
                                pass

            conn.commit()

    logger.info(f"NER completed. Created {links_created} entity links.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ejecutar_ner()
