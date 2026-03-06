"""
Named Entity Recognition (NER) for news articles.

Primary:  GROQ API (batched, 5 articles per request).
Fallback: Spanish BERT model (mrm8488/bert-spanish-cased-finetuned-ner).

Falls back to BERT per batch if GROQ is unavailable or fails.
"""

import html as _html_mod
import json
import logging
import re
import time
import yaml
from transformers import pipeline

from config.settings import KEYWORDS_PATH, GROQ_API_KEY, GROQ_MODEL
from analisis.utils import get_db_connection, normalizar_entidad

logger = logging.getLogger(__name__)

GROQ_BATCH_SIZE = 5

# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace. Truncates to 1500 chars."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)          # remove tags
    text = _html_mod.unescape(text)                # &amp; &nbsp; etc.
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1500]


def _limpiar_token_bert(word: str) -> str:
    """Remove WordPiece '##' subword prefix that BERT leaves when a token is its own entity span.

    convert_tokens_to_string(["##inbaum"]) → "##inbaum" (no leading space to replace).
    This function catches those stragglers.
    """
    return re.sub(r"\s*##", "", word).strip()


# ---------------------------------------------------------------------------
# Model initialization (lazy)
# ---------------------------------------------------------------------------

_ner_pipeline = None
_groq_client = None


def _get_bert_pipeline():
    global _ner_pipeline
    if _ner_pipeline is None:
        try:
            import torch  # noqa: F401 — required by transformers pipeline
            _ner_pipeline = pipeline(
                "ner",
                model="mrm8488/bert-spanish-cased-finetuned-ner",
                aggregation_strategy="simple",
            )
        except Exception as e:
            logger.warning(f"BERT pipeline unavailable (torch missing?): {e}")
            return None
    return _ner_pipeline


def _get_groq_client():
    global _groq_client
    if _groq_client is None:
        if not GROQ_API_KEY:
            return None
        try:
            from groq import Groq

            _groq_client = Groq(api_key=GROQ_API_KEY)
        except Exception as e:
            logger.warning(f"Could not initialize GROQ client: {e}")
            return None
    return _groq_client


# ---------------------------------------------------------------------------
# GROQ NER (batched)
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


def _extraer_con_groq(batch: list) -> dict:
    """
    Extract entities for a batch of (id, titulo, descripcion) tuples via GROQ.
    Returns dict {noticia_id: {personas, organizaciones, lugares}} or empty on failure.
    """
    client = _get_groq_client()
    if client is None:
        return {}

    noticias_text = "\n\n".join(
        f"{i+1}. {titulo} {_strip_html(descripcion or '')}".strip()
        for i, (nid, titulo, descripcion) in enumerate(batch)
    )

    prompt = _PROMPT_NER.format(noticias_text=noticias_text)

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content.strip()

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
        logger.warning(f"GROQ NER failed for batch: {e}")
        return {}


# ---------------------------------------------------------------------------
# BERT NER (fallback)
# ---------------------------------------------------------------------------

def _extraer_con_bert(batch: list) -> dict:
    """
    Extract entities for a batch using Spanish BERT NER.
    Returns dict {noticia_id: {personas, organizaciones, lugares}}.
    Returns empty dict if BERT/torch is unavailable.
    """
    nlp = _get_bert_pipeline()
    if nlp is None:
        logger.warning("BERT fallback skipped — torch not installed. Articles will have empty entities.")
        return {}
    result = {}

    for noticia_id, titulo, descripcion in batch:
        desc_limpia = _strip_html(descripcion or "")
        texto = f"{titulo} {desc_limpia}".strip()
        encoded = nlp.tokenizer(texto, truncation=True, max_length=512)
        texto = nlp.tokenizer.decode(encoded["input_ids"], skip_special_tokens=True)

        entidades = nlp(texto)

        personas, organizaciones, lugares = set(), set(), set()
        for ent in entidades:
            valor = _limpiar_token_bert(ent["word"])
            # Drop pure subword fragments or single-char tokens
            if len(valor) < 2 or valor.startswith("#"):
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


def inferir_nivel_geografico(lugares: set, config: dict, titulo: str = "") -> str:
    """Infer geographic level from extracted locations AND the article title.

    Checking the title directly catches cases where NER misses a location
    (e.g. "Coahuila" in the headline but not extracted as a LOC entity).
    Priority: internacional > estatal > nacional > indeterminado.
    """
    lugares_norm = [normalizar_entidad(loc) for loc in lugares]
    titulo_norm = normalizar_entidad(titulo)
    estado_objetivo = config["estado_objetivo"]
    estados_mexico = config["estados_mexico"]
    paises_clave = config["paises_clave"]
    paises_no_mexico = [p for p in paises_clave if p not in ("mexico", "méxico")]

    def _en_lugares(terms):
        return any(t in lugares_norm for t in terms)

    def _en_titulo(terms):
        return any(t in titulo_norm for t in terms)

    if _en_lugares(paises_no_mexico) or _en_titulo(paises_no_mexico):
        return "internacional"
    if estado_objetivo in lugares_norm or estado_objetivo in titulo_norm:
        return "estatal"
    if _en_lugares(estados_mexico) or _en_titulo(estados_mexico):
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
    lug_norm = [normalizar_entidad(loc) for loc in lugares]
    if any(empresa in org for org in org_norm for empresa in config["empresas_clave"]):
        return 1
    paises_no_mexico = [p for p in config["paises_clave"] if p not in ("mexico", "méxico")]
    if any(p in lug_norm for p in paises_no_mexico):
        return 1
    return 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ejecutar_ner():
    """Run NER on all unprocessed relevant news. Uses GROQ if API key is set, else BERT."""
    geo_config = cargar_config_geografia()
    use_groq = bool(GROQ_API_KEY)

    if use_groq:
        logger.info(f"NER mode: GROQ ({GROQ_MODEL}) with BERT fallback")
    else:
        logger.info("NER mode: BERT only (set GROQ_API_KEY to enable GROQ)")

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
        for batch_start in range(0, len(noticias), GROQ_BATCH_SIZE):
            batch = noticias[batch_start: batch_start + GROQ_BATCH_SIZE]

            # Try GROQ first, fall back to BERT if it fails
            if use_groq:
                entities_by_id = _extraer_con_groq(batch)
                if len(entities_by_id) < len(batch):
                    # GROQ missed some articles — process missing ones with BERT
                    missing = [row for row in batch if row[0] not in entities_by_id]
                    if missing:
                        logger.debug(f"BERT fallback for {len(missing)} articles in batch")
                        entities_by_id.update(_extraer_con_bert(missing))
                # Rate limit: stay safely under 30 RPM
                time.sleep(2)
            else:
                entities_by_id = _extraer_con_bert(batch)

            # Persist results
            for noticia_id, titulo, descripcion in batch:
                ents = entities_by_id.get(noticia_id, {"personas": set(), "organizaciones": set(), "lugares": set()})
                personas = ents["personas"]
                organizaciones = ents["organizaciones"]
                lugares = ents["lugares"]

                nivel_geo = inferir_nivel_geografico(lugares, geo_config, titulo)
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
