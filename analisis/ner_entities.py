import sqlite3
from pathlib import Path
from transformers import pipeline

# =========================
# RUTAS
# =========================
BASE_DIR = Path(r"D:\DS\projects\monitoreo_medios")
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

# =========================
# MODELO NER
# =========================
ner_pipeline = pipeline(
    "ner",
    model="dslim/bert-base-NER",
    aggregation_strategy="simple"
)

# =========================
# LISTAS BÁSICAS PARA INFERENCIA
# =========================
ESTADOS_MEXICO = [
    "coahuila", "chiapas", "nuevo leon", "jalisco", "cdmx", "mexico"
]

PAISES_CLAVE = [
    "mexico", "estados unidos", "china", "canada", "alemania"
]

EMPRESAS_CLAVE = [
    "gm", "general motors", "ford", "stellantis",
    "tesla", "amazon", "walmart"
]

# =========================
# FUNCIONES AUXILIARES
# =========================
def inferir_nivel_geografico(lugares):
    lugares_norm = [l.lower() for l in lugares]

    if any(p in lugares_norm for p in PAISES_CLAVE if p != "mexico"):
        return "internacional"

    if any(e in lugares_norm for e in ESTADOS_MEXICO):
        return "nacional"

    if "coahuila" in lugares_norm:
        return "estatal"

    return "indeterminado"


def requiere_analisis(lugares, organizaciones):
    org_norm = [o.lower() for o in organizaciones]
    lug_norm = [l.lower() for l in lugares]

    if any(e in org_norm for e in EMPRESAS_CLAVE):
        return 1

    if any(p in lug_norm for p in PAISES_CLAVE if p != "mexico"):
        return 1

    return 0


# =========================
# FUNCIÓN PRINCIPAL
# =========================
def ejecutar_ner():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, titulo
        FROM noticias
        WHERE relevante = 1
          AND personas IS NULL
    """)
    noticias = cursor.fetchall()

    print(f"Noticias a procesar con NER: {len(noticias)}")

    for noticia_id, titulo in noticias:
        texto = titulo

        entidades = ner_pipeline(texto)

        personas = set()
        organizaciones = set()
        lugares = set()

        for ent in entidades:
            etiqueta = ent["entity_group"]
            valor = ent["word"]

            if etiqueta == "PER":
                personas.add(valor)
            elif etiqueta == "ORG":
                organizaciones.add(valor)
            elif etiqueta == "LOC":
                lugares.add(valor)

        personas_str = ",".join(personas)
        organizaciones_str = ",".join(organizaciones)
        lugares_str = ",".join(lugares)

        nivel_geo = inferir_nivel_geografico(lugares)
        flag_analisis = requiere_analisis(lugares, organizaciones)

        cursor.execute("""
            UPDATE noticias
            SET personas = ?,
                organizaciones = ?,
                lugares = ?,
                nivel_geografico = ?,
                requiere_analisis_profundo = ?
            WHERE id = ?
        """, (
            personas_str,
            organizaciones_str,
            lugares_str,
            nivel_geo,
            flag_analisis,
            noticia_id
        ))

    conn.commit()
    conn.close()

    print("NER completado correctamente")


if __name__ == "__main__":
    ejecutar_ner()
