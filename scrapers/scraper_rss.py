import sqlite3
import yaml
import feedparser
from datetime import datetime
from pathlib import Path


# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"
FUENTES_PATH = BASE_DIR / "config" / "fuentes.yaml"


def cargar_fuentes():
    with open(FUENTES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("rss", [])


def guardar_noticias_rss():
    fuentes = cargar_fuentes()

    if not fuentes:
        print("No hay fuentes RSS configuradas.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for fuente in fuentes:
        nombre_medio = fuente.get("nombre")
        rss_url = fuente.get("url")

        if not nombre_medio or not rss_url:
            continue

        print(f"Procesando RSS: {nombre_medio}")

        try:
            feed = feedparser.parse(rss_url)

            if not feed.entries:
                print(f"  ⚠️ Sin entradas: {nombre_medio}")
                continue

            for entry in feed.entries:
                try:
                    titulo = entry.get("title", "").strip()
                    url = entry.get("link", "").strip()

                    descripcion = entry.get("summary", "") or entry.get("description", "")

                    fecha = entry.get("published", None)
                    if fecha is None:
                        fecha = datetime.now().strftime("%Y-%m-%d")

                    if not titulo or not url:
                        continue

                    cursor.execute("""
                        INSERT OR IGNORE INTO noticias
                        (titulo, descripcion, url, fecha, medio)
                        VALUES (?, ?, ?, ?, ?)
                    """, (titulo, descripcion, url, fecha, nombre_medio))

                except Exception as e:
                    print(f"    ⚠️ Error en nota individual ({nombre_medio}): {e}")
                    continue

        except Exception as e:
            print(f"  ❌ Error en fuente {nombre_medio}: {e}")
            continue

    conn.commit()
    conn.close()
    print("Scraping RSS completado.")


if __name__ == "__main__":
    guardar_noticias_rss()
