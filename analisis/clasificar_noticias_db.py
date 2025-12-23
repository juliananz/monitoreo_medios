import sys
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))


from analisis.clasificador_temas import clasificar_noticia

DB_PATH = BASE_DIR / "data" / "noticias_medios.db"


def clasificar_noticias():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, titulo, descripcion
        FROM noticias
        WHERE relevante IS NULL
    """)

    noticias = cursor.fetchall()
    print(f"Noticias a clasificar: {len(noticias)}")

    for noticia_id, titulo, descripcion in noticias:
        resultado = clasificar_noticia(titulo, descripcion or "")

        temas_str = ",".join(resultado["temas"])
        score = resultado["score"]
        relevante = resultado["relevante"]

        cursor.execute("""
            UPDATE noticias
            SET temas = ?, score = ?, relevante = ?
            WHERE id = ?
        """, (temas_str, score, relevante, noticia_id))

    conn.commit()
    conn.close()
    print("Clasificación temática completada.")


if __name__ == "__main__":
    clasificar_noticias()
