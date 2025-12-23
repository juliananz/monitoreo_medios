import sys
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))


from analisis.clasificador_riesgo_oportunidad import clasificar_riesgo_oportunidad

DB_PATH = BASE_DIR / "data" / "noticias_medios.db"


def clasificar_riesgo_oportunidad_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Solo noticias aún no evaluadas
    cursor.execute("""
        SELECT id, titulo, descripcion
        FROM noticias
        WHERE riesgo = 0 AND oportunidad = 0
    """)
    filas = cursor.fetchall()
    print(f"Noticias a evaluar: {len(filas)}")

    for nid, titulo, descripcion in filas:
        texto = f"{titulo} {descripcion or ''}"
        riesgo, oportunidad = clasificar_riesgo_oportunidad(texto)

        cursor.execute("""
            UPDATE noticias
            SET riesgo = ?, oportunidad = ?
            WHERE id = ?
        """, (riesgo, oportunidad, nid))

    conn.commit()
    conn.close()
    print("Clasificación de riesgo/oportunidad completada.")


if __name__ == "__main__":
    clasificar_riesgo_oportunidad_db()
