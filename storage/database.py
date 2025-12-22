import sqlite3
from pathlib import Path


# Base directory
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "noticias_medios.db"


def crear_base_datos():
    """
    Crea la base de datos y la tabla principal de noticias.
    Este es el esquema CANÓNICO del proyecto.
    """

    # Asegurar que exista la carpeta /data
    DATA_DIR.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS noticias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        titulo TEXT NOT NULL,
        descripcion TEXT,
        url TEXT UNIQUE,
        fecha TEXT,
        medio TEXT,

        temas TEXT,
        score INTEGER,
        relevante INTEGER,

        riesgo INTEGER DEFAULT 0,
        oportunidad INTEGER DEFAULT 0
    );
    """)

    conn.commit()
    conn.close()

    print(f"Base de datos creada correctamente en: {DB_PATH}")


if __name__ == "__main__":
    crear_base_datos()
