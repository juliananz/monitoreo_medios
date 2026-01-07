import sqlite3
from pathlib import Path

BASE_DIR = Path(r"D:\DS\projects\monitoreo_medios")
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"

def run_migration():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Personas
    cursor.execute("""
        ALTER TABLE noticias
        ADD COLUMN personas TEXT;
    """)

    # Organizaciones
    cursor.execute("""
        ALTER TABLE noticias
        ADD COLUMN organizaciones TEXT;
    """)

    # Lugares
    cursor.execute("""
        ALTER TABLE noticias
        ADD COLUMN lugares TEXT;
    """)

    # Nivel geográfico
    cursor.execute("""
        ALTER TABLE noticias
        ADD COLUMN nivel_geografico TEXT;
    """)

    # Flag para análisis profundo
    cursor.execute("""
        ALTER TABLE noticias
        ADD COLUMN requiere_analisis_profundo INTEGER DEFAULT 0;
    """)

    conn.commit()
    conn.close()

    print("Migración 001 aplicada correctamente")

if __name__ == "__main__":
    run_migration()
