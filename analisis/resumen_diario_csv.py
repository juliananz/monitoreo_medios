import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "noticias_medios.db"
OUTPUT_DIR = BASE_DIR / "data" / "salidas"

OUTPUT_DIR.mkdir(exist_ok=True)


def generar_resumen_diario():
    conn = sqlite3.connect(DB_PATH)

    query = """
        SELECT
            fecha,
            medio,
            titulo,
            temas,
            score,
            riesgo,
            oportunidad
        FROM noticias
        WHERE riesgo = 1 OR oportunidad = 1
        ORDER BY fecha DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("No hay noticias de riesgo u oportunidad para exportar.")
        return

    def clasificar_tipo(row):
        if row["riesgo"] == 1 and row["oportunidad"] == 1:
            return "MIXTO"
        elif row["riesgo"] == 1:
            return "RIESGO"
        elif row["oportunidad"] == 1:
            return "OPORTUNIDAD"
        return "NEUTRO"

    df["tipo"] = df.apply(clasificar_tipo, axis=1)

    df = df[["fecha", "medio", "titulo", "temas", "tipo", "score"]]

    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    output_file = OUTPUT_DIR / f"resumen_diario_{fecha_hoy}.csv"

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"Resumen diario generado: {output_file}")
    print(f"Total noticias exportadas: {len(df)}")


if __name__ == "__main__":
    generar_resumen_diario()
