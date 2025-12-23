"""
Main pipeline for media monitoring.

Order:
1. Initialize database (if needed)
2. Scrape RSS sources
3. Thematic classification
4. Risk vs Opportunity classification
"""

from datetime import datetime
from pathlib import Path
import sys

# --- PATH FIX ---
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
# ----------------

# Storage
from storage.database import crear_base_datos

# Scrapers
from scrapers.scraper_rss import guardar_noticias_rss

# Analysis
from analisis.clasificar_noticias_db import clasificar_noticias
from analisis.clasificar_riesgo_oportunidad_db import clasificar_riesgo_oportunidad_db
from analisis.resumen_diario_csv import generar_resumen_diario

def main():
    inicio = datetime.now()

    print("=" * 70)
    print("MEDIA MONITORING PIPELINE")
    print(f"Start time: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # 1. Database
    print("\n[1/5] Initializing database...")
    crear_base_datos()
    print("✔ Database ready")

    # 2. RSS scraping
    print("\n[2/5] Scraping RSS sources...")
    guardar_noticias_rss()
    print("✔ RSS scraping completed")

    # 3. Thematic classification
    print("\n[3/5] Running thematic classification...")
    clasificar_noticias()
    print("✔ Thematic classification completed")

    # 4. Risk vs Opportunity
    print("\n[4/5] Evaluating risk vs opportunity...")
    clasificar_riesgo_oportunidad_db()
    print("✔ Risk/Opportunity classification completed")
    
    # 5. Daily CSV summary
    print("\n[5/5] Generating daily CSV summary...")
    generar_resumen_diario()
    print("✔ Daily CSV summary generated")
    
    fin = datetime.now()
    duracion = fin - inicio

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"End time: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duracion}")
    print("=" * 70)


if __name__ == "__main__":
    main()
