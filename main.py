"""
Main pipeline for media monitoring.

Order:
1. Run pending migrations
2. Initialize database (if needed)
3. Scrape RSS sources
4. Thematic classification
5. Named Entity Recognition (NER)
6. Risk vs Opportunity classification
7. Daily outputs
8. Daily aggregations
9. LLM executive summary (optional, requires Ollama)

Usage:
    python main.py           # Run full pipeline
    python main.py --backfill  # Backfill aggregations for all historical data
"""

import logging
import sys
from datetime import datetime

from config.settings import LOG_LEVEL

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Migrations
from migrations.manager import run_pending_migrations

# Storage
from storage.database import crear_base_datos

# Scrapers
from scrapers.scraper_rss import guardar_noticias_rss

# Analysis
from analisis.clasificar_noticias_db import clasificar_noticias
from analisis.ner_entities import ejecutar_ner
from analisis.clasificar_riesgo_oportunidad_db import clasificar_riesgo_oportunidad_db
from analisis.resumen_diario_csv import generar_resumen_diario
from analisis.agregacion import ejecutar_agregaciones, backfill_agregaciones
from analisis.resumen_diario_llm import generar_resumen_diario_llm


def main():
    inicio = datetime.now()

    logger.info("=" * 60)
    logger.info("MEDIA MONITORING PIPELINE")
    logger.info(f"Start time: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 1. Run pending migrations
    logger.info("[1/9] Checking for pending migrations...")
    run_pending_migrations()

    # 2. Database
    logger.info("[2/9] Initializing database...")
    crear_base_datos()

    # 3. RSS scraping
    logger.info("[3/9] Scraping RSS sources...")
    guardar_noticias_rss()

    # 4. Thematic classification
    logger.info("[4/9] Running thematic classification...")
    clasificar_noticias()

    # 5. Named Entity Recognition (NER)
    logger.info("[5/9] Running NER (entity extraction)...")
    ejecutar_ner()

    # 6. Risk vs Opportunity
    logger.info("[6/9] Evaluating risk vs opportunity...")
    clasificar_riesgo_oportunidad_db()

    # 7. Daily CSV summary
    logger.info("[7/9] Generating daily CSV summary...")
    generar_resumen_diario()

    # 8. Daily aggregations
    logger.info("[8/9] Computing daily aggregations...")
    ejecutar_agregaciones()

    # 9. LLM executive summary (optional)
    logger.info("[9/9] Generating LLM executive summary...")
    try:
        result = generar_resumen_diario_llm()
        if result:
            logger.info(f"LLM summary saved to: {result}")
        else:
            logger.warning("LLM summary generation failed (no data or LLM error)")
    except Exception as e:
        logger.warning(f"LLM summary skipped: {e}")

    fin = datetime.now()
    duracion = fin - inicio

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"End time: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {duracion}")
    logger.info("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        logger.info("=" * 60)
        logger.info("AGGREGATION BACKFILL")
        logger.info("=" * 60)

        # Run migrations first to ensure tables exist
        run_pending_migrations()

        # Backfill all historical aggregations
        backfill_agregaciones()

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETED")
        logger.info("=" * 60)
    else:
        main()
