"""
RSS feed scraper for news ingestion with retry logic.
"""

import logging
import time
import yaml
import feedparser
from datetime import datetime
from email.utils import parsedate_to_datetime

from config.settings import FUENTES_PATH, RSS_MAX_RETRIES, RSS_RETRY_DELAY
from analisis.utils import get_db_connection

logger = logging.getLogger(__name__)


def cargar_fuentes():
    """Load RSS sources from YAML config."""
    with open(FUENTES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("rss", [])


def parse_fecha(fecha_str: str) -> str:
    """
    Parse RSS date string to ISO format (YYYY-MM-DD).
    Falls back to current date if parsing fails.
    """
    if not fecha_str:
        return datetime.now().strftime("%Y-%m-%d")

    try:
        # RFC 2822 format (common in RSS)
        dt = parsedate_to_datetime(fecha_str)
        return dt.strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        pass

    # Fallback to current date
    return datetime.now().strftime("%Y-%m-%d")


def fetch_feed_with_retry(rss_url: str, nombre_medio: str):
    """
    Fetch RSS feed with retry logic.
    Returns feed object or None if all retries fail.
    """
    for attempt in range(1, RSS_MAX_RETRIES + 1):
        try:
            feed = feedparser.parse(rss_url)

            # Check for HTTP errors
            if hasattr(feed, 'status') and feed.status >= 400:
                logger.warning(
                    f"HTTP {feed.status} for {nombre_medio} (attempt {attempt}/{RSS_MAX_RETRIES})"
                )
                if attempt < RSS_MAX_RETRIES:
                    time.sleep(RSS_RETRY_DELAY)
                    continue
                return None

            # Check for bozo (malformed feed)
            if feed.bozo and not feed.entries:
                logger.warning(
                    f"Malformed feed for {nombre_medio} (attempt {attempt}/{RSS_MAX_RETRIES})"
                )
                if attempt < RSS_MAX_RETRIES:
                    time.sleep(RSS_RETRY_DELAY)
                    continue
                return None

            return feed

        except Exception as e:
            logger.warning(
                f"Error fetching {nombre_medio} (attempt {attempt}/{RSS_MAX_RETRIES}): {e}"
            )
            if attempt < RSS_MAX_RETRIES:
                time.sleep(RSS_RETRY_DELAY)
            else:
                return None

    return None


def guardar_noticias_rss():
    """Scrape all configured RSS feeds and store news in database."""
    fuentes = cargar_fuentes()

    if not fuentes:
        logger.warning("No RSS sources configured.")
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        total_inserted = 0
        failed_sources = []

        for fuente in fuentes:
            nombre_medio = fuente.get("nombre")
            rss_url = fuente.get("url")

            if not nombre_medio or not rss_url:
                continue

            logger.info(f"Processing RSS: {nombre_medio}")

            feed = fetch_feed_with_retry(rss_url, nombre_medio)

            if feed is None:
                failed_sources.append(nombre_medio)
                logger.error(f"Failed to fetch: {nombre_medio} (after {RSS_MAX_RETRIES} attempts)")
                continue

            if not feed.entries:
                logger.warning(f"No entries: {nombre_medio}")
                continue

            source_inserted = 0
            for entry in feed.entries:
                try:
                    titulo = entry.get("title", "").strip()
                    url = entry.get("link", "").strip()
                    descripcion = entry.get("summary", "") or entry.get("description", "")
                    fecha_raw = entry.get("published", None)
                    fecha = parse_fecha(fecha_raw)

                    if not titulo or not url:
                        continue

                    cursor.execute("""
                        INSERT OR IGNORE INTO noticias
                        (titulo, descripcion, url, fecha, medio, fecha_scraping)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """, (titulo, descripcion, url, fecha, nombre_medio))

                    if cursor.rowcount > 0:
                        source_inserted += 1
                        total_inserted += 1

                except Exception as e:
                    logger.error(f"Error processing entry ({nombre_medio}): {e}")
                    continue

            logger.info(f"  {nombre_medio}: {source_inserted} new entries")

        conn.commit()

    logger.info(f"RSS scraping completed. Total new entries: {total_inserted}")

    if failed_sources:
        logger.warning(f"Failed sources ({len(failed_sources)}): {', '.join(failed_sources)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    guardar_noticias_rss()
