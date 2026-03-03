"""
RSS Source Validation Script

Use this script to validate RSS sources before adding them to production.
Does NOT modify the database or pipeline - read-only validation.

Usage:
    python scripts/validate_sources.py                    # Validate all sources
    python scripts/validate_sources.py --url URL          # Test single URL
    python scripts/validate_sources.py --tipo regional    # Filter by type
    python scripts/validate_sources.py --region norte     # Filter by region
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
import feedparser
from datetime import datetime, timedelta
from config.settings import FUENTES_PATH


def validate_feed(url: str, nombre: str = "Test") -> dict:
    """
    Validate a single RSS feed and return quality metrics.
    """
    result = {
        "nombre": nombre,
        "url": url,
        "status": "unknown",
        "entries_count": 0,
        "recent_entries": 0,  # Last 7 days
        "has_titles": False,
        "has_dates": False,
        "has_descriptions": False,
        "sample_titles": [],
        "errors": []
    }

    try:
        feed = feedparser.parse(url, agent="Mozilla/5.0")

        # Check HTTP status
        if hasattr(feed, 'status'):
            if feed.status >= 400:
                result["status"] = f"http_error_{feed.status}"
                result["errors"].append(f"HTTP {feed.status}")
                return result

        # Check for malformed feed
        if feed.bozo and not feed.entries:
            result["status"] = "malformed"
            result["errors"].append(f"Parse error: {feed.bozo_exception}")
            return result

        if not feed.entries:
            result["status"] = "empty"
            result["errors"].append("No entries found")
            return result

        result["entries_count"] = len(feed.entries)
        result["status"] = "ok"

        # Analyze entries
        now = datetime.now()
        week_ago = now - timedelta(days=7)

        titles_present = 0
        dates_present = 0
        descriptions_present = 0
        recent_count = 0

        for entry in feed.entries[:20]:  # Sample first 20
            if entry.get("title", "").strip():
                titles_present += 1
                if len(result["sample_titles"]) < 3:
                    result["sample_titles"].append(entry["title"][:80])

            if entry.get("published") or entry.get("updated"):
                dates_present += 1
                try:
                    from email.utils import parsedate_to_datetime
                    date_str = entry.get("published") or entry.get("updated")
                    entry_date = parsedate_to_datetime(date_str)
                    if entry_date.replace(tzinfo=None) > week_ago:
                        recent_count += 1
                except:
                    pass

            if entry.get("summary", "").strip() or entry.get("description", "").strip():
                descriptions_present += 1

        sample_size = min(20, len(feed.entries))
        result["has_titles"] = titles_present >= sample_size * 0.8
        result["has_dates"] = dates_present >= sample_size * 0.5
        result["has_descriptions"] = descriptions_present >= sample_size * 0.5
        result["recent_entries"] = recent_count

        # Quality warnings
        if not result["has_titles"]:
            result["errors"].append("Missing titles in >20% entries")
        if not result["has_dates"]:
            result["errors"].append("Missing dates in >50% entries")
        if recent_count == 0:
            result["errors"].append("No entries from last 7 days")

    except Exception as e:
        result["status"] = "error"
        result["errors"].append(str(e))

    return result


def load_sources(tipo: str = None, region: str = None) -> list:
    """Load sources from YAML, optionally filtered."""
    with open(FUENTES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sources = config.get("rss", [])

    if tipo:
        sources = [s for s in sources if s.get("tipo") == tipo]
    if region:
        sources = [s for s in sources if s.get("region") == region]

    return sources


def print_result(result: dict, verbose: bool = False):
    """Print validation result."""
    status_icons = {
        "ok": "✓",
        "empty": "○",
        "malformed": "✗",
        "error": "✗",
        "unknown": "?"
    }
    icon = status_icons.get(result["status"], "✗" if "http_error" in result["status"] else "?")

    quality = ""
    if result["status"] == "ok":
        quality_score = sum([
            result["has_titles"],
            result["has_dates"],
            result["has_descriptions"],
            result["recent_entries"] > 0
        ])
        quality = f"[{'★' * quality_score}{'☆' * (4 - quality_score)}]"

    print(f"{icon} {result['nombre']:<30} {quality}")
    print(f"  URL: {result['url']}")
    print(f"  Status: {result['status']} | Entries: {result['entries_count']} | Recent (7d): {result['recent_entries']}")

    if result["errors"]:
        print(f"  Warnings: {', '.join(result['errors'])}")

    if verbose and result["sample_titles"]:
        print("  Sample titles:")
        for title in result["sample_titles"]:
            print(f"    - {title}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Validate RSS sources")
    parser.add_argument("--url", help="Test a single URL")
    parser.add_argument("--nombre", default="Test", help="Name for single URL test")
    parser.add_argument("--tipo", help="Filter by type (nacional, regional, industria, gobierno, agencia)")
    parser.add_argument("--region", help="Filter by region (nacional, norte, centro, sur, bajio, occidente)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample titles")
    args = parser.parse_args()

    if args.url:
        # Single URL validation
        print(f"\nValidating: {args.url}\n")
        result = validate_feed(args.url, args.nombre)
        print_result(result, verbose=True)
        return

    # Validate configured sources
    sources = load_sources(tipo=args.tipo, region=args.region)

    if not sources:
        print("No sources found matching criteria.")
        return

    print(f"\n{'='*60}")
    print(f"RSS Source Validation Report")
    print(f"{'='*60}")
    print(f"Sources: {len(sources)}")
    if args.tipo:
        print(f"Type filter: {args.tipo}")
    if args.region:
        print(f"Region filter: {args.region}")
    print(f"{'='*60}\n")

    stats = {"ok": 0, "warning": 0, "failed": 0}

    for source in sources:
        nombre = source.get("nombre", "Unknown")
        url = source.get("url", "")

        if not url:
            continue

        result = validate_feed(url, nombre)
        print_result(result, verbose=args.verbose)

        if result["status"] == "ok" and not result["errors"]:
            stats["ok"] += 1
        elif result["status"] == "ok":
            stats["warning"] += 1
        else:
            stats["failed"] += 1

    print(f"{'='*60}")
    print(f"Summary: {stats['ok']} OK | {stats['warning']} Warnings | {stats['failed']} Failed")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
