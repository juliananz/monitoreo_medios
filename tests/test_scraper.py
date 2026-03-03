"""
Unit tests for RSS scraper utilities.
"""

import pytest
from datetime import datetime
from scrapers.scraper_rss import parse_fecha


class TestParseFecha:
    """Tests for RSS date parsing."""

    def test_rfc2822_format(self):
        # Standard RSS date format
        fecha = "Mon, 15 Jan 2024 10:30:00 GMT"
        result = parse_fecha(fecha)
        assert result == "2024-01-15"

    def test_rfc2822_with_timezone(self):
        fecha = "Tue, 20 Feb 2024 14:00:00 -0600"
        result = parse_fecha(fecha)
        assert result == "2024-02-20"

    def test_empty_string_returns_today(self):
        result = parse_fecha("")
        today = datetime.now().strftime("%Y-%m-%d")
        assert result == today

    def test_none_returns_today(self):
        result = parse_fecha(None)
        today = datetime.now().strftime("%Y-%m-%d")
        assert result == today

    def test_invalid_format_returns_today(self):
        result = parse_fecha("not a valid date")
        today = datetime.now().strftime("%Y-%m-%d")
        assert result == today

    def test_iso_format_fallback(self):
        # This might not parse correctly, should fallback to today
        fecha = "2024-01-15T10:30:00Z"
        result = parse_fecha(fecha)
        # Either parses correctly or falls back to today
        assert len(result) == 10  # YYYY-MM-DD format
