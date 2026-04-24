"""
Centralized configuration for the media monitoring system.
All paths and shared settings are defined here.

Supports .env file for environment-specific configuration.
"""

import os
from pathlib import Path

# HF_HOME supersedes TRANSFORMERS_CACHE since transformers v5
os.environ.pop("TRANSFORMERS_CACHE", None)

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass  # python-dotenv not installed, use defaults

# Base directories
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", DATA_DIR / "salidas"))

# Database
DB_PATH = Path(os.getenv("DB_PATH", DATA_DIR / "noticias_medios.db"))

# BigQuery (dashboard data layer)
BQ_PROJECT = os.getenv("BQ_PROJECT", "monitoreo-medios-489503")
BQ_DATASET = os.getenv("BQ_DATASET", "monitoreo_medios")

# Config files
FUENTES_PATH = CONFIG_DIR / "fuentes.yaml"
KEYWORDS_PATH = CONFIG_DIR / "keywords.yaml"

# RSS Scraper settings
RSS_MAX_RETRIES = int(os.getenv("RSS_MAX_RETRIES", "3"))
RSS_RETRY_DELAY = int(os.getenv("RSS_RETRY_DELAY", "5"))

# GROQ API
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
MAX_NER_PER_RUN = int(os.getenv("MAX_NER_PER_RUN", "300"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
