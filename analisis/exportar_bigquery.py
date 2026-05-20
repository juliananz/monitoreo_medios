"""
Re-export SQLite source tables to BigQuery and refresh dbt marts.

The dbt project at config.settings.DBT_PROJECT_DIR sources from raw tables in
BigQuery (see its sources.yml). This module truncates and reloads those tables
from SQLite each run, then triggers `dbt build` to rebuild the mart_* tables
that the dashboard reads.

Auth: prefers the service-account keyfile at GOOGLE_APPLICATION_CREDENTIALS,
falling back to the keyfile shared with the dbt profile under .dbt/.
"""

import logging
import os
import subprocess

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from analisis.utils import get_db_connection
from config.settings import BASE_DIR, BQ_DATASET, BQ_PROJECT, DBT_EXECUTABLE, DBT_PROJECT_DIR

logger = logging.getLogger(__name__)

# Same keyfile that the dbt profile uses, so both paths authenticate identically.
_FALLBACK_KEYFILE = BASE_DIR / ".dbt" / "monitoreo-medios-489503-524ea4b1fd03.json"


def _bq_client() -> bigquery.Client:
    """Build a BigQuery client from an explicit service-account keyfile."""
    keyfile = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or str(_FALLBACK_KEYFILE)
    if not os.path.exists(keyfile):
        raise FileNotFoundError(
            f"BigQuery service-account keyfile not found at {keyfile!r}. "
            "Set GOOGLE_APPLICATION_CREDENTIALS or place the keyfile at "
            f"{_FALLBACK_KEYFILE}."
        )
    credentials = service_account.Credentials.from_service_account_file(keyfile)
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)

# Tables that dbt sources expect, plus the columns to parse as datetimes so
# BigQuery autodetect maps them to TIMESTAMP/DATE instead of STRING.
TABLES: dict[str, list[str]] = {
    "noticias": ["fecha", "fecha_scraping"],
    "temas": ["fecha_creacion"],
    "regiones": [],
    "entidades": ["fecha_creacion", "ultima_mencion"],
    "noticia_tema": ["fecha_asignacion"],
    "noticia_entidad": ["fecha_asignacion"],
}


def exportar_a_bigquery() -> None:
    """Truncate-and-replace each source table in BigQuery from SQLite."""
    client = _bq_client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    with get_db_connection() as conn:
        for table, date_cols in TABLES.items():
            df = pd.read_sql_query(
                f"SELECT * FROM {table}",
                conn,
                parse_dates=date_cols or None,
            )
            destination = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
            load_job = client.load_table_from_dataframe(
                df, destination, job_config=job_config
            )
            load_job.result()
            logger.info(f"Loaded {len(df):,} rows -> {destination}")


def run_dbt() -> None:
    """Invoke `dbt build` in the sibling dbt project.

    Uses the dbt binary at config.settings.DBT_EXECUTABLE (defaults to `dbt`
    on PATH; set DBT_EXECUTABLE env var to a full path if dbt lives inside a
    venv of the sibling project).
    """
    cmd = [DBT_EXECUTABLE, "build", "--project-dir", str(DBT_PROJECT_DIR)]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.stdout:
        logger.info(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            logger.error(result.stderr)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")
