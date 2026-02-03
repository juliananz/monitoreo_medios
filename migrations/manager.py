"""
Migration manager for tracking and applying database migrations.
"""

import sqlite3
import logging
import importlib
from pathlib import Path

from config.settings import DB_PATH

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent


def init_migrations_table():
    """Create migrations tracking table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS migrations_applied (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_name TEXT UNIQUE NOT NULL,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    conn.close()


def get_applied_migrations() -> set:
    """Get set of migration names that have been applied."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT migration_name FROM migrations_applied")
        applied = {row[0] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        applied = set()

    conn.close()
    return applied


def mark_migration_applied(migration_name: str):
    """Record that a migration has been applied."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO migrations_applied (migration_name) VALUES (?)",
        (migration_name,)
    )

    conn.commit()
    conn.close()


def get_pending_migrations() -> list:
    """Get list of migration files that haven't been applied yet."""
    applied = get_applied_migrations()

    # Find all migration files (001_*.py, 002_*.py, etc.)
    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))

    pending = []
    for mig_file in migration_files:
        mig_name = mig_file.stem  # e.g., "001_add_ner_columns"
        if mig_name not in applied:
            pending.append(mig_file)

    return pending


def run_pending_migrations():
    """Run all pending migrations in order."""
    # Ensure tracking table exists
    init_migrations_table()

    pending = get_pending_migrations()

    if not pending:
        logger.info("No pending migrations.")
        return

    logger.info(f"Found {len(pending)} pending migration(s).")

    for mig_file in pending:
        mig_name = mig_file.stem
        logger.info(f"Applying migration: {mig_name}")

        try:
            # Import and run the migration
            spec = importlib.util.spec_from_file_location(mig_name, mig_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, "run_migration"):
                module.run_migration()
            else:
                logger.warning(f"Migration {mig_name} has no run_migration() function")
                continue

            # Mark as applied
            mark_migration_applied(mig_name)
            logger.info(f"Migration {mig_name} applied successfully.")

        except Exception as e:
            logger.error(f"Failed to apply migration {mig_name}: {e}")
            raise


def migration_status():
    """Print status of all migrations."""
    init_migrations_table()
    applied = get_applied_migrations()

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py"))

    print("\nMigration Status:")
    print("-" * 50)

    for mig_file in migration_files:
        mig_name = mig_file.stem
        status = "APPLIED" if mig_name in applied else "PENDING"
        print(f"  [{status}] {mig_name}")

    print("-" * 50)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    migration_status()
