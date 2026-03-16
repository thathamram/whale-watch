"""
🐋 Whale Watch — Dagster Orchestration
Ties together ingestion + dbt into one automated pipeline.
"""
import os
import subprocess
from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..")
INGEST_SCRIPT = os.path.join(PROJECT_DIR, "ingest", "ingest.py")
DBT_PROJECT_DIR = os.path.join(PROJECT_DIR, "transform", "whale_watch")
VENV_PYTHON = os.path.join(PROJECT_DIR, ".venv", "bin", "python3")
DBT_BIN = os.path.join(PROJECT_DIR, ".venv", "bin", "dbt")


@asset(group_name="ingestion")
def raw_whale_transactions():
    """Pull latest whale transactions from Etherscan API into DuckDB."""
    result = subprocess.run(
        [VENV_PYTHON, INGEST_SCRIPT],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")
    return result.stdout


@asset(group_name="transformation", deps=[raw_whale_transactions])
def dbt_models():
    """Run dbt transformations: staging → intermediate → mart."""
    result = subprocess.run(
        [DBT_BIN, "run"],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return result.stdout


@asset(group_name="testing", deps=[dbt_models])
def dbt_tests():
    """Run dbt tests to validate data quality."""
    result = subprocess.run(
        [DBT_BIN, "test"],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed: {result.stderr}")
    return result.stdout


# Define a job that runs the full pipeline
whale_pipeline_job = define_asset_job(
    name="whale_pipeline",
    selection=AssetSelection.all(),
    description="Full whale watch pipeline: ingest → transform → test",
)

# Schedule it every 15 minutes
whale_schedule = ScheduleDefinition(
    job=whale_pipeline_job,
    cron_schedule="*/15 * * * *",  # every 15 minutes
    description="Run whale pipeline every 15 minutes",
)

# Wire it all together
defs = Definitions(
    assets=[raw_whale_transactions, dbt_models, dbt_tests],
    jobs=[whale_pipeline_job],
    schedules=[whale_schedule],
)
