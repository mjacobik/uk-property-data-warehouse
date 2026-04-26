"""
BGD Property Data Warehouse – Airflow DAG
==========================================
Orchestrates:
  1. ingest_reference_data  – Polars Truncate & Load reference CSVs into Bronze
  2. produce_ppd_to_kafka   – Downloads PPD from UK Gov and publishes to Kafka topic `ppd.raw`
  3. consume_ppd_from_kafka – Reads from `ppd.raw` and bulk-inserts into Bronze via Polars + ADBC
  4. dbt_run                – dbt run  (all Silver + Gold models)
  5. dbt_test               – dbt test (data quality constraints)

Trigger from Airflow UI or CLI:
  Full Refresh : airflow dags trigger bgd_pipeline --conf '{"ppd_mode":"full"}'
  Incremental  : airflow dags trigger bgd_pipeline --conf '{"ppd_mode":"incremental"}'
  (default is incremental)
"""

from datetime import datetime, timedelta
import os, glob, logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

logger = logging.getLogger(__name__)

# ── Paths (inside the Airflow containers) ───────────────────────────
LANDING_DIR   = "/opt/airflow/landing"
SCRIPT_PATH   = "/opt/airflow/scripts/ingest_to_bronze.py"
DBT_DIR       = "/opt/airflow/dbt"
DBT_PROFILES  = "/opt/airflow/dbt/docker_profiles"
DB_URI        = os.environ.get(
    "BRONZE_DB_URI",
    "postgresql://myuser:mypassword@postgres_db:5432/mydatabase",
)
KAFKA_DIR     = "/opt/airflow/kafka"
KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# ── Reference-file → Bronze-table mapping ───────────────────────────
# Drop a file whose name starts with one of these prefixes into
# data/landing/ and it will be loaded into the corresponding table.
REFERENCE_MAP = {
    "ONSPD":       "onspd",
    "Rural_Urban": "ruc_lsoa_2021",
    "IoD2019":     "imd",
    "File_7":      "imd",
}

# ── DAG default args ────────────────────────────────────────────────
default_args = {
    "owner": "bgd",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ── Python callables ────────────────────────────────────────────────
def ingest_reference_files(**context):
    """Scan landing zone, load each new CSV into Bronze (Truncate & Load)."""
    import polars as pl

    csv_files = glob.glob(os.path.join(LANDING_DIR, "*.csv"))
    if not csv_files:
        logger.info("No CSV files found in landing zone – skipping.")
        return

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        table = None
        for prefix, tbl in REFERENCE_MAP.items():
            if fname.startswith(prefix):
                table = tbl
                break

        if table is None:
            logger.warning(f"Unknown file {fname} – skipping.")
            continue

        logger.info(f"Loading {fname} → raw.{table} (truncate & load)...")
        t0 = datetime.now()

        df = pl.read_csv(fpath, infer_schema_length=10000, ignore_errors=True)
        df = df.with_columns(
            pl.lit(datetime.now()).alias("_ingested_at"),
            pl.lit("full").alias("_source_mode"),
        )

        df.write_database(
            table_name=f"raw.{table}",
            connection=DB_URI,
            if_table_exists="replace",
            engine="adbc",
        )

        elapsed = (datetime.now() - t0).total_seconds()
        logger.info(f"  ✓ {len(df)} rows in {elapsed:.1f}s")

        # Archive the processed file so the sensor doesn't re-trigger
        archive_dir = os.path.join(LANDING_DIR, "processed")
        os.makedirs(archive_dir, exist_ok=True)
        os.rename(fpath, os.path.join(archive_dir, fname))
        logger.info(f"  Archived → landing/processed/{fname}")


def ingest_ppd(**context):
    """Run the Polars PPD ingestion script with the selected mode."""
    import subprocess, sys

    mode = context["dag_run"].conf.get("ppd_mode", "incremental")
    logger.info(f"Ingesting PPD data in '{mode}' mode...")

    result = subprocess.run(
        [sys.executable, SCRIPT_PATH, "--mode", mode],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"PPD ingestion failed (exit {result.returncode})")


# ── DAG definition ──────────────────────────────────────────────────
with DAG(
    dag_id="bgd_pipeline",
    default_args=default_args,
    description="BGD Medallion Pipeline: Landing → Bronze → Silver",
    # UK Land Registry publishes PPD updates around the 20th working day
    # of each month.  Running on the 25th gives a safe buffer.
    schedule_interval="0 6 25 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bgd", "medallion", "dbt"],
) as dag:

    # 1. Load reference CSVs from landing zone → Bronze
    # (If the folder is empty, this task gracefully exits and allows the DAG to continue)
    ingest_ref = PythonOperator(
        task_id="ingest_reference_data",
        python_callable=ingest_reference_files,
    )

    # 2. Publish PPD rows to Kafka topic `ppd.raw`
    produce_ppd = BashOperator(
        task_id="produce_ppd_to_kafka",
        bash_command=(
            "python {{ params.kafka_dir }}/ppd_producer.py "
            "--mode {{ dag_run.conf.get('ppd_mode', 'incremental') }}"
        ),
        params={"kafka_dir": KAFKA_DIR},
        env={**os.environ, "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BROKERS},
    )

    # 3. Consume PPD rows from Kafka and write to Bronze (raw.ppd)
    consume_ppd = BashOperator(
        task_id="consume_ppd_from_kafka",
        bash_command=(
            "python {{ params.kafka_dir }}/ppd_consumer.py "
            "--mode {{ dag_run.conf.get('ppd_mode', 'incremental') }}"
        ),
        params={"kafka_dir": KAFKA_DIR},
        env={**os.environ,
             "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BROKERS,
             "BRONZE_DB_URI": DB_URI},
    )

    # 3. dbt run – All layers (Silver + Gold)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES}"
        ),
    )

    # 4. dbt test – Data quality checks
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES}"
        ),
    )

    # ── Task dependencies ───────────────────────────────────────────
    ingest_ref >> produce_ppd >> consume_ppd >> dbt_run >> dbt_test
