"""
PPD Kafka Consumer → Bronze Layer
==================================
Subscribes to the Kafka topic `ppd.raw`, buffers incoming JSON messages into
a Polars DataFrame, and bulk-inserts them into PostgreSQL `raw.ppd` via ADBC
(Arrow Database Connectivity) once the buffer reaches a configurable threshold.

Kafka offsets are committed only **after** a successful DB write, providing
at-least-once delivery semantics (duplicates in Bronze are handled by the dbt
Silver incremental model's MD5 hash key).

Usage:
  python kafka/ppd_consumer.py --mode incremental

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default: kafka:9092
  KAFKA_TOPIC_PPD           default: ppd.raw
  KAFKA_GROUP_ID            default: bgd-bronze-consumer
  CONSUMER_BUFFER_SIZE      default: 50000  (rows per DB write)
  BRONZE_DB_URI             default: postgresql://myuser:mypassword@postgres_db:5432/mydatabase
  CONSUMER_POLL_TIMEOUT_MS  default: 5000   (ms to wait for new messages before exiting)
  CONSUMER_MAX_IDLE_POLLS   default: 3      (consecutive empty polls before clean exit)
"""

import json
import logging
import os
import sys
import datetime

import polars as pl
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s – %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC              = os.environ.get("KAFKA_TOPIC_PPD", "ppd.raw")
GROUP_ID           = os.environ.get("KAFKA_GROUP_ID", "bgd-bronze-consumer")
BUFFER_SIZE        = int(os.environ.get("CONSUMER_BUFFER_SIZE", "50000"))
DB_URI             = os.environ.get(
    "BRONZE_DB_URI",
    "postgresql://myuser:mypassword@postgres_db:5432/mydatabase",
)
POLL_TIMEOUT_MS    = int(os.environ.get("CONSUMER_POLL_TIMEOUT_MS", "5000"))
MAX_IDLE_POLLS     = int(os.environ.get("CONSUMER_MAX_IDLE_POLLS", "3"))

PPD_COLUMNS = [
    "transaction_id", "price", "transfer_date", "postcode", "property_type",
    "new_build", "tenure", "paon", "saon", "street", "locality", "town",
    "district", "county", "category_type", "record_status",
    "_ingested_at", "_source_mode",
]


def flush_buffer(buffer: list[dict], write_mode: str) -> None:
    """Bulk-write a list of row dicts to raw.ppd via psycopg2 upsert.

    Uses INSERT ... ON CONFLICT (transaction_id) DO UPDATE to handle
    at-least-once Kafka delivery without crashing on duplicate primary keys.
    """
    if not buffer:
        return

    import psycopg2
    from psycopg2.extras import execute_values

    df = pl.DataFrame(buffer)

    # Ensure all expected columns exist even if some messages were sparse
    for col in PPD_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    df = df.select(PPD_COLUMNS)

    # Cast to correct types to match the Postgres raw.ppd schema
    df = df.with_columns([
        pl.col("price").cast(pl.Decimal(precision=12, scale=2)),
        pl.col("transfer_date").str.strptime(pl.Date, "%Y-%m-%d %H:%M", strict=False),
        pl.col("new_build").map_elements(
            lambda v: True if str(v).lower() == "true" else False,
            return_dtype=pl.Boolean,
        ),
        pl.col("_ingested_at").str.strptime(
            pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False
        ),
    ])

    logger.info(f"Writing buffer of {len(df):,} rows → raw.ppd (mode={write_mode}) …")
    start = datetime.datetime.utcnow()

    # Build upsert SQL: update all non-key columns if transaction_id already exists
    upsert_sql = """
        INSERT INTO raw.ppd (
            transaction_id, price, transfer_date, postcode, property_type,
            new_build, tenure, paon, saon, street, locality, town,
            district, county, category_type, record_status,
            _ingested_at, _source_mode
        )
        VALUES %s
        ON CONFLICT (transaction_id) DO UPDATE SET
            price          = EXCLUDED.price,
            transfer_date  = EXCLUDED.transfer_date,
            postcode       = EXCLUDED.postcode,
            property_type  = EXCLUDED.property_type,
            new_build      = EXCLUDED.new_build,
            tenure         = EXCLUDED.tenure,
            _ingested_at   = EXCLUDED._ingested_at,
            _source_mode   = EXCLUDED._source_mode
    """

    rows = [tuple(row) for row in df.iter_rows()]

    conn = psycopg2.connect(DB_URI)
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows)
        conn.commit()
    finally:
        conn.close()

    elapsed = (datetime.datetime.utcnow() - start).total_seconds()
    logger.info(f"  ✓ Flushed {len(df):,} rows in {elapsed:.1f}s")


def consume(mode: str) -> int:
    """Main consumer loop.  Returns total rows written to the database."""
    # The first chunk replaces (or appends) the table; subsequent chunks append.
    first_write = True
    write_mode  = "replace" if mode == "full" else "append"

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",       # Start from the beginning of the topic
        enable_auto_commit=False,           # We commit manually after each DB flush
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )

    buffer: list[dict] = []
    total_written = 0
    idle_polls    = 0

    logger.info(
        f"Consuming from topic '{TOPIC}' (group={GROUP_ID}, buffer={BUFFER_SIZE:,}) …"
    )

    try:
        for message in consumer:
            idle_polls = 0                  # Reset because we received something
            buffer.append(message.value)

            if len(buffer) >= BUFFER_SIZE:
                flush_buffer(buffer, write_mode if first_write else "append")
                consumer.commit()
                total_written += len(buffer)
                buffer.clear()
                first_write = False

    except StopIteration:
        # `consumer_timeout_ms` elapsed with no messages — normal exit condition
        idle_polls += 1

    finally:
        # Flush any remaining messages that did not fill a full buffer
        if buffer:
            flush_buffer(buffer, write_mode if first_write else "append")
            consumer.commit()
            total_written += len(buffer)
            buffer.clear()

        consumer.close()

    return total_written


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PPD Kafka Consumer → Bronze Layer")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="'full' replaces raw.ppd; 'incremental' appends to it.",
    )
    args = parser.parse_args()

    logger.info(f"Starting PPD Kafka Consumer — mode={args.mode}, broker={BOOTSTRAP_SERVERS}")

    try:
        total = consume(args.mode)
        logger.info(f"Consumer finished. {total:,} rows written to raw.ppd.")
    except KafkaError as exc:
        logger.error(f"Kafka error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
