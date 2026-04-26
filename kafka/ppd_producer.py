"""
PPD Kafka Producer
==================
Downloads UK Price Paid Data from the Land Registry and publishes each row
as a JSON message to the Kafka topic `ppd.raw`.

Supports two modes (matching the existing Polars pipeline convention):
  --mode full         → downloads the full historical CSV (~31M rows)
  --mode incremental  → downloads the monthly update CSV (~100k rows)

Usage (inside the Airflow container or standalone):
  python kafka/ppd_producer.py --mode incremental

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default: kafka:9092
  KAFKA_TOPIC_PPD           default: ppd.raw
  KAFKA_BATCH_SIZE          default: 5000  (rows published per send flush)
"""

import argparse
import datetime
import json
import logging
import os
import sys

import polars as pl
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s – %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC             = os.environ.get("KAFKA_TOPIC_PPD", "ppd.raw")
BATCH_SIZE        = int(os.environ.get("KAFKA_BATCH_SIZE", "5000"))

URLS = {
    "full":        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
    "incremental": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv",
}

PPD_COLUMNS = [
    "transaction_id", "price", "transfer_date", "postcode", "property_type",
    "new_build", "tenure", "paon", "saon", "street", "locality", "town",
    "district", "county", "category_type", "record_status",
]


def build_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",            # Wait for all in-sync replicas to confirm
        retries=5,
        linger_ms=20,          # Small batching window for throughput
        compression_type="gzip",
    )


def publish(producer: KafkaProducer, mode: str) -> int:
    """Download PPD CSV and publish every row to the Kafka topic.

    Returns the total number of messages published.
    """
    url = URLS[mode]
    logger.info(f"Downloading PPD ({mode}) from {url} …")
    ingested_at = datetime.datetime.utcnow().isoformat()

    # Polars streams the CSV without loading it all into RAM at once
    df = pl.read_csv(
        url,
        has_header=False,
        new_columns=PPD_COLUMNS,
        ignore_errors=True,
        schema_overrides={"new_build": pl.Utf8},
    )

    # Normalise new_build Y/N → boolean string so JSON stays portable
    df = df.with_columns(
        pl.col("new_build").map_elements(
            lambda x: "true" if x == "Y" else "false",
            return_dtype=pl.Utf8,
        ).alias("new_build"),
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(mode).alias("_source_mode"),
    )

    total = len(df)
    published = 0

    logger.info(f"Publishing {total:,} rows → topic '{TOPIC}' (batch_size={BATCH_SIZE:,}) …")

    for chunk_start in range(0, total, BATCH_SIZE):
        chunk = df.slice(chunk_start, BATCH_SIZE)
        for row in chunk.iter_rows(named=True):
            producer.send(TOPIC, value=row)
        producer.flush()          # Ensure the batch is sent before moving on
        published += len(chunk)
        logger.info(f"  Published {published:,}/{total:,} rows …")

    logger.info(f"✓ All {published:,} rows published to '{TOPIC}'.")
    return published


def main():
    parser = argparse.ArgumentParser(description="PPD Kafka Producer")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="'full' publishes the entire historical dataset; 'incremental' publishes the monthly update.",
    )
    args = parser.parse_args()

    logger.info(f"Starting PPD Kafka Producer — mode={args.mode}, broker={BOOTSTRAP_SERVERS}")
    producer = build_producer()
    try:
        count = publish(producer, args.mode)
        logger.info(f"Producer finished. {count:,} messages on topic '{TOPIC}'.")
    except KafkaError as exc:
        logger.error(f"Kafka error: {exc}")
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
