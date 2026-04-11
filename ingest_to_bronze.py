import argparse
import logging
import os
import requests
import datetime
import sys
import polars as pl

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Constants and Configuration
# Inside Docker/Airflow the env var BRONZE_DB_URI points to the container host;
# locally it falls back to localhost.
DB_URI = os.environ.get(
    "BRONZE_DB_URI",
    "postgresql://myuser:mypassword@localhost:5432/mydatabase"
)

URLS = {
    "ppd_full": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
    "ppd_incremental": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
}

PPD_COLUMNS = [
    "transaction_id", "price", "transfer_date", "postcode", "property_type", 
    "new_build", "tenure", "paon", "saon", "street", "locality", "town", 
    "district", "county", "category_type", "record_status"
]

def check_url(url: str) -> bool:
    """Basic check to see if the URL is reachable (uses head request)."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"URL unreachable {url}: {e}")
        return False

def write_to_postgres(df: pl.DataFrame, table_name: str, mode: str):
    """
    Writes a Polars DataFrame to PostgreSQL.
    mode="replace" will truncate and load.
    mode="append" will append data.
    """
    write_mode = "replace" if mode == "full" else "append"
    logger.info(f"Writing {len(df)} rows to {table_name} (mode: {write_mode})...")
    
    # Using adbc for highest speed write via Polars
    try:
        df.write_database(
            table_name=f"raw.{table_name}",
            connection=DB_URI,
            if_table_exists=write_mode,
            engine="adbc"
        )
        logger.info(f"Successfully wrote {table_name} to database.")
    except Exception as e:
        logger.error(f"Failed to write to database for {table_name}: {e}")
        raise

def process_ppd(mode: str):
    """Process Price Paid Data (PPD)."""
    logger.info("--- Processing PPD Data ---")
    url = URLS["ppd_full"] if mode == "full" else URLS["ppd_incremental"]
    
    if not check_url(url):
        logger.warning("PPD URL check failed. Attempting to download via Polars anyways...")

    logger.info(f"Downloading and reading PPD data from {url}...")
    start_time = datetime.datetime.now()
    
    # Read CSV lazily using Polars
    try:
        # PPD has no headers, we specify them manually
        df = pl.read_csv(
            url, 
            has_header=False,
            new_columns=PPD_COLUMNS,
            ignore_errors=True,
            schema_overrides={"new_build": pl.Utf8} # Initially parse as string to avoid bool parsing issues, cast later
        )
        
        # Determine boolean casting for "new_build": 'Y' -> True, 'N' -> False
        df = df.with_columns(
            pl.col("new_build").map_elements(lambda x: True if x == 'Y' else False, return_dtype=pl.Boolean).alias("new_build"),
            pl.lit(datetime.datetime.now()).alias("_ingested_at"),
            pl.lit(mode).alias("_source_mode")
        )
        
        read_time = datetime.datetime.now() - start_time
        logger.info(f"Read {len(df)} rows in {read_time.total_seconds():.2f} seconds.")
        
        write_to_postgres(df, "ppd", mode)
    except Exception as e:
        logger.error(f"Error processing PPD data: {e}")

def main():
    parser = argparse.ArgumentParser(description="Automated Ingestion for Bronze Layer (Polars)")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["full", "incremental"], 
        required=True,
        help="Mode of ingestion: 'full' truncates and loads everything, 'incremental' appends new data."
    )
    args = parser.parse_args()

    mode = args.mode
    logger.info(f"Starting Bronze Ingestion Pipeline - Mode: {mode.upper()}")
    
    # 1. Process Transactional Data (PPD)
    process_ppd(mode)
    
    # Note: Reference Data (ONSPD, RUC, IMD) is now directly handled by 
    # the Airflow DAG's Python logic (bgd_pipeline.py) natively pulling 
    # from the Data/landing/ directory.
    
    logger.info("Bronze Ingestion Pipeline Completed Successfully.")

if __name__ == "__main__":
    main()
