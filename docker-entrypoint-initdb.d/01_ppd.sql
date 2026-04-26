create table if not exists raw.ppd (
  transaction_id text primary key,
  price numeric(12,2),
  transfer_date date,
  postcode text,
  property_type char(1),       -- D/S/T/F/O
  new_build boolean,
  tenure char(1),              -- F/L
  paon text, saon text, street text, locality text, town text, district text, county text,
  category_type char(1),
  record_status char(1),
  _ingested_at  timestamptz,   -- set by Polars/Kafka consumer at ingestion time
  _source_mode  text           -- 'full' or 'incremental'
);

TRUNCATE TABLE raw.ppd;
-- Explicitly list the 16 source columns so the CSV import ignores the metadata columns
COPY raw.ppd (transaction_id, price, transfer_date, postcode, property_type,
              new_build, tenure, paon, saon, street, locality, town,
              district, county, category_type, record_status)
FROM '/Data/pp-complete.csv' CSV HEADER;
