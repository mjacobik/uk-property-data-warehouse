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
  record_status char(1)
);

TRUNCATE TABLE raw.ppd;
COPY raw.ppd FROM '/Data/pp-complete.csv' CSV HEADER;
