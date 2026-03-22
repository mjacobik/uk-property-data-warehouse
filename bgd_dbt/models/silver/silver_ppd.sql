{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH raw_ppd AS (
    SELECT * FROM {{ source('raw', 'ppd') }}
)

SELECT 
    transaction_id,
    price,
    transfer_date,
    postcode AS raw_postcode,
    REPLACE(UPPER(postcode), ' ', '') AS normalized_postcode,
    
    property_type,
    new_build,
    tenure,
    paon, 
    saon, 
    street, 
    locality, 
    town, 
    district, 
    county,
    category_type,
    record_status,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_ppd

{% if is_incremental() %}

  -- Fetch only records from raw.ppd that are newer than the latest transfer_date in our silver database
  -- OR simply rely on the `unique_key='transaction_id'` mechanism, 
  -- which performs an UPSERT operation on Postgres (updating changed records and inserting new ones).
  
  -- If you want to filter data volume to avoid processing 30M rows, uncomment:
  -- WHERE transfer_date >= (SELECT COALESCE(MAX(transfer_date), '1900-01-01') FROM {{ this }})

{% endif %}
