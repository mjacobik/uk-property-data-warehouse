{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    post_hook="CREATE INDEX IF NOT EXISTS idx_silver_ppd_txn ON {{ this }} (transaction_id)"
) }}

WITH bronze_ppd AS (
    SELECT * FROM {{ source('raw', 'ppd') }}
)

SELECT
    -- Natural primary key assigned by the UK Land Registry
    transaction_id,
    price,
    transfer_date,
    postcode AS raw_postcode,
    REPLACE(UPPER(COALESCE(postcode, '')), ' ', '') AS normalized_postcode,

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
FROM bronze_ppd

{% if is_incremental() %}
  -- Only process rows whose transfer_date is newer than the latest
  -- transfer_date already present in the Silver table.
  WHERE transfer_date > (SELECT COALESCE(MAX(transfer_date), '1900-01-01') FROM {{ this }})
{% endif %}
