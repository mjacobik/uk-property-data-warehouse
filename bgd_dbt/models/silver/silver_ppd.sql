{{ config(
    materialized='incremental',
    unique_key='ppd_hash_key'
) }}

WITH bronze_ppd AS (
    SELECT * FROM {{ source('raw', 'ppd') }}
)

SELECT
    -- Composite surrogate key to prevent duplicates
    MD5(
        transaction_id || '|' ||
        COALESCE(CAST(price AS TEXT), '') || '|' ||
        COALESCE(CAST(transfer_date AS TEXT), '')
    ) AS ppd_hash_key,

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
