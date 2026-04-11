{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_sales_txn ON {{ this }} (transaction_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON {{ this }} (date_key)"
    ]
) }}

WITH source_sales AS (
    SELECT * FROM {{ ref('silver_ppd') }}
    {% if is_incremental() %}
    WHERE dbt_updated_at > (SELECT COALESCE(MAX(silver_updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT 
    transaction_id,
    price,
    
    -- Time key
    CAST(TO_CHAR(transfer_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    
    -- Dimension keys
    normalized_postcode,
    
    MD5(
        COALESCE(property_type, 'NULL') || '|' || 
        COALESCE(CAST(new_build AS TEXT), 'NULL') || '|' || 
        COALESCE(tenure, 'NULL') || '|' || 
        COALESCE(category_type, 'NULL')
    ) AS property_key,

    MD5(
        COALESCE(paon, 'NULL') || '|' || 
        COALESCE(saon, 'NULL') || '|' || 
        COALESCE(street, 'NULL') || '|' || 
        COALESCE(locality, 'NULL') || '|' || 
        COALESCE(town, 'NULL') || '|' || 
        COALESCE(district, 'NULL') || '|' || 
        COALESCE(county, 'NULL')
    ) AS location_key,
    
    dbt_updated_at AS silver_updated_at,
    CURRENT_TIMESTAMP AS gold_updated_at
FROM source_sales

{% if is_incremental() %}
    WHERE dbt_updated_at >= (SELECT COALESCE(MAX(silver_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
