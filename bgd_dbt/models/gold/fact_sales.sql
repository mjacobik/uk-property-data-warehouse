{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH source_sales AS (
    SELECT * FROM {{ ref('silver_ppd') }}
)

SELECT 
    transaction_id,
    -- Fakty
    price,
    transfer_date,
    
    -- Klucze wymiarów
    normalized_postcode,
    
    -- Atrybuty transakcji
    property_type,
    new_build,
    tenure,
    category_type,
    
    -- Metadane
    dbt_updated_at AS silver_updated_at,
    CURRENT_TIMESTAMP AS gold_updated_at
FROM source_sales

{% if is_incremental() %}
    -- Jeżeli ładujemy przyrostowo, filtrujemy to, co jest nowe w silver_ppd
    WHERE dbt_updated_at >= (SELECT COALESCE(MAX(silver_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
