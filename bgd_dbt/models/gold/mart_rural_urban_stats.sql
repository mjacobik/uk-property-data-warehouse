{{ config(
    materialized='table'
) }}

WITH facts AS (
    SELECT * FROM {{ ref('fact_sales') }}
),

dim_geo AS (
    SELECT * FROM {{ ref('dim_geography') }}
)

SELECT 
    -- Wymiary (grupowanie)
    EXTRACT(YEAR FROM f.transfer_date) AS sale_year,
    g.urban_rural_flag,
    f.property_type,
    f.new_build,
    
    -- Agregacje
    COUNT(f.transaction_id) AS total_transactions,
    SUM(f.price) AS total_sales_volume,
    AVG(f.price) AS average_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price

FROM facts f
LEFT JOIN dim_geo g 
    ON f.normalized_postcode = g.normalized_postcode
GROUP BY 
    sale_year,
    g.urban_rural_flag,
    f.property_type,
    f.new_build
