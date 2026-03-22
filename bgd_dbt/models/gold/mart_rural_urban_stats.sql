{{ config(
    materialized='table'
) }}

WITH facts AS (
    SELECT * FROM {{ ref('fact_sales') }}
),

dim_geo AS (
    SELECT * FROM {{ ref('dim_geography') }}
),

dim_prop AS (
    SELECT * FROM {{ ref('dim_property') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT 
    d.year AS sale_year,
    g.urban_rural_flag,
    p.property_type_desc AS property_type,
    p.new_build,
    
    COUNT(f.transaction_id) AS total_transactions,
    SUM(f.price) AS total_sales_volume,
    AVG(f.price) AS average_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price,
    
    -- New measures from IMD
    AVG(g.imd_score) AS avg_imd_score,
    AVG(g.crime_score) AS avg_crime_score,
    AVG(g.education_score) AS avg_education_score

FROM facts f
LEFT JOIN dim_date d
    ON f.date_key = d.date_key
LEFT JOIN dim_geo g 
    ON f.normalized_postcode = g.normalized_postcode
    AND d.full_date >= g.valid_from 
    AND d.full_date <= g.valid_to
LEFT JOIN dim_prop p
    ON f.property_key = p.property_key
GROUP BY 
    d.year,
    g.urban_rural_flag,
    p.property_type_desc,
    p.new_build
