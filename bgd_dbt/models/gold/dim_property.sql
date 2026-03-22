{{ config(
    materialized='table',
    unique_key='property_key'
) }}

WITH source_sales AS (
    SELECT * FROM {{ ref('silver_ppd') }}
),

unique_properties AS (
    SELECT DISTINCT
        property_type,
        new_build,
        tenure,
        category_type
    FROM source_sales
)

SELECT
    MD5(
        COALESCE(property_type, 'NULL') || '|' || 
        COALESCE(CAST(new_build AS TEXT), 'NULL') || '|' || 
        COALESCE(tenure, 'NULL') || '|' || 
        COALESCE(category_type, 'NULL')
    ) AS property_key,
    
    property_type,
    CASE property_type
        WHEN 'D' THEN 'Detached'
        WHEN 'S' THEN 'Semi-Detached'
        WHEN 'T' THEN 'Terraced'
        WHEN 'F' THEN 'Flats/Maisonettes'
        WHEN 'O' THEN 'Other'
        ELSE 'Unknown'
    END AS property_type_desc,

    new_build,
    
    tenure,
    CASE tenure
        WHEN 'F' THEN 'Freehold'
        WHEN 'L' THEN 'Leasehold'
        ELSE 'Unknown'
    END AS tenure_desc,

    category_type,
    CASE category_type
        WHEN 'A' THEN 'Standard Price Paid'
        WHEN 'B' THEN 'Additional Price Paid'
        ELSE 'Unknown'
    END AS category_type_desc

FROM unique_properties
