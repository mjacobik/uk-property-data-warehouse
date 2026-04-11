{{ config(
    materialized='table',
    unique_key='location_key'
) }}

WITH source_sales AS (
    SELECT * FROM {{ ref('silver_ppd') }}
),

unique_locations AS (
    SELECT DISTINCT
        paon,
        saon,
        street,
        locality,
        town,
        district,
        county
    FROM source_sales
)

SELECT
    MD5(
        COALESCE(paon, 'NULL') || '|' || 
        COALESCE(saon, 'NULL') || '|' || 
        COALESCE(street, 'NULL') || '|' || 
        COALESCE(locality, 'NULL') || '|' || 
        COALESCE(town, 'NULL') || '|' || 
        COALESCE(district, 'NULL') || '|' || 
        COALESCE(county, 'NULL')
    ) AS location_key,
    
    paon,
    saon,
    street,
    locality,
    town,
    district,
    county

FROM unique_locations
