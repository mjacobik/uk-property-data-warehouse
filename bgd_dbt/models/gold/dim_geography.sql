{{ config(
    materialized='table',
    unique_key='normalized_postcode'
) }}

WITH onspd AS (
    SELECT * FROM {{ ref('silver_onspd') }}
),

ruc AS (
    SELECT * FROM {{ ref('silver_ruc') }}
)

SELECT 
    o.normalized_postcode,
    o.raw_postcode,
    
    -- Powiązane granice statystyczne i administracyjne
    o.lsoa21cd,
    o.msoa21cd,
    o.lad_code,
    
    -- Klasyfikacja wiejska/miejska z RUC
    r.ruc21cd,
    r.ruc21nm,
    r.urban_rural_flag,
    
    -- Współrzędne geograficzne
    o.latitude,
    o.longitude
FROM onspd o
LEFT JOIN ruc r 
    ON o.lsoa21cd = r.lsoa21cd
