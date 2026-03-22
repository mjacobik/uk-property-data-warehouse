{{ config(
    materialized='table',
    unique_key='geography_key'
) }}

WITH onspd AS (
    SELECT * FROM {{ ref('silver_onspd') }}
),

ruc AS (
    SELECT * FROM {{ ref('silver_ruc') }}
),

imd AS (
    SELECT * FROM {{ ref('silver_imd') }}
)

SELECT 
    o.onspd_key AS geography_key,
    o.normalized_postcode,
    o.raw_postcode,
    o.valid_from,
    o.valid_to,
    
    -- Related statistical and administrative boundaries
    o.lsoa21cd,
    o.lsoa11cd,
    o.msoa21cd,
    o.lad_code,
    
    -- Rural/urban classification from RUC
    r.ruc21cd,
    r.ruc21nm,
    r.urban_rural_flag,
    
    -- Indices of Multiple Deprivation (IMD) metrics
    i.imd_score,
    i.imd_decile,
    i.income_score,
    i.income_decile,
    i.employment_score,
    i.employment_decile,
    i.education_score,
    i.education_decile,
    i.health_score,
    i.health_decile,
    i.crime_score,
    i.crime_decile,
    i.housing_barriers_score,
    i.housing_barriers_decile,
    i.living_environment_score,
    i.living_environment_decile,

    -- Geographical coordinates
    o.latitude,
    o.longitude
FROM onspd o
LEFT JOIN ruc r 
    ON o.lsoa21cd = r.lsoa21cd
LEFT JOIN imd i
    ON o.lsoa11cd = i.lsoa11cd
