{{ config(
    materialized='table',
    unique_key='lsoa21cd'
) }}

WITH raw_ruc AS (
    SELECT * FROM {{ source('raw', 'ruc_lsoa_2021') }}
)

SELECT 
    "LSOA21CD" AS lsoa21cd,
    TRIM("LSOA21NM") AS lsoa21nm,
    TRIM("RUC21CD") AS ruc21cd,
    TRIM("RUC21NM") AS ruc21nm,
    TRIM("Urban_rural_flag") AS urban_rural_flag
FROM raw_ruc
