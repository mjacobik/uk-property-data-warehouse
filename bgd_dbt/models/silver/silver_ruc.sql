{{ config(
    materialized='table',
    unique_key='lsoa21cd'
) }}

WITH raw_ruc AS (
    SELECT * FROM {{ source('raw', 'ruc_lsoa_2021') }}
)

SELECT 
    lsoa21cd,
    TRIM(lsoa21nm) AS lsoa21nm,
    -- Kod i nazwa klasyfikacji RUC (Rural-Urban Classification)
    TRIM(ruc21cd) AS ruc21cd,
    TRIM(ruc21nm) AS ruc21nm,
    -- Główna gałąź klasyfikacji (np. Urban, Rural)
    TRIM(urban_rural_flag) AS urban_rural_flag
FROM raw_ruc
