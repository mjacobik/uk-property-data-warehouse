{{ config(
    materialized='table'
) }}

WITH raw_onspd AS (
    SELECT * FROM {{ source('raw', 'onspd') }}
    WHERE pcds IS NOT NULL AND pcds != ''
),

scd2_onspd AS (
    SELECT 
        MD5(REPLACE(UPPER(pcds), ' ', '') || COALESCE(dointr, '')) AS onspd_key,
        pcds AS raw_postcode,
        REPLACE(UPPER(pcds), ' ', '') AS normalized_postcode,
        
        lsoa21cd,
        lsoa11cd,
        msoa21cd,
        lad25cd AS lad_code, 
        lat AS latitude,
        long AS longitude,
        
        -- Convert YYYYMM introduction date to a real Date (first day of the month)
        -- If missing, default to far past
        CASE 
            WHEN dointr IS NULL OR dointr = '' THEN '1900-01-01'::DATE
            ELSE TO_DATE(dointr, 'YYYYMM')
        END AS valid_from,
        
        -- Convert YYYYMM termination date to a real Date (last day of the month)
        -- If missing, default to far future
        CASE 
            WHEN doterm IS NULL OR doterm = '' THEN '2099-12-31'::DATE
            ELSE (TO_DATE(doterm, 'YYYYMM') + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        END AS valid_to

    FROM raw_onspd
)

SELECT * FROM scd2_onspd
