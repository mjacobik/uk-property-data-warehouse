{{ config(
    materialized='table',
    unique_key='normalized_postcode'
) }}

WITH raw_onspd AS (
    SELECT * FROM {{ source('raw', 'onspd') }}
),

deduplicated_onspd AS (
    SELECT 
        pcds AS raw_postcode,
        -- Taka sama normalizacja jak w PPD dla idealnego JOINA
        REPLACE(UPPER(pcds), ' ', '') AS normalized_postcode,
        
        -- Kody jednostek statystycznych i terytorialnych
        lsoa21cd,
        msoa21cd,
        lad25cd AS lad_code, 
        
        -- Współrzędne
        lat AS latitude,
        long AS longitude,
        
        -- Obsługa duplikatów: bierzemy te, które nie są wygasłe (doterm IS NULL lub puste), 
        -- a potem te ze statusem najnowszej daty wprowadzenia (dointr)
        ROW_NUMBER() OVER (
            PARTITION BY REPLACE(UPPER(pcds), ' ', '') 
            ORDER BY 
                CASE WHEN doterm IS NULL OR doterm = '' THEN 1 ELSE 0 END DESC, 
                dointr DESC
        ) as rn
    FROM raw_onspd
    -- Wyrzucamy puste kody pocztowe, by nie psuć joina
    WHERE pcds IS NOT NULL AND pcds != ''
)

SELECT 
    raw_postcode,
    normalized_postcode,
    lsoa21cd,
    msoa21cd,
    lad_code,
    latitude,
    longitude
FROM deduplicated_onspd
WHERE rn = 1
