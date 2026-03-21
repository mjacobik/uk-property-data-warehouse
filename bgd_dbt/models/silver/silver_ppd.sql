{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH raw_ppd AS (
    SELECT * FROM {{ source('raw', 'ppd') }}
)

SELECT 
    transaction_id,
    price,
    transfer_date,
    postcode AS raw_postcode,
    -- Normalizacja kodu pocztowego: usunięcie spacji i zamiana na wielkie litery
    REPLACE(UPPER(postcode), ' ', '') AS normalized_postcode,
    
    property_type,
    new_build,
    tenure,
    paon, 
    saon, 
    street, 
    locality, 
    town, 
    district, 
    county,
    category_type,
    record_status,
    -- Dodajemy stempel czasowy, żeby wiedzieć, kiedy dany rekord trafił do Silver (lub kiedy był aktualizowany)
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_ppd

-- Blok inkrementalny (uruchamia się tylko przy kolejnych wywołaniach dbt run, a nie za pierwszym razem)
{% if is_incremental() %}

  -- Pobieramy z raw.ppd tylko te rekordy, które są nowsze niż najnowszy transfer_date w naszej bazie silver
  -- LUB opieramy się po prostu na mechanizmie `unique_key='transaction_id'`, 
  -- który na Postgresie wykona operację UPSERT (czyli zaktualizuje zmienione i wstawi nowe).
  
  -- Jeśli chcesz filtrować wolumen danych, żeby nie przetwarzać 30M wierszy, odkomentuj:
  -- WHERE transfer_date >= (SELECT COALESCE(MAX(transfer_date), '1900-01-01') FROM {{ this }})

{% endif %}
