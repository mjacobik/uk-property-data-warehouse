{{
    config(
        materialized='table'
    )
}}

WITH imd AS (
    SELECT *
    FROM {{ source('raw', 'imd') }}
)

SELECT
    "LSOA code (2011)" AS lsoa11cd,
    "LSOA name (2011)" AS lsoa11nm,
    "Local Authority District name (2019)" AS local_authority_name,
    CAST("Index of Multiple Deprivation (IMD) Score" AS DECIMAL(10,3)) AS imd_score,
    CAST("Index of Multiple Deprivation (IMD) Decile (where 1 is most dep" AS INTEGER) AS imd_decile,
    
    CAST("Income Score (rate)" AS DECIMAL(10,3)) AS income_score,
    CAST("Income Decile (where 1 is most deprived 10% of LSOAs)" AS INTEGER) AS income_decile,
    
    CAST("Employment Score (rate)" AS DECIMAL(10,3)) AS employment_score,
    CAST("Employment Decile (where 1 is most deprived 10% of LSOAs)" AS INTEGER) AS employment_decile,
    
    CAST("Education, Skills and Training Score" AS DECIMAL(10,3)) AS education_score,
    CAST("Education, Skills and Training Decile (where 1 is most deprived" AS INTEGER) AS education_decile,
    
    CAST("Health Deprivation and Disability Score" AS DECIMAL(10,3)) AS health_score,
    CAST("Health Deprivation and Disability Decile (where 1 is most depri" AS INTEGER) AS health_decile,
    
    CAST("Crime Score" AS DECIMAL(10,3)) AS crime_score,
    CAST("Crime Decile (where 1 is most deprived 10% of LSOAs)" AS INTEGER) AS crime_decile,
    
    CAST("Barriers to Housing and Services Score" AS DECIMAL(10,3)) AS housing_barriers_score,
    CAST("Barriers to Housing and Services Decile (where 1 is most depriv" AS INTEGER) AS housing_barriers_decile,
    
    CAST("Living Environment Score" AS DECIMAL(10,3)) AS living_environment_score,
    CAST("Living Environment Decile (where 1 is most deprived 10% of LSOA" AS INTEGER) AS living_environment_decile

FROM imd
