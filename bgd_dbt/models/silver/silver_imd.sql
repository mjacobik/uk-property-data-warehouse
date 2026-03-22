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
    lsoa_code_2011 AS lsoa11cd,
    lsoa_name_2011 AS lsoa11nm,
    local_authority_district_name_2019 AS local_authority_name,
    CAST(index_of_multiple_deprivation_imd_score AS DECIMAL(10,3)) AS imd_score,
    CAST(index_of_multiple_deprivation_imd_decile_where_1_is_most_dep AS INTEGER) AS imd_decile,
    
    CAST(income_score_rate AS DECIMAL(10,3)) AS income_score,
    CAST(income_decile_where_1_is_most_deprived_10_of_lsoas AS INTEGER) AS income_decile,
    
    CAST(employment_score_rate AS DECIMAL(10,3)) AS employment_score,
    CAST(employment_decile_where_1_is_most_deprived_10_of_lsoas AS INTEGER) AS employment_decile,
    
    CAST(education_skills_and_training_score AS DECIMAL(10,3)) AS education_score,
    CAST(education_skills_and_training_decile_where_1_is_most_deprive AS INTEGER) AS education_decile,
    
    CAST(health_deprivation_and_disability_score AS DECIMAL(10,3)) AS health_score,
    CAST(health_deprivation_and_disability_decile_where_1_is_most_dep AS INTEGER) AS health_decile,
    
    CAST(crime_score AS DECIMAL(10,3)) AS crime_score,
    CAST(crime_decile_where_1_is_most_deprived_10_of_lsoas AS INTEGER) AS crime_decile,
    
    CAST(barriers_to_housing_and_services_score AS DECIMAL(10,3)) AS housing_barriers_score,
    CAST(barriers_to_housing_and_services_decile_where_1_is_most_depr AS INTEGER) AS housing_barriers_decile,
    
    CAST(living_environment_score AS DECIMAL(10,3)) AS living_environment_score,
    CAST(living_environment_decile_where_1_is_most_deprived_10_of_lso AS INTEGER) AS living_environment_decile

FROM imd
