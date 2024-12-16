-- stg_weather_data.sql
{{ config(
    materialized='view'
) }}

WITH cleaned_data AS (
    SELECT
        LOWER(location) AS location,
        LOWER(country) AS country,
        LOWER(region) AS region,
        CAST(local_time AS TIMESTAMP) AS local_time,
        CONCAT(CAST(temperature AS FLOAT), ' Celsius') AS temperature,
        LOWER(weather_description) AS weather_description,
        CAST(wind_spd AS FLOAT) AS wind_speed,
        CONCAT(CAST(pressure AS INTEGER), ' MB') AS pressure,
        CAST(humidity AS INTEGER) AS humidity,
        -- Fix for pipeline_process_date
        TO_TIMESTAMP(pipeline_process_date, 'YYYY-MM-DD_HH24-MI-SS') AS pipeline_process_date,
        audit_digest
    FROM {{ source('LANDING_ZONE', 'LZ_WEATHER_TBL') }}
)

SELECT DISTINCT * FROM cleaned_data