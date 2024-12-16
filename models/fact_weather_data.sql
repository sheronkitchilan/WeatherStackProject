{{ config(
    materialized='view'
) }}

WITH weather_data AS (
    SELECT
        location,
        local_time,
        CAST(SPLIT_PART(temperature, ' ', 1) AS FLOAT) AS temperature_value,
        SPLIT_PART(temperature, ' ', 2) AS temperature_unit,
        weather_description,
        wind_speed,
        CAST(SPLIT_PART(pressure, ' ', 1) AS INTEGER) AS pressure_value,
        SPLIT_PART(pressure, ' ', 2) AS pressure_unit,
        humidity
    FROM {{ ref('stg_weather_data') }}
)

SELECT DISTINCT * FROM weather_data