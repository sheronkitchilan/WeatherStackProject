-- dim_location.sql
{{ config(
    materialized='view'
) }}

WITH location_data AS (
    SELECT
        location,
        country,
        region,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY pipeline_process_date DESC) AS row_num
    FROM {{ ref('stg_weather_data') }}
)

SELECT
    location,
    country,
    region
FROM location_data
WHERE row_num = 1