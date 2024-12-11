{{ config(
    materialized='view'
) }}

select *
from {{ source('LANDING_ZONE', 'LZ_WEATHER_TBL') }}