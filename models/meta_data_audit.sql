-- metadata_audit.sql
{{ config(
    materialized='view'
) }}

WITH metadata AS (
    SELECT
        pipeline_process_date,
        audit_digest,
        ROW_NUMBER() OVER (PARTITION BY pipeline_process_date ORDER BY local_time DESC) AS row_num
    FROM {{ ref('stg_weather_data') }}
)

SELECT
    pipeline_process_date,
    audit_digest
FROM metadata
WHERE row_num = 1