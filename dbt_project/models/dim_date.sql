{{ config(materialized='table', file_format='iceberg', alias='dim_date__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT date, index, month, day_of_month
    FROM {{ source('raw', 'raw_data') }} 
)
SELECT
    CONCAT(COALESCE(date, "YYMMDD"), '|', COALESCE(index, 0))             AS date_key,
    CAST(date AS DATE)                                        AS full_date,
    EXTRACT(YEAR  FROM CAST(date AS DATE))                    AS year,
    COALESCE(month, 0)                                        AS month,
    COALESCE(day_of_month, 0)                                 AS day_of_month,
    EXTRACT(DAYOFWEEK FROM CAST(date AS DATE))                AS day_of_week,
    EXTRACT(HOUR   FROM CAST(date AS TIMESTAMP))              AS hour,
    COALESCE(index, 0)                                        AS index
FROM source_data