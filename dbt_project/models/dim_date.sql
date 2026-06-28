{{ config(materialized='table', file_format='iceberg', alias='dim_date__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT DISTINCT date, month, day_of_month
    FROM {{ source('raw', 'raw_data') }}
    WHERE date IS NOT NULL
),
indexed AS (
    SELECT
        date,
        month,
        day_of_month,
        ROW_NUMBER() OVER (ORDER BY date) AS index
    FROM source_data
)
SELECT
    CONCAT(date, '|', index)                             AS date_key,
    CAST(date AS DATE)                                   AS full_date,
    EXTRACT(YEAR FROM CAST(date AS DATE))                AS year,
    COALESCE(month, 0)                                   AS month,
    COALESCE(day_of_month, 0)                            AS day_of_month,
    EXTRACT(DAYOFWEEK FROM CAST(date AS DATE))           AS day_of_week,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP))           AS hour,
    index
FROM indexed