{{ config(materialized='table', file_format='iceberg') }}

WITH all_airports AS (
    SELECT origin AS airport_code FROM {{ source('raw', 'raw_data') }}
    UNION
    SELECT dest AS airport_code FROM {{ source('raw', 'raw_data') }}
)
SELECT DISTINCT
    COALESCE(airport_code, 'Unknown') AS airport_key,
    airport_code                      AS airport_code
    -- Join a master airport geographic dataset here to add City, State, Country columns
FROM all_airports
WHERE airport_code IS NOT NULL