{{ config(materialized='table', file_format='iceberg', alias='dim_airplane__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT plane_tail_number, flight_number, origin, dest, distance, air_time
    FROM {{ source('raw', 'raw_data') }}
)
SELECT DISTINCT
    CONCAT(COALESCE(plane_tail_number, 'Unknown'), '|', COALESCE(origin, 'Unknown')) AS airplane_key,
    COALESCE(plane_tail_number, 'Unknown') AS tail_number,
    MAX(COALESCE(flight_number, 'Unknown')) AS flight_number,
    COALESCE(origin, 'Unknown')            AS origin,
    MAX(COALESCE(dest, 'Unknown'))         AS destination,
    MAX(COALESCE(distance, 0))             AS distance,
    MAX(COALESCE(air_time, 0))             AS air_time
FROM source_data
GROUP BY plane_tail_number, origin