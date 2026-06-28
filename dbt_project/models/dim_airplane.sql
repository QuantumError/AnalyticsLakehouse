{{ config(materialized='table', file_format='iceberg', alias='dim_airplane__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT plane_tail_number, flight_number, origin, dest, distance, air_time
    FROM {{ source('raw', 'raw_data') }}
)
SELECT DISTINCT
    CONCAT(COALESCE(plane_tail_number, 'Unknown'), '|', COALESCE(origin, 'Unknown')) AS airplane_key,
    COALESCE(plane_tail_number, 'Unknown') AS tail_number,
    COALESCE(flight_number, 'Unknown')     AS flight_number,
    COALESCE(origin, 'Unknown')            AS origin,
    COALESCE(dest, 'Unknown')              AS destination,
    COALESCE(distance, 0)                  AS distance,
    COALESCE(air_time, 0)                  AS air_time
FROM source_data