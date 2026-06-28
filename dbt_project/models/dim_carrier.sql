{{ config(materialized='table', file_format='iceberg', alias='dim_carriers__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT unique_carrier, taxi_in, taxi_out, origin, dest
    FROM {{ source('raw', 'raw_data') }}
)
SELECT DISTINCT
    COALESCE(unique_carrier, 'Unknown') AS carrier_key,
    COALESCE(taxi_in, 0)               AS taxi_in,
    COALESCE(taxi_out, 0)              AS taxi_out,
    COALESCE(origin, 'Unknown')        AS origin,
    COALESCE(dest, 'Unknown')          AS destination
FROM source_data