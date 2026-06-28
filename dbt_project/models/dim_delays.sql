{{ config(materialized='table', file_format='iceberg', alias='dim_delays__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT DISTINCT cancellation_code, departure_delay, arrival_delay, carrier_delay, weather_delay, nas_delay, security_delay
    FROM {{ source('raw', 'raw_data') }}
)
SELECT
    COALESCE(cancellation_code, 'Unknown') AS delay_key,
    COALESCE(departure_delay, 0) AS departure_delay,
    COALESCE(arrival_delay, 0) AS arrival_delay,
    COALESCE(carrier_delay, 0) AS carrier_delay,
    COALESCE(weather_delay, 0) AS weather_delay,
    COALESCE(nas_delay, 0) AS nas_delay,
    COALESCE(security_delay, 0) AS security_delay
FROM source_data