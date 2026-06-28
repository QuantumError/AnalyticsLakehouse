{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    partition_by=['year(flight_date)'],
    tblproperties={
        'write.format.default': 'parquet',
        'write.parquet.compression-codec': 'zstd',
        'write.spark.accept-any-schema': 'true'
    },
    alias='fact_flights__' ~ var('snapshot_ts')
) }}

WITH source_data AS (
    SELECT
        date, index, unique_carrier,
        flight_number, plane_tail_number, cancellation_code,
        actual_departure_time, diverted, cancelled, origin
    FROM {{ source('raw', 'raw_data') }}

    {% if is_incremental() %}
        WHERE CAST(date AS DATE) > (
            SELECT MAX(flight_date) FROM {{ this }}
        )
    {% endif %}
)
SELECT
    CONCAT(COALESCE(plane_tail_number, 'Unknown'), '|', COALESCE(flight_number, 'Unknown')) AS flight_key,
    CAST(date AS DATE)                                                              AS flight_date,
    COALESCE(unique_carrier, 'Unknown')                                             AS carrier_key,
    CONCAT(COALESCE(plane_tail_number, 'Unknown'), '|', COALESCE(origin, 'Unknown')) AS airplane_key,
    COALESCE(cancellation_code, 'Unknown')                                          AS delay_key,
    CONCAT(COALESCE(date, "YYMMDD"), '|', COALESCE(index, 0))             AS date_key,
    CONCAT(COALESCE(actual_departure_time, 0), '|', COALESCE(index, 0)) AS time_key,
    COALESCE(diverted, 0)                                                           AS diverted,
    COALESCE(cancelled, 0)                                                          AS cancelled,
    COALESCE(flight_number, 'Unknown')                                              AS flight_number,
    COALESCE(plane_tail_number, 'Unknown')                                          AS plane_tail_number
FROM source_data