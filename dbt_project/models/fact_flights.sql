{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    partition_by=['year(flight_date)'],
    tblproperties={
        'write.format.default': 'parquet',
        'write.parquet.compression-codec': 'zstd',
        'write.spark.accept-any-schema': 'true'
    }
) }}

SELECT
    -- Dimension Keys (Foreign Keys)
    CAST(date AS DATE)                                   AS flight_date,
    COALESCE(unique_carrier, 'Unknown')                  AS carrier_key,
    COALESCE(origin, 'Unknown')                          AS origin_airport_key,
    COALESCE(dest, 'Unknown')                            AS dest_airport_key,
    
    -- Degenerate Dimensions (kept in fact table for granularity)
    `index`                                              AS flight_index_id,
    COALESCE(flight_number, 0)                           AS flight_number,
    COALESCE(plane_tail_number, -1)                      AS plane_tail_number,
    COALESCE(cancelled, 0)                               AS is_cancelled,
    COALESCE(cancellation_code, 'Unknown')               AS cancellation_code,
    COALESCE(diverted, 0)                                AS is_diverted,

    -- Metrics / Measures
    COALESCE(CAST(distance AS DOUBLE), 0.0)              AS distance,
    COALESCE(scheduled_departure_time, 0)                AS scheduled_departure_time,
    COALESCE(scheduled_arrival_time, 0)                  AS scheduled_arrival_time,
    COALESCE(scheduled_elapsed_time, 0)                  AS scheduled_elapsed_time,
    COALESCE(actual_departure_time, 0)                   AS actual_departure_time,
    COALESCE(actual_arrival_time, 0)                     AS actual_arrival_time,
    COALESCE(actual_elapsed_time, 0)                     AS actual_elapsed_time,
    COALESCE(arrival_delay, 0)                           AS arrival_delay,
    COALESCE(departure_delay, 0)                         AS departure_delay,
    COALESCE(carrier_delay, 0)                           AS carrier_delay,
    COALESCE(weather_delay, 0)                           AS weather_delay,
    COALESCE(nas_delay, 0)                               AS nas_delay,
    COALESCE(security_delay, 0)                          AS security_delay,
    COALESCE(late_aircraft_delay, 0)                     AS late_aircraft_delay,
    COALESCE(taxi_in, 0)                                 AS taxi_in,
    COALESCE(taxi_out, 0)                                AS taxi_out,
    COALESCE(air_time, 0)                                AS air_time

FROM {{ source('raw', 'raw_data') }}

{% if is_incremental() %}
    -- Assumes 'date' is string/timestamp in source. Adjust casting as needed.
    WHERE CAST(date AS DATE) > (SELECT MAX(flight_date) FROM {{ this }})
{% endif %}