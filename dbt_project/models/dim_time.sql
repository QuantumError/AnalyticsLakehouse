{{ config(materialized='table', file_format='iceberg', alias='dim_time__' ~ var('snapshot_ts')) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_data') }}
)

SELECT
    CONCAT(COALESCE(actual_departure_time, 0), '|', COALESCE(index, 0)) AS time_key,
    COALESCE(actual_departure_time, 0)                                  AS actual_departure_time,
    COALESCE(scheduled_departure_time, 0)                               AS scheduled_departure_time,
    COALESCE(actual_arrival_time, 0)                                    AS actual_arrival_time,
    COALESCE(scheduled_arrival_time, 0)                                 AS scheduled_arrival_time,
    COALESCE(index, 0)                                                          AS index

FROM source_data