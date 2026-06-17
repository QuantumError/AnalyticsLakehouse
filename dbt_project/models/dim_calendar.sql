{{ config(materialized='table', file_format='iceberg') }}

WITH date_range AS (
    SELECT DISTINCT CAST(date AS DATE) AS flight_date
    FROM {{ source('raw', 'raw_data') }}
)
SELECT
    flight_date                                  AS date_key,
    flight_date                                  AS full_date,
    EXTRACT(YEAR FROM flight_date)               AS `year`,
    EXTRACT(MONTH FROM flight_date)              AS `month`,
    EXTRACT(DAY FROM flight_date)                AS day_of_month,
    EXTRACT(QUARTER FROM flight_date)            AS `quarter`,
    DATE_FORMAT(flight_date, 'EEEE')            AS day_of_week_name
FROM date_range