{{ config(materialized='table', file_format='iceberg') }}

SELECT DISTINCT
    COALESCE(unique_carrier, 'Unknown') AS carrier_key,
    unique_carrier                      AS carrier_code
    -- If you have a carrier mapping source, JOIN it here to get the 'Carrier Name'
FROM {{ source('raw', 'raw_data') }}