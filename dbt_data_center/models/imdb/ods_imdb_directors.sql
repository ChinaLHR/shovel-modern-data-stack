{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('data_center', 'data_center_raw__stream_imdb_directors')}}
),

ods_imdb_directors as (
SELECT 
    toUInt32(JSONExtractString(_airbyte_data, 'id')) AS id,
    JSONExtractString(_airbyte_data, 'first_name') AS first_name,
    JSONExtractString(_airbyte_data, 'last_name') AS last_name,
    parseDateTimeBestEffort(JSONExtractString(_airbyte_data, '_ab_cdc_updated_at')) AS updated_at
FROM 
    source
)

select *
from ods_imdb_directors