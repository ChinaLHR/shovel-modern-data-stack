{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('data_center', 'data_center_raw__stream_imdb_movies')}}
),

ods_imdb_movies as (
    SELECT 
        toUInt32(JSONExtractString(_airbyte_data, 'id')) AS id,
        JSONExtractString(_airbyte_data, 'name') AS name,
        toUInt16(JSONExtractString(_airbyte_data, 'year')) AS year,
        toFloat32OrNull(JSONExtractString(_airbyte_data, 'rank')) AS rank,
        parseDateTimeBestEffort(JSONExtractString(_airbyte_data, '_ab_cdc_updated_at')) AS updated_at
    FROM 
        source
)

select *
from ods_imdb_movies