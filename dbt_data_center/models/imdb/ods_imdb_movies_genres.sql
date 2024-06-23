{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('data_center', 'data_center_raw__stream_imdb_movies_genres')}}
),

ods_imdb_movies_genres as (
    SELECT 
        toUInt32(JSONExtractString(_airbyte_data, 'movie_id')) AS movie_id,
        JSONExtractString(_airbyte_data, 'genre') AS genre,
        parseDateTimeBestEffort(JSONExtractString(_airbyte_data, '_ab_cdc_updated_at')) AS updated_at
    FROM 
        source
)

select *
from ods_imdb_movies_genres