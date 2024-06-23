{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('data_center', 'data_center_raw__stream_imdb_roles')}}
),

ods_imdb_roles as (
    SELECT 
        toUInt32(JSONExtractString(_airbyte_data, 'actor_id')) AS actor_id,
        toUInt32(JSONExtractString(_airbyte_data, 'movie_id')) AS movie_id,
        JSONExtractString(_airbyte_data, 'role') AS role,
        parseDateTimeBestEffort(JSONExtractString(_airbyte_data, '_ab_cdc_updated_at')) AS updated_at
    FROM 
        source
)

select *
from ods_imdb_roles