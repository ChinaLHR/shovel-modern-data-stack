{{ config(engine='MergeTree()', materialized='incremental', unique_key='id') }}

with dwd_imdb_actors_movies as (
     SELECT {{ref('ods_imdb_actors')}}.id  as id,
                concat({{ref('ods_imdb_actors')}}.first_name, ' ', {{ref('ods_imdb_actors')}}.last_name)  as actor_name,
                {{ref('ods_imdb_movies')}}.id as movie_id,
                {{ref('ods_imdb_movies')}}.name as movie_name,
                {{ref('ods_imdb_movies')}}.rank as movie_rank,
                {{ref('ods_imdb_movies_genres')}}.genre as movie_genre,
                concat({{ref('ods_imdb_directors')}}.first_name, ' ', {{ref('ods_imdb_directors')}}.last_name) as director_name,
                {{ref('ods_imdb_actors')}}.updated_at as updated_at 
         FROM {{ref('ods_imdb_actors')}}
                  LEFT JOIN {{ref('ods_imdb_roles')}} ON {{ref('ods_imdb_roles')}}.actor_id = {{ref('ods_imdb_actors')}}.id
                  LEFT JOIN {{ref('ods_imdb_movies')}} ON {{ref('ods_imdb_movies')}}.id = {{ref('ods_imdb_roles')}}.movie_id
                  LEFT JOIN {{ref('ods_imdb_movies_genres')}} ON {{ref('ods_imdb_movies_genres')}}.movie_id = {{ref('ods_imdb_movies')}}.id
                  LEFT JOIN {{ref('ods_imdb_movies_directors')}} ON {{ref('ods_imdb_movies_directors')}}.movie_id = {{ref('ods_imdb_movies')}}.id
                  LEFT JOIN {{ref('ods_imdb_directors')}} ON {{ref('ods_imdb_directors')}}.id = {{ref('ods_imdb_movies_directors')}}.director_id

)

select * from dwd_imdb_actors_movies

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
where id > (select max(id) from {{ this }}) or updated_at > (select max(updated_at) from {{this}})

{% endif %}