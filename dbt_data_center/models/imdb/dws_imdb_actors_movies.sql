{{ config(materialized='view') }}

with dws_imdb_actors_movies as (
    SELECT {{ref('dwd_imdb_actors_movies')}}.id,
        any({{ref('dwd_imdb_actors_movies')}}.actor_name)          as name,
        uniqExact({{ref('dwd_imdb_actors_movies')}}.movie_name)    as num_movies,
        avg({{ref('dwd_imdb_actors_movies')}}.movie_rank)          as avg_rank,
        uniqExact({{ref('dwd_imdb_actors_movies')}}.movie_genre)   as unique_genres,
        uniqExact({{ref('dwd_imdb_actors_movies')}}.director_name) as uniq_directors
    FROM 
        {{ref('dwd_imdb_actors_movies')}}
        GROUP BY {{ref('dwd_imdb_actors_movies')}}.id
)

select *
from dws_imdb_actors_movies