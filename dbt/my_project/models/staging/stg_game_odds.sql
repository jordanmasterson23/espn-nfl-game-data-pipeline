with source as (

    select
        game_id::bigint         as game_id,
        provider::text          as game_odds_provider,
        spread::numeric         as spread,
        over_under::numeric     as over_under,
        details::text           as game_odds_details,
        favorite_team_id::int   as favorite_team_id

    from {{ source('nfl_game_data', 'game_odds') }}

)

select *
from source