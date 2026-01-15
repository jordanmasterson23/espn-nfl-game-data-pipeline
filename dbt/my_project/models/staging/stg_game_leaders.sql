with source as (

    select
        game_id::bigint         as game_id,
        team_id::int            as team_id,
        athlete_id::int         as player_id,
        category::text          as category,
        stat_value::numeric     as stat_value,
        display_value::text     as display_value
    
    from {{ source('nfl_game_data', 'game_leaders') }}

)

select *
from source