with source as (

    select
        athlete_id::int         as player_id,
        full_name::text         as player_full_name,
        short_name::text        as player_short_name,
        jersey::text            as player_jersey_number,
        position::text          as player_position,
        headshot_url::text      as player_headshot_url,
        active::boolean         as player_active

    from {{ source('nfl_game_data', 'athletes') }}

)

select *
from source