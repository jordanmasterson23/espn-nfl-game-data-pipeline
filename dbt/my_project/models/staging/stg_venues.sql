with source as (

    select
        venue_id::int       as venue_id,
        name::text          as venue_name,
        city::text          as venue_city,
        state::text         as venue_state,
        indoor::boolean     as venue_indoor,
        capacity::int       as venue_capacity

    from {{ source('nfl_game_data', 'venues') }}

)

select *
from source