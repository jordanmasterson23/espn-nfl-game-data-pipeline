with source as (

    select
        team_id::int                as team_id,
        uid::text                   as team_uid,
        location::text              as team_location,
        name::text                  as team_name,
        abbreviation::text          as team_abbreviation,
        display_name::text          as team_full_name,
        short_display_name::text    as team_short_name,
        color::text                 as team_color,
        alternate_color::text       as team_alt_color,
        logo_url::text              as team_logo_url,
        is_active::boolean          as team_is_active

    from {{ source('nfl_game_data', 'teams') }}

)

select *
from source