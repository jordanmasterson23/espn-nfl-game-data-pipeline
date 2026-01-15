with source as (

    select
        game_id::bigint                             as game_id,
        uid::text                                   as game_uid,
        game_date::timestamp without time zone      as game_date,
        season_year::int                            as game_season_year,
        season_type::int                            as game_season_type,
        name::text                                  as game_name,
        short_name::text                            as game_short_name,
        status::text                                as game_status,
        status_detail::text                         as game_status_detail,
        period::int                                 as game_quarter,
        venue_id::int                               as game_venue_id,
        attendance::int                             as game_attendance,
        neutral_site::boolean                       as game_neutral_site

    from {{ source('nfl_game_data', 'games') }}

)

select * 
from source