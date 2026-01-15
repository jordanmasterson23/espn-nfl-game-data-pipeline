with source as (

    select
        game_id::bigint           as game_id,
        team_id::int              as team_id,
        lower(home_away)          as home_away,
        score::int                as team_score,
        record_type               as team_record_type,
        record_summary            as team_record_summary

    from {{ source('nfl_game_data', 'game_teams') }}

)

select *
from source