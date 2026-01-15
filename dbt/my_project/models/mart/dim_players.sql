{{ config(
    materialized='table'
) }}

with players as (
    select 
        p.*,
        t.*
    from {{ ref('stg_players') }} p
    join {{ ref('stg_game_leaders') }} l
    on p.player_id = l.player_id
    join {{ ref('stg_teams') }} t
    on l.team_id = t.team_id
)

select
    player_id,
    player_full_name as player_name,
    player_short_name as short_name,
    player_position as position,
    player_jersey_number as jersey_number,
    team_full_name as team,
    team_id,
    case
        when player_active = 't' THEN 'Active'
        else 'Inactive'
    end as active_status
from players