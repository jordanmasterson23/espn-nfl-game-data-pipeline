{{ config(
    materialized='table'
) }}

with teams as (
    select 
        t.*,
        s.team_record_summary
    from {{ ref('stg_teams') }} t
    join {{ ref('stg_game_scores') }} s
    on t.team_id = s.team_id
)

select
    t.team_id,
    t.team_full_name as team_name,
    t.team_location as team_city,
    t.team_name as short_name,
    t.team_abbreviation,
    t.team_record_summary as team_record
from teams t