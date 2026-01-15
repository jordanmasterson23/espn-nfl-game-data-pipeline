{{ config(
    materialized='table'
) }}

with games as (
    select *
    from {{ ref('stg_games') }}
),

home_scores as (
    select
        s.game_id,
        t.team_id as home_team_id,
        t.team_full_name as home_team_name,
        s.team_score as home_team_score,
        s.team_record_summary as home_team_record
    from {{ ref('stg_game_scores') }} s
    join {{ ref('stg_teams') }} t
      on s.team_id = t.team_id
    where s.home_away = 'home'
),

away_scores as (
    select
        s.game_id,
        t.team_id as away_team_id,
        t.team_full_name as away_team_name,
        s.team_score as away_team_score,
        s.team_record_summary as away_team_record
    from {{ ref('stg_game_scores') }} s
    join {{ ref('stg_teams') }} t
      on s.team_id = t.team_id
    where s.home_away = 'away'
),

venues as (
    select *
    from {{ ref('stg_venues') }}
)

select
    g.game_id,
    g.game_season_year as season_year,
    case
        when game_season_type = '1' then 'pre-season'
        when game_season_type = '2' then 'regular-season'
        when game_season_type = '3' then 'post-season'
    end as season_type,
    g.game_short_name,
    g.game_name,
    to_char(
        g.game_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver',
        'FMMonth DD, YYYY FMHH12:MI AM'
    ) as mst_formatted_date,
    a.away_team_id,
    a.away_team_name,
    a.away_team_record,
    a.away_team_score,
    h.home_team_id,
    h.home_team_name,
    h.home_team_record,
    h.home_team_score,
    v.venue_name,
    concat(v.venue_city, ', ', v.venue_state) as venue_location,
    v.venue_id,
    g.game_date as game_date_utc
from games g
left join home_scores h
    on g.game_id = h.game_id
left join away_scores a
    on g.game_id = a.game_id
left join venues v
    on g.game_venue_id = v.venue_id