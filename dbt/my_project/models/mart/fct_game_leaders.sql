{{ config(
    materialized='table'
) }}

with leaders as (
    select *
    from {{ ref('stg_game_leaders') }}
),

games as (
    select *
    from {{ ref('stg_games') }}
),

players as (
    select *
    from {{ ref('stg_players') }}
)

select
    l.game_id,
    g.game_name,
    l.player_id,
    p.player_full_name,
    l.category,
    l.display_value as player_stats
from leaders l
join games g
on l.game_id = g.game_id
join players p
on l.player_id = p.player_id
