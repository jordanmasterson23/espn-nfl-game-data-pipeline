{{ config(
    materialized='table'
) }}

with odds as (
    select *
    from {{ ref('stg_game_odds') }}
)

select
    game_id,
    game_odds_provider as odds_provider,
    spread,
    over_under,
    game_odds_details as odds_details
from odds