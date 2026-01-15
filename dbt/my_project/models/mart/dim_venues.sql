{{ config(
    materialized='table'
) }}

with venues as (
    select *
    from {{ ref('stg_venues') }}
)

select
    venue_id,
    venue_name,
    venue_city,
    venue_state,
    case
        when venue_indoor = 'f' then 'Outdoor'
        else 'Indoor'
    end as venue_type 
from venues