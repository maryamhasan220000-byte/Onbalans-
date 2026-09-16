with per_day as (
    select (period_start at time zone 'Europe/Amsterdam')::date as dutch_date,
    count(*) as slots,
    count(*) filter (where was_forward_filled) as filled 
    from {{ ref('stg_entsoe__day_ahead_prices')}}
    group by 1
)
select dutch_date, slots, filled,
round(filled::numeric / slots, 4) as filled_share
from per_day
where filled::numeric / slots > 0.25