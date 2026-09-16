with ordered as (
    select period_start, period_end,
    lead(period_start) over (order by period_start) as next_start
    from {{ ref('stg_entsoe__day_ahead_prices')}}
)
select period_start, period_end, next_start from ordered
where next_start is not null and next_start <> period_end 