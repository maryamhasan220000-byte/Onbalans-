-- The daily rollup must account for every fact row exactly once. Catches a
-- filter in the aggregate or a fan-out that inflated the counts.
with counts as (
    select
        (select count(*) from {{ ref('fct_day_ahead_price') }})            as fact_rows,
        (select sum(slots_present) from {{ ref('agg_day_ahead_price_daily') }}) as rolled_up,
        (select count(distinct dutch_date) from {{ ref('fct_day_ahead_price') }}) as fact_days,
        (select count(*) from {{ ref('agg_day_ahead_price_daily') }})      as agg_days
)
select * from counts
where fact_rows <> rolled_up or fact_days <> agg_days