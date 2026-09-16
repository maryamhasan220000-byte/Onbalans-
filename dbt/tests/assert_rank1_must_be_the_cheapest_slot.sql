-- rank 1 must belong to the cheapest slot of the Dutch DAY. A wrong
-- PARTITION BY (hour instead of date, say) still produces ranks starting at
-- 1, so checking the range alone is not enough: this checks that the rank-1
-- row actually carries the day's minimum price, and that the top rank
-- reaches the day's slot count.
with day_bounds as (
    select dutch_date,
           count(*)             as slots,
           min(price_eur_mwh)   as day_min_price,
           max(price_rank_in_day) as top_rank
    from {{ ref('int_day_ahead_prices__enriched') }}
    group by 1
),

rank_one_rows as (
    select i.dutch_date, i.period_start, i.price_eur_mwh, b.day_min_price, b.slots, b.top_rank
    from {{ ref('int_day_ahead_prices__enriched') }} i
    join day_bounds b on b.dutch_date = i.dutch_date
    where i.price_rank_in_day = 1
)

select * from rank_one_rows
where price_eur_mwh <> day_min_price
   or top_rank > slots 
         