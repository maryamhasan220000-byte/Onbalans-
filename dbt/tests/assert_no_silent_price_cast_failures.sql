select s.period_start, s.price_text as raw_text,
from {{ source('entsoe', 'day_ahead_prices')}} s
join {{ ref('stg_entsoe__day_ahead_prices')}} t
on t.period_start = s.period_start
where s.price_text is not null 
and trim(s.price_text) <> ''
and t.price_eur_mwh is null
