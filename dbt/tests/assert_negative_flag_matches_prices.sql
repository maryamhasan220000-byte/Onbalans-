-- The flag must agree with the value it was derived from.
select period_start, price_eur_mwh, is_negative_price
from {{ ref('int_day_ahead_prices__enriched') }}
where (is_negative_price and price_eur_mwh >= 0)
   or (not is_negative_price and price_eur_mwh < 0)