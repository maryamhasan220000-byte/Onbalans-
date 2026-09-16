select period_start, price_eur_mwh, daily_mean_price_eur_mwh,
price_deviation_from_daily_mean_eur_mwh
from {{ ref('int_day_ahead_prices__enriched')}}
where abs(price_deviation_from_daily_mean_eur_mwh -
(price_eur_mwh - daily_mean_price_eur_mwh)) > 0.0001 