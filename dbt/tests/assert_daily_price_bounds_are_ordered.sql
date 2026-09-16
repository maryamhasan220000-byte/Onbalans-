select dutch_date, min_price_eur_mwh, mean_price_eur_mwh, 
       median_price_eur_mwh, max_price_eur_mwh
from {{ ref('agg_day_ahead_price_daily') }}
where min_price_eur_mwh > mean_price_eur_mwh
or mean_price_eur_mwh > max_price_eur_mwh
or min_price_eur_mwh > median_price_eur_mwh
or median_price_eur_mwh > max_price_eur_mwh 