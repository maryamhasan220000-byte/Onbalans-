-- min <= p10 <= median <= p90 <= max is true of any set of numbers.
-- A violation means a percentile was computed over the wrong ordering or
-- the wrong column.
select dutch_hour, is_weekend, min_price_eur_mwh, p10_price_eur_mwh,
       median_price_eur_mwh, p90_price_eur_mwh, max_price_eur_mwh
from {{ ref('agg_day_ahead_price_hourly_profile') }}
where min_price_eur_mwh > p10_price_eur_mwh
   or p10_price_eur_mwh > median_price_eur_mwh
   or median_price_eur_mwh > p90_price_eur_mwh
   or p90_price_eur_mwh > max_price_eur_mwh