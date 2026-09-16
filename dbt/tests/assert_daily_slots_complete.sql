-- Every day must hold exactly the number of slots its own 
-- span and resolution imply. Fewer means the ingester fetched 
-- a partial documents: more means two documents overlapped and both
-- landed.

select dutch_date, slots_present, slots_expected 
from {{ref('agg_day_ahead_price_daily') }}
where slots_present <> slots_expected 