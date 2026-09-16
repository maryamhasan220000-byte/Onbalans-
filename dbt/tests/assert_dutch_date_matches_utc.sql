-- Dutch local time is always ahead of UTC (+1 winter, +2 summer), so the
-- Dutch date can never be earlier than the UTC date, and never more than one
-- day ahead. Catches an offset applied in the wrong direction.
select
    interval_start,
    interval_start::date as utc_date,
    dutch_date
from {{ ref('int_balance_delta__enriched') }}
where dutch_date < interval_start::date
   or dutch_date > interval_start::date + 1