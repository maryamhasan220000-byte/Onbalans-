-- The four state counts must account for every interval in the hour. If they
-- do not, a fifth state exists that the rollup is silently dropping.
select bucket_start, intervals_present,
       intervals_up + intervals_down + intervals_both + intervals_none as state_total
from {{ ref('agg_balance_delta_hourly') }}
where intervals_up + intervals_down + intervals_both + intervals_none
      <> intervals_present