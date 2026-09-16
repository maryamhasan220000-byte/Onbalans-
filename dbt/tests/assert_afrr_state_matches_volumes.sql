-- The classification must agree with the aFRR volumes it was derived from.
select interval_start, afrr_up_mw, afrr_down_mw, afrr_regulation_state
from {{ ref('int_balance_delta__enriched') }}
where (afrr_regulation_state = 'up'
       and not (coalesce(afrr_up_mw,0) > 0 and coalesce(afrr_down_mw,0) = 0))
   or (afrr_regulation_state = 'down'
       and not (coalesce(afrr_down_mw,0) > 0 and coalesce(afrr_up_mw,0) = 0))
   or (afrr_regulation_state = 'both'
       and not (coalesce(afrr_up_mw,0) > 0 and coalesce(afrr_down_mw,0) > 0))
   or (afrr_regulation_state = 'none'
       and not (coalesce(afrr_up_mw,0) = 0 and coalesce(afrr_down_mw,0) = 0))