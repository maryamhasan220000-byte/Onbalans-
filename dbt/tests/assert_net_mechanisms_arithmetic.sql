-- Regression guard
-- These are exact numeric types, so no tolerance needed.

select interval_start
from {{ ref('int_balance_delta__enriched') }}
where net_afrr_mw    <> coalesce(afrr_up_mw, 0)    - coalesce(afrr_down_mw, 0)
   or net_igcc_mw    <> coalesce(igcc_up_mw, 0)    - coalesce(igcc_down_mw, 0)
   or net_picasso_mw <> coalesce(picasso_up_mw, 0) - coalesce(picasso_down_mw, 0)
   or net_mfrrda_mw  <> coalesce(mfrrda_up_mw, 0)  - coalesce(mfrrda_down_mw, 0)
   or net_mari_mw    <> coalesce(mari_up_mw, 0)    - coalesce(mari_down_mw, 0)