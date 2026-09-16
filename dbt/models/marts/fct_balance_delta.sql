{{ config(materialized='table') }}

-- Analysis-ready fact table. One row per 12-second grid measurement.
-- Materialized as a table because Grafana queries this repeatedly.
--
-- Source is TenneT Balance Delta: operational snapshot data. TenneT states
-- the Settlement Prices publication is leading for settlement.

select
    interval_start,
    interval_start_dutch,
    dutch_date,
    dutch_hour,

    net_afrr_mw,
    net_igcc_mw,
    net_picasso_mw,
    net_mfrrda_mw,
    net_local_activation_mw,

    afrr_regulation_state,

    max_up_price_eur_mwh,
    min_down_price_eur_mwh,
    mid_price_eur_mwh,

   last_refetch_lag_seconds,
    null_component_count,
    fetched_at

from {{ ref('int_balance_delta__enriched') }}
