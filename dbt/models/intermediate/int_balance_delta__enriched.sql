{{ config(materialized='view') }}

-- One row per 12-second measurement, with derived columns.
--
-- IMPORTANT: the five mechanisms are NOT summable into a single "system
-- imbalance" figure, and this model deliberately does not produce one.
--
--   aFRR     local activation. TenneT states the balancing energy and
--            imbalance price are determined from local activated aFRR and
--            incident reserves, so this is the settlement-relevant volume.
--   IGCC     imbalance netting: aFRR that was AVOIDED by netting against
--            other TSOs. Capped at available aFRR in the LFC. Adding it to
--            aFRR double-counts the same underlying imbalance.
--   PICASSO  cross-border aFRR exchange. TenneT's own definition gives each
--            direction two opposite physical meanings, so the sign alone
--            does not identify what happened.
--   mFRRda   manual activation. A genuinely separate physical lever.
--   MARI     TenneT NL is connected but does not actively exchange mFRR,
--            so these columns are expected to be zero.
--
-- Balance Delta is also a SNAPSHOT. TenneT states the Settlement Prices
-- publication is leading for settlement; treat these as operational values.

with staged as (

    select * from {{ ref('stg_tennet__balance_delta') }}

),

component_nulls as (

    select
        *,
        (case when afrr_up_mw       is null then 1 else 0 end)
      + (case when afrr_down_mw     is null then 1 else 0 end)
      + (case when igcc_up_mw       is null then 1 else 0 end)
      + (case when igcc_down_mw     is null then 1 else 0 end)
      + (case when mfrrda_up_mw     is null then 1 else 0 end)
      + (case when mfrrda_down_mw   is null then 1 else 0 end)
      + (case when picasso_up_mw    is null then 1 else 0 end)
      + (case when picasso_down_mw  is null then 1 else 0 end)
      + (case when mari_up_mw       is null then 1 else 0 end)
      + (case when mari_down_mw     is null then 1 else 0 end)
        as null_component_count
    from staged

),

per_mechanism_net as (

    -- Each mechanism netted within itself only. Never across mechanisms:
    -- they measure different things on different bases.
    select
        *,
        coalesce(afrr_up_mw, 0)    - coalesce(afrr_down_mw, 0)    as net_afrr_mw,
        coalesce(igcc_up_mw, 0)    - coalesce(igcc_down_mw, 0)    as net_igcc_mw,
        coalesce(picasso_up_mw, 0) - coalesce(picasso_down_mw, 0) as net_picasso_mw,
        coalesce(mfrrda_up_mw, 0)  - coalesce(mfrrda_down_mw, 0)  as net_mfrrda_mw,
        coalesce(mari_up_mw, 0)    - coalesce(mari_down_mw, 0)    as net_mari_mw
    from component_nulls

),

final as (

    select
        interval_start,
        interval_end,
        sequence_within_dutch_day,

        interval_start at time zone 'Europe/Amsterdam'          as interval_start_dutch,
        (interval_start at time zone 'Europe/Amsterdam')::date  as dutch_date,
        extract(hour from interval_start at time zone 'Europe/Amsterdam')::int
                                                               as dutch_hour,

        afrr_up_mw, afrr_down_mw,
        igcc_up_mw, igcc_down_mw,
        mfrrda_up_mw, mfrrda_down_mw,
        picasso_up_mw, picasso_down_mw,
        mari_up_mw, mari_down_mw,

        net_afrr_mw,
        net_igcc_mw,
        net_picasso_mw,
        net_mfrrda_mw,
        net_mari_mw,

        -- The settlement-relevant volume: local physical activation only.
        -- Excludes IGCC (avoided activation) and PICASSO (ambiguous sign).
        net_afrr_mw + net_mfrrda_mw                            as net_local_activation_mw,

        null_component_count,

        -- Classified on aFRR alone, because that is what drives the price.
        case
            when coalesce(afrr_up_mw, 0) > 0 and coalesce(afrr_down_mw, 0) > 0 then 'both'
            when coalesce(afrr_up_mw, 0) > 0                                   then 'up'
            when coalesce(afrr_down_mw, 0) > 0                                 then 'down'
            else 'none'
        end                                                    as afrr_regulation_state,

        max_up_price_eur_mwh,
        min_down_price_eur_mwh,
        mid_price_eur_mwh,

        extract(epoch from (fetched_at - interval_end))::numeric(10,3)
                                                               as last_refetch_lag_seconds,

        fetched_at,
        source_file,
        loaded_at

    from per_mechanism_net

)

select * from final