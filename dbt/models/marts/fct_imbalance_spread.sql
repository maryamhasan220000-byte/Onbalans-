{{ config(materialized='table') }}

-- THE mart. Every 12-second TenneT grid measurement, matched to the ENTSO-E
-- day-ahead price that was fixed for the 15-minute block containing it.
--
-- spread_eur_mwh = what balancing actually cost, minus what the market
-- expected it to cost the previous afternoon. It is the price of being
-- wrong, and it is the reason both pipelines exist.
--
-- THE JOIN IS A RANGE JOIN, NOT AN EQUALITY JOIN.
-- TenneT ticks every 12 seconds; ENTSO-E prices cover 15-minute blocks. The
-- two clocks never land on the same instant, so a measurement is matched to
-- the block it falls INSIDE:
--
--     interval_start >= period_start   AND   interval_start < period_end
--
-- The asymmetry is load-bearing. A measurement at exactly 22:15:00 belongs
-- to the 22:15-22:30 block, not to 22:00-22:15 which has already ended.
-- Using <= on the end would match it to both blocks and silently duplicate
-- the row; using > on the start would match it to neither and silently
-- drop it. Neither failure raises an error.
--
-- 900 seconds / 12 seconds = exactly 75 measurements per 15-minute block,
-- with no remainder. Verified against the database: 192 blocks, every one
-- holding exactly 75 rows, 14,400 in and 14,400 out.
--
-- INNER JOIN is deliberate. A measurement with no covering price should not
-- appear with a null price pretending to be a spread of zero. If rows go
-- missing, the ENTSO-E backfill does not cover the TenneT range, and the
-- row-count test will say so rather than the number quietly being wrong.
--
-- Materialized as a table: Grafana queries this constantly, and a range
-- join over millions of rows is not something to recompute per refresh.
--
-- CAVEAT: TenneT Balance Delta is operational snapshot data. TenneT states
-- the Settlement Prices publication is leading for settlement, so these
-- spreads describe operational grid state rather than settled amounts.

with grid as (

    select * from {{ ref('fct_balance_delta') }}

),

prices as (

    select
        period_start,
        period_end,
        price_eur_mwh,
        position_in_day,
        is_negative_price,
        is_peak_period,
        is_weekend,
        was_forward_filled,
        daily_mean_price_eur_mwh,
        price_deviation_from_daily_mean_eur_mwh,
        price_rank_in_day,
        price_change_from_previous_eur_mwh
    from {{ ref('fct_day_ahead_price') }}

),

joined as (

    select
        -- Grain: one row per TenneT measurement. Unchanged by the join,
        -- because each measurement falls inside exactly one price block.
        g.interval_start,
        g.interval_start_dutch,
        g.dutch_date,
        g.dutch_hour,

        -- Which price block this measurement was matched to. Kept so the
        -- join is auditable from the output rather than having to be
        -- re-derived or trusted.
        p.period_start                                  as price_period_start,
        p.period_end                                    as price_period_end,
        p.position_in_day                               as price_position_in_day,

        -- What balancing actually cost, in real time.
        g.mid_price_eur_mwh                             as imbalance_mid_price_eur_mwh,
        g.max_up_price_eur_mwh                          as imbalance_max_up_price_eur_mwh,
        g.min_down_price_eur_mwh                        as imbalance_min_down_price_eur_mwh,

        -- What the market expected it to cost, decided yesterday afternoon.
        p.price_eur_mwh                                 as day_ahead_price_eur_mwh,

        -- What the grid was physically doing at that moment.
        g.net_afrr_mw,
        g.net_local_activation_mw,
        g.afrr_regulation_state,

        -- Market context for the block, carried through so the spread can
        -- be sliced without re-joining to the price fact table.
        p.is_negative_price                             as day_ahead_is_negative,
        p.is_peak_period,
        p.is_weekend,
        p.was_forward_filled                            as price_was_forward_filled,
        p.daily_mean_price_eur_mwh,
        p.price_deviation_from_daily_mean_eur_mwh,
        p.price_rank_in_day,
        p.price_change_from_previous_eur_mwh            as day_ahead_ramp_eur_mwh,

        -- Lineage and trust signals.
        g.null_component_count,
        g.last_refetch_lag_seconds,
        g.fetched_at

    from grid g
    join prices p
      on g.interval_start >= p.period_start
     and g.interval_start <  p.period_end

),

final as (

    select
        *,

        -- The number this whole project exists to produce.
        -- Positive: balancing cost MORE than the market expected.
        -- Negative: balancing cost LESS than the market expected.
        imbalance_mid_price_eur_mwh - day_ahead_price_eur_mwh
                                                        as spread_eur_mwh,

        -- Magnitude regardless of direction. Signed spreads cancel when
        -- averaged, so an hour that swings +200 and -200 would look calm.
        -- This measures how far off the forecast was, either way.
        abs(imbalance_mid_price_eur_mwh - day_ahead_price_eur_mwh)
                                                        as abs_spread_eur_mwh,

        -- Explicit null handling: a missing price on either side must not
        -- silently become a direction. Null in, null out.
        case
            when imbalance_mid_price_eur_mwh is null
              or day_ahead_price_eur_mwh is null                    then null
            when imbalance_mid_price_eur_mwh > day_ahead_price_eur_mwh
                                                                    then 'above_forecast'
            when imbalance_mid_price_eur_mwh < day_ahead_price_eur_mwh
                                                                    then 'below_forecast'
            else 'at_forecast'
        end                                             as spread_direction,

        -- True when either side of the spread is not a directly observed
        -- value: the price was forward-filled from a position ENTSO-E
        -- omitted, or a TenneT volume component failed to parse. Lets an
        -- analyst restrict to fully observed rows.
        (price_was_forward_filled or null_component_count > 0)
                                                        as has_imputed_input

    from joined

)

select * from final
