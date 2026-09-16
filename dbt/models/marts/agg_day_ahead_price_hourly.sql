{{ config(materialized='table') }}

-- The average shape of a Dutch day. One row per hour-of-day (0-23),
-- averaged across every day in the dataset.
--
-- Grain differs from every other model: it is not a time series. Row
-- "hour 13" is not a moment, it is the behaviour of 13:00 across all days
-- pooled together. Never join this to anything on a timestamp.
--
-- Split by weekday and weekend because industrial demand differs, so the
-- pooled average would hide two different shapes.

with slots as (

    select * from {{ ref('fct_day_ahead_price') }}

),

profile as (

    select
        dutch_hour,
        is_weekend,

        count(*)                                            as slots_observed,
        count(distinct dutch_date)                          as days_observed,

        avg(price_eur_mwh)                                  as mean_price_eur_mwh,
        (percentile_cont(0.5) within group (order by price_eur_mwh))::numeric
                                                            as median_price_eur_mwh,
        (percentile_cont(0.1) within group (order by price_eur_mwh))::numeric
                                                            as p10_price_eur_mwh,
        (percentile_cont(0.9) within group (order by price_eur_mwh))::numeric
                                                            as p90_price_eur_mwh,
        min(price_eur_mwh)                                  as min_price_eur_mwh,
        max(price_eur_mwh)                                  as max_price_eur_mwh,

        -- Spread ACROSS days for this hour: how unpredictable this hour is,
        -- as distinct from how much prices move within a single day.
        stddev_samp(price_eur_mwh)                          as stddev_price_eur_mwh,

        -- Deviation from each day's own mean, averaged. Removes the level
        -- of each day so only the SHAPE remains: a cheap day and an
        -- expensive day contribute equally.
        avg(price_deviation_from_daily_mean_eur_mwh)        as mean_deviation_eur_mwh,

        count(*) filter (where is_negative_price)           as negative_slots,

        -- Absolute ramp: how fast the price moves in this hour, ignoring
        -- direction. Peaks at the solar transitions.
        avg(abs(price_change_from_previous_eur_mwh))        as mean_abs_ramp_eur_mwh,
        max(abs(price_change_from_previous_eur_mwh))        as max_abs_ramp_eur_mwh

    from slots
    group by dutch_hour, is_weekend

),

final as (

    select
        dutch_hour,
        is_weekend,

        days_observed,
        slots_observed,

        round(mean_price_eur_mwh, 2)                        as mean_price_eur_mwh,
        round(median_price_eur_mwh, 2)                      as median_price_eur_mwh,
        round(p10_price_eur_mwh, 2)                         as p10_price_eur_mwh,
        round(p90_price_eur_mwh, 2)                         as p90_price_eur_mwh,
        round(min_price_eur_mwh, 2)                         as min_price_eur_mwh,
        round(max_price_eur_mwh, 2)                         as max_price_eur_mwh,
        round(p90_price_eur_mwh - p10_price_eur_mwh, 2)     as p10_p90_spread_eur_mwh,
        round(stddev_price_eur_mwh, 2)                      as stddev_price_eur_mwh,

        round(mean_deviation_eur_mwh, 2)                    as mean_deviation_eur_mwh,

        negative_slots,
        round(negative_slots::numeric / slots_observed, 4)  as negative_slot_share,

        round(mean_abs_ramp_eur_mwh, 2)                     as mean_abs_ramp_eur_mwh,
        round(max_abs_ramp_eur_mwh, 2)                      as max_abs_ramp_eur_mwh

    from profile

)

select * from final