{{ config(materialized='view') }}

-- One row per market time unit, with derived columns. Grain is unchanged
-- from staging: no aggregation, no filtering, no join to TenneT.
--
-- The join to grid measurements happens in the marts layer, because the two
-- sources have different grains (12 seconds vs 15 minutes) and joining here
-- would couple this chain to TenneT for every downstream consumer.

with staged as (

    select * from {{ ref('_stg_entsoe__day_ahead_prices') }}

),

dutch_calendar as (

    -- Named timezone, never a hardcoded offset, so daylight saving is
    -- handled by Postgres. Matches the TenneT intermediate model so both
    -- sides group identically.
    select
        *,
        period_start at time zone 'Europe/Amsterdam'          as period_start_dutch,
        (period_start at time zone 'Europe/Amsterdam')::date  as dutch_date,
        extract(hour from period_start at time zone 'Europe/Amsterdam')::int
                                                             as dutch_hour,
        extract(isodow from period_start at time zone 'Europe/Amsterdam')::int
                                                             as dutch_iso_weekday
    from staged

),

windowed as (

    -- Window functions compute across a group of rows while keeping every
    -- row, so the grain is preserved. GROUP BY would collapse 96 slots into
    -- one row per day and lose the detail.
    select
        *,

        -- The day's own mean. Absolute price is not comparable across days:
        -- EUR 150 was expensive on 2026-08-21 (range 126-212) and cheap on
        -- 2026-09-05 (range -15 to 245).
        avg(price_eur_mwh) over (partition by dutch_date)
                                                    as daily_mean_price_eur_mwh,

        stddev_samp(price_eur_mwh) over (partition by dutch_date)
                                                    as daily_stddev_price_eur_mwh,

        -- 1 = cheapest slot of the day. Ordinal position avoids picking a
        -- euro threshold that would mean different things on different days.
        rank() over (partition by dutch_date order by price_eur_mwh)
                                                    as price_rank_in_day,

        -- Deliberately NOT partitioned by day: the change from 23:45 to
        -- 00:00 is a real ramp, and partitioning would null it every night.
        lag(price_eur_mwh) over (order by period_start)
                                                    as previous_price_eur_mwh
    from dutch_calendar

),

final as (

    select
        period_start,
        period_end,
        period_start_dutch,
        dutch_date,
        dutch_hour,
        dutch_iso_weekday,
        dutch_iso_weekday >= 6 as is_weekend,
        position_in_day,

        price_eur_mwh,
        was_forward_filled,

        price_eur_mwh < 0                            as is_negative_price,

        round(daily_mean_price_eur_mwh, 4)           as daily_mean_price_eur_mwh,
        round(daily_stddev_price_eur_mwh, 4)         as daily_stddev_price_eur_mwh,
        round(price_eur_mwh - daily_mean_price_eur_mwh, 4)
                                            as price_deviation_from_daily_mean_eur_mwh,
        price_rank_in_day,

        previous_price_eur_mwh,
        price_eur_mwh - previous_price_eur_mwh       as price_change_from_previous_eur_mwh,

        -- EPEX/EEX convention: peak is 08:00-20:00 CET on working days.
        -- CONVENTION, not derived from the data. Verify against the specific
        -- contract before relying on it for settlement work; changing it is
        -- a one-line edit here rather than a hunt through downstream models.
        (dutch_iso_weekday <= 5 and dutch_hour >= 8 and dutch_hour < 20)
                                                     as is_peak_period,

        resolution,
        curve_type,
        currency,
        price_unit,
        document_mrid,
        created_at_source,
        source_file,
        loaded_at

    from windowed

)

select * from final