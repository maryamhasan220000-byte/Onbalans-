{{ config(materialized='table') }}

-- One row per Dutch market day. The overview table: how expensive, how
-- volatile, how much of the day cleared below zero, and how the peak block
-- compared to off-peak.
--
-- Grouped on dutch_date rather than a UTC date because the market trades
-- Dutch days: a document runs Dutch midnight to Dutch midnight, which is
-- 22:00Z in summer and 23:00Z in winter.

with slots as (

    select * from {{ ref('fct_day_ahead_price') }}

),

daily as (

    select
        dutch_date,
        min(dutch_iso_weekday)                              as dutch_iso_weekday,
      bool_or(is_weekend) as is_weekend ,


        count(*)                                            as slots_present,
        min(period_start)                                   as first_period_start,
        max(period_end)                                     as last_period_end,
        min(resolution)                                     as resolution,

        avg(price_eur_mwh)                                  as mean_price_eur_mwh,
        min(price_eur_mwh)                                  as min_price_eur_mwh,
        max(price_eur_mwh)                                  as max_price_eur_mwh,
        stddev_samp(price_eur_mwh)                          as stddev_price_eur_mwh,

        -- Median is more robust than the mean when a handful of slots go
        -- deeply negative or spike; reporting both shows the skew.
        -- percentile_cont returns double precision, and Postgres has no
        -- round(double precision, int). Cast to numeric so the rounding
        -- below works and the value stays exact decimal like the others.
        (percentile_cont(0.5) within group (order by price_eur_mwh))::numeric
                                                            as median_price_eur_mwh,

        count(*) filter (where is_negative_price)           as negative_slots,
        min(price_eur_mwh) filter (where is_negative_price) as most_negative_price_eur_mwh,

        avg(price_eur_mwh) filter (where is_peak_period)    as mean_peak_price_eur_mwh,
        avg(price_eur_mwh) filter (where not is_peak_period)
                                                            as mean_offpeak_price_eur_mwh,
        count(*) filter (where is_peak_period)              as peak_slots,

        -- Largest single-slot move within the day, in either direction.
        max(abs(price_change_from_previous_eur_mwh))        as max_abs_ramp_eur_mwh,

        count(*) filter (where was_forward_filled)          as forward_filled_slots,
        max(loaded_at)                                      as last_loaded_at

    from slots
    group by dutch_date

),

final as (

    select
        dutch_date,
        dutch_iso_weekday,
        is_weekend, -- bool_or return true if any rows in the group is true 
        -- here we drived this purely because in group by we need this 

        slots_present,
        resolution,
        -- Expected slot count derived from the day's actual span and
        -- resolution, so 23- and 25-hour DST days are handled without a
        -- special case.
        (extract(epoch from (last_period_end - first_period_start))
         / extract(epoch from resolution::interval))::int   as slots_expected,

        round(mean_price_eur_mwh, 2)                        as mean_price_eur_mwh,
        round(median_price_eur_mwh, 2)                      as median_price_eur_mwh,
        round(min_price_eur_mwh, 2)                         as min_price_eur_mwh,
        round(max_price_eur_mwh, 2)                         as max_price_eur_mwh,
        round(max_price_eur_mwh - min_price_eur_mwh, 2)     as price_range_eur_mwh,
        round(stddev_price_eur_mwh, 2)                      as stddev_price_eur_mwh,

        negative_slots,
        round(negative_slots::numeric / slots_present, 4)   as negative_slot_share,
        round(most_negative_price_eur_mwh, 2)               as most_negative_price_eur_mwh,

        peak_slots,
        round(mean_peak_price_eur_mwh, 2)                   as mean_peak_price_eur_mwh,
        round(mean_offpeak_price_eur_mwh, 2)                as mean_offpeak_price_eur_mwh,
        round(mean_peak_price_eur_mwh - mean_offpeak_price_eur_mwh, 2)
                                                            as peak_premium_eur_mwh,

        round(max_abs_ramp_eur_mwh, 2)                      as max_abs_ramp_eur_mwh,

        forward_filled_slots,
        last_loaded_at

    from daily

)

select * from final 
