{{ config(materialized='table') }}

-- Hourly rollup for dashboards. Charting 300 points per hour is slow and
-- unreadable; charting one row per hour is instant.
--
-- The Netherlands is a whole number of hours from UTC, so UTC hour buckets
-- and Dutch hour buckets share the same boundaries.

with bucketed as (

    select
        date_trunc('hour', interval_start)                  as bucket_start,
        count(*)                                            as intervals_present,

        avg(net_afrr_mw)                                    as avg_net_afrr_mw,
        min(net_afrr_mw)                                    as min_net_afrr_mw,
        max(net_afrr_mw)                                    as max_net_afrr_mw,
        avg(net_local_activation_mw)                        as avg_net_local_mw,

        avg(mid_price_eur_mwh)                              as avg_mid_price_eur_mwh,
        min(mid_price_eur_mwh)                              as min_mid_price_eur_mwh,
        max(mid_price_eur_mwh)                              as max_mid_price_eur_mwh,
        stddev_samp(mid_price_eur_mwh)                      as stddev_mid_price_eur_mwh,

        count(*) FILTER (WHERE afrr_regulation_state = 'up')   as intervals_up,
        count(*) FILTER (WHERE afrr_regulation_state = 'down') as intervals_down,
        count(*) FILTER (WHERE afrr_regulation_state = 'both') as intervals_both,
        count(*) FILTER (WHERE afrr_regulation_state = 'none') as intervals_none,

        avg(last_refetch_lag_seconds)                        as avg_last_refetch_lag_seconds,
        sum(null_component_count)                           as total_null_components

    from {{ ref('fct_balance_delta') }}
    group by 1

)

select
    bucket_start,
    bucket_start at time zone 'Europe/Amsterdam'            as bucket_start_dutch,
    (bucket_start at time zone 'Europe/Amsterdam')::date    as dutch_date,
    extract(hour from bucket_start at time zone 'Europe/Amsterdam')::int
                                                            as dutch_hour,

    intervals_present,
    -- A complete hour holds 3600/12 = 300 intervals. Below that means the
    -- tailer was down or TenneT published nothing.
    round(intervals_present / 300.0, 4)                     as completeness_ratio,

    round(avg_net_afrr_mw, 2)                               as avg_net_afrr_mw,
    round(min_net_afrr_mw, 2)                               as min_net_afrr_mw,
    round(max_net_afrr_mw, 2)                               as max_net_afrr_mw,
    round(avg_net_local_mw, 2)                              as avg_net_local_mw,

    round(avg_mid_price_eur_mwh, 2)                         as avg_mid_price_eur_mwh,
    round(min_mid_price_eur_mwh, 2)                         as min_mid_price_eur_mwh,
    round(max_mid_price_eur_mwh, 2)                         as max_mid_price_eur_mwh,
    round(stddev_mid_price_eur_mwh, 2)                      as stddev_mid_price_eur_mwh,

    intervals_up,
    intervals_down,
    intervals_both,
    intervals_none,

    round(avg_last_refetch_lag_seconds, 1)                   as avg_last_refetch_lag_seconds,
    total_null_components

from bucketed