{{ config(materialized='table') }}

-- Analysis-ready fact table for Dutch day-ahead prices.
-- One row per market time unit (15 minutes at PT15M, 60 at PT60M).

-- Prices are the day-ahead auction clearing price: what the market EXPECTED
-- this period to cost, decided the previous afternoon.
-- Compare against TENNET imbalance prices to see what being wrong costed

select 
      period_start,
      period_end,
      period_start_dutch,
      dutch_date,
      dutch_hour,
      dutch_iso_weekday,
      is_weekend,
      position_in_day,

      price_eur_mwh,

      is_negative_price,
      is_peak_period,
      was_forward_filled,

      daily_mean_price_eur_mwh,
      daily_stddev_price_eur_mwh,
      price_deviation_from_daily_mean_eur_mwh,
      price_rank_in_day,
      price_change_from_previous_eur_mwh,

      resolution,
      loaded_at 

from {{ ref('int_day_ahead_prices__enriched')}}