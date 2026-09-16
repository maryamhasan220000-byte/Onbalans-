with source as (select * from {{ source('tennet', 'balance_delta') }}),
renamed as (
    select
          interval_start,
          interval_end::timestamptz     as interval_end,
          sequence::integer             as sequence_within_dutch_day,

          {{ safe_numeric('power_afrr_in') }}    as afrr_up_mw,
          {{ safe_numeric('power_afrr_out' )}}   as afrr_down_mw,
          {{ safe_numeric('power_igcc_in' )}}    as igcc_up_mw,
          {{ safe_numeric('power_igcc_out') }}   as igcc_down_mw,
          {{ safe_numeric('power_mfrrda_in') }}    as mfrrda_up_mw,
          {{ safe_numeric('power_mfrrda_out') }}   as mfrrda_down_mw,
          {{ safe_numeric('power_picasso_in') }}   as picasso_up_mw,
          {{ safe_numeric('power_picasso_out') }}  as picasso_down_mw,
          {{ safe_numeric('power_mari_in') }}      as mari_up_mw,
          {{ safe_numeric('power_mari_out') }}     as mari_down_mw,

          {{ safe_numeric('max_upw_regulation_price') }}    as max_up_price_eur_mwh,
          {{ safe_numeric('min_downw_regulation_price') }}  as min_down_price_eur_mwh,
          {{ safe_numeric('mid_price') }}                   as mid_price_eur_mwh,

          fetched_at,
          source_file,
          loaded_at
    from source        
)
SELECT * FROM renamed 