
-- The intermediate model must not gain or lose rows relative to staging.
-- Catches a filter or a fan-out join accidentally introduced.
with counts as (
    select
        (select count(*) from {{ ref('_stg_entsoe__day_ahead_prices') }}) as stg_rows,
        (select count(*) from {{ ref('int_day_ahead_prices__enriched') }}) as int_rows
)
select * from counts where stg_rows <> int_rows