with source as
 (
    select * from {{ source('entsoe', 'day_ahead_prices')}}
), 
renamed as(
    select 
        -- ALready typed by the loader: primary key and time dimension.
          period_start,
          period_end,
          position as position_in_day,
        -- Prices can legitimately be negative
        -- Any validation that assumes positive would drop the most interesting hours.
        {{ safe_numeric('price_text') }}  as price_eur_mwh,
        resolution,
        curve_type,
        currency,
        price_unit,
        -- True when ENTSO-E omitted this position under curveType A03 
        -- Not a published value
        -- callers filtering to observed values should exclude these.
        was_forward_filled,
        document_mrid,
        created_at_source,
        source_file,
        loaded_at

        from source 

)
 select * from renamed 