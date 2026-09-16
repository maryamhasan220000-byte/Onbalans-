-- The trap this test is guarding against 
-- postgres has two different ways to number the days of the week
-- isodow and dow, Model uses isodow - hence, checking
-- mean_peak_price comes from avg(price) of the peak-period
-- would be null if its weekend and vice versa

select dutch_date, dutch_iso_weekday, is_weekend, peak_slots,
      mean_peak_price_eur_mwh
from {{ref('agg_day_ahead_price_daily')}}
where (is_weekend and (peak_slots <> 0 or mean_peak_price_eur_mwh is not null ))
or (not is_weekend and peak_slots = 0)
