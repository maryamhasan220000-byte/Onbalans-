-- The peak flag must agree with the stated convention: 08:00-20:00 Dutch
-- local, Monday to Friday. If the convention is ever revised, this test is
-- the second place to change, which is deliberate: it forces the edit to be
-- conscious rather than accidental.
select period_start, dutch_iso_weekday, dutch_hour, is_peak_period
from {{ ref('int_day_ahead_prices__enriched') }}
where is_peak_period
      <> (dutch_iso_weekday <= 5 and dutch_hour >= 8 and dutch_hour < 20)