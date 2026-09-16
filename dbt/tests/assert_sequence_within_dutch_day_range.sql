-- The Dutch day holds 7,200 twelve-second blocks normally, 7,500
-- on the October DST and 6,900 in March. Anything outside 1...7500 
-- means our understanding of the sequence field is wrong.

select interval_start, sequence_within_dutch_day 
from {{ ref('stg_tennet__balance_delta') }}
where sequence_within_dutch_day < 1
or sequence_within_dutch_day > 7500