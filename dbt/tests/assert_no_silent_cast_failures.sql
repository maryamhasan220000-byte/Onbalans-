-- A null in the typed column is fine when the source text was null. It is a
-- problem when the source held text that our regex refused to parse: that
-- means TenneT's format changed and we are losing data silently.

select s.interval_start,
       s.power_afrr_in as raw_text 
from {{ source('tennet', 'balance_delta') }} s
join {{ ref('stg_tennet__balance_delta') }} t
on t.interval_start = s.interval_start
where s.power_afrr_in is not null
and trim(s.power_afrr_in) <> ''
and t.afrr_up_mw is null 