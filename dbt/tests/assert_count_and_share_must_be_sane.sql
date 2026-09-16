select dutch_date, slots_present, negative_slots, peak_slots,
       forward_filled_slots, negative_slot_share
       from {{ ref('agg_day_ahead_price_daily') }}
where  negative_slots > slots_present
or peak_slots > slots_present 
or forward_filled_slots > slots_present 
or negative_slot_share < 0
or negative_slot_share > 1 