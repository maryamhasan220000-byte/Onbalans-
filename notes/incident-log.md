## 2026-08-25 — Five bugs in the inspector, found by testing not reading

**Context.** The bronze inspector had been written and reviewed by eye twice.
Before running it against real captured data, it was tested deliberately: once
to check an uncertain language behaviour, once by feeding it seven records built
to be malformed.

**Reading found nothing. Testing found five problems, three of them crashes.**

---

### 1. Naive vs timezone-aware datetimes → crash

**Where:** `analyse`, parsing `fetched_at`; and `parse_utc`.

Python has two kinds of datetime — *aware* (carries a timezone) and *naive*
(does not) — and refuses to compare or subtract one of each, because a naive
value has no defined meaning to compare against.

`datetime.fromisoformat` returns whichever it is given, silently. The tailer
always writes `+00:00`, so records are aware. But a single naive timestamp
(hand-edited file, foreign component, copied data) would crash the whole run at
`min(stats.fetch_times)` in the report and at the lag subtraction in `analyse`.

Verified with a six-line script:

```
min() FAILS: can't compare offset-naive and offset-aware datetimes
subtraction FAILS: can't subtract offset-naive and offset-aware datetimes
```

**Fix.** Attach UTC when the label is missing:

```python
if fetched_at.tzinfo is None:
    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
```

`.replace(tzinfo=...)` labels without shifting the clock value. Correct here
because everything this pipeline writes genuinely is UTC — the label was absent,
not wrong. It would be the wrong fix if the timestamp might be local time.

---

### 2. Health ratio used the wrong denominator → warned on every healthy run

**Where:** `report`, grid interval coverage.

The check was meant to confirm that responses overlap — the assumption the whole
no-retry design rests on. Two different quantities got conflated:

- *fraction of handled measurements that are new* ≈ 0.007
- *new intervals per response* ≈ 1.0

The threshold (`> 0.5`) was written for the first; the code computed the second.

| responses | unique | ÷ responses | ÷ total points |
|---|---|---|---|
| 200 | 349 | 1.745 | 0.0116 |
| 1,000 | 1,149 | 1.149 | 0.0077 |
| 4,872 | 5,021 | 1.031 | 0.0069 |

Healthy data gives ~1.03, which exceeds 0.5 — so the warning fired **every
time**. A check that always fires is worse than no check: it trains you to
ignore your own tooling.

The second column is also the only stable one. Dividing by responses moves with
run length (1.745 → 1.031), so no single threshold works for both a short and a
long run. Dividing by total points stays ~0.007 regardless, and goes to 1.0 when
overlap genuinely fails — so 0.5 sits sensibly between the two.

**Fix.** Added `total_points` to `CaptureStats`, incremented it in `analyse`,
and divided by it instead. Verified on a 600-response fixture: **0.0083, no
warning, exit 0**, and 749 unique intervals from 600 responses — exactly the
600 + 149 the arithmetic predicts.

---

### 3–5. Container checked, contents trusted → three crashes

**Where:** `extract_points`, at all three nesting levels.

`isinstance(series_list, list)` asks whether the container is a list. It never
asks whether the things *inside* it are the expected type — and a list can hold
anything.

So `"TimeSeries": ["some string"]` passes the container check, and then:

```python
series.get("Period", [])
→ AttributeError: 'str' object has no attribute 'get'
```

Strings have no `.get`. Only dictionaries do. The same hole existed three times:
`series.get(...)`, `period.get(...)`, `point.get(...)`.

Found by feeding seven deliberately malformed records. **The first one crashed
it immediately** — a tool built to inspect damaged data, dying on the first
damaged thing it saw.

**Fix.** `isinstance(..., dict)` at each level, recording an anomaly and using
`continue` rather than `return` — one broken entry does not invalidate its
siblings.

After the fix, the same seven records produced a clean report naming every
problem, with exit code 1 and no crash:

```
-- Structure anomalies --
  empty body                                       2
  TimeSeries entry is not an object                1
  Period entry is not an object                    1
  point is not an object                           1
  unexpected top-level structure                   1
```

---

### What this changed about how the project is built

**Reading code checks whether it matches your intention. The bug is usually in
the intention.** Two careful read-throughs found nothing; two short experiments
found five problems.

Two habits adopted:

- When unsure how a language feature behaves, spend two minutes running it
  rather than reasoning about it.
- Feed each component the input it is specifically supposed to handle badly. All
  three `AttributeError`s came from asking one question — *what if the data is
  shaped wrong?* — which was the tool's stated purpose and had never been tested.

This is also the concrete argument for the outstanding unit tests: not ceremony,
but the only reliable way to find this class of bug.

2026-09-01 — publication lag column was measuring the wrong thing. int_balance_delta__enriched.publication_lag_seconds reported ~1,450s while the bronze inspector reported ~126s from the same data. Cause: each interval arrives in ~150 overlapping responses, and the loader keeps the most recent fetched_at, so fetched_at - interval_end measures time-to-last-refetch, not time-to-publication. Renamed to last_refetch_lag_seconds. Proper fix requires a first_seen_at column set on insert and preserved on conflict.

Mechanism usage verified on 47,125 rows: aFRR 93%, PICASSO 92%, IGCC 65%, mFRRda 0%, MARI 0%. net_local_activation_mw is therefore currently identical to net_afrr_mw.

State distribution: up 43.0%, down 43.1%, both 7.2%, none 6.6%. Near-symmetric, as expected for a grid oscillating around balance.