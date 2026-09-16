# Design Decisions

Why Onbalans is built the way it is. Each entry states the decision, the
alternative that was rejected, and the reason.

---

## Why this project exists

TenneT publishes Dutch grid imbalance data at 12-second resolution through two
endpoints with very different economics:

- **`/latest`** — live, 10 requests/minute, no daily cap. Returns ~30 minutes.
- **historical** — 8 requests **per day**, maximum 4-hour window.

A full day of history costs 6 requests against a daily budget of 8. So roughly
1.3 days of history can be recovered per day that passes — while time itself
moves forward at one day per day. Backfilling a year would take about 274 days
of spending the entire quota on nothing else.

**Bulk historical backfill is therefore not viable.** The live tailer is the
archive. The dataset only comes into existence by capturing it continuously as
it happens.

That is the project's reason to exist: *high-resolution grid data can only be
assembled in real time, because the rate limits make retroactive collection
impractical. Uptime is the product.*

It also reframes the second component. The 8/day budget is not a backfill
budget — it is a **recovery** budget for patching small gaps, and it needs a
spending policy.

---

## Storage

### JSONL rather than a JSON array

A JSON array must close with `]`, so appending a record means rewriting the
whole file. At ~300 records per hour that is absurd, and a crash mid-rewrite
corrupts everything already written.

JSON Lines — one complete JSON object per line — gives three properties:

- **Appendable.** Adding a record is writing one line at the end.
- **Crash-safe.** A process killed mid-write loses at most the final partial
  line. Every complete line before it remains valid.
- **Streamable.** A 400 MB file can be processed line by line without loading
  it into memory.

### JSONL rather than Parquet for bronze

Parquet is columnar, compressed, and far faster for analytical queries. It is
the right choice for silver and gold. It is the wrong choice for capture:

- It cannot be appended row by row. Records buffer in memory and are written in
  row groups, so a crash loses everything still buffered.
- A truncated Parquet file is entirely unreadable — the metadata footer sits at
  the end, and without it the file is a brick. A truncated JSONL file loses one
  line.
- It requires a schema up front, which is precisely what bronze is designed not
  to assume.
- It cannot hold a failed fetch. An error record with a null body and an
  exception string is not a row of grid data.

**JSONL for capture because it is append-safe and schema-free; Parquet for
analysis because it is fast and compact.** Different layers, different physics.

### Files rather than writing straight to Postgres

Capture is the only irreplaceable step. Everything downstream can be re-run
from bronze; nothing can rebuild bronze.

A database write can fail in ways a file append cannot: connection pool
exhausted, deadlock, network partition, disk full on the database server,
Postgres restarting, a migration holding a lock. Each of those would become a
reason capture stops — letting a recoverable problem cause an unrecoverable
loss.

A second reason: a database write requires a schema, and a schema requires
having already decided what the fields mean. TenneT sends every number as a
string, some fields are null, and a new column next year would either fail the
insert or be silently dropped. A file append stores whatever arrived.

**Accepted cost:** two systems to operate, and data is not queryable until the
loader runs.

### Full responses stored, not just new intervals

99.3% of what is fetched is duplicate. Parsing each response and storing only
unseen intervals would cut storage roughly 150×, to about 2 MB/day.

Rejected because:

- It requires parsing inside the tailer, so a malformed response would crash
  the process — destroying the evidence of the anomaly most worth having.
- It requires the tailer to remember what it has already seen. State means a
  file or table that can be lost or corrupted, and a restart that has to reload
  it correctly. That adds a failure category to the most critical process.
- It destroys evidence permanently. If TenneT ever *restates* a value —
  publishes 237.0 for an interval and later corrects it to 241.0 — full
  responses preserve both versions. Capture-time deduplication makes the
  correction invisible forever.

Deduplicating at capture is cheap in storage and expensive in information.
Compressed bronze costs roughly 11 GB/year, about €1/month of disk.

*(Open question: whether restatements actually occur. Comparing overlapping
windows for the same `timeInterval_start` would answer it.)*

### Body stored as text, not parsed JSON

Escaped text is uglier to read. It is stored that way because **bronze means
exactly what arrived, unaltered.** If TenneT returns malformed JSON, an HTML
error page, or a truncated response, parsing would raise an exception and lose
the evidence at the moment it matters most.

Parsing is a silver-layer job. Bronze does not interpret; it preserves.

### One file per hour

One file per response would mean 7,200 files/day and 2.6 million/year. Listing
such a folder takes minutes, and opening a file has a fixed cost regardless of
size, so reading many small files is far slower than reading a few large ones.

Hourly files: 24/day, ~300 responses each, ~15 MB.

### Hive-style partitioning: `date=.../hour=.../`

The folder name carries information. Query engines (DuckDB, Spark, Fabric) read
the directory names, open only the partitions needed, and expose `date` and
`hour` as columns automatically — without those values being stored in the data.
This is partition pruning, and it is also the convention Microsoft Fabric uses
for lakehouse files.

### Partitioned by UTC ingestion time, not event time

The Netherlands is UTC+2 in summer, so a UTC-dated folder spans two Dutch
evenings, and `sequence` (which resets at Dutch local midnight) has two
generations inside one file. This is deliberate:

**Bronze partitions by ingestion time. Silver partitions by event time.**

Ingestion time is a fact about our system that can never change and requires no
parsing to determine. Event time is a fact about the data, and is what analysis
needs — so silver reorganises around it.

Partitioning bronze by event time would require parsing the response to decide
where to write it, reintroducing every problem above, and a late-arriving
record would mean writing into an already-closed partition.

### Filename repeats the date and hour already in the path

Files get copied, downloaded, and emailed away from their folders.
`responses-2026-08-20T14.jsonl` is self-describing anywhere it lands.

### No compression in the tailer

A gzip file interrupted mid-write is often unreadable in its entirety. A plain
text file interrupted mid-write loses one line. The tailer is the process most
likely to be killed unexpectedly, so it writes in the format that survives being
killed. A separate job compresses hours that are already finished.

---

## The tailer

### 12-second poll interval

TenneT allows 10 requests/minute; 12 seconds gives 5/minute — half the ceiling.
TenneT's own recommendation is 5 polls/minute.

The headroom is not timidity. Exceeding the limit may temporarily block the API
key, and a blocked key stops capture, which is the unrecoverable step. The cost
of being conservative is nothing.

**Deviation from TenneT's advice:** they suggest polling one second after each
12-second event (`:13`, `:25`, `:37`) to catch each event just after it lands.
Measured publication lag is ~2m13s–2m22s, so aligning to the second buys
nothing. We follow their *rate* and ignore their *phase*.

### Timeout (10s) must be shorter than the poll interval (12s)

If a request could hang for longer than the interval, requests would stack on
top of each other — producing multiple simultaneous connections precisely when
the API is already unhealthy, and breaching the rate limit.

This is a constraint between two constants, not a preference. Changing one
requires checking the other.

### No retry logic

Each response covers ~30 minutes and polling is every 12 seconds, so any
interval appears in roughly 150 consecutive responses. A failed poll loses
nothing — the next one already contains it.

**The window is the retry.** Adding retries would spend rate limit (the
scarcest resource) to buy tolerance already available for free, and would add a
code path that could itself breach the limit.

Consequence: the tailer can be dead for 29 minutes and lose no data at all.
That is stronger than any retry policy would provide.

### Skip missed slots; never burst to catch up

When the work overruns and the scheduled poll time has passed, the tempting
response is to fire requests back to back until back on schedule. That is
exactly how the 10/minute limit gets breached — at the moment the API is
already struggling.

Missed slots are abandoned instead. Safe because of the window geometry above:
skipping polls does not skip data.

### Absolute scheduling rather than `sleep(12)`

Sleeping a fixed duration *after* each poll adds the request duration on top of
the interval. At 0.4s per request the real cycle is 12.4s — 232 fewer polls per
day than intended, silently, and it worsens as the API slows.

Instead, `next_poll` holds an absolute due time advanced by exactly 12 each
cycle, and the sleep absorbs whatever remains. The cadence stays exactly 12
seconds regardless of how long the work took.

### Session created outside the loop

Every HTTPS request otherwise pays DNS lookup, TCP handshake and TLS
negotiation — ~250ms of pure overhead. A `requests.Session` keeps the connection
open and reuses it.

Created inside the loop it would be new every cycle, with nothing to reuse.
Observed in practice: first request 1312ms, subsequent requests ~570ms.

### fsync on every write

`f.write()` fills a Python buffer. `f.flush()` hands it to the operating system
(survives a process crash). `os.fsync()` forces it to physical disk (survives a
power cut).

fsync costs a few milliseconds because it waits for hardware, which is why
high-throughput systems avoid it per-write. At 5 writes/minute the cost is
irrelevant, so the guarantee is taken.

*Reasoned, not measured.*

### Every fetch writes a record, including failures

A record with an error means the tailer was running and TenneT did not answer.
A record with status 500 means TenneT's server broke. No record at all means
the tailer was not running.

Three different problems with three different fixes — distinguishable only
because failures leave a trace.

### Disk write failure is logged, not fatal

If the disk write raises, the error is logged and polling continues. Stopping
would turn a temporary disk problem into permanent data loss, since capture
cannot be redone. Continuing costs nothing because the next response still
covers the same window.

*(This was a bug in the first draft: `append_record` was called outside any
try/except, so a full disk would have killed capture.)*

### Lock file using `O_CREAT | O_EXCL`

Two tailers appending to the same file can interleave mid-line and corrupt each
other, invisibly — both processes report success.

A naive check-then-create leaves a gap between the two steps where both
processes see "doesn't exist" and both proceed. `O_CREAT | O_EXCL` makes the OS
perform check-and-create as a single **atomic** operation, so exactly one
succeeds.

The process ID is written into the file so a stale lock can be diagnosed.

### Graceful shutdown sets a flag rather than stopping directly

A signal arrives at an arbitrary moment, possibly mid-write. The handler
therefore does the minimum safe thing — set one boolean — and returns. Python
then resumes the interrupted operation and completes it. The loop checks the
flag at the top of the next cycle and exits normally.

If the handler tried to close files or clean up, it would be doing so from
inside a half-finished operation, causing exactly the corruption being avoided.

### Shared logging module rather than per-file setup

Five components would otherwise carry five copies of the same twelve lines,
which drift apart the day one is changed and the others are not.

- One log file per component (`tailer.log`, `recovery.log`) — the tailer writes
  7,200 lines/day and the recovery job maybe ten; in a shared file the rare
  events would be lost, and rotation driven by the tailer's volume would discard
  recovery history that is months old and still interesting.
- `RotatingFileHandler` caps total disk use at ~60 MB. Without it, an unbounded
  log would eventually fill the disk — and a full disk stops capture.
- Timestamps forced to UTC via `formatter.converter = time.gmtime`. Everything
  else in the pipeline is UTC; logging defaults to local time, which would mean
  correlating a 14:30 IST log line with a 09:00Z data record, and would silently
  change meaning when deployed to a European server.
- `logger.propagate = False` so messages are not also emitted by the root logger
  if anything else configures it.

### UTC everywhere

`datetime.now(timezone.utc)`, never bare `datetime.now()`. Local time carries no
record of which zone it was, and goes backwards an hour every autumn.

Two clocks, deliberately: **wall clock for "when did this happen"** (`fetched_at`),
**monotonic clock for "how long did it take"** (`duration_ms`). The wall clock can
jump — NTP corrections, DST, manual changes — which would occasionally produce
negative durations.

---

## Data semantics

### Deduplication key is `timeInterval_start`, not `sequence`

`sequence` looks like an identifier but resets at Dutch **local** midnight, so
the same value recurs daily. Verified arithmetically (see api-findings.md).

`timeInterval_start` is stable, unique, and independent of fetch time — which
also makes running two redundant collectors safe by construction, since their
duplicate records collapse correctly in silver.

### Gap detection compares observed timestamps, never an expected count

The obvious check is *7,200 minus how many we have*. That breaks twice a year:
the Dutch day has 7,500 intervals in October and 6,900 in March.

Sorting the observed timestamps and looking for consecutive pairs more than 12
seconds apart makes DST a non-issue by construction rather than by special case.

**Choosing a formulation where the hard case cannot arise beats handling the
hard case.**

### Bronze is read-only after write

The tailer opens files only in append mode; the inspector only in read mode.
Nothing in the codebase opens a bronze file for writing. Currently a convention
enforced by review — the stronger version would be object storage with an
immutability policy.

---

## Rejected alternatives

| Alternative | Why not |
|---|---|
| `asyncio` | Solves overlapping many concurrent waits. Five sequential requests per minute have nothing to overlap. Async is contagious — every calling function must also become async, and `requests` would be replaced by `aiohttp`. Would earn its place with many concurrent endpoints. |
| `cron` | Cannot schedule below one minute. Every run is a new process, so connection reuse is lost and backoff state has nowhere to live. |
| APScheduler | Solves those, but adds a dependency to replace ~15 understood lines, and its default missed-run behaviour is to fire immediately — the bursting that must be avoided. |
| Kafka / Redpanda | Not yet built, and possibly not needed. A broker earns its place with multiple independent consumers of one stream, or when producer and consumer run at very different speeds. Files already provide durability and replay. Adding it for the CV would be unjustified complexity. |
| Retry logic | See above — the window already provides it. |
| Direct-to-database capture | See above — couples the irreplaceable step to a system with many failure modes. |