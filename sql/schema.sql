-- Onbalans raw layer.
--
-- The loader writes here. Values arrive as TenneT sent them (text), except
-- interval_start, which must be a real timestamp because it is the primary
-- key and because TimescaleDB partitions on a time column.
--
-- Casting, null handling and business logic all happen later, in dbt.
--
-- Safe to run repeatedly: every statement is IF NOT EXISTS.
-- Onbalans raw layer.
--
-- The loader writes here. Values arrive as TenneT sent them (text), except
-- interval_start, which must be a real timestamp because it is the primary
-- key and because TimescaleDB partitions on a time column.
--
-- Casting, null handling and business logic all happen later, in dbt.
--
-- Safe to run repeatedly: every statement is IF NOT EXISTS.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS raw;

-- ----------------------------------------------------------------- data

CREATE TABLE IF NOT EXISTS raw.balance_delta (
    -- Typed: primary key, and TimescaleDB partitions on it.
    interval_start              timestamptz NOT NULL,

    -- Everything TenneT sends, stored exactly as sent.
    interval_end                text,
    sequence                    text,

    power_afrr_in               text,
    power_afrr_out              text,
    power_igcc_in               text,
    power_igcc_out              text,
    power_mfrrda_in             text,
    power_mfrrda_out            text,
    power_picasso_in            text,
    power_picasso_out           text,
    power_mari_in               text,
    power_mari_out              text,

    max_upw_regulation_price    text,
    min_downw_regulation_price  text,
    mid_price                   text,

    -- Lineage: not from TenneT. Records how this row got here.
    fetched_at                  timestamptz NOT NULL,
    source_file                 text        NOT NULL,
    loaded_at                   timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (interval_start)
);

-- ------------------------------------------------------------- tracking

CREATE TABLE IF NOT EXISTS raw.loaded_files (
    path          text PRIMARY KEY,
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    record_count  integer     NOT NULL,
    point_count   integer     NOT NULL
);

-- ------------------------------------------------------------ indexing

-- Convert to a hypertable: same SQL interface, but physically split into
-- one chunk per day so time-filtered queries skip irrelevant data.
SELECT create_hypertable(
    'raw.balance_delta',
    'interval_start',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- interval_start is already indexed by the primary key. This second index
-- makes "what did we load recently?" fast, for monitoring.
CREATE INDEX IF NOT EXISTS idx_balance_delta_fetched_at
    ON raw.balance_delta (fetched_at DESC);