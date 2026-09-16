CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.day_ahead_prices (
    period_start         timestamptz NOT NULL,
    period_end           timestamptz NOT NULL,
    position             integer     NOT NULL,
    price_text           text,
    resolution           text,
    curve_type           text,
    currency             text,
    price_unit           text,
    was_forward_filled   boolean     NOT NULL DEFAULT false,
    document_mrid        text,
    created_at_source    timestamptz,
    source_file          text        NOT NULL,
    loaded_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (period_start)
);

CREATE TABLE IF NOT EXISTS raw.loaded_entsoe_files (
    path                  text PRIMARY KEY,
    loaded_at             timestamptz NOT NULL DEFAULT now(),
    row_count             integer     NOT NULL,
    forward_filled_count  integer     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_day_ahead_loaded_at
    ON raw.day_ahead_prices (loaded_at DESC);