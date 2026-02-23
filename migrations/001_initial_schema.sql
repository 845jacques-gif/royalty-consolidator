-- 001_initial_schema.sql
-- PostgreSQL schema for Royalty Consolidator

-- Track applied migrations
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    description TEXT
);

-- Core: deals
CREATE TABLE IF NOT EXISTS deals (
    id              SERIAL PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    currency_symbol TEXT DEFAULT '$',
    total_gross     NUMERIC(18,4) DEFAULT 0,
    total_net       NUMERIC(18,4) DEFAULT 0,
    ltm_gross       NUMERIC(18,4) DEFAULT 0,
    ltm_net         NUMERIC(18,4) DEFAULT 0,
    isrc_count      INTEGER DEFAULT 0,
    total_files     INTEGER DEFAULT 0,
    analytics       JSONB,
    audit_trail     JSONB,
    audit_summary   JSONB
);

-- Core: payor configs per deal
CREATE TABLE IF NOT EXISTS payor_configs (
    id                  SERIAL PRIMARY KEY,
    deal_id             INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    code                TEXT NOT NULL,
    name                TEXT NOT NULL,
    fmt                 TEXT DEFAULT 'auto',
    fee                 NUMERIC(8,6) DEFAULT 0,
    fx_currency         TEXT DEFAULT 'USD',
    fx_rate             NUMERIC(12,6) DEFAULT 1.0,
    statement_type      TEXT DEFAULT 'masters',
    deal_type           TEXT DEFAULT 'artist',
    artist_split        NUMERIC(8,4),
    territory           TEXT,
    contract_pdf_gcs    TEXT,
    contract_summary    JSONB,
    expected_start      INTEGER,
    expected_end        INTEGER,
    file_count          INTEGER DEFAULT 0,
    detected_currencies TEXT[],
    file_inventory      JSONB,
    UNIQUE(deal_id, code)
);

-- Core: statement rows (can be millions)
CREATE TABLE IF NOT EXISTS statement_rows (
    id              BIGSERIAL PRIMARY KEY,
    deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    payor_config_id INTEGER NOT NULL REFERENCES payor_configs(id) ON DELETE CASCADE,
    statement_date  TEXT,
    royalty_type    TEXT,
    payor           TEXT,
    isrc            TEXT,
    iswc            TEXT,
    upc             TEXT,
    other_identifier TEXT,
    title           TEXT,
    artist          TEXT,
    release_date    TEXT,
    release_date_source TEXT,
    source          TEXT,
    deal            TEXT,
    delivery_type   TEXT,
    territory       TEXT,
    fx_original     TEXT,
    units           NUMERIC(18,4) DEFAULT 0,
    gross_earnings  NUMERIC(18,4) DEFAULT 0,
    fees            NUMERIC(18,4) DEFAULT 0,
    net_receipts    NUMERIC(18,4) DEFAULT 0,
    payable_share   NUMERIC(18,4) DEFAULT 0,
    third_party_share NUMERIC(18,4) DEFAULT 0,
    net_earnings    NUMERIC(18,4) DEFAULT 0,
    period          INTEGER,
    distributor     TEXT,
    country         TEXT,
    keep_columns    JSONB
);

CREATE INDEX IF NOT EXISTS idx_statement_rows_deal ON statement_rows(deal_id);
CREATE INDEX IF NOT EXISTS idx_statement_rows_deal_period ON statement_rows(deal_id, period);
CREATE INDEX IF NOT EXISTS idx_statement_rows_deal_isrc ON statement_rows(deal_id, isrc);

-- Core: ISRC metadata per payor
CREATE TABLE IF NOT EXISTS isrc_meta (
    id              SERIAL PRIMARY KEY,
    deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    payor_config_id INTEGER NOT NULL REFERENCES payor_configs(id) ON DELETE CASCADE,
    identifier      TEXT NOT NULL,
    title           TEXT,
    artist          TEXT,
    total_gross     NUMERIC(18,4) DEFAULT 0,
    total_net       NUMERIC(18,4) DEFAULT 0,
    total_sales     NUMERIC(18,4) DEFAULT 0,
    first_period    INTEGER,
    last_period     INTEGER,
    UNIQUE(deal_id, payor_config_id, identifier)
);

-- Core: monthly summary
CREATE TABLE IF NOT EXISTS monthly_summary (
    id              SERIAL PRIMARY KEY,
    deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    payor_config_id INTEGER NOT NULL REFERENCES payor_configs(id) ON DELETE CASCADE,
    identifier      TEXT NOT NULL,
    period          INTEGER NOT NULL,
    gross           NUMERIC(18,4) DEFAULT 0,
    net             NUMERIC(18,4) DEFAULT 0,
    sales           NUMERIC(18,4) DEFAULT 0,
    UNIQUE(deal_id, payor_config_id, identifier, period)
);

-- Core: distributor summary
CREATE TABLE IF NOT EXISTS distributor_summary (
    id              SERIAL PRIMARY KEY,
    deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
    payor_config_id INTEGER NOT NULL REFERENCES payor_configs(id) ON DELETE CASCADE,
    distributor     TEXT NOT NULL,
    total_gross     NUMERIC(18,4) DEFAULT 0,
    total_net       NUMERIC(18,4) DEFAULT 0,
    total_sales     NUMERIC(18,4) DEFAULT 0
);

-- Mappings (migrated from SQLite)
CREATE TABLE IF NOT EXISTS fingerprints (
    fingerprint     TEXT PRIMARY KEY,
    column_names    JSONB NOT NULL,
    mapping         JSONB NOT NULL,
    source_label    TEXT DEFAULT '',
    use_count       INTEGER DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS synonyms (
    raw_name        TEXT PRIMARY KEY,
    canonical       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_log (
    id              SERIAL PRIMARY KEY,
    filename        TEXT NOT NULL,
    fingerprint     TEXT,
    mapping_used    JSONB,
    row_count       INTEGER DEFAULT 0,
    qc_warnings     INTEGER DEFAULT 0,
    qc_errors       INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Caches
CREATE TABLE IF NOT EXISTS enrichment_cache (
    cache_key       TEXT PRIMARY KEY,
    release_date    TEXT,
    source          TEXT,
    track_name      TEXT,
    artist_name     TEXT,
    looked_up       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS isrc_cache (
    isrc            TEXT PRIMARY KEY,
    release_date    TEXT,
    track_name      TEXT,
    artist_name     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GCS file tracking
CREATE TABLE IF NOT EXISTS gcs_files (
    id              SERIAL PRIMARY KEY,
    deal_id         INTEGER REFERENCES deals(id) ON DELETE CASCADE,
    file_type       TEXT NOT NULL,
    gcs_path        TEXT UNIQUE NOT NULL,
    original_name   TEXT,
    size_bytes      BIGINT,
    content_type    TEXT,
    uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Record this migration
INSERT INTO schema_version (version, description)
VALUES (1, 'Initial schema: deals, payor_configs, statement_rows, isrc_meta, monthly_summary, distributor_summary, fingerprints, synonyms, import_log, enrichment_cache, isrc_cache, gcs_files')
ON CONFLICT (version) DO NOTHING;
