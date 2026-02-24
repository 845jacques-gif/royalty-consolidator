-- 003_forecast_and_templates.sql
-- Adds tables for forecasts, deal templates, and delta reports

-- Guard: only run if not already applied
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM schema_version WHERE version = 3) THEN

        -- Forecast configurations and results
        CREATE TABLE IF NOT EXISTS forecasts (
            id              SERIAL PRIMARY KEY,
            deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
            config          JSONB NOT NULL DEFAULT '{}',
            result_summary  JSONB,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_forecasts_deal ON forecasts(deal_id);

        -- Reusable deal templates (payor configs + settings)
        CREATE TABLE IF NOT EXISTS deal_templates (
            id              SERIAL PRIMARY KEY,
            name            TEXT UNIQUE NOT NULL,
            payor_configs   JSONB NOT NULL DEFAULT '[]',
            settings        JSONB NOT NULL DEFAULT '{}',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        -- Delta reports (before/after comparison on re-runs)
        CREATE TABLE IF NOT EXISTS delta_reports (
            id              SERIAL PRIMARY KEY,
            deal_id         INTEGER NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
            report          JSONB NOT NULL DEFAULT '{}',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_delta_reports_deal ON delta_reports(deal_id);

        -- Add forecast and delta columns to deals
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS forecast_config JSONB;
        ALTER TABLE deals ADD COLUMN IF NOT EXISTS latest_delta JSONB;

        INSERT INTO schema_version (version, description)
        VALUES (3, 'Add forecasts, deal_templates, delta_reports tables');

    END IF;
END $$;
