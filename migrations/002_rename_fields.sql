-- Migration 002: Rename fields and add share calculation columns
-- fx_currency -> source_currency (conceptual rename; keep DB column name for backward compat)
-- Add calc_payable, payable_pct, calc_third_party, third_party_pct to payor_configs

ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS calc_payable BOOLEAN DEFAULT FALSE;
ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS payable_pct NUMERIC(8,4) DEFAULT 0;
ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS calc_third_party BOOLEAN DEFAULT FALSE;
ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS third_party_pct NUMERIC(8,4) DEFAULT 0;

-- Note: fx_currency column is kept as-is in the database for backward compatibility.
-- The Python code maps source_currency <-> fx_currency at the application layer.
-- fx_rate is kept but always set to 1.0 (conversion moved to dashboard-time).

INSERT INTO schema_version (version, description) VALUES (2, 'Add share calculation toggles to payor_configs');
