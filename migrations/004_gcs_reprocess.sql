-- Migration 004: Add gcs_files column to payor_configs for reprocessing without re-upload
-- Also add source_currency which was missing from the schema

ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS gcs_files JSONB;
ALTER TABLE payor_configs ADD COLUMN IF NOT EXISTS source_currency TEXT DEFAULT 'auto';

INSERT INTO schema_version (version, description)
VALUES (4, 'Add gcs_files JSONB and source_currency to payor_configs for reprocessing')
ON CONFLICT (version) DO NOTHING;
