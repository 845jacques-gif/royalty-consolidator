#!/usr/bin/env python3
"""
One-time data migration script.
Migrates existing local deals, mappings, and caches to PostgreSQL + GCS.

Usage:
    python migrate_data.py                  # migrate everything
    python migrate_data.py --deals-only     # only deals
    python migrate_data.py --caches-only    # only ISRC + enrichment caches
    python migrate_data.py --mappings-only  # only SQLite mappings
"""

import argparse
import json
import logging
import os
import pickle
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('migrate')

from dotenv import load_dotenv
load_dotenv()

import db
import storage


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEALS_DIR = os.path.join(BASE_DIR, 'deals')
MAPPINGS_DB = os.path.join(BASE_DIR, 'mappings.db')
ISRC_CACHE = os.path.join(BASE_DIR, 'isrc_cache.json')
ENRICHMENT_CACHE = os.path.join(BASE_DIR, 'release_date_cache.json')


def migrate_mappings():
    """Migrate fingerprints, synonyms, and import_log from SQLite to PostgreSQL."""
    if not os.path.isfile(MAPPINGS_DB):
        log.info("No mappings.db found — skipping")
        return

    conn = sqlite3.connect(MAPPINGS_DB)
    conn.row_factory = sqlite3.Row

    # Fingerprints
    rows = conn.execute('SELECT * FROM fingerprints').fetchall()
    count = 0
    for r in rows:
        try:
            cols = json.loads(r['column_names'])
            mapping = json.loads(r['mapping'])
            db.save_mapping_db(r['fingerprint'], cols, mapping, r['source_label'] or '')
            count += 1
        except Exception as e:
            log.warning("Failed to migrate fingerprint %s: %s", r['fingerprint'][:16], e)
    log.info("Migrated %d fingerprints", count)

    # Synonyms
    rows = conn.execute('SELECT * FROM synonyms').fetchall()
    syn_mapping = {r['raw_name']: r['canonical'] for r in rows}
    if syn_mapping:
        db.save_synonyms_db(syn_mapping)
        log.info("Migrated %d synonyms", len(syn_mapping))

    # Import log
    rows = conn.execute('SELECT * FROM import_log ORDER BY created_at DESC').fetchall()
    count = 0
    for r in rows:
        try:
            mapping = json.loads(r['mapping_used']) if r['mapping_used'] else {}
            db.log_import_db(r['filename'], r['fingerprint'] or '', mapping,
                             r['row_count'] or 0, r['qc_warnings'] or 0,
                             r['qc_errors'] or 0, r['status'] or 'approved')
            count += 1
        except Exception as e:
            log.warning("Failed to migrate import log entry: %s", e)
    log.info("Migrated %d import log entries", count)

    conn.close()


def migrate_caches():
    """Migrate ISRC cache and enrichment cache from JSON files to PostgreSQL."""
    # ISRC cache
    if os.path.isfile(ISRC_CACHE):
        try:
            with open(ISRC_CACHE, 'r') as f:
                isrc_data = json.load(f)
            if isrc_data:
                db.save_isrc_cache_db(isrc_data)
                log.info("Migrated %d ISRC cache entries", len(isrc_data))
        except Exception as e:
            log.error("Failed to migrate ISRC cache: %s", e)
    else:
        log.info("No isrc_cache.json found — skipping")

    # Enrichment cache
    if os.path.isfile(ENRICHMENT_CACHE):
        try:
            with open(ENRICHMENT_CACHE, 'r') as f:
                enrich_data = json.load(f)
            if enrich_data:
                db.save_enrichment_cache_db(enrich_data)
                log.info("Migrated %d enrichment cache entries", len(enrich_data))
        except Exception as e:
            log.error("Failed to migrate enrichment cache: %s", e)
    else:
        log.info("No release_date_cache.json found — skipping")


def migrate_deals():
    """Migrate all local deal directories to PostgreSQL + GCS."""
    if not os.path.isdir(DEALS_DIR):
        log.info("No deals/ directory found — skipping")
        return

    slugs = [d for d in os.listdir(DEALS_DIR)
             if os.path.isfile(os.path.join(DEALS_DIR, d, 'deal_meta.json'))]

    log.info("Found %d deals to migrate", len(slugs))

    for slug in slugs:
        deal_dir = os.path.join(DEALS_DIR, slug)
        try:
            # Load metadata
            with open(os.path.join(deal_dir, 'deal_meta.json'), 'r') as f:
                meta = json.load(f)

            # Load analytics
            analytics_path = os.path.join(deal_dir, 'analytics.json')
            analytics = {}
            if os.path.isfile(analytics_path):
                with open(analytics_path, 'r') as f:
                    analytics = json.load(f)

            # Load payor results (pickle)
            pkl_path = os.path.join(deal_dir, 'payor_results.pkl')
            payor_results = {}
            if os.path.isfile(pkl_path):
                with open(pkl_path, 'rb') as f:
                    payor_results = pickle.load(f)

            # Save to DB
            deal_name = meta.get('name', slug)
            csym = meta.get('currency_symbol', analytics.get('currency_symbol', '$'))
            deal_id = db.save_deal_to_db(slug, deal_name, payor_results, analytics,
                                         currency_symbol=csym)
            log.info("  [DB] Deal '%s' -> id=%d", slug, deal_id)

            # Upload exports to GCS
            if storage.is_available():
                exports_dir = os.path.join(deal_dir, 'exports')
                if os.path.isdir(exports_dir):
                    for fname in os.listdir(exports_dir):
                        fpath = os.path.join(exports_dir, fname)
                        if os.path.isfile(fpath):
                            try:
                                storage.upload_export(slug, fname, fpath)
                            except Exception as e:
                                log.warning("  [GCS] Failed to upload %s: %s", fname, e)

                    pp_dir = os.path.join(exports_dir, 'per_payor')
                    if os.path.isdir(pp_dir):
                        for fname in os.listdir(pp_dir):
                            fpath = os.path.join(pp_dir, fname)
                            if os.path.isfile(fpath):
                                try:
                                    storage.upload_per_payor_export(slug, fname, fpath)
                                except Exception as e:
                                    log.warning("  [GCS] Failed to upload per-payor %s: %s", fname, e)

                # Upload contracts
                contracts_dir = os.path.join(deal_dir, 'contracts')
                if os.path.isdir(contracts_dir):
                    for fname in os.listdir(contracts_dir):
                        fpath = os.path.join(contracts_dir, fname)
                        if os.path.isfile(fpath):
                            try:
                                # Try to match to a payor code
                                storage.upload_contract(slug, 'unknown', fname, fpath)
                            except Exception as e:
                                log.warning("  [GCS] Failed to upload contract %s: %s", fname, e)

        except Exception as e:
            log.error("Failed to migrate deal '%s': %s", slug, e, exc_info=True)

    log.info("Deal migration complete")


def main():
    parser = argparse.ArgumentParser(description='Migrate data to PostgreSQL + GCS')
    parser.add_argument('--deals-only', action='store_true', help='Only migrate deals')
    parser.add_argument('--caches-only', action='store_true', help='Only migrate caches')
    parser.add_argument('--mappings-only', action='store_true', help='Only migrate mappings')
    args = parser.parse_args()

    # Init DB
    if not db.init_pool():
        log.error("Cannot connect to PostgreSQL — check DB_HOST/DB_PASSWORD in .env")
        sys.exit(1)

    # Run migrations
    migrations_dir = os.path.join(BASE_DIR, 'migrations')
    if os.path.isdir(migrations_dir):
        db.run_migrations(migrations_dir)

    # Init GCS (optional)
    gcs_ok = storage.init_gcs()
    if not gcs_ok:
        log.warning("GCS not available — deal files won't be uploaded")

    any_flag = args.deals_only or args.caches_only or args.mappings_only

    if not any_flag or args.mappings_only:
        log.info("=== Migrating mappings ===")
        migrate_mappings()

    if not any_flag or args.caches_only:
        log.info("=== Migrating caches ===")
        migrate_caches()

    if not any_flag or args.deals_only:
        log.info("=== Migrating deals ===")
        migrate_deals()

    log.info("Migration complete!")


if __name__ == '__main__':
    main()
