"""
PostgreSQL connection pool and all DB operations for the Royalty Consolidator.
Graceful degradation: all public functions handle unavailability cleanly.
"""

import io
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

log = logging.getLogger('royalty')

_pool = None


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

def init_pool() -> bool:
    """Initialise a threaded connection pool. Returns True on success."""
    global _pool
    try:
        import psycopg2
        from psycopg2 import pool as pg_pool

        host = os.getenv('DB_HOST', '')
        if not host:
            log.info("DB_HOST not set — PostgreSQL disabled")
            return False

        _pool = pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=int(os.getenv('DB_MAX_CONN', '10')),
            host=host,
            port=int(os.getenv('DB_PORT', '5432')),
            dbname=os.getenv('DB_NAME', 'royalty_consolidator'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            connect_timeout=5,
        )
        # Quick connectivity test
        conn = _pool.getconn()
        conn.cursor().execute('SELECT 1')
        conn.commit()
        _pool.putconn(conn)
        log.info("PostgreSQL pool initialised (%s:%s/%s)",
                 host, os.getenv('DB_PORT', '5432'), os.getenv('DB_NAME', 'royalty_consolidator'))
        return True
    except Exception as e:
        log.warning("PostgreSQL unavailable: %s", e)
        _pool = None
        return False


def is_available() -> bool:
    """Check if PostgreSQL pool is ready."""
    return _pool is not None


@contextmanager
def get_conn():
    """Context manager: yields a connection, auto-commits on success, rollbacks on error."""
    if _pool is None:
        raise RuntimeError("PostgreSQL pool not initialised")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------

def run_migrations(migrations_dir: str):
    """Apply numbered .sql files that haven't been applied yet."""
    if not is_available():
        return

    sql_files = sorted(f for f in os.listdir(migrations_dir) if f.endswith('.sql'))
    if not sql_files:
        return

    with get_conn() as conn:
        cur = conn.cursor()
        # Ensure schema_version exists (bootstrap)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                description TEXT
            )
        """)
        conn.commit()

        cur.execute("SELECT version FROM schema_version")
        applied = {row[0] for row in cur.fetchall()}

        for fname in sql_files:
            # Extract version number from filename like "001_initial_schema.sql"
            try:
                version = int(fname.split('_')[0])
            except (ValueError, IndexError):
                continue
            if version in applied:
                continue

            log.info("Applying migration %s ...", fname)
            with open(os.path.join(migrations_dir, fname), 'r') as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            log.info("Migration %s applied", fname)


# ---------------------------------------------------------------------------
# Deal CRUD
# ---------------------------------------------------------------------------

def save_deal_to_db(slug, deal_name, payor_results, analytics,
                    currency_symbol='$') -> Optional[int]:
    """Insert or update a deal and all child rows. Returns deal_id."""
    import time as _time
    _t0 = _time.time()
    with get_conn() as conn:
        cur = conn.cursor()

        # Compute totals from analytics
        total_gross = _parse_num(analytics.get('total_gross', 0))
        total_net = _parse_num(analytics.get('total_net', 0))
        ltm_gross = _parse_num(analytics.get('ltm_gross_total', analytics.get('total_gross', 0)))
        ltm_net = _parse_num(analytics.get('ltm_net_total', analytics.get('total_net', 0)))
        isrc_count = analytics.get('isrc_count', 0)
        total_files = analytics.get('total_files', 0)

        # Upsert deal
        cur.execute("""
            INSERT INTO deals (slug, name, currency_symbol, total_gross, total_net,
                               ltm_gross, ltm_net, isrc_count, total_files, analytics,
                               updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (slug) DO UPDATE SET
                name = EXCLUDED.name,
                currency_symbol = EXCLUDED.currency_symbol,
                total_gross = EXCLUDED.total_gross,
                total_net = EXCLUDED.total_net,
                ltm_gross = EXCLUDED.ltm_gross,
                ltm_net = EXCLUDED.ltm_net,
                isrc_count = EXCLUDED.isrc_count,
                total_files = EXCLUDED.total_files,
                analytics = EXCLUDED.analytics,
                updated_at = now()
            RETURNING id
        """, (slug, deal_name, currency_symbol, total_gross, total_net,
              ltm_gross, ltm_net, isrc_count, total_files,
              json.dumps(analytics, default=str)))

        deal_id = cur.fetchone()[0]

        # Delete old child rows for this deal (will re-insert)
        cur.execute("DELETE FROM payor_configs WHERE deal_id = %s", (deal_id,))

        # Insert payor configs + child data
        for code, pr in payor_results.items():
            c = pr.config
            detected_currencies = list(pr.detected_currencies) if pr.detected_currencies else []
            file_inventory = pr.file_inventory if pr.file_inventory else []

            gcs_files_json = json.dumps(c.gcs_files) if c.gcs_files else None
            cur.execute("""
                INSERT INTO payor_configs
                    (deal_id, code, name, fmt, fee, fx_currency, fx_rate,
                     source_currency, statement_type, deal_type, artist_split, territory,
                     contract_pdf_gcs, contract_summary,
                     expected_start, expected_end, file_count,
                     detected_currencies, file_inventory,
                     calc_payable, payable_pct, calc_third_party, third_party_pct,
                     gcs_files)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (deal_id, code, c.name, c.fmt, c.fee,
                  c.source_currency, 1.0,
                  c.source_currency, c.statement_type, c.deal_type, c.artist_split, c.territory,
                  getattr(c, 'contract_pdf_gcs', None),
                  json.dumps(c.contract_summary) if c.contract_summary else None,
                  c.expected_start, c.expected_end, pr.file_count,
                  detected_currencies,
                  json.dumps(file_inventory, default=str),
                  c.calc_payable, c.payable_pct, c.calc_third_party, c.third_party_pct,
                  gcs_files_json))

            pc_id = cur.fetchone()[0]

            # Bulk insert statement_rows from detail DataFrame
            if pr.detail is not None and not pr.detail.empty:
                _bulk_insert_statement_rows(conn, deal_id, pc_id, pr.detail)

            # Insert ISRC meta
            if pr.isrc_meta is not None and not pr.isrc_meta.empty:
                _bulk_insert_isrc_meta(conn, deal_id, pc_id, pr.isrc_meta)

            # Insert monthly summary
            if pr.monthly is not None and not pr.monthly.empty:
                _bulk_insert_monthly(conn, deal_id, pc_id, pr.monthly)

            # Insert store summary (maps to distributor table for backward compat)
            if pr.by_store is not None and not pr.by_store.empty:
                _bulk_insert_distributor(conn, deal_id, pc_id, pr.by_store)

        log.info("DB save complete for %s in %.1fs", slug, _time.time() - _t0)
        return deal_id


def load_deal_from_db(slug):
    """Load a deal from PostgreSQL. Returns (deal_name, analytics) or raises."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, analytics FROM deals WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            raise FileNotFoundError(f"Deal '{slug}' not found in database")

        deal_id, deal_name, analytics_json = row
        analytics = json.loads(analytics_json) if analytics_json else {}

        return deal_name, analytics


def load_payor_configs_from_db(slug) -> List[dict]:
    """Load payor configs for a deal, including GCS file manifests for reprocessing.
    Returns list of dicts with all config fields needed to reconstruct PayorConfig objects.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM deals WHERE slug = %s", (slug,))
        row = cur.fetchone()
        if not row:
            raise FileNotFoundError(f"Deal '{slug}' not found in database")
        deal_id = row[0]

        cur.execute("""
            SELECT code, name, fmt, fee, source_currency, fx_currency,
                   statement_type, deal_type, artist_split, territory,
                   contract_pdf_gcs, contract_summary,
                   expected_start, expected_end,
                   calc_payable, payable_pct, calc_third_party, third_party_pct,
                   gcs_files, file_inventory
            FROM payor_configs WHERE deal_id = %s
        """, (deal_id,))
        rows = cur.fetchall()

        configs = []
        for r in rows:
            gcs_files_raw = r[18]
            if isinstance(gcs_files_raw, str):
                gcs_files_raw = json.loads(gcs_files_raw)
            file_inv_raw = r[19]
            if isinstance(file_inv_raw, str):
                file_inv_raw = json.loads(file_inv_raw)

            configs.append({
                'code': r[0],
                'name': r[1],
                'fmt': r[2] or 'auto',
                'fee': float(r[3]) if r[3] is not None else 0.0,
                'source_currency': r[4] or r[5] or 'auto',
                'statement_type': r[6] or 'masters',
                'deal_type': r[7] or 'artist',
                'artist_split': float(r[8]) if r[8] is not None else None,
                'territory': r[9],
                'contract_pdf_gcs': r[10],
                'contract_summary': json.loads(r[11]) if isinstance(r[11], str) else r[11],
                'expected_start': r[12],
                'expected_end': r[13],
                'calc_payable': r[14] or False,
                'payable_pct': float(r[15]) if r[15] is not None else 0.0,
                'calc_third_party': r[16] or False,
                'third_party_pct': float(r[17]) if r[17] is not None else 0.0,
                'gcs_files': gcs_files_raw,
                'file_inventory': file_inv_raw,
            })
        return configs


def list_deals_from_db() -> List[dict]:
    """List all deals from PostgreSQL."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT slug, name, created_at, updated_at, currency_symbol,
                   total_gross, total_net, ltm_gross, ltm_net,
                   isrc_count, total_files
            FROM deals ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        return [{
            'slug': r[0], 'name': r[1],
            'timestamp': r[3].isoformat() if r[3] else r[2].isoformat() if r[2] else '',
            'currency_symbol': r[4] or '$',
            'total_gross': _fmt_num(r[5]),
            'total_net': _fmt_num(r[6]),
            'ltm_gross': _fmt_num(r[7]),
            'ltm_net': _fmt_num(r[8]),
            'isrc_count': r[9] or 0,
            'total_files': r[10] or 0,
            'payor_codes': [],
            'payor_names': [],
        } for r in rows]


def delete_deal_from_db(slug) -> bool:
    """Delete a deal and all child rows (CASCADE). Returns True if deleted."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM deals WHERE slug = %s RETURNING id", (slug,))
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Bulk inserts
# ---------------------------------------------------------------------------

def _bulk_insert_statement_rows(conn, deal_id, pc_id, df, batch_size=5000):
    """Insert statement_rows using COPY protocol for maximum speed.

    COPY is 5-10x faster than execute_values for large DataFrames because it
    streams tab-separated data directly to Postgres, bypassing SQL parsing.
    """
    import time
    t0 = time.time()

    # Map DataFrame columns to DB columns
    col_map = {
        'Statement Date': 'statement_date',
        'Royalty Type': 'royalty_type',
        'Payor': 'payor',
        'ISRC': 'isrc',
        'ISWC': 'iswc',
        'UPC': 'upc',
        'Other Identifier': 'other_identifier',
        'Title': 'title',
        'Artist': 'artist',
        'Release Date': 'release_date',
        'Release Date Source': 'release_date_source',
        'Source': 'source',
        'Deal': 'deal',
        'Media Type': 'delivery_type',
        'Delivery Type': 'delivery_type',
        'Territory': 'territory',
        'FX Original': 'fx_original',
        'Units': 'units',
        'Gross Earnings': 'gross_earnings',
        'Fees': 'fees',
        'Net Receipts': 'net_receipts',
        'Payable Share': 'payable_share',
        'Third Party Share': 'third_party_share',
        'Net Earnings': 'net_earnings',
    }

    extra_map = {
        'Period': 'period',
        '_period': 'period',
        'Store': 'distributor',
        'Distributor': 'distributor',
        'Country': 'country',
    }

    NUMERIC_DB_COLS = {'units', 'gross_earnings', 'fees', 'net_receipts',
                       'payable_share', 'third_party_share', 'net_earnings',
                       'fx_original'}

    # Build column list and source mappings
    db_cols = ['deal_id', 'payor_config_id']
    val_extractors = []  # (db_col, df_col)
    seen_db = set()

    for df_col, db_col in {**col_map, **extra_map}.items():
        if df_col in df.columns and db_col not in seen_db:
            db_cols.append(db_col)
            val_extractors.append((db_col, df_col))
            seen_db.add(db_col)

    keep_cols = [c for c in df.columns if c.startswith('KEEP_')]
    has_keep = len(keep_cols) > 0
    if has_keep:
        db_cols.append('keep_columns')

    # ---- Build a clean DataFrame for COPY ----
    out = pd.DataFrame(index=df.index)
    out['deal_id'] = deal_id
    out['payor_config_id'] = pc_id

    for db_col, df_col in val_extractors:
        col_data = df[df_col]
        if db_col in NUMERIC_DB_COLS:
            out[db_col] = pd.to_numeric(col_data, errors='coerce').fillna(0)
        elif db_col == 'period':
            out[db_col] = pd.to_numeric(col_data, errors='coerce').fillna(0).astype(int)
        else:
            out[db_col] = col_data.astype(str).where(col_data.notna(), None)

    if has_keep:
        # Vectorised JSON: build per-row dicts from KEEP_ columns
        keep_df = df[keep_cols]
        def _keep_to_json(row):
            d = {k: str(v) for k, v in row.items() if pd.notna(v)}
            return json.dumps(d) if d else None
        out['keep_columns'] = keep_df.apply(_keep_to_json, axis=1)

    # ---- Stream via COPY protocol ----
    cols_str = ', '.join(db_cols)
    buf = io.StringIO()
    # Write tab-separated, \N for nulls — matches COPY TEXT format
    out.to_csv(buf, sep='\t', header=False, index=False, na_rep='\\N')
    buf.seek(0)

    cur = conn.cursor()
    cur.copy_expert(
        f"COPY statement_rows ({cols_str}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
        buf
    )

    elapsed = time.time() - t0
    log.info("COPY %s statement_rows in %.1fs (%.0f rows/sec)",
             f"{len(df):,}", elapsed, len(df) / elapsed if elapsed > 0 else 0)


def _bulk_insert_isrc_meta(conn, deal_id, pc_id, df):
    """Insert ISRC metadata rows."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        identifier = str(row.get('identifier', row.get('ISRC', ''))).strip()
        if not identifier or identifier in ('nan', 'None'):
            continue
        rows.append((
            deal_id, pc_id, identifier,
            str(row.get('title', row.get('Title', ''))),
            str(row.get('artist', row.get('Artist', ''))),
            _safe_float(row.get('total_gross', row.get('gross', 0))),
            _safe_float(row.get('total_net', row.get('net', 0))),
            _safe_float(row.get('total_sales', row.get('sales', 0))),
            _safe_int(row.get('first_period', None)),
            _safe_int(row.get('last_period', None)),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO isrc_meta (deal_id, payor_config_id, identifier, title, artist,
                                   total_gross, total_net, total_sales, first_period, last_period)
            VALUES %s
            ON CONFLICT (deal_id, payor_config_id, identifier) DO UPDATE SET
                total_gross = EXCLUDED.total_gross,
                total_net = EXCLUDED.total_net,
                total_sales = EXCLUDED.total_sales
        """, rows)


def _bulk_insert_monthly(conn, deal_id, pc_id, df):
    """Insert monthly summary rows."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        identifier = str(row.get('identifier', row.get('ISRC', ''))).strip()
        period = _safe_int(row.get('period', row.get('_period', None)))
        if not identifier or identifier in ('nan', 'None') or not period:
            continue
        rows.append((
            deal_id, pc_id, identifier, period,
            _safe_float(row.get('gross', 0)),
            _safe_float(row.get('net', 0)),
            _safe_float(row.get('sales', 0)),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO monthly_summary (deal_id, payor_config_id, identifier, period,
                                         gross, net, sales)
            VALUES %s
            ON CONFLICT (deal_id, payor_config_id, identifier, period) DO UPDATE SET
                gross = EXCLUDED.gross, net = EXCLUDED.net, sales = EXCLUDED.sales
        """, rows)


def _bulk_insert_distributor(conn, deal_id, pc_id, df):
    """Insert store/distributor summary rows (DB column still called 'distributor')."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        dist = str(row.get('store', row.get('Store', row.get('distributor', row.get('Distributor', ''))))).strip()
        if not dist or dist in ('nan', 'None'):
            continue
        rows.append((
            deal_id, pc_id, dist,
            _safe_float(row.get('total_gross', row.get('Total Gross', row.get('gross', 0)))),
            _safe_float(row.get('total_net', row.get('Total Net', row.get('net', 0)))),
            _safe_float(row.get('total_sales', row.get('Total Sales', row.get('sales', 0)))),
        ))

    if rows:
        execute_values(cur, """
            INSERT INTO distributor_summary (deal_id, payor_config_id, distributor,
                                             total_gross, total_net, total_sales)
            VALUES %s
        """, rows)


# ---------------------------------------------------------------------------
# Mapping functions (replace SQLite)
# ---------------------------------------------------------------------------

def get_fingerprint_mapping_db(fingerprint: str) -> Optional[dict]:
    """Look up a mapping by fingerprint. Returns mapping dict or None."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT mapping FROM fingerprints WHERE fingerprint = %s", (fingerprint,))
        row = cur.fetchone()
        if row:
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return None


def save_mapping_db(fingerprint: str, cols: list, mapping: dict, source_label: str = ''):
    """Upsert a fingerprint mapping."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fingerprints (fingerprint, column_names, mapping, source_label, use_count,
                                      created_at, updated_at)
            VALUES (%s, %s, %s, %s, 1, now(), now())
            ON CONFLICT (fingerprint) DO UPDATE SET
                mapping = EXCLUDED.mapping,
                column_names = EXCLUDED.column_names,
                source_label = EXCLUDED.source_label,
                use_count = fingerprints.use_count + 1,
                updated_at = now()
        """, (fingerprint, json.dumps(cols), json.dumps(mapping), source_label))


def save_synonyms_db(mapping: dict):
    """Insert/update synonym mappings."""
    with get_conn() as conn:
        cur = conn.cursor()
        for raw_col, canonical in mapping.items():
            if not canonical:
                continue
            raw_lower = raw_col.strip().lower()
            if not raw_lower:
                continue
            cur.execute("""
                INSERT INTO synonyms (raw_name, canonical)
                VALUES (%s, %s)
                ON CONFLICT (raw_name) DO UPDATE SET canonical = EXCLUDED.canonical
            """, (raw_lower, canonical))


def log_import_db(filename, fingerprint, mapping, row_count, qc_warnings, qc_errors,
                  status='approved'):
    """Write to import_log in PostgreSQL."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO import_log (filename, fingerprint, mapping_used, row_count,
                                    qc_warnings, qc_errors, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        """, (filename, fingerprint, json.dumps(mapping), row_count,
              qc_warnings, qc_errors, status))


def get_import_history_db(limit=20) -> List[dict]:
    """Return recent import log entries from PostgreSQL."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, filename, fingerprint, mapping_used, row_count,
                   qc_warnings, qc_errors, status, created_at
            FROM import_log ORDER BY created_at DESC LIMIT %s
        """, (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_saved_formats_db() -> List[dict]:
    """Return all saved fingerprint mappings from PostgreSQL."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT fingerprint, source_label, column_names, use_count, updated_at
            FROM fingerprints ORDER BY use_count DESC
        """)
        results = []
        for r in cur.fetchall():
            entry = {
                'fingerprint': r[0],
                'source_label': r[1],
                'column_names': json.loads(r[2]) if isinstance(r[2], str) else r[2],
                'use_count': r[3],
                'updated_at': r[4].isoformat() if r[4] else '',
            }
            results.append(entry)
        return results


def get_synonyms_db(raw_name: str) -> Optional[str]:
    """Look up a single synonym."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT canonical FROM synonyms WHERE raw_name = %s", (raw_name,))
        row = cur.fetchone()
        return row[0] if row else None


def increment_fingerprint_use_db(fingerprint: str):
    """Increment use_count for a fingerprint."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE fingerprints SET use_count = use_count + 1, updated_at = now()
            WHERE fingerprint = %s
        """, (fingerprint,))


# ---------------------------------------------------------------------------
# Cache functions
# ---------------------------------------------------------------------------

def get_enrichment_cache_db(key: str) -> Optional[dict]:
    """Get a single enrichment cache entry."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT release_date, source, track_name, artist_name, looked_up
            FROM enrichment_cache WHERE cache_key = %s
        """, (key,))
        row = cur.fetchone()
        if row:
            return {
                'release_date': row[0] or '',
                'source': row[1] or '',
                'track_name': row[2] or '',
                'artist_name': row[3] or '',
                'looked_up': row[4],
            }
        return None


def save_enrichment_cache_db(entries: dict):
    """Bulk upsert enrichment cache entries. entries = {key: {release_date, source, ...}}"""
    if not entries:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        for key, entry in entries.items():
            cur.execute("""
                INSERT INTO enrichment_cache (cache_key, release_date, source, track_name,
                                              artist_name, looked_up, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (cache_key) DO UPDATE SET
                    release_date = EXCLUDED.release_date,
                    source = EXCLUDED.source,
                    track_name = EXCLUDED.track_name,
                    artist_name = EXCLUDED.artist_name,
                    looked_up = EXCLUDED.looked_up,
                    updated_at = now()
            """, (key, entry.get('release_date', ''), entry.get('source', ''),
                  entry.get('track_name', ''), entry.get('artist_name', ''),
                  entry.get('looked_up', False)))


def get_isrc_cache_db(isrc: str) -> Optional[dict]:
    """Get a single ISRC cache entry."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT release_date, track_name, artist_name
            FROM isrc_cache WHERE isrc = %s
        """, (isrc,))
        row = cur.fetchone()
        if row:
            return {
                'release_date': row[0] or '',
                'track_name': row[1] or '',
                'artist_name': row[2] or '',
            }
        return None


def save_isrc_cache_db(entries: dict):
    """Bulk upsert ISRC cache entries. entries = {isrc: {release_date, track_name, artist_name}}"""
    if not entries:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        for isrc, entry in entries.items():
            cur.execute("""
                INSERT INTO isrc_cache (isrc, release_date, track_name, artist_name, created_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (isrc) DO UPDATE SET
                    release_date = EXCLUDED.release_date,
                    track_name = EXCLUDED.track_name,
                    artist_name = EXCLUDED.artist_name
            """, (isrc, entry.get('release_date', ''),
                  entry.get('track_name', ''), entry.get('artist_name', '')))


def load_full_isrc_cache_db() -> dict:
    """Load entire ISRC cache from DB into a dict. For migration/sync."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT isrc, release_date, track_name, artist_name FROM isrc_cache")
        result = {}
        for r in cur.fetchall():
            result[r[0]] = {
                'release_date': r[1] or '',
                'track_name': r[2] or '',
                'artist_name': r[3] or '',
            }
        return result


def load_full_enrichment_cache_db() -> dict:
    """Load entire enrichment cache from DB into a dict. For migration/sync."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT cache_key, release_date, source, track_name, artist_name, looked_up FROM enrichment_cache")
        result = {}
        for r in cur.fetchall():
            result[r[0]] = {
                'release_date': r[1] or '',
                'source': r[2] or '',
                'track_name': r[3] or '',
                'artist_name': r[4] or '',
                'looked_up': r[5],
            }
        return result


# ---------------------------------------------------------------------------
# GCS file tracking
# ---------------------------------------------------------------------------

def track_gcs_file(deal_id, file_type, gcs_path, original_name, size_bytes=None,
                   content_type=None):
    """Record a GCS file upload in the database."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO gcs_files (deal_id, file_type, gcs_path, original_name,
                                   size_bytes, content_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (gcs_path) DO UPDATE SET
                deal_id = EXCLUDED.deal_id,
                file_type = EXCLUDED.file_type,
                original_name = EXCLUDED.original_name,
                size_bytes = EXCLUDED.size_bytes,
                content_type = EXCLUDED.content_type,
                uploaded_at = now()
        """, (deal_id, file_type, gcs_path, original_name, size_bytes, content_type))


def get_deal_id_by_slug(slug: str) -> Optional[int]:
    """Get deal ID by slug."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM deals WHERE slug = %s", (slug,))
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Forecasts CRUD
# ---------------------------------------------------------------------------

def save_forecast(deal_id: int, config: dict, result_summary: dict = None) -> Optional[int]:
    """Save a forecast result. Returns the forecast ID."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO forecasts (deal_id, config, result_summary)
            VALUES (%s, %s, %s) RETURNING id
        """, (deal_id, json.dumps(config), json.dumps(result_summary) if result_summary else None))
        row = cur.fetchone()
        return row[0] if row else None


def get_latest_forecast(deal_id: int) -> Optional[dict]:
    """Get the most recent forecast for a deal."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, config, result_summary, created_at
            FROM forecasts WHERE deal_id = %s
            ORDER BY created_at DESC LIMIT 1
        """, (deal_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'id': row[0],
            'config': row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {},
            'result_summary': row[2] if isinstance(row[2], dict) else json.loads(row[2]) if row[2] else None,
            'created_at': row[3].isoformat() if row[3] else None,
        }


def list_forecasts(deal_id: int) -> list:
    """List all forecasts for a deal."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, config, result_summary, created_at
            FROM forecasts WHERE deal_id = %s
            ORDER BY created_at DESC
        """, (deal_id,))
        rows = cur.fetchall()
        return [{
            'id': r[0],
            'config': r[1] if isinstance(r[1], dict) else json.loads(r[1]) if r[1] else {},
            'result_summary': r[2] if isinstance(r[2], dict) else json.loads(r[2]) if r[2] else None,
            'created_at': r[3].isoformat() if r[3] else None,
        } for r in rows]


# ---------------------------------------------------------------------------
# Deal Templates CRUD
# ---------------------------------------------------------------------------

def save_template(name: str, payor_configs: list, settings: dict = None) -> Optional[int]:
    """Save or update a deal template. Returns template ID."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO deal_templates (name, payor_configs, settings)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                payor_configs = EXCLUDED.payor_configs,
                settings = EXCLUDED.settings,
                updated_at = now()
            RETURNING id
        """, (name, json.dumps(payor_configs), json.dumps(settings or {})))
        row = cur.fetchone()
        return row[0] if row else None


def list_templates() -> list:
    """List all deal templates."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, payor_configs, settings, created_at, updated_at
            FROM deal_templates ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        return [{
            'id': r[0],
            'name': r[1],
            'payor_configs': r[2] if isinstance(r[2], list) else json.loads(r[2]) if r[2] else [],
            'settings': r[3] if isinstance(r[3], dict) else json.loads(r[3]) if r[3] else {},
            'created_at': r[4].isoformat() if r[4] else None,
            'updated_at': r[5].isoformat() if r[5] else None,
        } for r in rows]


def get_template(name: str) -> Optional[dict]:
    """Get a deal template by name."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, payor_configs, settings, created_at, updated_at
            FROM deal_templates WHERE name = %s
        """, (name,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'id': row[0],
            'name': row[1],
            'payor_configs': row[2] if isinstance(row[2], list) else json.loads(row[2]) if row[2] else [],
            'settings': row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {},
            'created_at': row[4].isoformat() if row[4] else None,
            'updated_at': row[5].isoformat() if row[5] else None,
        }


def delete_template(name: str) -> bool:
    """Delete a deal template."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM deal_templates WHERE name = %s", (name,))
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Delta Reports CRUD
# ---------------------------------------------------------------------------

def save_delta_report(deal_id: int, report: dict) -> Optional[int]:
    """Save a delta report. Returns the report ID."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO delta_reports (deal_id, report)
            VALUES (%s, %s) RETURNING id
        """, (deal_id, json.dumps(report)))
        # Also update deals.latest_delta
        cur.execute("""
            UPDATE deals SET latest_delta = %s WHERE id = %s
        """, (json.dumps(report), deal_id))
        row = cur.fetchone()
        return row[0] if row else None


def get_latest_delta(deal_id: int) -> Optional[dict]:
    """Get the most recent delta report for a deal."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, report, created_at
            FROM delta_reports WHERE deal_id = %s
            ORDER BY created_at DESC LIMIT 1
        """, (deal_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'id': row[0],
            'report': row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {},
            'created_at': row[2].isoformat() if row[2] else None,
        }


def update_deal_forecast_config(slug: str, config: dict):
    """Update the forecast_config on a deal."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE deals SET forecast_config = %s WHERE slug = %s
        """, (json.dumps(config), slug))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_num(val):
    """Parse a number from various formats."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val.replace(',', '').replace('$', '').replace('€', '').replace('£', ''))
        except (ValueError, TypeError):
            return 0.0
    return 0.0


def _fmt_num(val):
    """Format a numeric value with commas."""
    if val is None:
        return '0'
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return '0'


def _safe_float(val):
    """Safely convert to float."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _safe_int(val):
    """Safely convert to int or None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None
