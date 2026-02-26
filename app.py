"""
Royalty Statement Consolidator - Web Dashboard
Flask app with auto-consolidation from local dirs, polished dark UI, and Chart.js visuals.
"""

import json
import logging
import os
import pickle
import re
import shutil
import tempfile
import threading
import uuid
import zipfile
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
_log_dir = os.path.dirname(os.path.abspath(__file__))
_log_handlers = [logging.StreamHandler()]
# Only add file handler when filesystem is writable (local dev, not Cloud Run)
if not os.getenv('DB_HOST'):
    try:
        _log_handlers.append(logging.FileHandler(os.path.join(_log_dir, 'app.log'), encoding='utf-8'))
    except OSError:
        pass
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=_log_handlers,
)
log = logging.getLogger('royalty')

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template_string, request, send_file, flash, redirect, url_for, jsonify, make_response, session
import pandas as pd

# Lazy Gemini import to avoid ~2-3s delay from deprecation warnings on startup
genai = None

_gemini_api_key = os.getenv('GEMINI_API_KEY', '')

def _get_genai():
    """Lazy-load google.genai client on first use."""
    global genai
    if genai is None:
        from google import genai as _genai
        genai = _genai
    return genai

from consolidator import (
    PayorConfig, load_all_payors, write_consolidated_excel, write_consolidated_csv,
    write_per_payor_exports, write_per_payor_csv_exports,
    populate_template, load_supplemental_metadata,
    compute_analytics, DEFAULT_PAYORS, parse_file_with_mapping,
    apply_enrichment_to_raw_detail,
)
import mapper
import formula_engine
import validator
import enrichment
import db
import storage
import delta as delta_engine
import forecast as forecast_engine

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2 GB


@app.after_request
def _no_cache(response):
    """Prevent browser from caching HTML pages (stale JS/CSS)."""
    if 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response


@app.errorhandler(413)
def _request_too_large(e):
    log.error("413 Request Entity Too Large: %s %s (content-length: %s)",
              request.method, request.path, request.content_length)
    if request.path.startswith('/api/'):
        return jsonify(error='File too large', message='Upload exceeds the maximum allowed size.'), 413
    return '<h1>Upload Too Large</h1><p>The file you uploaded exceeds the maximum allowed size. Try a smaller file or upload fewer files at once.</p>', 413


@app.errorhandler(500)
def _internal_error(e):
    log.error("500 Internal Server Error: %s %s — %s", request.method, request.path, e)
    if request.path.startswith('/api/'):
        return jsonify(error='Internal server error', message=str(e)), 500
    return '<h1>Internal Server Error</h1><p>Something went wrong. Check the Cloud Run logs for details.</p>', 500

def _log_memory():
    """Log current memory usage (works on Linux/Docker and Windows)."""
    try:
        import resource
        mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        log.info("Memory usage: %.0f MB", mb)
    except ImportError:
        try:
            import psutil
            mb = psutil.Process().memory_info().rss / (1024 * 1024)
            log.info("Memory usage: %.0f MB", mb)
        except ImportError:
            pass  # No memory info available

WORK_DIR = os.path.join(tempfile.gettempdir(), 'royalty_consolidator', 'current')
os.makedirs(WORK_DIR, exist_ok=True)

DEALS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'deals')
os.makedirs(DEALS_DIR, exist_ok=True)

# Initialise PostgreSQL + GCS (graceful — app works without them)
_db_ok = db.init_pool()
if _db_ok:
    _migrations_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'migrations')
    if os.path.isdir(_migrations_dir):
        db.run_migrations(_migrations_dir)
_gcs_ok = storage.init_gcs()

app.jinja_env.filters['basename'] = lambda p: os.path.basename(p) if p else ''

# Cache results in memory so we don't re-parse on every page load
_cached_results = {}
_cached_analytics = {}
_cached_deal_name = ''


# Ingest wizard session state (keyed by session ID for isolation)
_ingest_sessions = {}
_processing_locks = {}          # sid -> threading.Lock()  – prevents duplicate runs
_processing_locks_guard = threading.Lock()  # protects _processing_locks dict
INGEST_TEMP = os.path.join(tempfile.gettempdir(), 'royalty_consolidator', 'ingest')
os.makedirs(INGEST_TEMP, exist_ok=True)


def _get_ingest_session():
    """Get or create an ingest session for the current user. Returns (session_dict, session_id)."""
    sid = request.cookies.get('session_id') or str(uuid.uuid4())
    if sid not in _ingest_sessions:
        _ingest_sessions[sid] = {}
    return _ingest_sessions[sid], sid


def _clear_ingest_session():
    """Clear the ingest session for the current user."""
    sid = request.cookies.get('session_id')
    if sid and sid in _ingest_sessions:
        del _ingest_sessions[sid]


def _get_custom_session():
    """Get or create the Phase 2 custom_flow sub-key in the ingest session."""
    sess, sid = _get_ingest_session()
    if 'custom_flow' not in sess:
        sess['custom_flow'] = {}
    return sess['custom_flow'], sid


def _clear_custom_session():
    """Clear the custom_flow sub-key."""
    sess, sid = _get_ingest_session()
    sess.pop('custom_flow', None)


# Background processing status
_processing_status = {
    'running': False,
    'progress': '',
    'done': False,
    'error': None,
    '_updated_at': 0.0,  # monotonic timestamp for staleness detection
}
_STALE_TIMEOUT = 600  # 10 minutes — if no status update, consider dead

def _set_processing_status(**kwargs):
    """Update _processing_status with staleness timestamp."""
    import time as _t
    _processing_status.update(kwargs)
    _processing_status['_updated_at'] = _t.monotonic()

# Chat session histories (session_id -> list of message dicts)
_chat_histories = {}

# Thread lock for protecting shared global state
_state_lock = threading.RLock()

# ---------------------------------------------------------------------------
# Chatbot System Prompt & Context Builder
# ---------------------------------------------------------------------------

CHATBOT_SYSTEM_PROMPT = """You are a Senior Royalty Data Analyst at Create Music Group, a data-driven music and technology company that empowers artists and labels worldwide.

Your role:
- Answer questions about the loaded royalty data with precision and clarity
- Identify trends, anomalies, and patterns in revenue streams
- Explain year-over-year (YoY) changes and their likely causes
- Compare payors (distributors/platforms) and highlight revenue concentration risks
- Flag missing statements or potential revenue gaps
- Calculate projections and estimates when asked
- Provide strategic insights about the artist/label's royalty portfolio

Domain knowledge:
- Royalty statements follow: Gross Revenue -> Distribution Fee (%) -> Net Revenue
- ISRC = International Standard Recording Code, a unique identifier per song recording. The same song can appear across multiple payors.
- LTM = Last Twelve Months, a standard trailing metric for smoothing seasonal variance
- Common distributors include Spotify, Apple Music, YouTube Music, Amazon Music, Tidal, Deezer, etc.
- Distribution fees are auto-detected from statement data; some payors may have unknown fees
- Currencies are normalized to the deal's base currency
- Missing months in a payor's statement history represent potential revenue gaps worth investigating
- YoY analysis compares the same annual periods to identify growth or decline trends

Instructions:
- ALWAYS reference actual numbers from the provided data context — never fabricate data
- Format currency values with commas and 2 decimal places (e.g., $1,234.56)
- Use percentages for comparisons (e.g., "Spotify accounts for 45.2% of gross revenue")
- Proactively highlight concerning patterns: declining revenue, high concentration, missing data
- Keep answers professional, direct, and data-driven — no filler or pleasantries
- If asked about data that isn't available, say so clearly rather than guessing
- When discussing trends, always specify the time period and direction
- Use tables or structured formatting when presenting comparative data
"""


def _build_chat_context():
    """Serialize _cached_analytics into a structured text summary for the LLM."""
    a = _cached_analytics
    if not a:
        return ""

    lines = ["=== ROYALTY DATA CONTEXT ==="]

    # Deal overview
    deal = _cached_deal_name or "Unknown Deal"
    lines.append(f"\nDeal: {deal}")
    lines.append(f"Period Range: {a.get('period_range', 'N/A')}")
    lines.append(f"Total Files: {a.get('total_files', 0)}")
    lines.append(f"Total ISRCs (unique songs): {a.get('isrc_count', 0)}")

    # LTM totals
    lines.append(f"\nLTM Gross Revenue: {a.get('ltm_gross', 'N/A')}")
    lines.append(f"LTM Net Revenue: {a.get('ltm_net', 'N/A')}")
    yoy = a.get('yoy_gross_pct')
    if yoy is not None:
        lines.append(f"YoY Gross Change: {yoy}")

    # Total gross/net
    lines.append(f"\nTotal Gross Revenue (all time): {a.get('total_gross', 'N/A')}")
    lines.append(f"Total Net Revenue (all time): {a.get('total_net', 'N/A')}")

    # Annual breakdown
    annual = a.get('annual_breakdown', [])
    if annual:
        lines.append("\n--- Annual Earnings Breakdown ---")
        for yr in annual:
            year = yr.get('year', '?')
            gross = yr.get('gross', 'N/A')
            net = yr.get('net', 'N/A')
            yoy_val = yr.get('yoy_gross_pct', '')
            yoy_str = f" (YoY: {yoy_val})" if yoy_val else ""
            lines.append(f"  {year}: Gross={gross}, Net={net}{yoy_str}")

    # Top songs
    top_songs = a.get('top_songs', [])
    if top_songs:
        lines.append(f"\n--- Top {len(top_songs)} Songs ---")
        for i, song in enumerate(top_songs[:10], 1):
            isrc = song.get('isrc', '?')
            title = song.get('title', 'Unknown')
            artist = song.get('artist', 'Unknown')
            ltm_g = song.get('ltm_gross', 'N/A')
            lines.append(f"  {i}. [{isrc}] {title} by {artist} — LTM Gross: {ltm_g}")
            yearly = song.get('yearly', [])
            if yearly:
                if isinstance(yearly, list):
                    yr_parts = [f"{y.get('year','?')}: gross={y.get('gross','?')}, net={y.get('net','?')}" for y in yearly]
                else:
                    yr_parts = [f"{y}={v}" for y, v in yearly.items()]
                lines.append(f"     Yearly: {', '.join(yr_parts)}")
            song_yoy = song.get('yoy_pct', '')
            if song_yoy:
                lines.append(f"     YoY: {song_yoy}")

    # Payor summaries
    payors = a.get('payor_summaries', [])
    if payors:
        lines.append(f"\n--- Payor Summaries ({len(payors)} payors) ---")
        for p in payors:
            name = p.get('name', '?')
            fee = p.get('fee', 'Unknown')
            files = p.get('files', 0)
            isrcs = p.get('isrcs', 0)
            latest = p.get('latest_statement', 'N/A')
            missing = p.get('missing_months', [])
            lines.append(f"\n  Payor: {name}")
            lines.append(f"    Fee: {fee}, Files: {files}, ISRCs: {isrcs}")
            lines.append(f"    Latest Statement: {latest}")
            if missing:
                lines.append(f"    Missing Months: {', '.join(str(m) for m in missing)}")
            payor_annual = p.get('annual_breakdown', [])
            if payor_annual:
                for yr in payor_annual:
                    year = yr.get('year', '?')
                    gross = yr.get('gross', 'N/A')
                    net = yr.get('net', 'N/A')
                    yoy_val = yr.get('yoy_gross_pct', '')
                    yoy_str = f" (YoY: {yoy_val})" if yoy_val else ""
                    lines.append(f"    {year}: Gross={gross}, Net={net}{yoy_str}")

    # Top stores
    top_dist = a.get('top_stores', [])
    if top_dist:
        lines.append("\n--- Top Stores ---")
        for d in top_dist:
            lines.append(f"  {d.get('name', '?')}: Gross={d.get('gross', 'N/A')}")

    # Earnings matrix
    matrix = a.get('earnings_matrix', [])
    if matrix:
        lines.append("\n--- Earnings Matrix (Payor x Year) ---")
        for row in matrix:
            name = row.get('name', '?')
            years = row.get('years', {})
            yr_parts = [f"{y}: gross={v.get('gross_fmt', v.get('gross', '?'))}" for y, v in years.items()]
            total = row.get('total_gross_fmt', row.get('total_gross', '?'))
            lines.append(f"  {name}: {', '.join(yr_parts)} | Total={total}")

    lines.append("\n=== END CONTEXT ===")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Deal Persistence Helpers
# ---------------------------------------------------------------------------

def _make_slug(deal_name):
    """Sanitize deal name to filesystem-safe uppercase slug."""
    slug = deal_name.strip().upper()
    slug = re.sub(r'[^A-Z0-9]+', '_', slug)
    slug = slug.strip('_')
    return slug or 'UNTITLED'


def save_deal(slug, deal_name, payor_results, analytics, xlsx_path, csv_path, per_payor_paths):
    """Persist a deal to DB + GCS + local disk."""
    # Save to PostgreSQL if available
    if db.is_available():
        try:
            csym = analytics.get('currency_symbol', '$')
            db.save_deal_to_db(slug, deal_name, payor_results, analytics,
                               currency_symbol=csym)
            log.info("Deal '%s' saved to PostgreSQL", slug)
        except Exception as e:
            log.error("DB save failed for %s: %s", slug, e, exc_info=True)

    # Upload exports to GCS if available
    if storage.is_available():
        try:
            if xlsx_path and os.path.exists(xlsx_path):
                storage.upload_export(slug, os.path.basename(xlsx_path), xlsx_path)
            if csv_path and os.path.exists(csv_path):
                storage.upload_export(slug, os.path.basename(csv_path), csv_path)
            if per_payor_paths:
                for code, path in per_payor_paths.items():
                    if path and os.path.exists(path):
                        storage.upload_per_payor_export(slug, os.path.basename(path), path)
            for pr in payor_results.values():
                if pr.config.contract_pdf_path and os.path.exists(pr.config.contract_pdf_path):
                    storage.upload_contract(slug, pr.config.code,
                                            os.path.basename(pr.config.contract_pdf_path),
                                            pr.config.contract_pdf_path)
            log.info("Deal '%s' exports uploaded to GCS", slug)
        except Exception as e:
            log.error("GCS upload failed for %s: %s", slug, e)

    # Always keep local as fallback
    deal_dir = os.path.join(DEALS_DIR, slug)
    os.makedirs(deal_dir, exist_ok=True)

    # Save deal metadata
    meta = {
        'name': deal_name,
        'slug': slug,
        'timestamp': datetime.now().isoformat(),
        'payor_codes': list(payor_results.keys()),
        'payor_names': [pr.config.name for pr in payor_results.values()],
        'total_gross': analytics.get('total_gross', '0'),
        'isrc_count': analytics.get('isrc_count', 0),
        'total_files': analytics.get('total_files', 0),
        'currency_symbol': analytics.get('currency_symbol', '$'),
        'ltm_gross': analytics.get('ltm_gross_total_fmt', analytics.get('total_gross', '0')),
        'ltm_net': analytics.get('ltm_net_total_fmt', analytics.get('total_net', '0')),
    }
    with open(os.path.join(deal_dir, 'deal_meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)

    # Save analytics
    with open(os.path.join(deal_dir, 'analytics.json'), 'w') as f:
        json.dump(analytics, f, indent=2, default=str)

    # Save payor results (pickled)
    # Note: Using pickle for DataFrame serialization. Only load trusted deal files.
    with open(os.path.join(deal_dir, 'payor_results.pkl'), 'wb') as f:
        pickle.dump(payor_results, f)

    # Copy export files
    exports_dir = os.path.join(deal_dir, 'exports')
    os.makedirs(exports_dir, exist_ok=True)
    if xlsx_path and os.path.exists(xlsx_path):
        dest = os.path.join(exports_dir, os.path.basename(xlsx_path))
        if os.path.normpath(xlsx_path) != os.path.normpath(dest):
            shutil.copy2(xlsx_path, dest)
    if csv_path and os.path.exists(csv_path):
        dest = os.path.join(exports_dir, os.path.basename(csv_path))
        if os.path.normpath(csv_path) != os.path.normpath(dest):
            shutil.copy2(csv_path, dest)

    # Copy per-payor exports
    if per_payor_paths:
        pp_dir = os.path.join(exports_dir, 'per_payor')
        os.makedirs(pp_dir, exist_ok=True)
        for code, path in per_payor_paths.items():
            if path and os.path.exists(path):
                dest = os.path.join(pp_dir, os.path.basename(path))
                if os.path.normpath(path) != os.path.normpath(dest):
                    shutil.copy2(path, dest)

    # Copy contract PDFs
    contracts_dir = os.path.join(deal_dir, 'contracts')
    for pr in payor_results.values():
        if pr.config.contract_pdf_path and os.path.exists(pr.config.contract_pdf_path):
            os.makedirs(contracts_dir, exist_ok=True)
            dest = os.path.join(contracts_dir, os.path.basename(pr.config.contract_pdf_path))
            if os.path.normpath(pr.config.contract_pdf_path) != os.path.normpath(dest):
                shutil.copy2(pr.config.contract_pdf_path, dest)


def load_deal(slug):
    """Load a saved deal from DB (preferred) or disk fallback.
    Returns (deal_name, payor_results, analytics, xlsx_path, csv_path, per_payor_paths) or raises.
    """
    # Try database first (Cloud Run — no local filesystem)
    if db.is_available():
        try:
            deal_name, analytics = db.load_deal_from_db(slug)
            log.info("Loaded deal '%s' from database", slug)
            return deal_name, {}, analytics, None, None, {}
        except FileNotFoundError:
            pass  # Not in DB, fall through to local
        except Exception as e:
            log.warning("DB load failed for %s, trying local: %s", slug, e)

    deal_dir = os.path.join(DEALS_DIR, slug)

    with open(os.path.join(deal_dir, 'deal_meta.json'), 'r') as f:
        meta = json.load(f)

    with open(os.path.join(deal_dir, 'payor_results.pkl'), 'rb') as f:
        payor_results = pickle.load(f)

    # Recompute analytics from pickle if LTM fields are missing (older saves)
    analytics_path = os.path.join(deal_dir, 'analytics.json')
    with open(analytics_path, 'r') as f:
        analytics = json.load(f)

    if ('ltm_stores' not in analytics or 'ltm_media_types' not in analytics
            or 'cohort_analysis' not in analytics or 'revenue_concentration' not in analytics):
        try:
            analytics = compute_analytics(payor_results)
            with open(analytics_path, 'w') as f:
                json.dump(analytics, f, indent=2, default=str)
            # Also update deal_meta.json with correct LTM values
            meta_path = os.path.join(deal_dir, 'deal_meta.json')
            if os.path.isfile(meta_path):
                with open(meta_path, 'r') as f:
                    dmeta = json.load(f)
                dmeta['ltm_gross'] = analytics.get('ltm_gross_total_fmt', dmeta.get('total_gross', '0'))
                dmeta['ltm_net'] = analytics.get('ltm_net_total_fmt', analytics.get('total_net', '0'))
                with open(meta_path, 'w') as f:
                    json.dump(dmeta, f, indent=2)
        except Exception as e:
            log.error("load_deal: failed to recompute analytics for %s: %s", slug, e, exc_info=True)

    # Re-sort top_songs by LTM gross descending (older saves may be unsorted)
    if 'top_songs' in analytics:
        def _parse_ltm(s):
            try:
                return float(s.get('ltm_gross', '0').replace(',', ''))
            except (ValueError, AttributeError):
                return 0.0
        analytics['top_songs'].sort(key=_parse_ltm, reverse=True)

    # Repoint contract paths to deal directory
    contracts_dir = os.path.join(deal_dir, 'contracts')
    for pr in payor_results.values():
        if pr.config.contract_pdf_path:
            basename = os.path.basename(pr.config.contract_pdf_path)
            new_path = os.path.join(contracts_dir, basename)
            if os.path.exists(new_path):
                pr.config.contract_pdf_path = new_path

    # Build paths for exports — find actual consolidated files by pattern
    exports_dir = os.path.join(deal_dir, 'exports')
    xlsx_path = None
    csv_path = None
    if os.path.isdir(exports_dir):
        for fname in os.listdir(exports_dir):
            fpath = os.path.join(exports_dir, fname)
            if not os.path.isfile(fpath):
                continue
            fl = fname.lower()
            if fl.endswith('.xlsx') and ('consolidated' in fl or fl.startswith('consolidated')):
                xlsx_path = fpath
            elif fl.endswith('.csv') and ('consolidated' in fl or fl.startswith('consolidated')):
                csv_path = fpath

    per_payor_paths = {}
    # Scan per_payor/ and per_payor_csv/ subdirs, plus exports/ root for per-payor files
    pp_search_dirs = [
        os.path.join(exports_dir, 'per_payor'),
        os.path.join(exports_dir, 'per_payor_csv'),
        exports_dir,
    ]
    for pp_dir in pp_search_dirs:
        if not os.path.isdir(pp_dir):
            continue
        for fname in os.listdir(pp_dir):
            fpath = os.path.join(pp_dir, fname)
            if not os.path.isfile(fpath):
                continue
            for code in payor_results.keys():
                if code in per_payor_paths:
                    continue
                if code in fname or fname.startswith(payor_results[code].config.name.replace(' ', '_')):
                    # Skip the main consolidated file
                    if fpath == xlsx_path or fpath == csv_path:
                        continue
                    per_payor_paths[code] = fpath
                    break

    return meta['name'], payor_results, analytics, xlsx_path, csv_path, per_payor_paths


def list_deals():
    """Return sorted list of deal metadata, merging DB and local sources."""
    seen_slugs = set()
    deals = []

    # Try PostgreSQL first
    if db.is_available():
        try:
            db_deals = db.list_deals_from_db()
            for d in db_deals:
                seen_slugs.add(d['slug'])
                deals.append(d)
        except Exception as e:
            log.warning("DB list_deals failed: %s", e)

    # Always scan local DEALS_DIR for deals not yet in DB
    if os.path.isdir(DEALS_DIR):
        for slug in os.listdir(DEALS_DIR):
            if slug in seen_slugs:
                continue
            meta_path = os.path.join(DEALS_DIR, slug, 'deal_meta.json')
            if os.path.isfile(meta_path):
                try:
                    with open(meta_path, 'r') as f:
                        meta = json.load(f)
                    meta['slug'] = slug
                    if 'ltm_gross' not in meta:
                        analytics_path = os.path.join(DEALS_DIR, slug, 'analytics.json')
                        if os.path.isfile(analytics_path):
                            try:
                                with open(analytics_path, 'r') as af:
                                    ana = json.load(af)
                                meta['ltm_gross'] = ana.get('ltm_gross_total_fmt', meta.get('total_gross', '0'))
                                meta['ltm_net'] = ana.get('ltm_net_total_fmt', ana.get('total_net', '0'))
                            except (json.JSONDecodeError, KeyError):
                                meta['ltm_gross'] = meta.get('total_gross', '0')
                                meta['ltm_net'] = '0'
                        else:
                            meta['ltm_gross'] = meta.get('total_gross', '0')
                            meta['ltm_net'] = '0'
                    deals.append(meta)
                except (json.JSONDecodeError, KeyError):
                    continue

    deals.sort(key=lambda d: d.get('timestamp', ''), reverse=True)
    return deals


# ---------------------------------------------------------------------------
# Auto-load the most recent saved deal on startup
# ---------------------------------------------------------------------------
def _auto_load_last_deal():
    global _cached_results, _cached_analytics, _cached_deal_name
    try:
        best_slug = None
        best_ts = ''
        for slug in os.listdir(DEALS_DIR):
            meta_path = os.path.join(DEALS_DIR, slug, 'deal_meta.json')
            if os.path.isfile(meta_path):
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                ts = meta.get('timestamp', '')
                if ts > best_ts:
                    best_ts = ts
                    best_slug = slug
        if best_slug:
            deal_name, payor_results, analytics, xlsx_path, csv_path, per_payor_paths = load_deal(best_slug)
            _cached_results = payor_results
            _cached_analytics = analytics
            _cached_deal_name = deal_name
            app.config['CONSOLIDATED_PATH'] = xlsx_path
            app.config['CONSOLIDATED_CSV_PATH'] = csv_path
            app.config['PER_PAYOR_PATHS'] = per_payor_paths
            log.info("Auto-loaded last deal '%s' (%s) on startup", deal_name, best_slug)
    except Exception as e:
        log.warning("Could not auto-load last deal on startup: %s", e)

_auto_load_last_deal()


# ---------------------------------------------------------------------------
# HTML Dashboard Template
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ deal_name or 'Royalty Consolidator' }}</title>
    <script>
        // Apply saved theme immediately to prevent flash
        (function(){
            var t = localStorage.getItem('rc-theme');
            if (t === 'light' || (!t && window.matchMedia('(prefers-color-scheme: light)').matches)) {
                document.documentElement.setAttribute('data-theme', 'light');
            }
        })();
    </script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

        *, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg-primary: #09090b;
            --bg-card: #111113;
            --bg-card-hover: #16161a;
            --bg-inset: #0c0c0e;
            --border: #1e1e22;
            --border-hover: #2a2a30;
            --text-primary: #fafafa;
            --text-secondary: #a1a1aa;
            --text-muted: #52525b;
            --text-dim: #3f3f46;
            --accent: #3b82f6;
            --accent-hover: #2563eb;
            --green: #4ade80;
            --green-dim: #166534;
            --red: #f87171;
            --red-dim: #7f1d1d;
            --yellow: #fbbf24;
            --purple: #a78bfa;
            --cyan: #22d3ee;
            --bg-secondary: #18181b;
            --radius: 12px;
            --radius-sm: 8px;
            --radius-xs: 6px;
        }

        /* Light theme */
        html[data-theme="light"] {
            --bg-primary: #f8f9fa;
            --bg-card: #ffffff;
            --bg-card-hover: #f1f3f5;
            --bg-inset: #eef0f2;
            --border: #dee2e6;
            --border-hover: #adb5bd;
            --text-primary: #111827;
            --text-secondary: #4b5563;
            --text-muted: #9ca3af;
            --text-dim: #d1d5db;
            --accent: #2563eb;
            --accent-hover: #1d4ed8;
            --green: #16a34a;
            --green-dim: #bbf7d0;
            --red: #dc2626;
            --red-dim: #fee2e2;
            --yellow: #d97706;
            --purple: #7c3aed;
            --cyan: #0891b2;
            --bg-secondary: #e5e7eb;
        }
        html[data-theme="light"] .nav { background: #ffffff; border-bottom-color: #dee2e6; }
        html[data-theme="light"] .card { box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
        html[data-theme="light"] table th { background: #f1f3f5; }
        html[data-theme="light"] .form-input, html[data-theme="light"] select.form-input {
            background: #ffffff; border-color: #dee2e6; color: #111827;
        }
        html[data-theme="light"] .pill-tab { background: #e5e7eb; color: #4b5563; }
        html[data-theme="light"] .pill-tab.active { background: var(--accent); color: #ffffff; }
        html[data-theme="light"] .btn-submit { background: var(--accent); }
        html[data-theme="light"] code, html[data-theme="light"] pre { background: #f1f3f5; }

        /* Theme toggle button */
        .theme-toggle {
            background: none; border: 1px solid var(--border); color: var(--text-muted);
            width: 32px; height: 32px; border-radius: 50%; cursor: pointer;
            display: flex; align-items: center; justify-content: center; font-size: 15px;
            transition: border-color 0.2s, color 0.2s;
        }
        .theme-toggle:hover { border-color: var(--border-hover); color: var(--text-primary); }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-primary);
            color: var(--text-secondary);
            min-height: 100vh;
            line-height: 1.5;
            -webkit-font-smoothing: antialiased;
        }

        /* ---- NAV ---- */
        .nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 16px 32px;
            border-bottom: 1px solid var(--border);
            background: var(--bg-card);
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(12px);
        }
        .nav-left { display: flex; align-items: center; gap: 16px; }
        .nav-logo {
            width: 32px; height: 32px; border-radius: 50%;
            background: var(--accent);
            display: flex; align-items: center; justify-content: center;
            font-weight: 800; font-size: 13px; color: #fff;
        }
        .nav-title { font-size: 14px; font-weight: 600; color: var(--text-primary); }
        .nav-links { display: flex; gap: 24px; }
        .nav-links a {
            font-size: 13px; color: var(--text-muted); text-decoration: none;
            font-weight: 500; transition: color 0.2s;
        }
        .nav-links a:hover, .nav-links a.active { color: var(--text-primary); }
        .nav-right { display: flex; align-items: center; gap: 8px; }
        .nav-btn {
            padding: 7px 16px; background: var(--bg-inset); border: 1px solid var(--border);
            border-radius: var(--radius-xs); color: var(--text-secondary); font-size: 12px;
            font-weight: 500; cursor: pointer; transition: all 0.2s; text-decoration: none;
        }
        .nav-btn:hover { border-color: var(--border-hover); color: var(--text-primary); background: var(--bg-card-hover); }
        .nav-btn.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
        .nav-btn.primary:hover { background: var(--accent-hover); }

        /* ---- LAYOUT ---- */
        .container { max-width: 1400px; margin: 0 auto; padding: 28px 32px 60px; }
        .page-header { margin-bottom: 28px; }
        .page-header h1 {
            font-size: 28px; font-weight: 700; color: var(--text-primary);
            letter-spacing: -0.02em;
        }
        .page-header p { font-size: 13px; color: var(--text-muted); margin-top: 4px; }

        /* ---- GRID ---- */
        .grid { display: grid; gap: 16px; }
        .grid-4 { grid-template-columns: repeat(4, 1fr); }
        .grid-3 { grid-template-columns: repeat(3, 1fr); }
        .grid-2 { grid-template-columns: repeat(2, 1fr); }
        .grid-hero { grid-template-columns: 1fr 1fr 1.2fr; }
        .grid-wide { grid-template-columns: 2fr 1fr; }
        .span-2 { grid-column: span 2; }
        .span-3 { grid-column: span 3; }
        .span-full { grid-column: 1 / -1; }

        /* ---- CARDS ---- */
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 24px;
            transition: border-color 0.2s;
        }
        .card:hover { border-color: var(--border-hover); }
        .card-header {
            display: flex; justify-content: space-between; align-items: center;
            margin-bottom: 16px;
        }
        .card-title {
            font-size: 13px; font-weight: 500; color: var(--text-muted);
            text-transform: uppercase; letter-spacing: 0.04em;
        }
        .card-icon {
            width: 28px; height: 28px; border-radius: 6px;
            border: 1px solid var(--border); display: flex;
            align-items: center; justify-content: center;
            color: var(--text-dim); font-size: 12px;
        }

        /* ---- STAT CARDS ---- */
        .stat-value {
            font-size: 36px; font-weight: 800; color: var(--text-primary);
            letter-spacing: -0.03em; line-height: 1.1;
        }
        .stat-value.medium { font-size: 28px; }
        .stat-value.small { font-size: 22px; }
        .stat-subtitle {
            font-size: 12px; color: var(--text-muted); margin-top: 4px;
            font-weight: 400;
        }
        .stat-change {
            display: inline-flex; align-items: center; gap: 4px;
            font-size: 12px; font-weight: 600; padding: 2px 8px;
            border-radius: 4px; margin-top: 8px;
        }
        .stat-change.up { color: var(--green); background: rgba(74, 222, 128, 0.1); }
        .stat-change.down { color: var(--red); background: rgba(248, 113, 113, 0.1); }

        /* ---- PAYOR LIST (like Country stats in screenshot) ---- */
        .payor-list { list-style: none; }
        .payor-item {
            display: flex; justify-content: space-between; align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid var(--border);
        }
        .payor-item:last-child { border-bottom: none; }
        .payor-name { font-size: 13px; color: var(--text-secondary); font-weight: 400; }
        .payor-value {
            font-size: 14px; font-weight: 600; color: var(--text-primary);
            font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
        }
        .payor-extra {
            display: flex; align-items: center; gap: 12px; margin-top: 14px;
        }
        .payor-extra a {
            font-size: 12px; color: var(--text-muted); text-decoration: none;
            padding: 6px 14px; border: 1px solid var(--border); border-radius: var(--radius-xs);
            transition: all 0.2s;
        }
        .payor-extra a:hover { border-color: var(--accent); color: var(--accent); }

        /* ---- PILL TABS ---- */
        .pill-tabs {
            display: flex; gap: 4px; padding: 3px; background: var(--bg-inset);
            border-radius: var(--radius-sm); width: fit-content; margin-bottom: 16px;
            border: 1px solid var(--border);
        }
        .pill-tab {
            padding: 6px 14px; border-radius: var(--radius-xs); font-size: 12px;
            font-weight: 500; color: var(--text-muted); cursor: pointer;
            background: none; border: none; transition: all 0.2s;
        }
        .pill-tab:hover { color: var(--text-secondary); }
        .pill-tab.active {
            background: var(--bg-card-hover); color: var(--text-primary);
            box-shadow: 0 1px 3px rgba(0,0,0,0.3);
        }

        /* ---- TABLES ---- */
        table { width: 100%; border-collapse: collapse; }
        thead th {
            text-align: left; padding: 8px 12px;
            font-size: 10px; font-weight: 600; color: var(--text-dim);
            text-transform: uppercase; letter-spacing: 0.06em;
            border-bottom: 1px solid var(--border);
        }
        tbody td {
            padding: 10px 12px; font-size: 13px;
            border-bottom: 1px solid rgba(30,30,34,0.5);
        }
        tbody tr:hover td { background: rgba(255,255,255,0.02); }
        .text-right { text-align: right; }
        .mono {
            font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
            font-size: 12px;
        }
        .text-green { color: var(--green); }
        .text-red { color: var(--red); }
        .text-accent { color: var(--accent); }
        .text-yellow { color: var(--yellow); }
        .text-purple { color: var(--purple); }
        .rank {
            width: 28px; height: 28px; border-radius: 6px;
            background: var(--bg-inset); border: 1px solid var(--border);
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 11px; font-weight: 600; color: var(--text-muted);
        }

        /* ---- CHART CONTAINER ---- */
        .chart-wrap { position: relative; width: 100%; margin-top: 12px; }
        .chart-wrap.tall { height: 280px; }
        .chart-wrap.medium { height: 200px; }
        .chart-wrap.short { height: 140px; }

        /* ---- DOWNLOAD LINKS ---- */
        .dl-link {
            display: flex; align-items: center; justify-content: space-between;
            padding: 14px 18px; background: var(--bg-inset);
            border: 1px solid var(--border); border-radius: var(--radius-sm);
            margin-bottom: 8px; text-decoration: none; color: var(--text-secondary);
            transition: all 0.2s;
        }
        .dl-link:hover { border-color: var(--accent); color: var(--text-primary); }
        .dl-link .name { font-weight: 500; font-size: 13px; }
        .dl-link .badge {
            font-size: 11px; color: var(--text-dim); padding: 2px 8px;
            background: var(--bg-card); border-radius: 4px;
        }

        /* ---- TAB CONTENT ---- */
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* ---- FLASH ---- */
        .flash { padding: 14px 18px; border-radius: var(--radius-sm); margin-bottom: 16px; font-size: 13px; }
        .flash.error { background: var(--red-dim); border: 1px solid #991b1b; color: #fca5a5; }
        .flash.success { background: var(--green-dim); border: 1px solid #15803d; color: #86efac; }

        /* ---- FORM (upload page) ---- */
        .form-card { max-width: 700px; }
        .form-group { margin-bottom: 16px; }
        .form-label {
            display: block; font-size: 11px; color: var(--text-muted); margin-bottom: 6px;
            font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
        }
        .form-input {
            width: 100%; padding: 10px 14px; background: var(--bg-inset);
            border: 1px solid var(--border); border-radius: var(--radius-xs);
            color: var(--text-primary); font-size: 13px; font-family: inherit;
        }
        .form-input:focus { border-color: var(--accent); outline: none; }
        .form-row { display: flex; gap: 12px; }
        .form-row .form-group { flex: 1; }
        .btn-submit {
            width: 100%; padding: 12px; background: var(--accent); color: #fff;
            border: none; border-radius: var(--radius-xs); font-size: 14px;
            font-weight: 600; cursor: pointer; font-family: inherit;
        }
        .btn-submit:hover { background: var(--accent-hover); }

        /* ---- LOADING ---- */
        .loading-overlay {
            display: none; position: fixed; inset: 0;
            background: rgba(9,9,11,0.85); z-index: 200;
            justify-content: center; align-items: center; flex-direction: column;
        }
        .loading-overlay.active { display: flex; }
        .loading-ring {
            width: 40px; height: 40px; border: 3px solid var(--border);
            border-top-color: var(--accent); border-radius: 50%;
            animation: spin 0.7s linear infinite;
        }
        .loading-text { color: var(--text-muted); font-size: 13px; margin-top: 16px; }
        @keyframes spin { to { transform: rotate(360deg); } }

        /* ---- TOAST NOTIFICATION ---- */
        .toast {
            position: fixed; top: 24px; right: 24px; z-index: 300;
            background: var(--green-dim); border: 1px solid #15803d; color: var(--green);
            padding: 14px 24px; border-radius: var(--radius-sm);
            font-size: 13px; font-weight: 500;
            animation: slideIn 0.4s ease-out;
            transition: opacity 0.3s;
        }
        .toast.fade-out { opacity: 0; }
        @keyframes slideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }

        /* ---- DISTRIBUTOR BAR ---- */
        .dist-bar-wrap { margin-bottom: 10px; }
        .dist-bar-label {
            display: flex; justify-content: space-between;
            font-size: 12px; margin-bottom: 4px;
        }
        .dist-bar-label .name { color: var(--text-secondary); }
        .dist-bar-label .val { color: var(--text-primary); font-weight: 600; font-family: 'SF Mono', monospace; }
        .dist-bar-track {
            height: 6px; background: var(--bg-inset); border-radius: 3px; overflow: hidden;
        }
        .dist-bar-fill {
            height: 100%; border-radius: 3px;
            background: linear-gradient(90deg, var(--accent), var(--purple));
            transition: width 0.6s ease;
        }

        /* ---- PAYOR BLOCK (upload form) ---- */
        .payor-block {
            background: var(--bg-inset);
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            padding: 20px;
        }
        .payor-block + .payor-block { margin-top: 12px; }

        /* ---- DROPZONE ---- */
        .dropzone {
            border: 2px dashed var(--border);
            border-radius: var(--radius-sm);
            padding: 24px 16px;
            text-align: center;
            cursor: pointer;
            transition: all 0.2s;
            background: var(--bg-primary);
        }
        .dropzone:hover, .dropzone.drag-over {
            border-color: var(--accent);
            background: rgba(59,130,246,0.05);
        }
        .dropzone .dz-icon {
            font-size: 28px;
            color: var(--text-dim);
            margin-bottom: 6px;
        }
        .dropzone .dz-text {
            font-size: 12px;
            color: var(--text-muted);
        }
        .dropzone .dz-text strong {
            color: var(--accent);
            cursor: pointer;
        }
        .dropzone .dz-files {
            margin-top: 8px;
            font-size: 11px;
            color: var(--text-secondary);
            text-align: left;
            max-height: 150px;
            overflow-y: auto;
        }
        .dropzone .dz-files div {
            padding: 2px 0;
            border-bottom: 1px solid var(--border);
        }

        /* ---- RESPONSIVE ---- */
        @media (max-width: 1100px) {
            .grid-hero { grid-template-columns: 1fr 1fr; }
            .grid-4 { grid-template-columns: repeat(2, 1fr); }
        }
        @media (max-width: 768px) {
            .container { padding: 16px; }
            .grid-hero, .grid-4, .grid-3, .grid-2, .grid-wide { grid-template-columns: 1fr; }
            .span-2, .span-3 { grid-column: span 1; }
            .nav { padding: 12px 16px; }
            .stat-value { font-size: 28px; }
        }
    </style>
</head>
<body>

<nav class="nav">
    <div class="nav-left">
        <div class="nav-logo">R</div>
        <span class="nav-title">{{ deal_name or 'Royalty Consolidator' }}</span>
        <div class="nav-links">
            <a href="/" class="{{ 'active' if page == 'dashboard' }}">Dashboard</a>
            <a href="/deals" class="{{ 'active' if page == 'deals' }}">Deals</a>
            <a href="/upload" class="{{ 'active' if page == 'upload' }}">Upload</a>
            <a href="/chat" class="{{ 'active' if page == 'chat' }}">Chat</a>
        </div>
    </div>
    <div class="nav-right">
        {% if results %}
        <a href="/refresh" class="nav-btn">Refresh Data</a>
        <a href="/download/consolidated" class="nav-btn">Export .xlsx</a>
        <a href="/download/csv" class="nav-btn primary">Export .csv</a>
        {% endif %}
        <button class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark mode" aria-label="Toggle theme">
            <span id="themeIcon">&#9790;</span>
        </button>
    </div>
</nav>

<div class="container">

{% with messages = get_flashed_messages(with_categories=true) %}
{% for category, message in messages %}
<div class="flash {{ category }}">{{ message }}</div>
{% endfor %}
{% endwith %}

{% if page == 'upload' %}
{# ==================== UPLOAD PAGE ==================== #}

{# ---- Step indicator macro (used by ingest wizard steps) ---- #}
{% macro step_indicator(current) %}
<div style="display:flex; gap:8px; margin-bottom:24px; align-items:center;">
    {% set steps = [('upload', '1. Upload'), ('detect', '2. Detect'), ('map', '3. Map'), ('qc', '4. Review'), ('done', '5. Done')] %}
    {% for key, label in steps %}
    <div style="display:flex; align-items:center; gap:6px;">
        <div style="width:28px; height:28px; border-radius:50%; display:flex; align-items:center; justify-content:center;
            font-size:12px; font-weight:600;
            {% if key == current %}
                background:var(--accent); color:#fff;
            {% elif steps | map(attribute=0) | list | batch(1) %}
                {% set step_keys = ['upload','detect','map','qc','done'] %}
                {% if step_keys.index(key) < step_keys.index(current) %}
                    background:var(--green-dim); color:var(--green); border:1px solid var(--green);
                {% else %}
                    background:var(--bg-inset); color:var(--text-dim); border:1px solid var(--border);
                {% endif %}
            {% endif %}
        ">{{ loop.index }}</div>
        <span style="font-size:12px; font-weight:500; {% if key == current %}color:var(--text-primary);{% else %}color:var(--text-muted);{% endif %}">{{ label.split('. ')[1] }}</span>
    </div>
    {% if not loop.last %}
    <div style="flex:1; height:1px; background:var(--border);"></div>
    {% endif %}
    {% endfor %}
</div>
{% endmacro %}

{# ---- Phase 2 stepper macro (8 steps) ---- #}
{% macro custom_stepper(current_step) %}
<div style="display:flex; gap:6px; margin-bottom:24px; align-items:center; flex-wrap:wrap;">
    {% set csteps = [('preview', 'Preview'), ('map', 'Map'), ('validate', 'Validate'), ('calc', 'Calc'), ('enrich', 'Enrich'), ('export', 'Export'), ('finalize', 'Finalize'), ('summary', 'Summary')] %}
    {% for key, label in csteps %}
    <div style="display:flex; align-items:center; gap:4px;">
        <div style="width:24px; height:24px; border-radius:50%; display:flex; align-items:center; justify-content:center;
            font-size:11px; font-weight:600;
            {% set step_keys = ['preview','map','validate','calc','enrich','export','finalize','summary'] %}
            {% if key == current_step %}
                background:var(--accent); color:#fff;
            {% elif step_keys.index(key) < step_keys.index(current_step) %}
                background:var(--green-dim); color:var(--green); border:1px solid var(--green);
            {% else %}
                background:var(--bg-inset); color:var(--text-dim); border:1px solid var(--border);
            {% endif %}
        ">{{ loop.index }}</div>
        <span style="font-size:11px; font-weight:500; {% if key == current_step %}color:var(--text-primary);{% else %}color:var(--text-muted);{% endif %}">{{ label }}</span>
    </div>
    {% if not loop.last %}<div style="flex:1; height:1px; background:var(--border); min-width:12px;"></div>{% endif %}
    {% endfor %}
</div>
{% endmacro %}

{% if custom_step == 'preview' %}
{# ==================== PHASE 2: PREVIEW / CLEAN ==================== #}
{{ custom_stepper('preview') }}
<div class="page-header">
    <h1>Preview &amp; Clean — Payor {{ custom_payor_idx + 1 }} of {{ custom_payor_count }}</h1>
    <p>{{ custom_payor_name }}</p>
</div>

{% if custom_struct_count|default(1) > 1 %}
<div style="background:rgba(99,102,241,0.08); border:1px solid rgba(99,102,241,0.25); border-radius:8px; padding:12px 16px; margin-bottom:16px;">
    <div style="font-size:13px; font-weight:600; color:var(--text-primary); margin-bottom:4px;">
        Structure {{ (custom_struct_idx|default(0)) + 1 }} of {{ custom_struct_count }} &mdash; {{ custom_struct_files|default([])|length }} file{{ 's' if custom_struct_files|default([])|length != 1 }}
    </div>
    <div style="font-size:11px; color:var(--text-muted);">
        Files: {{ custom_struct_files|default([])|join(', ') }}
    </div>
</div>
{% elif custom_struct_count|default(1) == 1 and custom_struct_files|default([])|length > 1 %}
<div style="font-size:11px; color:var(--text-muted); margin-bottom:12px;">
    {{ custom_struct_files|default([])|length }} files with identical structure
</div>
{% endif %}

<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">File Preview</span></div>
    <div style="display:flex; gap:16px; margin-bottom:16px; flex-wrap:wrap;">
        <div class="form-group" style="flex:0 0 120px;">
            <label class="form-label">Remove Top Rows</label>
            <input class="form-input" type="number" id="removeTop" value="{{ custom_remove_top | default(0) }}" min="0" max="50"
                   onchange="updateCleaningPreview()">
        </div>
        <div class="form-group" style="flex:0 0 120px;">
            <label class="form-label">Remove Bottom Rows</label>
            <input class="form-input" type="number" id="removeBottom" value="{{ custom_remove_bottom | default(0) }}" min="0" max="50"
                   onchange="updateCleaningPreview()">
        </div>
        {% if custom_sheets %}
        <div class="form-group" style="flex:0 0 200px;">
            <label class="form-label">Sheet</label>
            <select class="form-input" id="sheetSelect" onchange="updateCleaningPreview()">
                {% for s in custom_sheets %}<option value="{{ s }}" {{ 'selected' if s == custom_sheet }}>{{ s }}</option>{% endfor %}
            </select>
        </div>
        {% endif %}
    </div>

    <div style="overflow-x:auto; max-height:400px; border:1px solid var(--border); border-radius:6px; margin-bottom:16px;" id="previewTableWrap">
        <table style="font-size:11px; white-space:nowrap;">
            <thead>
                <tr>{% for h in custom_headers %}<th style="padding:6px 10px; background:var(--bg-inset); position:sticky; top:0;">{{ h }}</th>{% endfor %}</tr>
            </thead>
            <tbody id="previewBody">
                {% for row in custom_preview_rows %}
                <tr>{% for cell in row %}<td style="padding:4px 10px; color:var(--text-secondary);">{{ cell }}</td>{% endfor %}</tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    <p style="font-size:11px; color:var(--text-dim);" id="previewRowCount">{{ custom_total_rows }} data rows after cleaning</p>

    <form method="POST" action="/custom/preview/{{ custom_payor_idx }}/{{ custom_struct_idx|default(0) }}">
        <input type="hidden" name="action" value="clean">
        <input type="hidden" name="remove_top" id="removeTopVal" value="{{ custom_remove_top | default(0) }}">
        <input type="hidden" name="remove_bottom" id="removeBottomVal" value="{{ custom_remove_bottom | default(0) }}">
        <input type="hidden" name="sheet" id="sheetVal" value="{{ custom_sheet | default('') }}">
        <button type="submit" class="btn-submit" onclick="document.getElementById('removeTopVal').value=document.getElementById('removeTop').value; document.getElementById('removeBottomVal').value=document.getElementById('removeBottom').value; if(document.getElementById('sheetSelect')) document.getElementById('sheetVal').value=document.getElementById('sheetSelect').value;">
            Continue to Mapping &rarr;
        </button>
    </form>
</div>

<script>
function updateCleaningPreview() {
    const removeTop = parseInt(document.getElementById('removeTop').value) || 0;
    const removeBottom = parseInt(document.getElementById('removeBottom').value) || 0;
    const sheetEl = document.getElementById('sheetSelect');
    const sheet = sheetEl ? sheetEl.value : '';

    fetch('/api/custom/preview', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            payor_idx: {{ custom_payor_idx }},
            struct_idx: {{ custom_struct_idx|default(0) }},
            remove_top: removeTop,
            remove_bottom: removeBottom,
            sheet: sheet,
        }),
    })
    .then(r => r.json())
    .then(data => {
        if (data.error) return;
        // Update headers
        const table = document.querySelector('#previewTableWrap table');
        const thead = table.querySelector('thead tr');
        thead.innerHTML = data.headers.map(h => `<th style="padding:6px 10px; background:var(--bg-inset); position:sticky; top:0;">${h}</th>`).join('');
        // Update body
        const tbody = document.getElementById('previewBody');
        tbody.innerHTML = data.rows.map(row =>
            '<tr>' + row.map(c => `<td style="padding:4px 10px; color:var(--text-secondary);">${c}</td>`).join('') + '</tr>'
        ).join('');
        document.getElementById('previewRowCount').textContent = data.total_rows + ' data rows after cleaning';
    });
}
</script>

{% elif custom_step == 'map' %}
{# ==================== PHASE 2: COLUMN MAPPING ==================== #}
{{ custom_stepper('map') }}
<div class="page-header">
    <h1>Map Columns — Payor {{ custom_payor_idx + 1 }} of {{ custom_payor_count }}</h1>
    <p>{{ custom_payor_name }} &mdash; Assign each source column to a canonical field</p>
</div>

{% if custom_struct_count|default(1) > 1 %}
<div style="background:rgba(99,102,241,0.08); border:1px solid rgba(99,102,241,0.25); border-radius:8px; padding:12px 16px; margin-bottom:16px;">
    <div style="font-size:13px; font-weight:600; color:var(--text-primary); margin-bottom:4px;">
        Structure {{ (custom_struct_idx|default(0)) + 1 }} of {{ custom_struct_count }} &mdash; {{ custom_struct_files|default([])|length }} file{{ 's' if custom_struct_files|default([])|length != 1 }}
    </div>
    <div style="font-size:11px; color:var(--text-muted);">
        Files: {{ custom_struct_files|default([])|join(', ') }}
    </div>
</div>
{% elif custom_struct_count|default(1) == 1 and custom_struct_files|default([])|length > 1 %}
<div style="font-size:11px; color:var(--text-muted); margin-bottom:12px;">
    {{ custom_struct_files|default([])|length }} files with identical structure
</div>
{% endif %}

<div class="card" style="margin-bottom:16px;">
    <form method="POST" action="/custom/map/{{ custom_payor_idx }}/{{ custom_struct_idx|default(0) }}">
    <div style="overflow-x:auto; border:1px solid var(--border); border-radius:6px; margin-bottom:16px;">
        <table style="font-size:11px; white-space:nowrap;">
            {# Row 1: Mapping dropdowns #}
            <thead>
            <tr style="background:var(--bg-card);">
                {% for h in custom_headers %}
                <th style="padding:8px 6px; min-width:140px;">
                    <select class="form-input" name="map_{{ loop.index0 }}" style="font-size:11px; padding:3px 6px;" onchange="onMappingChange(this)">
                        <option value="">— skip —</option>
                        {% for opt in mapping_options %}
                        <option value="{{ opt }}" {{ 'selected' if custom_proposed.get(h, {}).get('canonical') == opt }}>{{ opt }}</option>
                        {% endfor %}
                    </select>
                </th>
                {% endfor %}
            </tr>
            {# Row 2: KEEP checkboxes #}
            <tr style="background:var(--bg-inset);">
                {% for h in custom_headers %}
                <th style="padding:4px 6px;">
                    <label style="font-size:10px; color:var(--text-dim); cursor:pointer; display:flex; align-items:center; gap:4px;">
                        <input type="checkbox" name="keep_{{ loop.index0 }}" value="1" checked style="accent-color:var(--accent);">
                        KEEP
                    </label>
                </th>
                {% endfor %}
            </tr>
            {# Row 3: Original column names #}
            <tr style="background:var(--bg-inset);">
                {% for h in custom_headers %}
                <th style="padding:6px; color:var(--text-secondary); font-weight:600; font-size:11px;">{{ h }}</th>
                {% endfor %}
            </tr>
            </thead>
            {# Data rows (15 preview) #}
            <tbody>
                {% for row in custom_preview_rows[:15] %}
                <tr>{% for cell in row %}<td style="padding:4px 6px; color:var(--text-muted);">{{ cell }}</td>{% endfor %}</tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    <input type="hidden" name="headers_json" value='{{ custom_headers | tojson }}'>
    <div id="mappingWarnings" style="margin-bottom:12px;"></div>
    <button type="submit" class="btn-submit" id="mapSubmitBtn">Save Mapping &amp; Continue &rarr;</button>
    </form>
</div>

<script>
function onMappingChange(sel) {
    // Check for duplicate mappings
    const allSelects = document.querySelectorAll('select[name^="map_"]');
    const counts = {};
    allSelects.forEach(s => {
        if (s.value) counts[s.value] = (counts[s.value] || 0) + 1;
    });
    const dupes = Object.entries(counts).filter(([k, v]) => v > 1).map(([k]) => k);
    const warn = document.getElementById('mappingWarnings');
    if (dupes.length > 0) {
        warn.innerHTML = '<div style="background:rgba(251,191,36,0.1); border:1px solid rgba(251,191,36,0.3); border-radius:6px; padding:8px 12px; font-size:12px; color:#fbbf24;">Duplicate mapping: ' + dupes.join(', ') + '</div>';
    } else {
        warn.innerHTML = '';
    }
    // Visual feedback: highlight mapped columns
    allSelects.forEach(s => {
        s.style.borderColor = s.value ? 'var(--accent)' : 'var(--border)';
    });
}
// Init on load + submit debounce
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('select[name^="map_"]').forEach(s => onMappingChange(s));
    // Prevent double-submit on mapping form
    const mapForm = document.getElementById('mapSubmitBtn')?.closest('form');
    if (mapForm) {
        mapForm.addEventListener('submit', function(e) {
            const btn = document.getElementById('mapSubmitBtn');
            if (btn.disabled) { e.preventDefault(); return; }
            btn.disabled = true;
            btn.textContent = 'Processing\u2026';
        });
    }
});
</script>

{% elif custom_step == 'validate' %}
{# ==================== PHASE 2: VALIDATION ==================== #}
{{ custom_stepper('validate') }}
<div class="page-header">
    <h1>Validation</h1>
    <p>Review data quality issues before finalizing.</p>
</div>

<div class="grid grid-3" style="margin-bottom:24px;">
    <div class="stat-card">
        <div class="stat-label">Files Processed</div>
        <div class="stat-value medium">{{ validation_result.total_files }}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Total Rows</div>
        <div class="stat-value medium">{{ '{:,}'.format(validation_result.total_rows) }}</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Issues Found</div>
        <div class="stat-value medium" style="{% if validation_result.error_count > 0 %}color:var(--red);{% elif validation_result.warning_count > 0 %}color:#fbbf24;{% else %}color:var(--green);{% endif %}">
            {{ validation_result.issue_count }}
        </div>
    </div>
</div>

{% if validation_result.has_issues %}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Issues</span></div>
    <table>
        <thead>
            <tr><th>Severity</th><th>Check</th><th>Payor</th><th>Description</th><th>Count</th></tr>
        </thead>
        <tbody>
        {% for issue in validation_result.issues %}
        <tr>
            <td><span style="font-size:11px; padding:2px 8px; border-radius:4px;
                {% if issue.severity == 'error' %}background:rgba(248,113,113,0.15); color:var(--red);
                {% else %}background:rgba(251,191,36,0.15); color:#fbbf24;{% endif %}
            ">{{ issue.severity | upper }}</span></td>
            <td style="color:var(--text-secondary); font-size:12px;">{{ issue.check }}</td>
            <td style="color:var(--text-muted); font-size:12px;">{{ issue.payor_code }}</td>
            <td style="color:var(--text-primary); font-size:12px;">{{ issue.message }}</td>
            <td class="mono">{{ issue.count }}</td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>
{% else %}
<div class="card" style="text-align:center; padding:40px;">
    <div style="font-size:36px; color:var(--green); margin-bottom:8px;">&#10003;</div>
    <p style="color:var(--text-primary); font-weight:600;">No issues found</p>
</div>
{% endif %}

<div style="display:flex; gap:12px; margin-top:16px;">
    <form method="POST" action="/custom/validate" style="margin:0;">
        <input type="hidden" name="action" value="continue">
        <button type="submit" class="btn-submit">Continue to Calculations &rarr;</button>
    </form>
    {% if validation_result.has_issues %}
    <form method="POST" action="/custom/validate" style="margin:0;">
        <input type="hidden" name="action" value="remove_rerun">
        <button type="submit" class="nav-btn" style="padding:10px 24px; color:#fbbf24; border-color:rgba(251,191,36,0.3);">Remove Flagged &amp; Re-run</button>
    </form>
    {% endif %}
    <a href="/upload" class="nav-btn" style="padding:10px 24px;">Cancel</a>
</div>

{% elif custom_step == 'calc' %}
{# ==================== PHASE 2: EARNINGS WATERFALL CALC ==================== #}
{{ custom_stepper('calc') }}
<div class="page-header">
    <h1>Earnings Waterfall</h1>
    <p>Review auto-calculated fields and define formulas for missing ones.</p>
</div>

<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Waterfall Fields</span></div>
    <table>
        <thead>
            <tr><th>Field</th><th>Status</th><th>Formula (for missing fields)</th></tr>
        </thead>
        <tbody>
        {% for field in waterfall_fields %}
        <tr>
            <td style="color:var(--text-primary); font-weight:600; font-size:13px;">{{ field.name }}</td>
            <td>
                {% if field.status == 'present' %}
                <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:var(--green-dim); color:var(--green);">Present</span>
                {% elif field.status == 'auto_calc' %}
                <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:rgba(59,130,246,0.15); color:var(--accent);">Auto-calculated</span>
                {% else %}
                <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:rgba(251,191,36,0.15); color:#fbbf24;">Needs Formula</span>
                {% endif %}
            </td>
            <td>
                {% if field.status == 'needs_formula' %}
                <div style="display:flex; gap:8px; align-items:center;">
                    <input class="form-input" type="text" name="formula_{{ field.name }}" id="formula_{{ field.name | replace(' ', '_') }}"
                           placeholder="=[Gross Earnings] * 0.15" style="font-size:12px; flex:1;"
                           value="{{ field.formula | default('') }}">
                    <button type="button" class="nav-btn" style="font-size:11px; padding:4px 10px;"
                            onclick="validateFormula('{{ field.name }}')">Check</button>
                    <span id="fcheck_{{ field.name | replace(' ', '_') }}" style="font-size:11px;"></span>
                </div>
                {% elif field.formula %}
                <span style="color:var(--text-dim); font-size:12px; font-family:monospace;">{{ field.formula }}</span>
                {% endif %}
            </td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>

<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Formula Preview</span></div>
    <div id="formulaPreview" style="overflow-x:auto;">
        <p style="color:var(--text-dim); font-size:12px;">Enter formulas above and click Preview to see computed values.</p>
    </div>
    <button type="button" class="nav-btn" style="margin-top:12px; padding:8px 16px; font-size:12px;" onclick="previewFormulas()">Preview Formulas</button>
</div>

<form method="POST" action="/custom/calc">
    {% for field in waterfall_fields %}
    {% if field.status == 'needs_formula' %}
    <input type="hidden" name="formula_{{ field.name }}" id="hformula_{{ field.name | replace(' ', '_') }}" value="{{ field.formula | default('') }}">
    {% endif %}
    {% endfor %}
    <button type="submit" class="btn-submit" id="finalizeBtn" onclick="syncFormulas();">Finalize &amp; Process &rarr;</button>
</form>
<script>
document.querySelector('form[action="/custom/calc"]').addEventListener('submit', function() {
    var btn = document.getElementById('finalizeBtn');
    setTimeout(function(){ btn.disabled=true; btn.textContent='Processing\u2026'; }, 50);
});
</script>

<script>
function validateFormula(fieldName) {
    const safeId = fieldName.replace(/ /g, '_');
    const input = document.getElementById('formula_' + safeId);
    const check = document.getElementById('fcheck_' + safeId);
    if (!input.value.trim()) { check.textContent = ''; return; }

    fetch('/api/custom/validate-formula', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({formula: input.value, field: fieldName}),
    })
    .then(r => r.json())
    .then(data => {
        if (data.valid) {
            check.textContent = '\u2713';
            check.style.color = 'var(--green)';
        } else {
            check.textContent = data.error || 'Invalid';
            check.style.color = 'var(--red)';
        }
    });
}

function previewFormulas() {
    const formulas = {};
    document.querySelectorAll('input[id^="formula_"]').forEach(inp => {
        const name = inp.id.replace('formula_', '').replace(/_/g, ' ');
        if (inp.value.trim()) formulas[name] = inp.value.trim();
    });
    if (Object.keys(formulas).length === 0) return;

    fetch('/api/custom/formula-preview', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({formulas}),
    })
    .then(r => r.json())
    .then(data => {
        const wrap = document.getElementById('formulaPreview');
        if (data.error) { wrap.innerHTML = '<p style="color:var(--red);">' + data.error + '</p>'; return; }
        let html = '<table style="font-size:11px;"><thead><tr>';
        data.columns.forEach(c => { html += '<th style="padding:4px 8px;">' + c + '</th>'; });
        html += '</tr></thead><tbody>';
        data.rows.forEach(row => {
            html += '<tr>';
            row.forEach(v => { html += '<td class="mono" style="padding:4px 8px;">' + v + '</td>'; });
            html += '</tr>';
        });
        html += '</tbody></table>';
        if (data.errors && data.errors.length) {
            html += '<div style="color:var(--red); font-size:11px; margin-top:8px;">' + data.errors.join('<br>') + '</div>';
        }
        wrap.innerHTML = html;
    });
}

function syncFormulas() {
    document.querySelectorAll('input[id^="formula_"]').forEach(inp => {
        const safeId = inp.id.replace('formula_', '');
        const hidden = document.getElementById('hformula_' + safeId);
        if (hidden) hidden.value = inp.value;
    });
}
</script>

{% elif custom_step == 'enrich' %}
{# ==================== PHASE 3: RELEASE DATE ENRICHMENT ==================== #}
{{ custom_stepper('enrich') }}
<div class="page-header">
    <h1>Release Date Enrichment</h1>
    <p>Look up release dates from MusicBrainz, Genius, and Gemini.</p>
</div>

{% if enrichment_done %}
{# --- Enrichment complete: show results --- #}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Enrichment Results</span></div>
    <div class="grid grid-3" style="margin-bottom:16px;">
        <div style="text-align:center; padding:12px;">
            <div style="font-size:24px; font-weight:700; color:var(--text-primary);">{{ enrichment_stats.total }}</div>
            <div style="font-size:11px; color:var(--text-muted);">Total Tracks</div>
        </div>
        <div style="text-align:center; padding:12px;">
            <div style="font-size:24px; font-weight:700; color:var(--green);">{{ enrichment_stats.total - enrichment_stats.not_found }}</div>
            <div style="font-size:11px; color:var(--text-muted);">Dates Found</div>
        </div>
        <div style="text-align:center; padding:12px;">
            <div style="font-size:24px; font-weight:700; color:var(--yellow);">{{ enrichment_stats.not_found }}</div>
            <div style="font-size:11px; color:var(--text-muted);">Not Found</div>
        </div>
    </div>

    <table>
        <thead><tr><th>Source</th><th class="text-right">Count</th><th class="text-right">%</th></tr></thead>
        <tbody>
            <tr><td>Source Data (SRC)</td><td class="text-right mono">{{ enrichment_stats.from_source }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.from_source / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
            <tr><td>Cache</td><td class="text-right mono">{{ enrichment_stats.from_cache }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.from_cache / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
            <tr><td>MusicBrainz (MB)</td><td class="text-right mono">{{ enrichment_stats.mb_found }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.mb_found / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
            <tr><td>Genius (GN)</td><td class="text-right mono">{{ enrichment_stats.gn_found }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.gn_found / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
            <tr><td>Gemini (GM)</td><td class="text-right mono">{{ enrichment_stats.gm_found }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.gm_found / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
            <tr style="border-top:2px solid var(--border);"><td style="font-weight:600;">Not Found</td><td class="text-right mono" style="font-weight:600;">{{ enrichment_stats.not_found }}</td><td class="text-right mono">{{ '%.1f' | format(enrichment_stats.not_found / enrichment_stats.total * 100 if enrichment_stats.total else 0) }}%</td></tr>
        </tbody>
    </table>

    {% if enrichment_no_dates and enrichment_no_dates | length > 0 %}
    <details style="margin-top:16px;">
        <summary style="cursor:pointer; font-size:12px; color:var(--text-muted); font-weight:500;">
            {{ enrichment_no_dates | length }} tracks without dates
        </summary>
        <div style="max-height:300px; overflow-y:auto; margin-top:8px;">
            <table style="font-size:11px;">
                <thead><tr><th>ISRC</th><th>Title</th><th>Artist</th></tr></thead>
                <tbody>
                {% for t in enrichment_no_dates[:20] %}
                <tr>
                    <td class="mono" style="color:var(--text-dim);">{{ t.isrc or '—' }}</td>
                    <td>{{ t.title }}</td>
                    <td style="color:var(--text-muted);">{{ t.artist }}</td>
                </tr>
                {% endfor %}
                {% if enrichment_no_dates | length > 20 %}
                <tr><td colspan="3" style="color:var(--text-dim); font-style:italic;">...and {{ enrichment_no_dates | length - 20 }} more</td></tr>
                {% endif %}
                </tbody>
            </table>
        </div>
    </details>
    {% endif %}
</div>

<form method="POST" action="/custom/enrich">
    <input type="hidden" name="action" value="continue">
    <button type="submit" class="btn-submit">Continue to Export Options &rarr;</button>
</form>

{% elif enrichment_running %}
{# --- Enrichment in progress: poll with ETA countdown --- #}
<div class="card" style="text-align:center; padding:40px;">
    <div class="loading-ring" style="margin:0 auto 16px;"></div>
    <div id="enrichProgress" style="font-size:14px; color:var(--text-primary); font-weight:600;">Starting enrichment...</div>
    <div id="enrichDetail" style="font-size:12px; color:var(--text-muted); margin-top:8px;"></div>
    <div id="enrichEta" style="font-size:13px; color:var(--accent); margin-top:10px; font-weight:600; font-variant-numeric:tabular-nums;"></div>
    <div style="margin-top:16px; background:var(--bg-inset); border-radius:8px; height:8px; overflow:hidden;">
        <div id="enrichBar" style="height:100%; background:var(--accent); border-radius:8px; width:0%; transition:width 0.5s;"></div>
    </div>
</div>

<script>
(function() {
    let etaSeconds = 0;
    let etaTimer = null;

    function formatEta(s) {
        if (s <= 0) return '';
        const m = Math.floor(s / 60);
        const sec = s % 60;
        if (m > 0) return 'Est. ' + m + ':' + String(sec).padStart(2, '0') + ' remaining';
        return 'Est. ' + sec + 's remaining';
    }

    function tickEta() {
        if (etaSeconds > 0) {
            etaSeconds--;
            document.getElementById('enrichEta').textContent = formatEta(etaSeconds);
        } else {
            document.getElementById('enrichEta').textContent = '';
        }
    }

    etaTimer = setInterval(tickEta, 1000);

    const interval = setInterval(function() {
        fetch('/api/enrichment-status')
            .then(r => r.json())
            .then(data => {
                document.getElementById('enrichProgress').textContent = data.message || 'Processing...';
                document.getElementById('enrichDetail').textContent = data.phase || '';
                if (data.total > 0) {
                    const pct = Math.round(data.current / data.total * 100);
                    document.getElementById('enrichBar').style.width = pct + '%';
                }
                // Update ETA from server (more accurate than local countdown)
                if (typeof data.eta_seconds === 'number') {
                    etaSeconds = data.eta_seconds;
                    document.getElementById('enrichEta').textContent = formatEta(etaSeconds);
                }
                if (data.done) {
                    clearInterval(interval);
                    clearInterval(etaTimer);
                    document.getElementById('enrichEta').textContent = '';
                    window.location.href = '/custom/enrich';
                }
                if (data.error) {
                    clearInterval(interval);
                    clearInterval(etaTimer);
                    document.getElementById('enrichEta').textContent = '';
                    document.getElementById('enrichProgress').textContent = 'Error: ' + data.error;
                    document.getElementById('enrichProgress').style.color = 'var(--red)';
                }
            })
            .catch(() => {});
    }, 2000);
})();
</script>

{% else %}
{# --- Enrichment config form --- #}
<form method="POST" action="/custom/enrich" style="margin-bottom:16px; max-width:600px;">
    <input type="hidden" name="action" value="skip">
    <div style="display:flex; align-items:center; gap:16px; padding:16px 20px; background:var(--bg-inset); border:1px solid var(--border); border-radius:10px;">
        <div style="flex:1;">
            <div style="font-size:14px; font-weight:600; color:var(--text-primary);">Don't need release dates?</div>
            <div style="font-size:12px; color:var(--text-muted); margin-top:2px;">Skip this step if you don't need release date enrichment.</div>
        </div>
        <button type="submit" class="btn-submit" style="background:var(--bg-secondary); color:var(--text-primary); border:1px solid var(--border); white-space:nowrap; padding:10px 24px; font-size:13px;">
            Skip Enrichment &rarr;
        </button>
    </div>
</form>

<div class="card" style="margin-bottom:16px; max-width:600px;">
    <div class="card-header"><span class="card-title">Enrichment Sources</span></div>
    <form method="POST" action="/custom/enrich">
        <input type="hidden" name="action" value="enrich">

        <div style="display:flex; flex-direction:column; gap:12px;">
            <label style="display:flex; align-items:center; gap:10px; padding:10px 12px; background:var(--bg-secondary); border-radius:6px; cursor:pointer;">
                <input type="checkbox" name="use_musicbrainz" value="1" checked disabled style="width:16px; height:16px; accent-color:var(--green);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">MusicBrainz</div>
                    <div style="font-size:11px; color:var(--text-dim);">Always enabled — free, no API key needed (1 req/sec)</div>
                </div>
                <span style="margin-left:auto; font-size:10px; padding:2px 8px; border-radius:4px; background:var(--green-dim); color:var(--green);">TIER 1</span>
            </label>

            <label style="display:flex; align-items:center; gap:10px; padding:10px 12px; background:var(--bg-secondary); border-radius:6px; cursor:pointer; opacity:{{ '1' if genius_available else '0.5' }};">
                <input type="checkbox" name="use_genius" value="1" {{ 'checked' if genius_available else 'disabled' }} style="width:16px; height:16px; accent-color:var(--green);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Genius</div>
                    <div style="font-size:11px; color:var(--text-dim);">Fuzzy title + artist search for tracks not found on MusicBrainz</div>
                </div>
                {% if genius_available %}
                <span style="margin-left:auto; font-size:10px; padding:2px 8px; border-radius:4px; background:var(--green-dim); color:var(--green);">TIER 2</span>
                {% else %}
                <span style="margin-left:auto; font-size:10px; padding:2px 8px; border-radius:4px; background:rgba(255,255,255,0.05); color:var(--text-dim);">NO KEY</span>
                {% endif %}
            </label>

            <label style="display:flex; align-items:center; gap:10px; padding:10px 12px; background:var(--bg-secondary); border-radius:6px; cursor:pointer; opacity:{{ '1' if gemini_available else '0.5' }};">
                <input type="checkbox" name="use_gemini" value="1" {{ 'checked' if gemini_available else 'disabled' }} style="width:16px; height:16px; accent-color:var(--green);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Google Search</div>
                    <div style="font-size:11px; color:var(--text-dim);">AI-powered batch lookup for remaining tracks — results flagged for verification</div>
                </div>
                {% if gemini_available %}
                <span style="margin-left:auto; font-size:10px; padding:2px 8px; border-radius:4px; background:var(--green-dim); color:var(--green);">TIER 3</span>
                {% else %}
                <span style="margin-left:auto; font-size:10px; padding:2px 8px; border-radius:4px; background:rgba(255,255,255,0.05); color:var(--text-dim);">NO KEY</span>
                {% endif %}
            </label>
        </div>

        <div style="font-size:11px; color:var(--text-dim); margin-top:12px;">API keys are configured in the server .env file. Contact your admin to add or change keys.</div>

        <button type="submit" class="btn-submit" style="margin-top:20px;">Run Enrichment</button>
    </form>
</div>
{% endif %}

{% elif custom_step == 'export' %}
{# ==================== PHASE 3: EXPORT OPTIONS ==================== #}
{{ custom_stepper('export') }}
<div class="page-header">
    <h1>Export Options</h1>
    <p>Choose which output formats to generate.</p>
</div>

<div class="card" style="margin-bottom:16px; max-width:600px;">
    <div class="card-header"><span class="card-title">Output Formats</span></div>
    <form method="POST" action="/custom/export">
        <div style="display:flex; flex-direction:column; gap:12px;">
            <label style="display:flex; align-items:center; gap:10px; padding:12px; background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; cursor:pointer;">
                <input type="checkbox" name="combined_csv" value="1" checked style="accent-color:var(--accent);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Combined CSV</div>
                    <div style="font-size:11px; color:var(--text-muted);">All payors in a single CSV file</div>
                </div>
            </label>

            <label style="display:flex; align-items:center; gap:10px; padding:12px; background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; cursor:pointer;">
                <input type="checkbox" name="per_payor_csv" value="1" checked style="accent-color:var(--accent);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Per-Payor CSVs</div>
                    <div style="font-size:11px; color:var(--text-muted);">One CSV file per payor: {% for code in export_payor_names %}{{ code }}{{ ', ' if not loop.last }}{% endfor %}</div>
                </div>
            </label>

            <label style="display:flex; align-items:center; gap:10px; padding:12px; background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; cursor:pointer;">
                <input type="checkbox" name="combined_excel" value="1" checked style="accent-color:var(--accent);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Combined Excel</div>
                    <div style="font-size:11px; color:var(--text-muted);">Multi-sheet Excel workbook with all payors</div>
                </div>
            </label>

            <label style="display:flex; align-items:center; gap:10px; padding:12px; background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; cursor:pointer;">
                <input type="checkbox" name="per_payor_excel" value="1" style="accent-color:var(--accent);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Per-Payor Excel</div>
                    <div style="font-size:11px; color:var(--text-muted);">One Excel workbook per payor</div>
                </div>
            </label>
        </div>

        <div class="card" style="margin-top:16px; border:1px solid var(--border);">
            <div class="card-header"><span class="card-title">Aggregate Data</span></div>
            <label style="display:flex; align-items:center; gap:10px; padding:12px; cursor:pointer;">
                <input type="checkbox" name="aggregate" value="1" id="aggToggle" onchange="document.getElementById('aggFields').style.display=this.checked?'block':'none'" style="accent-color:var(--accent);">
                <div>
                    <div style="font-size:13px; font-weight:500; color:var(--text-primary);">Enable aggregation</div>
                    <div style="font-size:11px; color:var(--text-muted);">Group rows by selected fields. Numeric columns (Earnings, Fees, Units, etc.) will be summed. Reduces row count for large datasets.</div>
                </div>
            </label>
            <div id="aggFields" style="display:none; padding:0 12px 12px;">
                <div style="font-size:12px; color:var(--text-muted); margin-bottom:8px;">Group by (only mapped columns shown):</div>
                <div style="display:flex; flex-direction:column; gap:6px;">
                    {% for af in available_agg_fields | default(['Statement Date', 'Payor', 'ISRC', 'Title', 'Artist']) %}
                    <label style="display:flex; align-items:center; gap:8px; font-size:12px; color:var(--text-primary); cursor:pointer;">
                        <input type="checkbox" name="aggregate_by" value="{{ af }}" {{ 'checked' if af in default_agg_checked | default([]) }} style="accent-color:var(--accent);">
                        {{ af }}
                    </label>
                    {% endfor %}
                </div>
            </div>
        </div>

        <button type="submit" class="btn-submit" style="margin-top:20px;" onclick="showLoading()">Finalize &amp; Generate &rarr;</button>
    </form>
</div>

{% elif ingest_step == 'detect' %}
{# ---- INGEST DETECT (Checkpoint 1) ---- #}
{{ step_indicator('detect') }}
<div class="page-header">
    <h1>Detect Headers</h1>
    <p>Review the raw data below. Confirm which row contains the column headers{% if detection.sheets %} and select the sheet{% endif %}.</p>
</div>

<form method="POST" action="/ingest/detect">
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">{{ ingest.filename }}</span></div>
    <div class="form-row" style="margin-bottom:16px;">
        <div class="form-group">
            <label class="form-label">Header Row</label>
            <select class="form-input" name="header_row">
                {% for i in range(detection.preview_rows | length) %}
                <option value="{{ i }}" {{ 'selected' if i == detection.header_row }}>Row {{ i + 1 }}{% if i == detection.header_row %} (detected){% endif %}</option>
                {% endfor %}
            </select>
        </div>
        {% if detection.sheets %}
        <div class="form-group">
            <label class="form-label">Sheet</label>
            <select class="form-input" name="sheet_name">
                {% for s in detection.sheets %}
                <option value="{{ s }}" {{ 'selected' if s == ingest.sheet_name }}>{{ s }}</option>
                {% endfor %}
            </select>
        </div>
        {% endif %}
    </div>

    <div style="overflow-x:auto; margin-bottom:16px;">
        <table>
            <tbody>
            {% for row in detection.preview_rows %}
            <tr style="{% if loop.index0 == detection.header_row %}background:rgba(59,130,246,0.15); border:2px solid var(--accent);{% endif %}">
                <td style="width:50px; color:var(--text-dim); font-size:11px; font-weight:600;">
                    Row {{ loop.index }}
                    {% if loop.index0 == detection.header_row %}
                    <span style="color:var(--accent); font-size:10px; display:block;">HEADER</span>
                    {% endif %}
                </td>
                {% for cell in row %}
                <td style="font-size:12px; white-space:nowrap; max-width:200px; overflow:hidden; text-overflow:ellipsis;
                    {% if loop.index0 == detection.header_row %}color:var(--text-primary); font-weight:600;{% else %}color:var(--text-secondary);{% endif %}
                ">{{ cell }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    <button type="submit" class="btn-submit" style="max-width:300px;">Confirm Headers</button>
</div>
</form>

<a href="/upload" style="font-size:12px; color:var(--text-muted);">Start over</a>

{% elif ingest_step == 'map' %}
{# ---- INGEST MAP (Checkpoint 2) ---- #}
{{ step_indicator('map') }}
<div class="page-header">
    <h1>Map Columns</h1>
    <p>Map each source column to the canonical schema. Required fields are marked with *.</p>
</div>

<form method="POST" action="/ingest/map">
<div class="grid grid-wide" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">Column Mapping</span></div>

        {% set required_mapped = [] %}
        <table>
            <thead>
                <tr>
                    <th>Source Column</th>
                    <th>Confidence</th>
                    <th>Map To</th>
                </tr>
            </thead>
            <tbody>
            {% for col, info in proposed.items() %}
            <tr>
                <td style="color:var(--text-primary); font-weight:500;">{{ col }}</td>
                <td>
                    {% if info.confidence >= 1.0 %}
                    <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:var(--green-dim); color:var(--green);">Saved</span>
                    {% elif info.confidence >= 0.9 %}
                    <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:rgba(162,139,250,0.15); color:var(--purple);">Synonym</span>
                    {% elif info.confidence >= 0.7 %}
                    <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:rgba(59,130,246,0.15); color:var(--accent);">Pattern</span>
                    {% else %}
                    <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:var(--bg-inset); color:var(--text-dim);">Manual</span>
                    {% endif %}
                </td>
                <td>
                    <select class="form-input" name="map_{{ col }}" style="max-width:200px;">
                        <option value="">-- skip --</option>
                        {% for f in canonical_fields %}
                        <option value="{{ f }}" {{ 'selected' if info.canonical == f }}>{{ f }}{% if f in required_fields %} *{% endif %}</option>
                        {% endfor %}
                    </select>
                </td>
            </tr>
            {% endfor %}
            </tbody>
        </table>

        <div style="margin-top:12px; font-size:11px; color:var(--text-dim);">
            * Required: {{ required_fields | join(', ') }}
        </div>

        <button type="submit" class="btn-submit" style="max-width:300px; margin-top:16px;">Confirm Mapping</button>
    </div>

    <div class="card">
        <div class="card-header"><span class="card-title">Data Preview</span></div>
        {% if data_preview %}
        <div style="overflow-x:auto; font-size:11px;">
            <table>
                <thead>
                    <tr>
                        {% for col in detection.headers %}
                        <th style="font-size:10px;">{{ col[:20] }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                {% for row in data_preview %}
                <tr>
                    {% for cell in row %}
                    <td style="font-size:11px; white-space:nowrap; max-width:150px; overflow:hidden; text-overflow:ellipsis;">{{ cell }}</td>
                    {% endfor %}
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
        {% else %}
        <p style="font-size:12px; color:var(--text-dim);">No preview data available.</p>
        {% endif %}
    </div>
</div>
</form>

<a href="/ingest/detect" style="font-size:12px; color:var(--text-muted);">Back to header detection</a>
<span style="color:var(--text-dim); margin:0 8px;">&middot;</span>
<a href="/upload" style="font-size:12px; color:var(--text-muted);">Start over</a>

{% elif ingest_step == 'qc' %}
{# ---- INGEST QC (Checkpoint 3) ---- #}
{{ step_indicator('qc') }}
<div class="page-header">
    <h1>Quality Check</h1>
    <p>Review the QC report below. Approve to export the mapped file.</p>
</div>

{# Stat cards #}
<div class="grid grid-4" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">Total Rows</span></div>
        <div class="stat-value medium">{{ qc.total_rows | default(0) }}</div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">Valid Rows</span></div>
        <div class="stat-value medium text-green">{{ qc.valid_rows | default(0) }}</div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">Warnings</span></div>
        <div class="stat-value medium {% if qc.warning_count > 0 %}text-yellow{% endif %}">{{ qc.warning_count | default(0) }}</div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">Errors</span></div>
        <div class="stat-value medium {% if qc.error_count > 0 %}text-red{% endif %}">{{ qc.error_count | default(0) }}</div>
    </div>
</div>

{# Aggregate stats #}
{% if stats %}
<div class="grid grid-4" style="margin-bottom:16px;">
    {% if stats.unique_ids is defined %}
    <div class="card">
        <div class="card-header"><span class="card-title">Unique IDs</span></div>
        <div class="stat-value small">{{ stats.unique_ids }}</div>
    </div>
    {% endif %}
    {% if stats.unique_periods is defined %}
    <div class="card">
        <div class="card-header"><span class="card-title">Periods</span></div>
        <div class="stat-value small">{{ stats.unique_periods }}</div>
    </div>
    {% endif %}
    {% if stats.gross_sum is defined %}
    <div class="card">
        <div class="card-header"><span class="card-title">Gross Sum</span></div>
        <div class="stat-value small">{{ results.currency_symbol | default('$') }}{{ stats.gross_sum }}</div>
    </div>
    {% endif %}
    {% if stats.net_sum is defined %}
    <div class="card">
        <div class="card-header"><span class="card-title">Net Sum</span></div>
        <div class="stat-value small">{{ results.currency_symbol | default('$') }}{{ stats.net_sum }}</div>
    </div>
    {% endif %}
</div>
{% endif %}

{# Issues table #}
{% if qc.issues %}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Issues ({{ qc.issues | length }})</span></div>
    <table>
        <thead>
            <tr><th>Severity</th><th>Check</th><th>Message</th><th class="text-right">Count</th></tr>
        </thead>
        <tbody>
        {% for issue in qc.issues %}
        <tr>
            <td>
                {% if issue.severity == 'ERROR' %}
                <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:var(--red-dim); color:var(--red);">ERROR</span>
                {% else %}
                <span style="font-size:11px; padding:2px 8px; border-radius:4px; background:rgba(251,191,36,0.15); color:var(--yellow);">WARNING</span>
                {% endif %}
            </td>
            <td class="mono" style="font-size:11px; color:var(--text-muted);">{{ issue.check }}</td>
            <td style="color:var(--text-secondary);">{{ issue.message }}</td>
            <td class="text-right mono">{{ issue.count }}</td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>
{% endif %}

{# Data preview #}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Data Preview (first 20 rows)</span></div>
    <div style="overflow-x:auto;">
        <table>
            <thead>
                <tr>
                    {% for col in preview_cols %}
                    <th>{{ col }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
            {% for row in preview_data %}
            <tr>
                {% for cell in row %}
                <td style="font-size:12px; white-space:nowrap; max-width:200px; overflow:hidden; text-overflow:ellipsis;">{{ cell }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
</div>

{# Action buttons #}
<div style="display:flex; gap:12px; align-items:center;">
    <a href="/ingest/map" class="nav-btn">Back to Mapping</a>

    <form method="POST" action="/ingest/approve" style="display:flex; gap:8px; align-items:center; margin:0;">
        <select class="form-input" name="export_format" style="width:100px;">
            <option value="xlsx">XLSX</option>
            <option value="csv">CSV</option>
        </select>
        <button type="submit" class="btn-submit" style="width:auto; padding:12px 32px;">Approve &amp; Export</button>
    </form>

    <a href="/upload" style="font-size:12px; color:var(--text-muted); margin-left:auto;">Start over</a>
</div>

{% elif ingest_step == 'done' %}
{# ---- INGEST DONE ---- #}
{{ step_indicator('done') }}
<div class="card" style="text-align:center; padding:60px; max-width:600px; margin:0 auto;">
    <div style="font-size:48px; color:var(--green); margin-bottom:16px;">&#10003;</div>
    <h2 style="color:var(--text-primary); margin-bottom:8px;">Export Complete</h2>
    <p style="color:var(--text-muted); margin-bottom:24px;">
        {{ ingest.filename }} has been mapped and exported successfully.
    </p>
    <div style="display:flex; gap:12px; justify-content:center;">
        <a href="/ingest/download" class="nav-btn primary" style="padding:10px 24px; font-size:13px;">Download Mapped File</a>
        <a href="/upload" class="nav-btn" style="padding:10px 24px; font-size:13px;">Upload Another</a>
    </div>
</div>

{% else %}
{# ---- NORMAL UPLOAD PAGE (no active ingest step) ---- #}
<div class="page-header">
    <h1>Upload Statements</h1>
    <p>Add any number of payors, configure each one, and upload their statement files.</p>
</div>

{# ---- Ingest file upload card + saved formats ---- #}
{% if not demo_autofill|default(false) %}
<div class="grid grid-2" style="margin-bottom:24px;">
    <div class="card">
        <div class="card-header"><span class="card-title">Ingest Statement</span></div>
        <p style="font-size:12px; color:var(--text-dim); margin-bottom:12px;">Upload an XLSX or CSV royalty statement file. The wizard will guide you through column mapping and quality checks.</p>
        <form method="POST" action="/ingest/upload" enctype="multipart/form-data" id="ingestForm">
            <div class="form-group">
                <label class="form-label">Statement File</label>
                <input class="form-input" type="file" name="statement_file" accept=".xlsx,.xls,.xlsb,.csv" required>
            </div>
            <div id="ingestProgress" style="display:none; margin-bottom:8px;">
                <div style="font-size:11px; color:var(--accent); margin-bottom:4px;" id="ingestProgressText">Uploading to storage...</div>
                <div style="height:3px; background:var(--border); border-radius:2px;">
                    <div id="ingestProgressBar" style="height:100%; background:var(--accent); border-radius:2px; width:0%; transition:width 0.2s;"></div>
                </div>
            </div>
            <input type="hidden" name="gcs_file_path" id="ingestGcsPath" value="">
            <input type="hidden" name="gcs_file_name" id="ingestGcsName" value="">
            <button type="submit" class="btn-submit" id="ingestBtn">Upload &amp; Detect</button>
        </form>
    </div>

    <div class="card">
        <div class="card-header"><span class="card-title">Saved Formats ({{ saved_formats | length }})</span></div>
        {% if saved_formats %}
        <div style="max-height:200px; overflow-y:auto;">
            {% for fmt in saved_formats %}
            <div style="padding:8px 0; border-bottom:1px solid var(--border); font-size:12px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <span style="color:var(--text-secondary);">{{ fmt.source_label or fmt.fingerprint[:12] + '...' }}</span>
                    <span style="color:var(--text-dim);">Used {{ fmt.use_count }}x</span>
                </div>
                <div style="color:var(--text-dim); font-size:11px; margin-top:2px;">{{ fmt.column_names | length }} columns &middot; {{ fmt.updated_at.strftime('%Y-%m-%d') if fmt.updated_at is not string and fmt.updated_at else (fmt.updated_at[:10] if fmt.updated_at else '') }}</div>
            </div>
            {% endfor %}
        </div>
        {% else %}
        <p style="font-size:12px; color:var(--text-dim);">No saved formats yet. Mappings are saved automatically after confirmation.</p>
        {% endif %}
    </div>
</div>

{% if import_history %}
<div class="card" style="margin-bottom:24px;">
    <div class="card-header"><span class="card-title">Recent Imports</span></div>
    <table>
        <thead>
            <tr><th>File</th><th>Status</th><th>Rows</th><th>Warnings</th><th>Errors</th><th>Date</th></tr>
        </thead>
        <tbody>
        {% for log in import_history %}
        <tr>
            <td style="color:var(--text-primary);">{{ log.filename }}</td>
            <td><span style="font-size:11px; padding:2px 8px; border-radius:4px;
                {% if log.status == 'approved' %}background:var(--green-dim); color:var(--green);
                {% else %}background:var(--bg-inset); color:var(--text-muted);{% endif %}
            ">{{ log.status }}</span></td>
            <td class="mono">{{ log.row_count }}</td>
            <td class="mono {% if log.qc_warnings > 0 %}text-yellow{% endif %}">{{ log.qc_warnings }}</td>
            <td class="mono {% if log.qc_errors > 0 %}text-red{% endif %}">{{ log.qc_errors }}</td>
            <td style="color:var(--text-dim); font-size:11px;">{{ log.created_at.strftime('%Y-%m-%d %H:%M') if log.created_at is not string and log.created_at else (log.created_at[:16].replace('T', ' ') if log.created_at else '') }}</td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>
{% endif %}
{% endif %}

{# ---- Quick-run card ---- #}
<div class="card" style="margin-bottom:16px;">
    <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px;">
        <div>
            <span class="card-title">Quick Run</span>
            <p style="font-size:12px; color:var(--text-dim); margin-top:2px;">
                Use the {{ default_payors | length }} default payors already configured on this machine
                ({% for cfg in default_payors %}{{ cfg.code }}{{ ', ' if not loop.last }}{% endfor %}).
            </p>
        </div>
        <form method="POST" action="/run-default" style="margin:0;">
            <button type="submit" class="nav-btn primary" style="padding:9px 20px; font-size:13px;" onclick="showLoading()">
                Consolidate Defaults
            </button>
        </form>
    </div>
</div>

{# ---- Custom payor upload form ---- #}
<form method="POST" action="/custom/upload" enctype="multipart/form-data" id="uploadForm">
    <div class="card" style="margin-bottom:16px;">
        <div class="card-header"><span class="card-title">Deal</span></div>
        <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">Deal / Project Name</label>
            <input class="form-input" type="text" name="deal_name" placeholder="e.g. PLYGRND, Artist X Catalog, Label Y Acquisition..." value="{{ deal_name or '' }}">
        </div>
    </div>

    <div class="card" style="margin-bottom:16px;">
        <div class="card-header">
            <span class="card-title">Payors</span>
            <button type="button" class="nav-btn" onclick="addPayor()">+ Add Payor</button>
        </div>
        <p style="font-size:12px; color:var(--text-dim); margin-bottom:20px;">
            Configure each payor. Provide a <strong style="color:var(--text-secondary);">local directory path</strong> or <strong style="color:var(--text-secondary);">upload files</strong> (one or the other).
        </p>

        <div id="payorList">
            {# Default first payor #}
            <div class="payor-block" data-idx="0">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                    <span style="font-size:14px; font-weight:600; color:var(--text-primary);">Payor 1</span>
                </div>
                {% if default_payors %}
                <div class="form-group" style="margin-bottom:12px;">
                    <label class="form-label">Preset</label>
                    <select class="form-input" onchange="applyPreset(this)">
                        <option value="">Custom</option>
                        {% for p in default_payors|sort(attribute='name') %}
                        <option value="{{ p.code }}">{{ p.name }}</option>
                        {% endfor %}
                    </select>
                </div>
                {% endif %}
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group" style="flex:0.7;">
                        <label class="form-label">Code</label>
                        <input class="form-input" type="text" name="payor_code_0" placeholder="B1, RJ, etc." required>
                    </div>
                    <div class="form-group" style="flex:1.5;">
                        <label class="form-label">Name</label>
                        <input class="form-input" type="text" name="payor_name_0" placeholder="Believe 15%, RecordJet, etc." required>
                    </div>
                    <div class="form-group" style="flex:1;">
                        <label class="form-label">Format</label>
                        <select class="form-input" name="payor_fmt_0">
                            <option value="auto" selected>Auto-detect</option>
                        </select>
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Statement Type</label>
                        <select class="form-input" name="payor_stype_0">
                            <option value="masters">Masters</option>
                            <option value="publishing">Publishing</option>
                            <option value="neighboring">Neighboring Rights</option>
                            <option value="pro">PRO (Performance)</option>
                            <option value="sync">Sync</option>
                            <option value="other">Other</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Fee %</label>
                        <input class="form-input" type="number" name="payor_fee_0" value="15" min="0" max="100" step="0.1">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Source Currency</label>
                        <select class="form-input" name="payor_source_currency_0">
                            <option value="auto" selected>Auto-detect</option>
                            <option value="USD">USD</option>
                            <option value="EUR">EUR</option>
                            <option value="GBP">GBP</option>
                            <option value="CAD">CAD</option>
                            <option value="AUD">AUD</option>
                            <option value="JPY">JPY</option>
                            <option value="SEK">SEK</option>
                            <option value="NOK">NOK</option>
                            <option value="DKK">DKK</option>
                            <option value="CHF">CHF</option>
                            <option value="BRL">BRL</option>
                        </select>
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group" style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" name="payor_calc_payable_0" id="calcPayable_0" onchange="document.getElementById('payablePctWrap_0').style.display=this.checked?'block':'none'">
                        <label for="calcPayable_0" class="form-label" style="margin:0;">Calculate Payable Amount</label>
                    </div>
                    <div class="form-group" id="payablePctWrap_0" style="display:none;">
                        <label class="form-label">Payable %</label>
                        <input class="form-input" type="number" name="payor_payable_pct_0" value="100" min="0" max="100" step="0.1">
                    </div>
                    <div class="form-group" style="display:flex; align-items:center; gap:8px;">
                        <input type="checkbox" name="payor_calc_third_party_0" id="calcTP_0" onchange="document.getElementById('tpPctWrap_0').style.display=this.checked?'block':'none'">
                        <label for="calcTP_0" class="form-label" style="margin:0;">Calculate Third Party Amount</label>
                    </div>
                    <div class="form-group" id="tpPctWrap_0" style="display:none;">
                        <label class="form-label">Third Party %</label>
                        <input class="form-input" type="number" name="payor_third_party_pct_0" value="0" min="0" max="100" step="0.1">
                    </div>
                </div>
                <div style="background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; padding:12px; margin-bottom:10px;">
                    <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
                        <div class="form-group" style="flex:1; margin:0;">
                            <label class="form-label">Contract PDFs</label>
                            <input class="form-input" type="file" name="payor_contract_0" accept=".pdf" multiple onchange="toggleAnalyzeBtn(this, 0)">
                        </div>
                        <div style="padding-top:18px;">
                            <button type="button" class="nav-btn primary" id="analyzeBtn_0" style="font-size:11px; padding:6px 14px; display:none;" onclick="analyzeContract(0)">Analyze Contract</button>
                        </div>
                    </div>
                    <div id="contractResult_0" style="display:none; font-size:11px; background:var(--bg-card); border:1px solid var(--border); border-radius:6px; padding:10px; margin-top:6px;"></div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Deal Type</label>
                        <select class="form-input" name="payor_deal_type_0">
                            <option value="artist" selected>Artist Deal</option>
                            <option value="label">Label Deal</option>
                        </select>
                        <span style="font-size:10px; color:var(--text-dim);">Whose earnings perspective</span>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Share Split %</label>
                        <input class="form-input" type="number" name="payor_split_0" placeholder="e.g. 50" min="0" max="100" step="0.1">
                        <span style="font-size:10px; color:var(--text-dim);">Your share after distro fees</span>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Territory</label>
                        <input class="form-input" type="text" name="payor_territory_0" placeholder="e.g. Worldwide">
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Expected Period Start</label>
                        <input class="form-input" type="text" name="payor_period_start_0" placeholder="e.g. 202001 (YYYYMM)">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Expected Period End</label>
                        <input class="form-input" type="text" name="payor_period_end_0" placeholder="e.g. 202412 (YYYYMM)">
                    </div>
                    <div class="form-group" style="flex:2;">
                        <label class="form-label">&nbsp;</label>
                        <span style="font-size:10px; color:var(--text-dim); display:block; margin-top:8px;">Optional. Overrides auto-detected range for missing month checks.</span>
                    </div>
                </div>
                <div class="form-group" style="margin-bottom:8px;">
                    <label class="form-label">Local Directory Path</label>
                    <input class="form-input" type="text" name="payor_dir_0" placeholder="C:\Users\jacques\Downloads\RecordJet_extracted">
                    <span style="font-size:10px; color:var(--text-dim);">Paste the folder path containing statement files (walks subfolders automatically)</span>
                </div>
                <div class="form-group" style="margin-bottom:0;">
                    <label class="form-label">Or Upload Files</label>
                    <div class="dropzone">
                        <input type="file" name="payor_files_0" multiple accept=".zip,.xlsx,.xls,.xlsb,.csv,.pdf" style="display:none;" onchange="updateDropzoneFiles(this)">
                        <input type="file" name="payor_folder_0" webkitdirectory style="display:none;" onchange="updateDropzoneFiles(this, 'payor_files_0')">
                        <div class="dz-icon">&#128194;</div>
                        <div class="dz-text">Drag files or folder here, or
                            <strong style="cursor:pointer;text-decoration:underline;" onclick="event.stopPropagation();this.closest('.dropzone').querySelector('input[name=payor_files_0]').click()">browse files</strong> /
                            <strong style="cursor:pointer;text-decoration:underline;" onclick="event.stopPropagation();this.closest('.dropzone').querySelector('input[name=payor_folder_0]').click()">browse folder</strong>
                        </div>
                        <div class="dz-files"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <input type="hidden" name="file_dates_json" id="fileDatesJson" value="">
    <button type="button" class="btn-submit" id="submitBtn" onclick="handleFormSubmit()">
        Process All Payors
    </button>
</form>

{# ---- Date Extraction Modal ---- #}
<div id="dateModal" style="display:none; position:fixed; inset:0; z-index:1000; background:rgba(0,0,0,0.7); backdrop-filter:blur(4px); overflow-y:auto;">
    <div style="max-width:700px; margin:60px auto; background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); padding:28px;">
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:20px;">
            <h2 style="font-size:18px; font-weight:700; color:var(--text-primary);">Confirm Statement Dates</h2>
            <button type="button" class="nav-btn" onclick="closeDateModal()" style="font-size:12px;">Cancel</button>
        </div>
        <p style="font-size:12px; color:var(--text-muted); margin-bottom:16px;">
            Review the auto-detected dates below. Edit any incorrect dates. Editing the first row will <strong style="color:var(--text-secondary);">Flash Fill</strong> matching patterns for remaining rows.
        </p>
        <table style="width:100%; border-collapse:collapse; font-size:12px;">
            <thead>
                <tr style="border-bottom:1px solid var(--border);">
                    <th style="text-align:left; padding:8px; color:var(--text-muted); font-weight:600;">Filename</th>
                    <th style="text-align:left; padding:8px; color:var(--text-muted); font-weight:600; width:140px;">Statement Date</th>
                </tr>
            </thead>
            <tbody id="dateModalBody"></tbody>
        </table>
        <div style="display:flex; gap:8px; margin-top:20px; justify-content:flex-end;">
            <button type="button" class="nav-btn" onclick="closeDateModal()">Cancel</button>
            <button type="button" class="nav-btn primary" onclick="confirmDates()">Confirm &amp; Submit</button>
        </div>
    </div>
</div>

<script>
const PRESET_PAYORS = {{ default_payors | tojson }};
const _gcsAvailable = {{ gcs_available|default(false)|tojson }};

function applyPreset(selectEl) {
    const block = selectEl.closest('.payor-block');
    const idx = block.dataset.idx;
    const code = selectEl.value;
    if (!code) {
        // Custom — clear fields
        block.querySelector('[name=payor_code_'+idx+']').value = '';
        block.querySelector('[name=payor_name_'+idx+']').value = '';
        block.querySelector('[name=payor_fmt_'+idx+']').value = 'auto';
        block.querySelector('[name=payor_stype_'+idx+']').value = 'masters';
        block.querySelector('[name=payor_fee_'+idx+']').value = '15';
        block.querySelector('[name=payor_source_currency_'+idx+']').value = 'auto';
        block.querySelector('[name=payor_split_'+idx+']').value = '';
        block.querySelector('[name=payor_territory_'+idx+']').value = '';
        return;
    }
    const p = PRESET_PAYORS.find(x => x.code === code);
    if (!p) return;
    block.querySelector('[name=payor_code_'+idx+']').value = p.code;
    block.querySelector('[name=payor_name_'+idx+']').value = p.name;
    block.querySelector('[name=payor_fmt_'+idx+']').value = p.fmt || 'auto';
    block.querySelector('[name=payor_stype_'+idx+']').value = p.statement_type || 'masters';
    block.querySelector('[name=payor_fee_'+idx+']').value = p.fee || 0;
    block.querySelector('[name=payor_source_currency_'+idx+']').value = p.source_currency || p.fx_currency || 'auto';
    if (p.artist_split !== '' && p.artist_split !== null)
        block.querySelector('[name=payor_split_'+idx+']').value = p.artist_split;
    if (p.territory)
        block.querySelector('[name=payor_territory_'+idx+']').value = p.territory;
}

function _presetOptions() {
    const sorted = [...PRESET_PAYORS].sort((a,b) => a.name.localeCompare(b.name));
    return '<option value="">Custom</option>' +
        sorted.map(p => '<option value="'+p.code+'">'+p.name+'</option>').join('');
}

function toggleAnalyzeBtn(input, idx) {
    const btn = document.getElementById('analyzeBtn_' + idx);
    if (btn) btn.style.display = input.files.length ? '' : 'none';
}

function analyzeContract(idx) {
    const block = document.querySelector('.payor-block[data-idx="'+idx+'"]') || document.querySelector('.payor-block');
    const fileInput = block ? block.querySelector('[name=payor_contract_'+idx+']') : document.querySelector('[name=payor_contract_'+idx+']');
    if (!fileInput || !fileInput.files.length) return;

    const btn = document.getElementById('analyzeBtn_' + idx);
    const resultDiv = document.getElementById('contractResult_' + idx);
    const numFiles = fileInput.files.length;
    btn.disabled = true;
    btn.textContent = 'Analyzing ' + numFiles + ' file' + (numFiles > 1 ? 's' : '') + '...';
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = '<span style="color:var(--text-dim);">Uploading and analyzing ' + numFiles + ' contract' + (numFiles > 1 ? 's' : '') + ' with Gemini...</span>';

    const fd = new FormData();
    for (let i = 0; i < fileInput.files.length; i++) {
        fd.append('contract_pdfs', fileInput.files[i]);
    }

    fetch('/api/analyze-contract', { method: 'POST', body: fd })
        .then(r => r.json())
        .then(data => {
            btn.disabled = false;
            btn.textContent = 'Analyze Contracts';
            if (data.error) {
                resultDiv.innerHTML = '<span style="color:var(--red);">Error: ' + data.error + '</span>';
                return;
            }
            // Store analysis for submission
            let hidden = block ? block.querySelector('[name=payor_contract_summary_'+idx+']') : document.querySelector('[name=payor_contract_summary_'+idx+']');
            if (!hidden) {
                hidden = document.createElement('input');
                hidden.type = 'hidden';
                hidden.name = 'payor_contract_summary_' + idx;
                (block || fileInput.closest('form')).appendChild(hidden);
            }
            hidden.value = JSON.stringify(data);

            // Show summary
            let html = '<div style="margin-bottom:6px; font-weight:600; color:var(--text-primary);">Contract Summary (' + numFiles + ' document' + (numFiles > 1 ? 's' : '') + ')</div>';
            if (data.summary) html += '<div style="margin-bottom:8px; color:var(--text-secondary);">' + data.summary + '</div>';
            html += '<div style="display:flex; flex-wrap:wrap; gap:6px;">';
            if (data.license_term) html += '<span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--text-secondary);">Term: ' + data.license_term + '</span>';
            if (data.matching_right !== null && data.matching_right !== undefined) html += '<span style="background:' + (data.matching_right ? 'var(--red-dim)' : 'var(--green-dim)') + '; padding:2px 8px; border-radius:4px; color:' + (data.matching_right ? 'var(--red)' : 'var(--green)') + ';">Matching: ' + (data.matching_right ? 'Yes' : 'No') + '</span>';
            if (data.assignment_language !== null && data.assignment_language !== undefined) html += '<span style="background:' + (data.assignment_language ? 'rgba(251,191,36,0.15)' : 'var(--green-dim)') + '; padding:2px 8px; border-radius:4px; color:' + (data.assignment_language ? 'var(--yellow)' : 'var(--green)') + ';">Assignment: ' + (data.assignment_language ? 'Yes' : 'No') + '</span>';
            if (data.distro_fee !== null && data.distro_fee !== undefined) html += '<span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--cyan);">Fee: ' + data.distro_fee + '%</span>';
            if (data.split_pct !== null && data.split_pct !== undefined) html += '<span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--accent);">Split: ' + data.split_pct + '%</span>';
            if (data.deal_type) html += '<span style="background:rgba(139,92,246,0.15); padding:2px 8px; border-radius:4px; color:var(--purple);">' + (data.deal_type === 'artist' ? 'Artist Deal' : 'Label Deal') + '</span>';
            html += '</div>';
            html += '<div style="margin-top:8px;"><button type="button" class="nav-btn" style="font-size:10px; padding:3px 10px;" onclick="applyContractTerms('+idx+')">Apply to form fields</button></div>';
            resultDiv.innerHTML = html;
        })
        .catch(err => {
            btn.disabled = false;
            btn.textContent = 'Analyze Contracts';
            resultDiv.innerHTML = '<span style="color:var(--red);">Error: ' + err.message + '</span>';
        });
}

function applyContractTerms(idx) {
    const block = document.querySelector('.payor-block[data-idx="'+idx+'"]') || document.querySelector('.payor-block');
    const hidden = block ? block.querySelector('[name=payor_contract_summary_'+idx+']') : document.querySelector('[name=payor_contract_summary_'+idx+']');
    if (!hidden || !hidden.value) return;
    const data = JSON.parse(hidden.value);
    const q = name => block ? block.querySelector('[name='+name+']') : document.querySelector('[name='+name+']');

    if (data.deal_type) { const el = q('payor_deal_type_'+idx); if (el) el.value = data.deal_type; }
    if (data.split_pct !== null && data.split_pct !== undefined) { const el = q('payor_split_'+idx); if (el) el.value = data.split_pct; }
    if (data.distro_fee !== null && data.distro_fee !== undefined) { const el = q('payor_fee_'+idx); if (el) el.value = data.distro_fee; }
}

let payorIdx = 1;
function addPayor() {
    const n = payorIdx;
    payorIdx++;
    const html = `
    <div class="payor-block" data-idx="${n}" style="margin-top:16px;">
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
            <span style="font-size:14px; font-weight:600; color:var(--text-primary);">Payor ${n + 1}</span>
            <button type="button" class="nav-btn" style="font-size:11px; padding:4px 10px; color:var(--red); border-color:var(--red-dim);" onclick="this.closest('.payor-block').remove()">Remove</button>
        </div>
        ${PRESET_PAYORS.length ? '<div class="form-group" style="margin-bottom:12px;"><label class="form-label">Preset</label><select class="form-input" onchange="applyPreset(this)">' + _presetOptions() + '</select></div>' : ''}
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group" style="flex:0.7;">
                <label class="form-label">Code</label>
                <input class="form-input" type="text" name="payor_code_${n}" placeholder="B2, RJ, etc." required>
            </div>
            <div class="form-group" style="flex:1.5;">
                <label class="form-label">Name</label>
                <input class="form-input" type="text" name="payor_name_${n}" required>
            </div>
            <div class="form-group" style="flex:1;">
                <label class="form-label">Format</label>
                <select class="form-input" name="payor_fmt_${n}">
                    <option value="auto" selected>Auto-detect</option>
                </select>
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Statement Type</label>
                <select class="form-input" name="payor_stype_${n}">
                    <option value="masters">Masters</option>
                    <option value="publishing">Publishing</option>
                    <option value="neighboring">Neighboring Rights</option>
                    <option value="pro">PRO (Performance)</option>
                    <option value="sync">Sync</option>
                    <option value="other">Other</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Fee %</label>
                <input class="form-input" type="number" name="payor_fee_${n}" value="15" min="0" max="100" step="0.1">
            </div>
            <div class="form-group">
                <label class="form-label">Source Currency</label>
                <select class="form-input" name="payor_source_currency_${n}">
                    <option value="auto" selected>Auto-detect</option>
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                    <option value="GBP">GBP</option>
                    <option value="CAD">CAD</option>
                    <option value="AUD">AUD</option>
                    <option value="JPY">JPY</option>
                    <option value="SEK">SEK</option>
                    <option value="NOK">NOK</option>
                    <option value="DKK">DKK</option>
                    <option value="CHF">CHF</option>
                    <option value="BRL">BRL</option>
                </select>
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group" style="display:flex; align-items:center; gap:8px;">
                <input type="checkbox" name="payor_calc_payable_${n}" id="calcPayable_${n}" onchange="document.getElementById('payablePctWrap_${n}').style.display=this.checked?'block':'none'">
                <label for="calcPayable_${n}" class="form-label" style="margin:0;">Calculate Payable Amount</label>
            </div>
            <div class="form-group" id="payablePctWrap_${n}" style="display:none;">
                <label class="form-label">Payable %</label>
                <input class="form-input" type="number" name="payor_payable_pct_${n}" value="100" min="0" max="100" step="0.1">
            </div>
            <div class="form-group" style="display:flex; align-items:center; gap:8px;">
                <input type="checkbox" name="payor_calc_third_party_${n}" id="calcTP_${n}" onchange="document.getElementById('tpPctWrap_${n}').style.display=this.checked?'block':'none'">
                <label for="calcTP_${n}" class="form-label" style="margin:0;">Calculate Third Party Amount</label>
            </div>
            <div class="form-group" id="tpPctWrap_${n}" style="display:none;">
                <label class="form-label">Third Party %</label>
                <input class="form-input" type="number" name="payor_third_party_pct_${n}" value="0" min="0" max="100" step="0.1">
            </div>
        </div>
        <div style="background:var(--bg-inset); border:1px solid var(--border); border-radius:8px; padding:12px; margin-bottom:10px;">
            <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
                <div class="form-group" style="flex:1; margin:0;">
                    <label class="form-label">Contract PDFs</label>
                    <input class="form-input" type="file" name="payor_contract_${n}" accept=".pdf" multiple onchange="toggleAnalyzeBtn(this, ${n})">
                </div>
                <div style="padding-top:18px;">
                    <button type="button" class="nav-btn primary" id="analyzeBtn_${n}" style="font-size:11px; padding:6px 14px; display:none;" onclick="analyzeContract(${n})">Analyze Contracts</button>
                </div>
            </div>
            <div id="contractResult_${n}" style="display:none; font-size:11px; background:var(--bg-card); border:1px solid var(--border); border-radius:6px; padding:10px; margin-top:6px;"></div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Deal Type</label>
                <select class="form-input" name="payor_deal_type_${n}">
                    <option value="artist" selected>Artist Deal</option>
                    <option value="label">Label Deal</option>
                </select>
                <span style="font-size:10px; color:var(--text-dim);">Whose earnings perspective</span>
            </div>
            <div class="form-group">
                <label class="form-label">Share Split %</label>
                <input class="form-input" type="number" name="payor_split_${n}" placeholder="e.g. 50" min="0" max="100" step="0.1">
                <span style="font-size:10px; color:var(--text-dim);">Your share after distro fees</span>
            </div>
            <div class="form-group">
                <label class="form-label">Territory</label>
                <input class="form-input" type="text" name="payor_territory_${n}" placeholder="e.g. Worldwide">
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Expected Period Start</label>
                <input class="form-input" type="text" name="payor_period_start_${n}" placeholder="e.g. 202001 (YYYYMM)">
            </div>
            <div class="form-group">
                <label class="form-label">Expected Period End</label>
                <input class="form-input" type="text" name="payor_period_end_${n}" placeholder="e.g. 202412 (YYYYMM)">
            </div>
            <div class="form-group" style="flex:2;">
                <label class="form-label">&nbsp;</label>
                <span style="font-size:10px; color:var(--text-dim); display:block; margin-top:8px;">Optional. Overrides auto-detected range for missing month checks.</span>
            </div>
        </div>
        <div class="form-group" style="margin-bottom:8px;">
            <label class="form-label">Local Directory Path</label>
            <input class="form-input" type="text" name="payor_dir_${n}" placeholder="C:\\path\\to\\statements">
            <span style="font-size:10px; color:var(--text-dim);">Paste folder path (walks subfolders automatically)</span>
        </div>
        <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">Or Upload Files</label>
            <div class="dropzone">
                <input type="file" name="payor_files_${n}" multiple accept=".zip,.xlsx,.xls,.xlsb,.csv,.pdf" style="display:none;" onchange="updateDropzoneFiles(this)">
                <input type="file" name="payor_folder_${n}" webkitdirectory style="display:none;" onchange="updateDropzoneFiles(this, 'payor_files_${n}')">
                <div class="dz-icon">&#128194;</div>
                <div class="dz-text">Drag files or folder here, or
                    <strong style="cursor:pointer;text-decoration:underline;" onclick="event.stopPropagation();this.closest('.dropzone').querySelector('input[name=payor_files_${n}]').click()">browse files</strong> /
                    <strong style="cursor:pointer;text-decoration:underline;" onclick="event.stopPropagation();this.closest('.dropzone').querySelector('input[name=payor_folder_${n}]').click()">browse folder</strong>
                </div>
                <div class="dz-files"></div>
            </div>
        </div>
    </div>`;
    document.getElementById('payorList').insertAdjacentHTML('beforeend', html);
    // Setup dropzone on the newly added block
    const newBlock = document.querySelector('.payor-block[data-idx="'+n+'"]');
    if (newBlock) setupDropzone(newBlock.querySelector('.dropzone'));
}

/* ---- Drag-and-Drop Helpers ---- */
const _dzFiles = {};  // maps input name (payor_files_N) -> File[]
const ACCEPTED_EXT = ['.csv', '.xlsx', '.xls', '.xlsb', '.zip', '.pdf'];

function _readEntryRecursive(entry) {
    return new Promise((resolve) => {
        if (entry.isFile) {
            entry.file(f => {
                const ext = f.name.toLowerCase().replace(/^.*(\.[^.]+)$/, '$1');
                resolve(ACCEPTED_EXT.includes(ext) ? [f] : []);
            }, () => resolve([]));
        } else if (entry.isDirectory) {
            const reader = entry.createReader();
            const allFiles = [];
            const readBatch = () => {
                reader.readEntries(entries => {
                    if (!entries.length) { resolve(allFiles); return; }
                    Promise.all(entries.map(e => _readEntryRecursive(e))).then(results => {
                        results.forEach(r => allFiles.push(...r));
                        readBatch();  // keep reading (readEntries returns batches of 100)
                    });
                }, () => resolve(allFiles));
            };
            readBatch();
        } else {
            resolve([]);
        }
    });
}

function _renderDzFiles(dz, files) {
    let filesDiv = dz.querySelector('.dz-files');
    if (!filesDiv) { filesDiv = document.createElement('div'); filesDiv.className = 'dz-files'; dz.appendChild(filesDiv); }
    filesDiv.innerHTML = '';
    for (const f of files) {
        const d = document.createElement('div');
        d.textContent = f.name + ' (' + (f.size/1024).toFixed(1) + ' KB)';
        filesDiv.appendChild(d);
    }
}

function setupDropzone(dz) {
    if (!dz) return;
    ['dragenter','dragover'].forEach(evt => {
        dz.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); dz.classList.add('drag-over'); });
    });
    dz.addEventListener('dragleave', e => { e.preventDefault(); e.stopPropagation(); dz.classList.remove('drag-over'); });
    dz.addEventListener('drop', e => {
        e.preventDefault(); e.stopPropagation(); dz.classList.remove('drag-over');

        const input = dz.querySelector('input[type=file][name^="payor_files_"]');
        const inputName = input ? input.name : null;
        if (!inputName) return;

        // Gather entries/files synchronously before dataTransfer is cleared
        const items = e.dataTransfer.items;
        const entries = [];
        const directFiles = [];
        if (items && items.length) {
            for (let i = 0; i < items.length; i++) {
                const getEntry = items[i].webkitGetAsEntry || items[i].getAsEntry;
                const entry = getEntry ? getEntry.call(items[i]) : null;
                if (entry) {
                    entries.push(entry);
                } else if (items[i].kind === 'file') {
                    const f = items[i].getAsFile();
                    if (f) directFiles.push(f);
                }
            }
        }
        const fallbackFiles = Array.from(e.dataTransfer.files || []);

        // Detect folder drop: single 0-byte file with no extension = folder
        const looksLikeFolder = fallbackFiles.length >= 1 && entries.length === 0 &&
            fallbackFiles.every(f => f.size === 0 && f.type === '' && !f.name.includes('.'));
        if (looksLikeFolder) {
            const folderInput = dz.querySelector('input[webkitdirectory]');
            if (folderInput) {
                // Show message and auto-open folder picker
                let filesDiv = dz.querySelector('.dz-files');
                if (!filesDiv) { filesDiv = document.createElement('div'); filesDiv.className = 'dz-files'; dz.appendChild(filesDiv); }
                filesDiv.innerHTML = '<div style="color:var(--accent);padding:8px 0;">Folder detected \u2014 opening folder picker...</div>';
                // Small delay so user sees the message, then trigger folder picker
                setTimeout(() => folderInput.click(), 300);
            }
            return;
        }

        // Async resolution of directory entries
        (async () => {
            try {
                const collected = [];
                if (entries.length || directFiles.length) {
                    const entryPromises = entries.map(ent => _readEntryRecursive(ent));
                    const results = await Promise.all(entryPromises);
                    results.forEach(r => collected.push(...r));
                    directFiles.forEach(f => {
                        const ext = f.name.toLowerCase().replace(/^.*(\.[^.]+)$/, '$1');
                        if (ACCEPTED_EXT.includes(ext)) collected.push(f);
                    });
                }

                if (collected.length) {
                    _dzFiles[inputName] = collected;
                    _renderDzFiles(dz, collected);
                } else if (fallbackFiles.length) {
                    const filtered = fallbackFiles.filter(f => {
                        const ext = f.name.toLowerCase().replace(/^.*(\.[^.]+)$/, '$1');
                        return ACCEPTED_EXT.includes(ext);
                    });
                    if (filtered.length) {
                        _dzFiles[inputName] = filtered;
                        _renderDzFiles(dz, filtered);
                    }
                }
            } catch(err) {
                console.error('[DZ] drop error:', err);
            }
        })();
    });
}

function updateDropzoneFiles(input, targetName) {
    // targetName: optional override for _dzFiles key (used by folder input)
    const dz = input.closest('.dropzone');
    if (!dz) return;
    const key = targetName || input.name;
    const files = Array.from(input.files).filter(f => {
        const ext = f.name.toLowerCase().replace(/^.*(\.[^.]+)$/, '$1');
        return ACCEPTED_EXT.includes(ext);
    });
    _dzFiles[key] = files;
    _renderDzFiles(dz, files);
}

// Prevent browser from opening dropped files/folders outside dropzones
document.addEventListener('dragover', e => e.preventDefault());
document.addEventListener('drop', e => e.preventDefault());

// Setup dropzones on page load — try both DOMContentLoaded and immediate
function _initDropzones() {
    document.querySelectorAll('.dropzone').forEach(dz => {
        if (!dz._dzSetup) { setupDropzone(dz); dz._dzSetup = true; }
    });
}
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initDropzones);
} else {
    _initDropzones();
}

/* ---- Statement Date Extraction Modal ---- */
let _pendingFilenames = [];
let _dateMap = {};
let _dateSources = {};

function handleFormSubmit() {
    // Collect all filenames from _dzFiles and native file inputs
    const allFiles = [];
    const fileInputs = document.querySelectorAll('input[type=file][name^="payor_files_"]');
    fileInputs.forEach(inp => {
        const dzList = _dzFiles[inp.name];
        if (dzList && dzList.length) {
            dzList.forEach(f => allFiles.push(f.name));
        } else {
            for (const f of inp.files) allFiles.push(f.name);
        }
    });

    // Also collect local directory paths
    const localDirs = [];
    document.querySelectorAll('input[name^="payor_dir_"]').forEach(inp => {
        if (inp.value.trim()) localDirs.push(inp.value.trim());
    });

    if (allFiles.length === 0 && localDirs.length === 0) {
        document.getElementById('uploadForm').submit();
        return;
    }

    // If we have uploaded files, use those directly
    if (allFiles.length > 0) {
        _pendingFilenames = allFiles;
        _fetchDatesAndShowModal(allFiles);
        return;
    }

    // For local dirs, first list the files via API, then show date modal
    fetch('/api/list-dir-files', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({dirs: localDirs}),
    })
    .then(r => r.json())
    .then(data => {
        const dirFiles = data.files || [];
        if (dirFiles.length === 0) {
            _submitViaFetch();
            return;
        }
        _pendingFilenames = dirFiles;
        _fetchDatesAndShowModal(dirFiles);
    })
    .catch(() => { _submitViaFetch(); });
}

function _fetchDatesAndShowModal(filenames) {
    // Collect directory paths so API can peek inside files for dates
    const dirs = [];
    document.querySelectorAll('input[name^="payor_dir_"]').forEach(inp => {
        if (inp.value.trim()) dirs.push(inp.value.trim());
    });
    fetch('/api/extract-dates', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({filenames: filenames, dirs: dirs}),
    })
    .then(r => r.json())
    .then(data => {
        _dateMap = data.dates || {};
        _dateSources = data.sources || {};
        showDateModal();
    })
    .catch(() => { _submitViaFetch(); });
}

function showDateModal() {
    const tbody = document.getElementById('dateModalBody');
    tbody.innerHTML = '';
    _pendingFilenames.forEach((fn, i) => {
        const date = _dateMap[fn] || '';
        const src = (_dateSources && _dateSources[fn]) || '';
        const srcBadge = src === 'content' ? '<span style="font-size:9px; background:var(--green-dim); color:var(--green); padding:1px 5px; border-radius:3px; margin-left:4px;">from file</span>'
                       : src === 'filename' ? '<span style="font-size:9px; background:rgba(59,130,246,0.15); color:var(--accent); padding:1px 5px; border-radius:3px; margin-left:4px;">from name</span>'
                       : '';
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid var(--border)';
        tr.innerHTML = `
            <td style="padding:6px 8px; color:var(--text-secondary); max-width:400px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${fn}">${fn}${srcBadge}</td>
            <td style="padding:6px 8px;">
                <input class="form-input date-input" type="text" data-filename="${fn}" data-idx="${i}"
                       value="${date}" placeholder="MM/DD/YY" style="font-size:12px; padding:4px 8px;"
                       onchange="onDateEdit(this)">
            </td>`;
        tbody.appendChild(tr);
    });
    document.getElementById('dateModal').style.display = 'block';
}

function onDateEdit(input) {
    // Flash Fill: if user edits first row, try to auto-fill others
    if (parseInt(input.dataset.idx) !== 0) return;
    const userDate = input.value.trim();
    if (!userDate) return;
    const firstName = input.dataset.filename;
    // Extract a date-like pattern from the first filename
    const digitPattern = firstName.replace(/[^0-9]/g, '');
    if (digitPattern.length < 4) return;
    // For each remaining row, if they have similar digit patterns, fill with extracted date
    document.querySelectorAll('#dateModalBody .date-input').forEach(inp => {
        if (inp === input) return;
        if (inp.value.trim()) return; // only fill blanks
        const fn = inp.dataset.filename;
        // Try to detect the date from this filename via the same pattern
        // This is client-side heuristic: if filename has YYYYMM or similar, we already got it from API
        // Flash Fill: if the API didn't detect it, copy the user's manual date
        if (!_dateMap[fn]) {
            inp.value = userDate;
        }
    });
}

function closeDateModal() {
    document.getElementById('dateModal').style.display = 'none';
}

function confirmDates() {
    const result = {};
    document.querySelectorAll('#dateModalBody .date-input').forEach(inp => {
        const fn = inp.dataset.filename;
        const val = inp.value.trim();
        if (val) result[fn] = val;
    });
    document.getElementById('fileDatesJson').value = JSON.stringify(result);
    document.getElementById('dateModal').style.display = 'none';
    _submitViaFetch();
}

/* ---- GCS Direct Upload Helpers ---- */

function _updateFileProgress(inputName, fileName, pct) {
    // Find the dropzone for this input and update/create a progress bar
    const input = document.querySelector('input[name="' + inputName + '"]');
    if (!input) return;
    const dz = input.closest('.dropzone');
    if (!dz) return;
    const filesDiv = dz.querySelector('.dz-files');
    if (!filesDiv) return;
    // Find or create progress element for this file
    let progEl = filesDiv.querySelector('[data-prog-file="' + CSS.escape(fileName) + '"]');
    if (!progEl) {
        progEl = document.createElement('div');
        progEl.setAttribute('data-prog-file', fileName);
        progEl.style.cssText = 'margin:2px 0;';
        progEl.innerHTML = '<div style="display:flex;align-items:center;gap:6px;">' +
            '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;" title="' + fileName + '">' + fileName + '</span>' +
            '<span class="prog-pct" style="font-size:11px;color:var(--accent);min-width:36px;text-align:right;">0%</span></div>' +
            '<div style="height:3px;background:var(--border);border-radius:2px;margin-top:2px;">' +
            '<div class="prog-bar" style="height:100%;background:var(--accent);border-radius:2px;width:0%;transition:width 0.2s;"></div></div>';
        filesDiv.appendChild(progEl);
    }
    const bar = progEl.querySelector('.prog-bar');
    const pctEl = progEl.querySelector('.prog-pct');
    if (bar) bar.style.width = Math.round(pct) + '%';
    if (pctEl) pctEl.textContent = Math.round(pct) + '%';
}

function _uploadSingleFileToGCS(file, uploadUrl, gcsPath, inputName) {
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('PUT', uploadUrl, true);
        xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
        xhr.upload.onprogress = function(e) {
            if (e.lengthComputable) {
                _updateFileProgress(inputName, file.name, (e.loaded / e.total) * 100);
            }
        };
        xhr.onload = function() {
            if (xhr.status >= 200 && xhr.status < 300) {
                _updateFileProgress(inputName, file.name, 100);
                resolve({name: file.name, gcs_path: gcsPath});
            } else {
                reject(new Error('GCS upload failed for ' + file.name + ': HTTP ' + xhr.status));
            }
        };
        xhr.onerror = function() {
            reject(new Error('Network error uploading ' + file.name + ' to GCS'));
        };
        xhr.send(file);
    });
}

async function _uploadFilesToGCS(largeFiles) {
    // largeFiles: [{file, inputName, payorIdx}]
    // Request resumable session URLs from backend
    const btn0 = document.getElementById('submitBtn');
    if (btn0) btn0.textContent = 'Preparing ' + largeFiles.length + ' files...';
    const reqFiles = largeFiles.map(lf => ({
        name: lf.file.name,
        size: lf.file.size,
        payor_idx: lf.payorIdx,
        content_type: lf.file.type || 'application/octet-stream',
    }));
    const resp = await fetch('/api/upload-urls', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({files: reqFiles}),
    });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to get upload URLs');
    }
    const data = await resp.json();
    const urlMap = {};
    for (const u of data.urls) {
        urlMap[u.payor_idx + '/' + u.name] = u;
    }
    // Upload each file to GCS with progress tracking
    const results = [];  // {inputName -> [{name, gcs_path}]}
    let completed = 0;
    const total = largeFiles.length;
    const btn = document.getElementById('submitBtn');
    if (btn) btn.textContent = 'Uploading 0/' + total + ' files...';
    const uploads = largeFiles.map(lf => {
        const key = lf.payorIdx + '/' + lf.file.name;
        const info = urlMap[key];
        if (!info) return Promise.reject(new Error('No upload URL for ' + lf.file.name));
        return _uploadSingleFileToGCS(lf.file, info.upload_url, info.gcs_path, lf.inputName)
            .then(result => {
                results.push({inputName: lf.inputName, ...result});
                completed++;
                if (btn) btn.textContent = 'Uploading ' + completed + '/' + total + ' files...';
            });
    });
    await Promise.all(uploads);
    // Group by inputName
    const grouped = {};
    for (const r of results) {
        const key = r.inputName;
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push({name: r.name, gcs_path: r.gcs_path});
    }
    return grouped;
}

function _submitViaFetch() {
    const form = document.getElementById('uploadForm');
    const btn = document.getElementById('submitBtn');
    const origText = btn.textContent;
    btn.textContent = 'Uploading...';
    btn.disabled = true;

    // Collect all files per payor
    const allFiles = [];   // [{file, inputName, payorIdx}]
    const fileInputs = form.querySelectorAll('input[type=file][name^="payor_files_"]');
    fileInputs.forEach(inp => {
        const dzList = _dzFiles[inp.name];
        const files = (dzList && dzList.length) ? dzList : Array.from(inp.files);
        const idx = inp.name.replace('payor_files_', '');
        for (const f of files) {
            allFiles.push({file: f, inputName: inp.name, payorIdx: parseInt(idx)});
        }
    });

    // Step 1: Upload all files to GCS (if available), then submit metadata only
    const gcsPromise = (_gcsAvailable && allFiles.length > 0)
        ? _uploadFilesToGCS(allFiles)
        : Promise.resolve(null);

    gcsPromise.then(gcsResults => {
        const formData = new FormData();

        // Add all non-file form fields (metadata only)
        const inputs = form.querySelectorAll('input:not([type=file]), select, textarea');
        inputs.forEach(inp => {
            if (inp.name) formData.append(inp.name, inp.value);
        });

        if (gcsResults) {
            // GCS path: add gcs_files_N JSON references — no file bytes in the request
            for (const [inputName, gcsFileList] of Object.entries(gcsResults)) {
                const idx = inputName.replace('payor_files_', '');
                formData.append('gcs_files_' + idx, JSON.stringify(gcsFileList));
            }
        } else {
            // Fallback: no GCS — send files as multipart (only works for small uploads)
            for (const af of allFiles) {
                formData.append(af.inputName, af.file, af.file.name);
            }
        }

        btn.textContent = 'Processing...';
        return fetch(form.action || '/custom/upload', {
            method: 'POST',
            body: formData,
            redirect: 'follow',
        });
    })
    .then(resp => {
        if (resp.redirected) {
            window.location.href = resp.url;
        } else {
            return resp.text().then(html => {
                document.open();
                document.write(html);
                document.close();
            });
        }
    })
    .catch(err => {
        console.error('Upload failed:', err);
        btn.textContent = origText;
        btn.disabled = false;
        alert('Upload failed: ' + err.message);
    });
}

/* ---- Ingest Form: GCS upload ---- */
(function() {
    const form = document.getElementById('ingestForm');
    if (!form) return;
    form.addEventListener('submit', function(e) {
        const fileInput = form.querySelector('input[name="statement_file"]');
        const file = fileInput && fileInput.files[0];
        if (!file || !_gcsAvailable) return; // no GCS — let normal submit proceed

        e.preventDefault();
        const btn = document.getElementById('ingestBtn');
        const origText = btn.textContent;
        btn.textContent = 'Uploading...';
        btn.disabled = true;
        document.getElementById('ingestProgress').style.display = 'block';

        // Request a resumable upload URL
        fetch('/api/upload-urls', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({files: [{name: file.name, size: file.size, payor_idx: 0, content_type: file.type || 'application/octet-stream'}]}),
        })
        .then(r => { if (!r.ok) throw new Error('Failed to get upload URL'); return r.json(); })
        .then(data => {
            const info = data.urls[0];
            return new Promise((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                xhr.open('PUT', info.upload_url, true);
                xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
                xhr.upload.onprogress = function(ev) {
                    if (ev.lengthComputable) {
                        const pct = Math.round((ev.loaded / ev.total) * 100);
                        document.getElementById('ingestProgressBar').style.width = pct + '%';
                        document.getElementById('ingestProgressText').textContent = 'Uploading to storage... ' + pct + '%';
                    }
                };
                xhr.onload = function() {
                    if (xhr.status >= 200 && xhr.status < 300) resolve(info);
                    else reject(new Error('GCS upload failed: HTTP ' + xhr.status));
                };
                xhr.onerror = function() { reject(new Error('Network error during upload')); };
                xhr.send(file);
            });
        })
        .then(info => {
            document.getElementById('ingestProgressText').textContent = 'Processing...';
            document.getElementById('ingestGcsPath').value = info.gcs_path;
            document.getElementById('ingestGcsName').value = file.name;
            // Clear the file input so it doesn't send the large file via multipart
            fileInput.value = '';
            // Remove required so the empty file input doesn't block submit
            fileInput.removeAttribute('required');
            form.submit();
        })
        .catch(err => {
            console.error('Ingest GCS upload failed:', err);
            btn.textContent = origText;
            btn.disabled = false;
            document.getElementById('ingestProgress').style.display = 'none';
            alert('Upload failed: ' + err.message);
        });
    });
})();
</script>

{% if demo_autofill|default(false) %}
<script>
(function() {
    const B = '{{ demo_data_dir }}';
    document.querySelector('input[name="deal_name"]').value = 'Demo Catalog';
    document.querySelector('input[name="payor_code_0"]').value = 'B1';
    document.querySelector('input[name="payor_name_0"]').value = 'Believe Digital';
    document.querySelector('select[name="payor_stype_0"]').value = 'masters';
    document.querySelector('input[name="payor_dir_0"]').value = B + '\\\\statements_B1';
    for (let i = 0; i < 3; i++) addPayor();
    setTimeout(function() {
        document.querySelector('input[name="payor_code_1"]').value = 'FUGA';
        document.querySelector('input[name="payor_name_1"]').value = 'FUGA';
        document.querySelector('select[name="payor_stype_1"]').value = 'masters';
        document.querySelector('input[name="payor_dir_1"]').value = B + '\\\\statements_FUGA';
        document.querySelector('input[name="payor_code_2"]').value = 'ST';
        document.querySelector('input[name="payor_name_2"]').value = 'Songtrust';
        document.querySelector('select[name="payor_stype_2"]').value = 'publishing';
        document.querySelector('input[name="payor_dir_2"]').value = B + '\\\\statements_ST';
        document.querySelector('input[name="payor_code_3"]').value = 'EMP';
        document.querySelector('input[name="payor_name_3"]').value = 'Empire';
        document.querySelector('select[name="payor_stype_3"]').value = 'masters';
        document.querySelector('input[name="payor_dir_3"]').value = B + '\\\\statements_EMP';

        // Statement coverage periods
        var el;
        el = document.querySelector('input[name="payor_period_start_0"]'); if(el) el.value = '202501';
        el = document.querySelector('input[name="payor_period_end_0"]');   if(el) el.value = '202503';
        el = document.querySelector('input[name="payor_period_start_1"]'); if(el) el.value = '202501';
        el = document.querySelector('input[name="payor_period_end_1"]');   if(el) el.value = '202502';
        el = document.querySelector('input[name="payor_period_start_2"]'); if(el) el.value = '202501';
        el = document.querySelector('input[name="payor_period_end_2"]');   if(el) el.value = '202502';
        el = document.querySelector('input[name="payor_period_start_3"]'); if(el) el.value = '202501';
        el = document.querySelector('input[name="payor_period_end_3"]');   if(el) el.value = '202501';
    }, 300);
})();
</script>
{% endif %}

{% endif %}

{% elif page == 'deals' %}
{# ==================== DEALS PAGE ==================== #}
<div class="page-header" style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
    <div>
        <h1>Saved Deals</h1>
        <p>Load a previous consolidation run or delete old ones.</p>
    </div>
    <a href="/templates" class="nav-btn" style="padding:8px 16px; font-size:12px;">Templates</a>
</div>

{% if deals %}
<div class="grid grid-3">
    {% for deal in deals %}
    <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:12px;">
            <div>
                <div style="font-size:16px; font-weight:700; color:var(--text-primary); letter-spacing:-0.01em;">{{ deal.name }}</div>
                <div style="font-size:11px; color:var(--text-dim); margin-top:2px;">{{ deal.timestamp.strftime('%Y-%m-%d %H:%M') if deal.timestamp is not string and deal.timestamp else (deal.timestamp[:16].replace('T', ' ') if deal.timestamp else '') }}</div>
            </div>
            <span style="font-size:10px; color:var(--text-muted); background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px;">{{ deal.slug }}</span>
        </div>
        <div style="display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px;">
            {% for code in deal.payor_codes %}
            <span style="font-size:11px; color:var(--accent); background:rgba(59,130,246,0.1); padding:2px 8px; border-radius:4px;">{{ code }}</span>
            {% endfor %}
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px 16px; margin-bottom:14px; padding:10px 0; border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
            <div>
                <div style="font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">LTM Gross</div>
                <div class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ deal.currency_symbol | default('$') }}{{ deal.ltm_gross | default(deal.total_gross) }}</div>
            </div>
            <div style="text-align:right;">
                <div style="font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">LTM Net</div>
                <div class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ deal.currency_symbol | default('$') }}{{ deal.ltm_net | default('0') }}</div>
            </div>
            <div>
                <div style="font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">ISRCs</div>
                <div style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ deal.isrc_count }}</div>
            </div>
            <div style="text-align:right;">
                <div style="font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">Files</div>
                <div style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ deal.total_files }}</div>
            </div>
        </div>
        <div style="display:flex; gap:6px; flex-wrap:wrap;">
            <a href="/deals/{{ deal.slug }}/load" class="nav-btn primary" style="flex:1; text-align:center; padding:9px 0; font-size:13px;">Load</a>
            <a href="/deals/{{ deal.slug }}/edit" class="nav-btn" style="padding:9px 14px; font-size:13px;">Edit</a>
            <form method="POST" action="/deals/{{ deal.slug }}/rerun-quick" style="margin:0;" onsubmit="showLoading();">
                <button type="submit" class="nav-btn" style="padding:9px 14px; font-size:13px; color:var(--cyan);" title="Quick re-run with same config">Re-run</button>
            </form>
            <form method="POST" action="/deals/{{ deal.slug }}/delete" style="margin:0;" onsubmit="return confirm('Delete deal {{ deal.name }}?');">
                <button type="submit" class="nav-btn" style="padding:9px 14px; font-size:13px; color:var(--red); border-color:var(--red-dim);">Delete</button>
            </form>
        </div>
        <div style="margin-top:6px; display:flex; gap:6px;">
            <form method="POST" action="/deals/{{ deal.slug }}/save-template" style="margin:0; flex:1;">
                <input type="hidden" name="template_name" value="Template from {{ deal.name }}">
                <button type="submit" class="nav-btn" style="width:100%; padding:6px 0; font-size:11px; color:var(--text-muted);">Save as Template</button>
            </form>
            <a href="/deals/{{ deal.slug }}/delta" class="nav-btn" style="padding:6px 12px; font-size:11px; color:var(--text-muted);">Delta</a>
            <a href="/deals/{{ deal.slug }}/forecast" class="nav-btn" style="padding:6px 12px; font-size:11px; color:var(--cyan);">{{ 'Forecast' if session.get('forecast_unlocked') else 'Forecast (Beta)' }}</a>
        </div>
    </div>
    {% endfor %}
</div>
{% else %}
<div class="card" style="text-align:center; padding:60px;">
    <div style="font-size:48px; color:var(--text-dim); margin-bottom:16px;">&#128190;</div>
    <p style="color:var(--text-muted); margin-bottom:24px;">No saved deals yet. Run a consolidation with a deal name to save it here.</p>
    <a href="/upload" class="nav-btn primary" style="padding:10px 24px; font-size:13px;">Go to Upload</a>
</div>
{% endif %}

{% elif page == 'templates' %}
{# ==================== TEMPLATES PAGE ==================== #}
<div class="page-header" style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
    <div>
        <h1>Deal Templates</h1>
        <p>Reusable payor configurations for quick deal setup. <a href="/deals" class="nav-btn" style="margin-left:12px; padding:6px 14px; font-size:12px;">Back to Deals</a></p>
    </div>
</div>

{% if templates is defined and templates %}
<div class="grid grid-3">
    {% for tpl in templates %}
    <div class="card">
        <div style="font-size:16px; font-weight:700; color:var(--text-primary); margin-bottom:8px;">{{ tpl.name }}</div>
        {% if tpl.get('settings', {}).get('source_deal') %}
        <div style="font-size:11px; color:var(--text-dim); margin-bottom:8px;">From: {{ tpl.settings.source_deal }}</div>
        {% endif %}
        <div style="display:flex; flex-wrap:wrap; gap:4px; margin-bottom:12px;">
            {% for pc in tpl.get('payor_configs', []) %}
            <span style="font-size:11px; color:var(--accent); background:rgba(59,130,246,0.1); padding:2px 8px; border-radius:4px;">{{ pc.get('code', pc.get('name', '?')) }}</span>
            {% endfor %}
        </div>
        <div style="font-size:11px; color:var(--text-muted); margin-bottom:10px;">{{ tpl.get('payor_configs', []) | length }} payor{{ 's' if tpl.get('payor_configs', []) | length != 1 }}</div>
        <div style="display:flex; gap:8px;">
            <form method="POST" action="/templates/{{ tpl.name }}/apply" style="margin:0; flex:1;">
                <button type="submit" class="nav-btn primary" style="width:100%; padding:9px 0; font-size:13px;">Apply</button>
            </form>
            <form method="POST" action="/templates/{{ tpl.name }}/delete" style="margin:0;" onsubmit="return confirm('Delete template?');">
                <button type="submit" class="nav-btn" style="padding:9px 14px; font-size:13px; color:var(--red); border-color:var(--red-dim);">Delete</button>
            </form>
        </div>
    </div>
    {% endfor %}
</div>
{% else %}
<div class="card" style="text-align:center; padding:60px;">
    <p style="color:var(--text-muted); margin-bottom:24px;">No templates saved yet. Save a deal as a template from the Deals page.</p>
    <a href="/deals" class="nav-btn primary" style="padding:10px 24px; font-size:13px;">Go to Deals</a>
</div>
{% endif %}

{% elif page == 'delta' %}
{# ==================== DELTA REPORT PAGE ==================== #}
<div class="page-header">
    <h1>Delta Report</h1>
    <p>Changes since last re-run for <strong>{{ delta_slug }}</strong>. <a href="/deals" class="nav-btn" style="margin-left:12px; padding:6px 14px; font-size:12px;">Back to Deals</a></p>
</div>

{% if delta_report %}
{# Summary banner #}
<div class="card" style="margin-bottom:16px; border:1px solid var(--accent);">
    <div style="font-size:14px; font-weight:600; color:var(--text-primary); margin-bottom:8px;">Summary</div>
    <div style="font-size:13px; color:var(--text-secondary);">{{ delta_report.summary }}</div>
    <div style="font-size:10px; color:var(--text-dim); margin-top:4px;">Generated {{ delta_report.created_at }}</div>
</div>

{# Period + ISRC changes #}
<div class="grid grid-2" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">Period Changes</span></div>
        {% if delta_report.new_periods %}
        <div style="margin-bottom:8px;">
            <span style="font-size:11px; color:var(--green); font-weight:600;">+{{ delta_report.new_periods | length }} new:</span>
            {% for p in delta_report.new_periods[:12] %}
            <span class="mono" style="font-size:11px; color:var(--text-secondary);">{{ p }}</span>{% if not loop.last %}, {% endif %}
            {% endfor %}
        </div>
        {% endif %}
        {% if delta_report.removed_periods %}
        <div>
            <span style="font-size:11px; color:var(--red); font-weight:600;">-{{ delta_report.removed_periods | length }} removed:</span>
            {% for p in delta_report.removed_periods[:12] %}
            <span class="mono" style="font-size:11px; color:var(--text-secondary);">{{ p }}</span>{% if not loop.last %}, {% endif %}
            {% endfor %}
        </div>
        {% endif %}
        {% if not delta_report.new_periods and not delta_report.removed_periods %}
        <div style="font-size:12px; color:var(--text-dim);">No period changes</div>
        {% endif %}
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">ISRC Changes</span></div>
        <div style="display:flex; gap:16px;">
            <div>
                <span style="font-size:11px; color:var(--green); font-weight:600;">+{{ delta_report.new_isrcs | length }} new</span>
            </div>
            <div>
                <span style="font-size:11px; color:var(--red); font-weight:600;">-{{ delta_report.removed_isrcs | length }} removed</span>
            </div>
        </div>
    </div>
</div>

{# Revenue comparison #}
<div class="grid grid-3" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">LTM Gross</span></div>
        <div style="display:flex; justify-content:space-between; align-items:baseline;">
            <div>
                <div style="font-size:11px; color:var(--text-dim);">Before</div>
                <div class="mono" style="font-size:14px; color:var(--text-muted);">{{ '{:,.2f}'.format(delta_report.old_ltm_gross) }}</div>
            </div>
            <div style="font-size:20px; color:var(--text-dim);">→</div>
            <div>
                <div style="font-size:11px; color:var(--text-dim);">After</div>
                <div class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ '{:,.2f}'.format(delta_report.new_ltm_gross) }}</div>
            </div>
        </div>
        <div class="stat-change {{ 'up' if delta_report.ltm_gross_change_pct >= 0 else 'down' }}" style="margin-top:8px;">
            {{ '%+.1f' | format(delta_report.ltm_gross_change_pct) }}%
        </div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">LTM Net</span></div>
        <div style="display:flex; justify-content:space-between; align-items:baseline;">
            <div>
                <div style="font-size:11px; color:var(--text-dim);">Before</div>
                <div class="mono" style="font-size:14px; color:var(--text-muted);">{{ '{:,.2f}'.format(delta_report.old_ltm_net) }}</div>
            </div>
            <div style="font-size:20px; color:var(--text-dim);">→</div>
            <div>
                <div style="font-size:11px; color:var(--text-dim);">After</div>
                <div class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ '{:,.2f}'.format(delta_report.new_ltm_net) }}</div>
            </div>
        </div>
        <div class="stat-change {{ 'up' if delta_report.ltm_net_change_pct >= 0 else 'down' }}" style="margin-top:8px;">
            {{ '%+.1f' | format(delta_report.ltm_net_change_pct) }}%
        </div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">ISRCs</span></div>
        <div style="display:flex; justify-content:space-between; align-items:baseline;">
            <div class="mono" style="font-size:14px; color:var(--text-muted);">{{ delta_report.old_isrc_count }}</div>
            <div style="font-size:20px; color:var(--text-dim);">→</div>
            <div class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ delta_report.new_isrc_count }}</div>
        </div>
    </div>
</div>

{# Per-payor variance #}
{% if delta_report.payor_variance %}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Revenue Variance by Payor</span></div>
    <table>
        <thead><tr><th>Payor</th><th class="text-right">Before</th><th class="text-right">After</th><th class="text-right">Change</th><th class="text-right">%</th></tr></thead>
        <tbody>
        {% for pv in delta_report.payor_variance %}
        <tr>
            <td style="font-weight:500; color:var(--text-primary);">{{ pv.name }}</td>
            <td class="text-right mono">{{ '{:,.2f}'.format(pv.old_gross) }}</td>
            <td class="text-right mono">{{ '{:,.2f}'.format(pv.new_gross) }}</td>
            <td class="text-right mono {{ 'text-green' if pv.change >= 0 else 'text-red' }}">{{ '{:+,.2f}'.format(pv.change) }}</td>
            <td class="text-right"><span class="stat-change {{ 'up' if pv.change_pct >= 0 else 'down' }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(pv.change_pct) }}%</span></td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>
{% endif %}
{% else %}
<div class="card" style="text-align:center; padding:40px;">
    <p style="color:var(--text-muted);">No delta report available.</p>
</div>
{% endif %}

{% elif page == 'edit_deal' %}
{# ==================== EDIT DEAL PAGE ==================== #}
<div class="page-header">
    <h1>Edit Deal: {{ edit_deal_name }}</h1>
    <p>Modify payor configs, add files, or rename the deal. <a href="/deals" class="nav-btn" style="margin-left:12px; padding:6px 14px; font-size:12px;">Back to Deals</a></p>
</div>

<form method="POST" action="/deals/{{ edit_slug }}/edit" enctype="multipart/form-data">
    <div class="card" style="margin-bottom:16px;">
        <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">Deal / Project Name</label>
            <input class="form-input" type="text" name="deal_name" value="{{ edit_deal_name }}" required>
        </div>
    </div>

    <div class="card" style="margin-bottom:16px;">
        <div class="card-header">
            <span class="card-title">Payors</span>
            <button type="button" class="nav-btn" onclick="addEditPayor()">+ Add Payor</button>
        </div>

        <div id="editPayorList">
            {% for p in edit_payors %}
            <div class="payor-block" data-idx="{{ loop.index0 }}" style="{% if not loop.first %}margin-top:16px;{% endif %}">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                    <span style="font-size:14px; font-weight:600; color:var(--text-primary);">Payor {{ loop.index }}: {{ p.code }}</span>
                    {% if edit_payors | length > 1 %}
                    <button type="button" class="nav-btn" style="font-size:11px; padding:4px 10px; color:var(--red); border-color:var(--red-dim);" onclick="this.closest('.payor-block').remove()">Remove Payor</button>
                    {% endif %}
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group" style="flex:0.7;">
                        <label class="form-label">Code</label>
                        <input class="form-input" type="text" name="payor_code_{{ loop.index0 }}" value="{{ p.code }}" required>
                    </div>
                    <div class="form-group" style="flex:1.5;">
                        <label class="form-label">Name</label>
                        <input class="form-input" type="text" name="payor_name_{{ loop.index0 }}" value="{{ p.name }}" required>
                    </div>
                    <div class="form-group" style="flex:1;">
                        <label class="form-label">Format</label>
                        <select class="form-input" name="payor_fmt_{{ loop.index0 }}">
                            <option value="auto" selected>Auto-detect</option>
                        </select>
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Statement Type</label>
                        <select class="form-input" name="payor_stype_{{ loop.index0 }}">
                            <option value="masters" {{ 'selected' if p.statement_type == 'masters' }}>Masters</option>
                            <option value="publishing" {{ 'selected' if p.statement_type == 'publishing' }}>Publishing</option>
                            <option value="neighboring" {{ 'selected' if p.statement_type == 'neighboring' }}>Neighboring Rights</option>
                            <option value="pro" {{ 'selected' if p.statement_type == 'pro' }}>PRO (Performance)</option>
                            <option value="sync" {{ 'selected' if p.statement_type == 'sync' }}>Sync</option>
                            <option value="other" {{ 'selected' if p.statement_type == 'other' }}>Other</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Fee %</label>
                        <input class="form-input" type="number" name="payor_fee_{{ loop.index0 }}" value="{{ (p.fee * 100) | round(1) }}" min="0" max="100" step="0.1">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Source Currency</label>
                        <select class="form-input" name="payor_source_currency_{{ loop.index0 }}">
                            <option value="auto" {{ 'selected' if p.source_currency is not defined or p.source_currency == 'auto' }}>Auto-detect</option>
                            <option value="USD" {{ 'selected' if p.source_currency == 'USD' or (p.fx_currency is defined and p.fx_currency == 'USD') }}>USD</option>
                            <option value="EUR" {{ 'selected' if p.source_currency == 'EUR' or (p.fx_currency is defined and p.fx_currency == 'EUR') }}>EUR</option>
                            <option value="GBP" {{ 'selected' if p.source_currency == 'GBP' or (p.fx_currency is defined and p.fx_currency == 'GBP') }}>GBP</option>
                            <option value="CAD" {{ 'selected' if p.source_currency == 'CAD' }}>CAD</option>
                            <option value="AUD" {{ 'selected' if p.source_currency == 'AUD' }}>AUD</option>
                            <option value="JPY" {{ 'selected' if p.source_currency == 'JPY' }}>JPY</option>
                            <option value="SEK" {{ 'selected' if p.source_currency == 'SEK' }}>SEK</option>
                            <option value="NOK" {{ 'selected' if p.source_currency == 'NOK' }}>NOK</option>
                            <option value="DKK" {{ 'selected' if p.source_currency == 'DKK' }}>DKK</option>
                            <option value="CHF" {{ 'selected' if p.source_currency == 'CHF' }}>CHF</option>
                            <option value="BRL" {{ 'selected' if p.source_currency == 'BRL' }}>BRL</option>
                        </select>
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Deal Type</label>
                        <select class="form-input" name="payor_deal_type_{{ loop.index0 }}">
                            <option value="artist" {{ 'selected' if p.get('deal_type', 'artist') == 'artist' }}>Artist Deal</option>
                            <option value="label" {{ 'selected' if p.get('deal_type', 'artist') == 'label' }}>Label Deal</option>
                        </select>
                        <span style="font-size:10px; color:var(--text-dim);">Whose earnings perspective</span>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Share Split %</label>
                        <input class="form-input" type="number" name="payor_split_{{ loop.index0 }}" value="{{ p.artist_split if p.artist_split is not none else '' }}" min="0" max="100" step="0.1">
                        <span style="font-size:10px; color:var(--text-dim);">Your share after distro fees</span>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Territory</label>
                        <input class="form-input" type="text" name="payor_territory_{{ loop.index0 }}" value="{{ p.territory or '' }}">
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Expected Period Start</label>
                        <input class="form-input" type="text" name="payor_period_start_{{ loop.index0 }}" value="{{ p.expected_start or '' }}" placeholder="YYYYMM">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Expected Period End</label>
                        <input class="form-input" type="text" name="payor_period_end_{{ loop.index0 }}" value="{{ p.expected_end or '' }}" placeholder="YYYYMM">
                    </div>
                </div>
                <div style="padding:8px 12px; background:var(--bg-inset); border-radius:6px; margin-bottom:10px;">
                    <span style="font-size:11px; color:var(--text-dim);">
                        Current statements: <strong style="color:var(--text-secondary);">{{ p.file_count }} files</strong> in {{ p.statements_dir }}
                    </span>
                </div>
                <div class="form-group" style="margin-bottom:0;">
                    <label class="form-label">Add More Statement Files</label>
                    <input class="form-input" type="file" name="payor_files_{{ loop.index0 }}" multiple accept=".zip,.xlsx,.xls,.xlsb,.csv,.pdf">
                    <span style="font-size:10px; color:var(--text-dim);">New files will be appended to existing statements for this payor.</span>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <div style="display:flex; gap:12px;">
        <button type="submit" name="action" value="rerun" class="btn-submit" style="flex:1;">
            Save &amp; Re-run Consolidation
        </button>
        <button type="submit" name="action" value="config_only" class="nav-btn" style="padding:12px 24px; font-size:14px; font-weight:600;">
            Save Config Only
        </button>
    </div>
</form>

<script>
let editPayorIdx = {{ edit_payors | length }};
function addEditPayor() {
    const n = editPayorIdx;
    editPayorIdx++;
    const html = `
    <div class="payor-block" data-idx="${n}" style="margin-top:16px;">
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
            <span style="font-size:14px; font-weight:600; color:var(--text-primary);">New Payor ${n + 1}</span>
            <button type="button" class="nav-btn" style="font-size:11px; padding:4px 10px; color:var(--red); border-color:var(--red-dim);" onclick="this.closest('.payor-block').remove()">Remove</button>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group" style="flex:0.7;">
                <label class="form-label">Code</label>
                <input class="form-input" type="text" name="payor_code_${n}" placeholder="B2, RJ, etc." required>
            </div>
            <div class="form-group" style="flex:1.5;">
                <label class="form-label">Name</label>
                <input class="form-input" type="text" name="payor_name_${n}" required>
            </div>
            <div class="form-group" style="flex:1;">
                <label class="form-label">Format</label>
                <select class="form-input" name="payor_fmt_${n}">
                    <option value="auto" selected>Auto-detect</option>
                </select>
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Statement Type</label>
                <select class="form-input" name="payor_stype_${n}">
                    <option value="masters">Masters</option>
                    <option value="publishing">Publishing</option>
                    <option value="neighboring">Neighboring Rights</option>
                    <option value="pro">PRO (Performance)</option>
                    <option value="sync">Sync</option>
                    <option value="other">Other</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Fee %</label>
                <input class="form-input" type="number" name="payor_fee_${n}" value="15" min="0" max="100" step="0.1">
            </div>
            <div class="form-group">
                <label class="form-label">Source Currency</label>
                <select class="form-input" name="payor_source_currency_${n}">
                    <option value="auto" selected>Auto-detect</option>
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                    <option value="GBP">GBP</option>
                    <option value="CAD">CAD</option>
                    <option value="AUD">AUD</option>
                    <option value="JPY">JPY</option>
                    <option value="SEK">SEK</option>
                    <option value="NOK">NOK</option>
                    <option value="DKK">DKK</option>
                    <option value="CHF">CHF</option>
                    <option value="BRL">BRL</option>
                </select>
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Deal Type</label>
                <select class="form-input" name="payor_deal_type_${n}">
                    <option value="artist" selected>Artist Deal</option>
                    <option value="label">Label Deal</option>
                </select>
                <span style="font-size:10px; color:var(--text-dim);">Whose earnings perspective</span>
            </div>
            <div class="form-group">
                <label class="form-label">Share Split %</label>
                <input class="form-input" type="number" name="payor_split_${n}" placeholder="e.g. 50" min="0" max="100" step="0.1">
                <span style="font-size:10px; color:var(--text-dim);">Your share after distro fees</span>
            </div>
            <div class="form-group">
                <label class="form-label">Territory</label>
                <input class="form-input" type="text" name="payor_territory_${n}" placeholder="e.g. Worldwide">
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Expected Period Start</label>
                <input class="form-input" type="text" name="payor_period_start_${n}" placeholder="e.g. 202001 (YYYYMM)">
            </div>
            <div class="form-group">
                <label class="form-label">Expected Period End</label>
                <input class="form-input" type="text" name="payor_period_end_${n}" placeholder="e.g. 202412 (YYYYMM)">
            </div>
        </div>
        <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">Statement Files</label>
            <input class="form-input" type="file" name="payor_files_${n}" multiple accept=".zip,.xlsx,.xls,.xlsb,.csv,.pdf">
        </div>
    </div>`;
    document.getElementById('editPayorList').insertAdjacentHTML('beforeend', html);
}
</script>

{% elif page == 'chat' %}
{# ==================== CHAT ==================== #}
<style>
    .chat-container {
        display: flex;
        flex-direction: column;
        height: calc(100vh - 120px);
        max-width: 900px;
        margin: 0 auto;
    }
    .chat-header {
        padding: 20px 24px 16px;
        border-bottom: 1px solid var(--border);
    }
    .chat-header h1 {
        font-size: 20px;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 4px;
    }
    .chat-header p {
        font-size: 13px;
        color: var(--text-muted);
    }
    .chat-messages {
        flex: 1;
        overflow-y: auto;
        padding: 24px;
        display: flex;
        flex-direction: column;
        gap: 16px;
    }
    .chat-messages::-webkit-scrollbar { width: 6px; }
    .chat-messages::-webkit-scrollbar-track { background: transparent; }
    .chat-messages::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    .chat-bubble {
        max-width: 80%;
        padding: 12px 16px;
        border-radius: var(--radius);
        font-size: 14px;
        line-height: 1.6;
        word-wrap: break-word;
    }
    .chat-bubble.user {
        align-self: flex-end;
        background: var(--accent);
        color: #fff;
        border-bottom-right-radius: 4px;
    }
    .chat-bubble.bot {
        align-self: flex-start;
        background: var(--bg-card);
        color: var(--text-secondary);
        border: 1px solid var(--border);
        border-bottom-left-radius: 4px;
    }
    .chat-bubble.bot p { margin-bottom: 8px; }
    .chat-bubble.bot p:last-child { margin-bottom: 0; }
    .chat-bubble.bot strong { color: var(--text-primary); }
    .chat-bubble.bot code {
        background: var(--bg-inset);
        padding: 2px 6px;
        border-radius: 4px;
        font-size: 13px;
    }
    .chat-bubble.bot pre {
        background: var(--bg-inset);
        padding: 12px;
        border-radius: var(--radius-xs);
        overflow-x: auto;
        margin: 8px 0;
    }
    .chat-bubble.bot pre code {
        background: none;
        padding: 0;
    }
    .chat-bubble.bot table {
        width: 100%;
        border-collapse: collapse;
        margin: 8px 0;
        font-size: 13px;
    }
    .chat-bubble.bot th, .chat-bubble.bot td {
        padding: 6px 10px;
        border: 1px solid var(--border);
        text-align: left;
    }
    .chat-bubble.bot th {
        background: var(--bg-inset);
        color: var(--text-primary);
        font-weight: 600;
    }
    .chat-bubble.bot ul, .chat-bubble.bot ol {
        padding-left: 20px;
        margin: 6px 0;
    }
    .chat-bubble.bot li { margin-bottom: 4px; }
    .chat-bubble.bot h1, .chat-bubble.bot h2, .chat-bubble.bot h3 {
        color: var(--text-primary);
        margin: 12px 0 6px;
    }
    .chat-bubble.bot h1 { font-size: 18px; }
    .chat-bubble.bot h2 { font-size: 16px; }
    .chat-bubble.bot h3 { font-size: 14px; }
    .chat-input-area {
        padding: 16px 24px;
        border-top: 1px solid var(--border);
        display: flex;
        gap: 12px;
        align-items: flex-end;
    }
    .chat-input {
        flex: 1;
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        color: var(--text-primary);
        font-family: inherit;
        font-size: 14px;
        padding: 12px 16px;
        resize: none;
        min-height: 44px;
        max-height: 160px;
        outline: none;
        transition: border-color 0.2s;
    }
    .chat-input:focus { border-color: var(--accent); }
    .chat-input::placeholder { color: var(--text-dim); }
    .chat-send {
        background: var(--accent);
        color: #fff;
        border: none;
        border-radius: var(--radius-sm);
        padding: 12px 20px;
        font-size: 14px;
        font-weight: 600;
        cursor: pointer;
        transition: background 0.2s;
        white-space: nowrap;
    }
    .chat-send:hover { background: var(--accent-hover); }
    .chat-send:disabled { opacity: 0.5; cursor: not-allowed; }
    .typing-indicator {
        display: flex;
        gap: 4px;
        padding: 8px 0;
    }
    .typing-indicator span {
        width: 8px;
        height: 8px;
        background: var(--text-dim);
        border-radius: 50%;
        animation: typing 1.4s infinite;
    }
    .typing-indicator span:nth-child(2) { animation-delay: 0.2s; }
    .typing-indicator span:nth-child(3) { animation-delay: 0.4s; }
    @keyframes typing {
        0%, 60%, 100% { opacity: 0.3; transform: translateY(0); }
        30% { opacity: 1; transform: translateY(-4px); }
    }
    .chat-welcome {
        text-align: center;
        padding: 60px 24px;
        color: var(--text-muted);
    }
    .chat-welcome h2 {
        color: var(--text-primary);
        font-size: 20px;
        margin-bottom: 8px;
    }
    .chat-welcome p { margin-bottom: 16px; font-size: 14px; }
    .chat-suggestions {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        justify-content: center;
        margin-top: 16px;
    }
    .chat-suggestion {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        padding: 8px 16px;
        color: var(--text-secondary);
        font-size: 13px;
        cursor: pointer;
        transition: border-color 0.2s, color 0.2s;
    }
    .chat-suggestion:hover {
        border-color: var(--accent);
        color: var(--text-primary);
    }
</style>

<div class="chat-container">
    <div class="chat-header">
        <h1>Data Analyst</h1>
        <p>{% if results %}Powered by Gemini &middot; {{ deal_name or 'Current Deal' }} &middot; Ask anything about your royalty data{% else %}Load a deal or run a consolidation to start chatting{% endif %}</p>
    </div>

    <div class="chat-messages" id="chatMessages">
        {% if not results %}
        <div class="chat-welcome">
            <h2>No Data Loaded</h2>
            <p>Upload royalty statements and run a consolidation first, or load a saved deal from the Deals tab.</p>
            <a href="/upload" class="nav-btn primary" style="padding:10px 24px; font-size:13px;">Go to Upload</a>
        </div>
        {% else %}
        <div class="chat-welcome" id="chatWelcome">
            <h2>Ask me anything about your data</h2>
            <p>I can analyze trends, compare payors, identify top earners, flag missing statements, and more.</p>
            <div class="chat-suggestions">
                <div class="chat-suggestion" onclick="sendSuggestion(this)">What are the top earning songs?</div>
                <div class="chat-suggestion" onclick="sendSuggestion(this)">Summarize YoY revenue trends</div>
                <div class="chat-suggestion" onclick="sendSuggestion(this)">Which payors have missing statements?</div>
                <div class="chat-suggestion" onclick="sendSuggestion(this)">Break down revenue by platform</div>
            </div>
        </div>
        {% endif %}
    </div>

    {% if results %}
    <div class="chat-input-area">
        <textarea class="chat-input" id="chatInput" placeholder="Ask about your royalty data..." rows="1"></textarea>
        <button class="chat-send" id="chatSend" onclick="sendMessage()">Send</button>
    </div>
    {% endif %}
</div>

{% if results %}
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script>
(function() {
    const sessionId = crypto.randomUUID ? crypto.randomUUID() : 'sess-' + Math.random().toString(36).substr(2, 16);
    const messagesEl = document.getElementById('chatMessages');
    const inputEl = document.getElementById('chatInput');
    const sendBtn = document.getElementById('chatSend');
    const welcomeEl = document.getElementById('chatWelcome');
    let sending = false;

    // Auto-resize textarea
    inputEl.addEventListener('input', function() {
        this.style.height = 'auto';
        this.style.height = Math.min(this.scrollHeight, 160) + 'px';
    });

    // Enter to send, Shift+Enter for newline
    inputEl.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    window.sendSuggestion = function(el) {
        inputEl.value = el.textContent;
        sendMessage();
    };

    window.sendMessage = function() {
        const text = inputEl.value.trim();
        if (!text || sending) return;

        // Remove welcome
        if (welcomeEl) welcomeEl.remove();

        // Add user bubble
        const userBubble = document.createElement('div');
        userBubble.className = 'chat-bubble user';
        userBubble.textContent = text;
        messagesEl.appendChild(userBubble);

        // Clear input
        inputEl.value = '';
        inputEl.style.height = 'auto';

        // Show typing indicator
        const typingEl = document.createElement('div');
        typingEl.className = 'chat-bubble bot';
        typingEl.innerHTML = '<div class="typing-indicator"><span></span><span></span><span></span></div>';
        messagesEl.appendChild(typingEl);
        messagesEl.scrollTop = messagesEl.scrollHeight;

        sending = true;
        sendBtn.disabled = true;

        fetch('/api/chat', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ message: text, session_id: sessionId })
        })
        .then(r => r.json())
        .then(data => {
            typingEl.remove();
            const botBubble = document.createElement('div');
            botBubble.className = 'chat-bubble bot';
            if (typeof marked !== 'undefined' && marked.parse) {
                botBubble.innerHTML = marked.parse(data.reply || 'No response.');
            } else {
                botBubble.textContent = data.reply || 'No response.';
            }
            messagesEl.appendChild(botBubble);
            messagesEl.scrollTop = messagesEl.scrollHeight;
        })
        .catch(err => {
            typingEl.remove();
            const errBubble = document.createElement('div');
            errBubble.className = 'chat-bubble bot';
            errBubble.textContent = 'Error: Could not reach the server. Please try again.';
            messagesEl.appendChild(errBubble);
            messagesEl.scrollTop = messagesEl.scrollHeight;
        })
        .finally(() => {
            sending = false;
            sendBtn.disabled = false;
            inputEl.focus();
        });
    };
})();
</script>
{% endif %}

{% elif page == 'forecast' %}
{# ==================== FORECAST PAGE ==================== #}
<div class="page-header">
    <h1>Forecast: {{ forecast_deal_name }}</h1>
    <p>DCF projection with dual terminal value methods, sensitivity &amp; returns analysis. <a href="/deals" class="nav-btn" style="margin-left:12px; padding:6px 14px; font-size:12px;">Back to Deals</a></p>
</div>

{# ---- Config Form: 3-Card Grid ---- #}
<form method="POST" action="/deals/{{ forecast_slug }}/forecast">
<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-bottom:16px;">
    {# Card 1: Projection Settings #}
    <div class="card">
        <div class="card-header"><span class="card-title">Projection Settings</span></div>
        <div class="form-group">
            <label class="form-label">Default Genre Curve</label>
            <select name="genre_default" class="form-input">
                {% for val, label in genre_choices %}
                <option value="{{ val }}" {% if forecast_result and forecast_result.config.genre_default == val %}selected{% endif %}>{{ label }}</option>
                {% endfor %}
            </select>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div class="form-group">
                <label class="form-label">Exit Year (Horizon)</label>
                <input type="number" name="horizon_years" class="form-input" value="{{ forecast_result.config.horizon_years if forecast_result else 5 }}" min="1" max="20">
            </div>
            <div class="form-group">
                <label class="form-label">WACC (%)</label>
                <input type="number" name="discount_rate" class="form-input" value="{{ (forecast_result.config.discount_rate * 100) | round(3) if forecast_result else 9.375 }}" min="1" max="30" step="0.125">
            </div>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div class="form-group">
                <label class="form-label">TGR (%)</label>
                <input type="number" name="terminal_growth" class="form-input" value="{{ (forecast_result.summary.terminal_growth * 100) | round(1) if forecast_result and forecast_result.summary else 1 }}" min="-5" max="5" step="0.5">
            </div>
            <div class="form-group">
                <label class="form-label">Exit Multiple (x)</label>
                <input type="number" name="exit_multiple" class="form-input" value="{{ forecast_result.config.exit_multiple if forecast_result else 15 }}" min="1" max="50" step="0.5">
            </div>
        </div>
    </div>

    {# Card 2: Transaction Assumptions #}
    <div class="card">
        <div class="card-header"><span class="card-title">Transaction Assumptions</span></div>
        <div class="form-group">
            <label class="form-label">Purchase Price ($)</label>
            <input type="number" name="purchase_price" class="form-input" placeholder="Leave blank to skip returns" value="{{ '{:.0f}'.format(forecast_result.config.purchase_price) if forecast_result and forecast_result.config.purchase_price else '' }}" min="0" step="1000">
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div class="form-group">
                <label class="form-label">Holdback ($)</label>
                <input type="number" name="holdback" class="form-input" placeholder="0" value="{{ '{:.0f}'.format(forecast_result.config.holdback) if forecast_result and forecast_result.config.holdback else '' }}" min="0" step="1000">
            </div>
            <div class="form-group">
                <label class="form-label">PCDPCDR ($)</label>
                <input type="number" name="pcdpcdr" class="form-input" placeholder="0" value="{{ '{:.0f}'.format(forecast_result.config.pcdpcdr) if forecast_result and forecast_result.config.pcdpcdr else '' }}" min="0" step="1000">
            </div>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div class="form-group">
                <label class="form-label">LTV (%)</label>
                <input type="number" name="ltv" class="form-input" value="{{ (forecast_result.config.ltv * 100) | round(0) | int if forecast_result else 55 }}" min="0" max="90" step="5">
            </div>
            <div class="form-group">
                <label class="form-label">CF Sweep (%)</label>
                <input type="number" name="cash_flow_sweep" class="form-input" value="{{ (forecast_result.config.cash_flow_sweep * 100) | round(0) | int if forecast_result else 100 }}" min="0" max="100" step="5">
            </div>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px;">
            <div class="form-group">
                <label class="form-label">SOFR (%)</label>
                <input type="number" name="sofr_rate" class="form-input" value="{{ (forecast_result.config.sofr_rate * 100) | round(2) if forecast_result else 4.5 }}" min="0" max="15" step="0.25">
            </div>
            <div class="form-group">
                <label class="form-label">Floor (%)</label>
                <input type="number" name="sofr_floor" class="form-input" value="{{ (forecast_result.config.sofr_floor * 100) | round(1) if forecast_result else 2.0 }}" min="0" max="10" step="0.25">
            </div>
            <div class="form-group">
                <label class="form-label">Spread (bps)</label>
                <input type="number" name="sofr_spread" class="form-input" value="{{ (forecast_result.config.sofr_spread * 10000) | round(0) | int if forecast_result else 275 }}" min="0" max="1000" step="25">
            </div>
        </div>
    </div>

    {# Card 3: Synergy Assumptions #}
    <div class="card">
        <div class="card-header"><span class="card-title">Synergy Assumptions</span></div>
        <div class="form-group">
            <label class="form-label">New Fee Rate (% — blank = no synergy)</label>
            <input type="number" name="new_fee_rate" class="form-input" placeholder="e.g. 8" value="{{ (forecast_result.config.new_fee_rate * 100) | round(1) if forecast_result and forecast_result.config.new_fee_rate else '' }}" min="0" max="50" step="0.5">
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div class="form-group">
                <label class="form-label">Start Year</label>
                <input type="number" name="synergy_start_year" class="form-input" value="{{ forecast_result.config.synergy_start_year if forecast_result else 1 }}" min="1" max="10">
            </div>
            <div class="form-group">
                <label class="form-label">Ramp (months)</label>
                <input type="number" name="synergy_ramp_months" class="form-input" value="{{ forecast_result.config.synergy_ramp_months if forecast_result else 12 }}" min="1" max="60">
            </div>
        </div>
        <div class="form-group">
            <label class="form-label">3P Synergy Rate (% — blank = none)</label>
            <input type="number" name="third_party_synergy_rate" class="form-input" placeholder="e.g. 5" value="{{ (forecast_result.config.third_party_synergy_rate * 100) | round(1) if forecast_result and forecast_result.config.third_party_synergy_rate else '' }}" min="0" max="100" step="1">
        </div>
    </div>
</div>

{# ---- Row 2: Deal Metadata + Advanced Config ---- #}
<details style="margin-bottom:16px;">
    <summary style="cursor:pointer; font-weight:600; color:var(--cyan); font-size:13px; padding:8px 0;">
        Advanced: Deal Metadata, SOFR Curve, Per-Payor Config &amp; FX
    </summary>
    <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-top:8px;">
        {# Card 4: Deal Metadata #}
        <div class="card">
            <div class="card-header"><span class="card-title">Deal Metadata</span></div>
            <div class="form-group">
                <label class="form-label">Opportunity Name</label>
                <input type="text" name="opportunity_name" class="form-input" value="{{ forecast_result.config.opportunity_name if forecast_result and forecast_result.config.opportunity_name else forecast_deal_name }}" placeholder="{{ forecast_deal_name }}">
            </div>
            <div class="form-group">
                <label class="form-label">Rights Included</label>
                <select name="rights_included" class="form-input">
                    {% for val in ['Masters', 'Publishing', 'Neighboring Rights', 'Masters + Publishing', 'Masters + NR'] %}
                    <option value="{{ val }}" {% if forecast_result and forecast_result.config.rights_included == val %}selected{% endif %}>{{ val }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Deal Type</label>
                <select name="deal_type" class="form-input">
                    {% for val in ['Catalog', 'Corporate', 'JV'] %}
                    <option value="{{ val }}" {% if forecast_result and forecast_result.config.deal_type == val %}selected{% endif %}>{{ val }}</option>
                    {% endfor %}
                </select>
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
                <div class="form-group">
                    <label class="form-label">Cash Date</label>
                    <input type="date" name="cash_date" class="form-input" value="{{ forecast_result.config.cash_date if forecast_result and forecast_result.config.cash_date else '' }}">
                </div>
                <div class="form-group">
                    <label class="form-label">Close Date</label>
                    <input type="date" name="close_date" class="form-input" value="{{ forecast_result.config.close_date if forecast_result and forecast_result.config.close_date else '' }}">
                </div>
            </div>
            <div class="form-group">
                <label class="form-label">Virtu WACC (% — blank = skip)</label>
                <input type="number" name="virtu_wacc" class="form-input" placeholder="e.g. 9" value="{{ (forecast_result.config.virtu_wacc * 100) | round(2) if forecast_result and forecast_result.config.virtu_wacc else '' }}" min="0" max="30" step="0.125">
            </div>
        </div>

        {# Card 5: SOFR Forward Curve — Excel Import #}
        <div class="card">
            <div class="card-header"><span class="card-title">SOFR Forward Curve</span></div>
            <div class="form-group">
                <label class="form-label">Import from Excel (Chatham SOFR sheet)</label>
                <div style="display:flex; gap:8px; align-items:center;">
                    <input type="file" id="sofrFileInput" accept=".xlsx,.xls" style="flex:1; font-size:11px;">
                    <button type="button" onclick="importSofrExcel()" class="nav-btn" style="padding:5px 14px; font-size:11px; white-space:nowrap;">Import</button>
                </div>
                <div id="sofrStatus" style="font-size:11px; margin-top:4px; color:var(--text-dim);"></div>
            </div>
            <input type="hidden" name="sofr_curve_json" id="sofrCurveJson" value="{{ forecast_result.config.sofr_curve | tojson if forecast_result and forecast_result.config.sofr_curve else '' }}">
            <div id="sofrPreview" style="max-height:180px; overflow-y:auto; font-size:10px; display:none;">
                <table style="width:100%; border-collapse:collapse;">
                    <thead><tr style="position:sticky; top:0; background:var(--bg-card);">
                        <th style="text-align:left; padding:2px 6px; border-bottom:1px solid var(--border);">Date</th>
                        <th style="text-align:right; padding:2px 6px; border-bottom:1px solid var(--border);">SOFR %</th>
                    </tr></thead>
                    <tbody id="sofrPreviewBody"></tbody>
                </table>
            </div>
            <div style="font-size:10px; color:var(--text-dim); margin-top:4px;">If populated, overrides flat SOFR rate from Card 2. Flat rate fields (SOFR Rate, Floor, Spread) still apply as fallback.</div>
        </div>

        {# Card 6: Per-Payor Config — Auto-Populated Table #}
        <div class="card" style="grid-column: span 2;">
            <div class="card-header"><span class="card-title">Per-Payor Config</span></div>
            <input type="hidden" name="payor_configs_json" id="payorConfigsJson" value="{{ forecast_result.config.payor_configs | tojson if forecast_result and forecast_result.config.payor_configs else '' }}">
            <input type="hidden" name="fx_rates_json" id="fxRatesJson" value="{{ forecast_result.config.fx_rates | tojson if forecast_result and forecast_result.config.fx_rates else '' }}">
            <div style="overflow-x:auto;">
                <table id="payorConfigTable" style="width:100%; border-collapse:collapse; font-size:11px;">
                    <thead><tr>
                        <th style="text-align:left; padding:4px 6px; border-bottom:1px solid var(--border);">Payor</th>
                        <th style="text-align:left; padding:4px 6px; border-bottom:1px solid var(--border);">Rights</th>
                        <th style="text-align:right; padding:4px 6px; border-bottom:1px solid var(--border);">Fee %</th>
                        <th style="text-align:center; padding:4px 6px; border-bottom:1px solid var(--border);">Source Ccy</th>
                        <th style="text-align:left; padding:4px 6px; border-bottom:1px solid var(--border);">Target Ccy</th>
                        <th style="text-align:right; padding:4px 6px; border-bottom:1px solid var(--border);">FX Rate</th>
                        <th style="text-align:center; padding:4px 6px; border-bottom:1px solid var(--border);">Synergy</th>
                        <th style="text-align:right; padding:4px 6px; border-bottom:1px solid var(--border);">Syn Fee %</th>
                        <th style="text-align:right; padding:4px 6px; border-bottom:1px solid var(--border);">Syn Start Yr</th>
                        <th style="text-align:right; padding:4px 6px; border-bottom:1px solid var(--border);">Ramp (mo)</th>
                    </tr></thead>
                    <tbody>
                    {% set saved_pc = forecast_result.config.payor_configs if forecast_result and forecast_result.config.payor_configs else {} %}
                    {% for ps in results.get('payor_summaries', []) %}
                    {% set pc = saved_pc.get(ps.code, {}) %}
                    <tr data-payor="{{ ps.code }}">
                        <td style="padding:4px 6px; font-weight:600;">{{ ps.name }}</td>
                        <td style="padding:4px 6px;">
                            <select class="pc-rights form-input" style="font-size:11px; padding:2px 4px;">
                                {% for val in ['Masters', 'Publishing', 'Neighboring Rights'] %}
                                <option value="{{ val }}" {% if pc.get('income_rights') == val %}selected{% elif not pc and ps.statement_type == val %}selected{% endif %}>{{ val }}</option>
                                {% endfor %}
                            </select>
                        </td>
                        <td style="padding:4px 6px;">
                            <input type="number" class="pc-fee form-input" style="font-size:11px; padding:2px 4px; width:60px; text-align:right;"
                                   value="{{ pc.get('fee_rate', ps.fee | replace('%','') | float / 100) }}" min="0" max="1" step="0.01">
                        </td>
                        <td style="padding:4px 6px; text-align:center; color:var(--text-muted);" class="pc-source-ccy">{{ ps.currency_code or ps.fx or 'USD' }}</td>
                        <td style="padding:4px 6px;">
                            <select class="pc-target-ccy form-input" style="font-size:11px; padding:2px 4px;">
                                {% for ccy in ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CHF'] %}
                                <option value="{{ ccy }}" {% if pc.get('fx_currency') == ccy %}selected{% elif not pc and ccy == 'USD' %}selected{% endif %}>{{ ccy }}</option>
                                {% endfor %}
                            </select>
                        </td>
                        <td style="padding:4px 6px;">
                            <input type="number" class="pc-fx-rate form-input" style="font-size:11px; padding:2px 4px; width:70px; text-align:right;"
                                   value="{{ pc.get('fx_rate', '') }}" min="0" step="0.0001"
                                   {% if (ps.currency_code or ps.fx or 'USD') == (pc.get('fx_currency', 'USD')) %}disabled{% endif %}>
                        </td>
                        <td style="padding:4px 6px; text-align:center;">
                            <input type="checkbox" class="pc-synergy" {% if pc.get('synergy') %}checked{% endif %}>
                        </td>
                        <td style="padding:4px 6px;">
                            <input type="number" class="pc-syn-fee form-input" style="font-size:11px; padding:2px 4px; width:60px; text-align:right;"
                                   value="{{ pc.get('synergy_new_fee_rate', '') }}" min="0" max="1" step="0.01"
                                   placeholder="Gbl" {% if not pc.get('synergy') %}disabled{% endif %}>
                        </td>
                        <td style="padding:4px 6px;">
                            <input type="number" class="pc-syn-start form-input" style="font-size:11px; padding:2px 4px; width:50px; text-align:right;"
                                   value="{{ pc.get('synergy_start_year', '') }}" min="1" max="10" step="1"
                                   placeholder="Gbl" {% if not pc.get('synergy') %}disabled{% endif %}>
                        </td>
                        <td style="padding:4px 6px;">
                            <input type="number" class="pc-syn-ramp form-input" style="font-size:11px; padding:2px 4px; width:50px; text-align:right;"
                                   value="{{ pc.get('synergy_ramp_months', '') }}" min="1" max="120" step="1"
                                   placeholder="Gbl" {% if not pc.get('synergy') %}disabled{% endif %}>
                        </td>
                    </tr>
                    {% endfor %}
                    </tbody>
                </table>
            </div>
            <div style="font-size:10px; color:var(--text-dim); margin-top:4px;">Auto-populated from deal analytics. FX Rate only needed when Source != Target currency.</div>
        </div>
    </div>
</details>

<div style="margin-top:8px; margin-bottom:16px;">
    <button type="submit" class="btn-submit" style="padding:10px 32px; font-size:14px;">Run Forecast</button>
</div>
</form>

{# ---- Forecast Form JS ---- #}
<script>
function importSofrExcel() {
    const fileInput = document.getElementById('sofrFileInput');
    const status = document.getElementById('sofrStatus');
    const preview = document.getElementById('sofrPreview');
    const body = document.getElementById('sofrPreviewBody');
    const hidden = document.getElementById('sofrCurveJson');

    if (!fileInput.files.length) {
        status.textContent = 'Please select an Excel file first.';
        status.style.color = 'var(--red)';
        return;
    }

    status.textContent = 'Importing...';
    status.style.color = 'var(--text-dim)';

    const fd = new FormData();
    fd.append('sofr_file', fileInput.files[0]);

    fetch('/api/parse-sofr-excel', { method: 'POST', body: fd })
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                status.textContent = 'Error: ' + data.error;
                status.style.color = 'var(--red)';
                return;
            }
            hidden.value = JSON.stringify(data.curve);
            renderSofrPreview(data.curve);
            status.textContent = data.count + ' data points imported.';
            status.style.color = 'var(--green, #22c55e)';
        })
        .catch(err => {
            status.textContent = 'Upload failed: ' + err.message;
            status.style.color = 'var(--red)';
        });
}

function renderSofrPreview(curve) {
    const preview = document.getElementById('sofrPreview');
    const body = document.getElementById('sofrPreviewBody');
    body.innerHTML = '';
    if (!curve || !curve.length) { preview.style.display = 'none'; return; }
    curve.forEach(pt => {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td style="padding:1px 6px;">' + pt.date + '</td>'
            + '<td style="padding:1px 6px; text-align:right;">' + (pt.rate * 100).toFixed(3) + '%</td>';
        body.appendChild(tr);
    });
    preview.style.display = '';
}

function collectPayorConfigs() {
    const rows = document.querySelectorAll('#payorConfigTable tbody tr');
    const configs = {};
    const fxRates = {};
    rows.forEach(tr => {
        const code = tr.dataset.payor;
        const rights = tr.querySelector('.pc-rights').value;
        const fee = parseFloat(tr.querySelector('.pc-fee').value) || 0;
        const sourceCcy = tr.querySelector('.pc-source-ccy').textContent.trim();
        const targetCcy = tr.querySelector('.pc-target-ccy').value;
        const fxInput = tr.querySelector('.pc-fx-rate');
        const fxRate = fxInput.disabled ? null : (parseFloat(fxInput.value) || null);
        const synergy = tr.querySelector('.pc-synergy').checked;
        const synFee = tr.querySelector('.pc-syn-fee');
        const synStart = tr.querySelector('.pc-syn-start');
        const synRamp = tr.querySelector('.pc-syn-ramp');

        configs[code] = {
            income_rights: rights,
            fee_rate: fee,
            fx_currency: targetCcy,
            synergy: synergy
        };
        if (synergy) {
            const sf = parseFloat(synFee.value);
            const ss = parseInt(synStart.value);
            const sr = parseInt(synRamp.value);
            if (!isNaN(sf)) configs[code].synergy_new_fee_rate = sf;
            if (!isNaN(ss)) configs[code].synergy_start_year = ss;
            if (!isNaN(sr)) configs[code].synergy_ramp_months = sr;
        }
        if (fxRate && sourceCcy !== targetCcy) {
            configs[code].fx_rate = fxRate;
            fxRates[sourceCcy] = fxRate;
        }
    });
    document.getElementById('payorConfigsJson').value = JSON.stringify(configs);
    document.getElementById('fxRatesJson').value = JSON.stringify(fxRates);
}

/* Enable/disable FX rate inputs when target currency changes */
document.querySelectorAll('.pc-target-ccy').forEach(sel => {
    sel.addEventListener('change', function() {
        const tr = this.closest('tr');
        const sourceCcy = tr.querySelector('.pc-source-ccy').textContent.trim();
        const fxInput = tr.querySelector('.pc-fx-rate');
        fxInput.disabled = (sourceCcy === this.value);
        if (fxInput.disabled) fxInput.value = '';
    });
});

/* Enable/disable per-payor synergy inputs when synergy checkbox changes */
document.querySelectorAll('.pc-synergy').forEach(cb => {
    cb.addEventListener('change', function() {
        const tr = this.closest('tr');
        const synInputs = tr.querySelectorAll('.pc-syn-fee, .pc-syn-start, .pc-syn-ramp');
        synInputs.forEach(inp => {
            inp.disabled = !this.checked;
            if (!this.checked) inp.value = '';
        });
    });
});

/* Collect configs before form submit */
document.querySelector('form[action*="/forecast"]').addEventListener('submit', function() {
    collectPayorConfigs();
});

/* On load: render SOFR preview if data exists from previous run */
(function() {
    const hidden = document.getElementById('sofrCurveJson');
    if (hidden && hidden.value) {
        try {
            const curve = JSON.parse(hidden.value);
            if (Array.isArray(curve) && curve.length) {
                renderSofrPreview(curve);
                document.getElementById('sofrStatus').textContent = curve.length + ' data points loaded from previous run.';
            }
        } catch(e) {}
    }
})();
</script>

{# ==================== RESULTS (Tabbed Interface) ==================== #}
{% if forecast_result %}

{# ---- Coverage Warning ---- #}
{% if forecast_result.isrc_coverage is defined and forecast_result.isrc_coverage < 0.95 %}
<div style="padding:10px 16px; margin-bottom:12px; border-radius:6px; font-size:12px;
    background:{% if forecast_result.isrc_coverage < 0.5 %}rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.3); color:var(--red);{% else %}rgba(251,191,36,0.1); border:1px solid rgba(251,191,36,0.3); color:#92400e;{% endif %}">
    <strong>ISRC Coverage: {{ forecast_result.ltm_waterfall.coverage_pct }}%</strong> &mdash;
    Only {{ CSYM }}{{ '{:,.0f}'.format(forecast_result.ltm_waterfall.gross) }} of {{ CSYM }}{{ '{:,.0f}'.format(forecast_result.ltm_waterfall.gross_all) }} LTM gross is attributed to identified ISRCs.
    Revenue without ISRC identifiers is not projected. The LTM column in the waterfall reflects ISRC-attributed revenue only.
</div>
{% endif %}

{# ---- Stale Payor Warning ---- #}
{% if forecast_result.payor_ltm_warnings is defined and forecast_result.payor_ltm_warnings | length > 0 %}
<div style="padding:10px 16px; margin-bottom:12px; border-radius:6px; font-size:12px;
    background:rgba(251,191,36,0.1); border:1px solid rgba(251,191,36,0.3); color:#92400e;">
    <strong>Stale Payor Data</strong> &mdash;
    {% for w in forecast_result.payor_ltm_warnings %}
    {{ w.name }} ({{ w.gap_months }} months behind{% set ym = w.max_period // 100 %}{% set mm = w.max_period % 100 %}, last: {{ '%04d-%02d' | format(ym, mm) }}){{ ', ' if not loop.last else '' }}
    {% endfor %}
    — Per-payor LTM windows are applied automatically; each payor uses its own trailing 12 months.
</div>
{% endif %}

{# ---- Tab Bar ---- #}
<div style="display:flex; gap:4px; margin-bottom:16px; border-bottom:2px solid var(--border); padding-bottom:0;">
    {% set fc_tabs = [('fc-summary','Summary'),('fc-waterfall','Waterfall'),('fc-dcf','DCF Analysis'),('fc-sensitivity','Sensitivity'),('fc-returns','Returns'),('fc-isrcs','Top ISRCs')] %}
    {% for tid, tlabel in fc_tabs %}
    <button class="pill-tab{% if loop.first %} active{% endif %}" onclick="document.querySelectorAll('.fc-panel').forEach(p=>p.style.display='none');document.getElementById('{{tid}}').style.display='block';document.querySelectorAll('.pill-tab').forEach(b=>b.classList.remove('active'));this.classList.add('active');" style="padding:8px 16px; font-size:12px; font-weight:600; border:none; background:{% if loop.first %}var(--accent){% else %}transparent{% endif %}; color:{% if loop.first %}#fff{% else %}var(--text-secondary){% endif %}; border-radius:6px 6px 0 0; cursor:pointer;">{{ tlabel }}</button>
    {% endfor %}
</div>

{% set CSYM = results.currency_symbol | default('$') %}
{% set CCY = results.currency_code | default('USD') %}

{# ==================== Tab 1: Summary ==================== #}
<div id="fc-summary" class="fc-panel" style="display:block;">

{# Hero Cards #}
<div style="display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-bottom:16px;">
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Implied Valuation</span></div>
        <div class="stat-value" style="font-size:22px;"><span class="data-money" data-raw="{{ forecast_result.summary.npv }}" data-ccy="{{ CCY }}">{{ CSYM }}{{ '{:,.0f}'.format(forecast_result.summary.npv) }}</span></div>
        <div class="stat-subtitle">{{ '%.1f' | format(forecast_result.summary.implied_multiple_net) }}x Net &middot; TM Method</div>
    </div>
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Perpetuity Value</span></div>
        <div class="stat-value" style="font-size:22px;"><span class="data-money" data-raw="{{ forecast_result.summary.npv_perpetuity }}" data-ccy="{{ CCY }}">{{ CSYM }}{{ '{:,.0f}'.format(forecast_result.summary.npv_perpetuity) }}</span></div>
        <div class="stat-subtitle">PG Method &middot; TGR {{ '%.1f' | format(forecast_result.summary.terminal_growth * 100) }}%</div>
    </div>
    {% if forecast_result.unlevered_returns and forecast_result.unlevered_returns.irr is defined %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Unlev. IRR</span></div>
        <div class="stat-value" style="font-size:22px;">{{ '%.1f' | format((forecast_result.unlevered_returns.irr or 0) * 100) }}%</div>
        <div class="stat-subtitle">MOIC: {{ '%.2f' | format(forecast_result.unlevered_returns.moic) }}x</div>
    </div>
    {% else %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Unlev. IRR</span></div>
        <div class="stat-value medium" style="color:var(--text-dim);">--</div>
        <div class="stat-subtitle">Set purchase price</div>
    </div>
    {% endif %}
    {% if forecast_result.levered_returns and forecast_result.levered_returns.irr is defined %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Lev. IRR</span></div>
        <div class="stat-value" style="font-size:22px;">{{ '%.1f' | format((forecast_result.levered_returns.irr or 0) * 100) }}%</div>
        <div class="stat-subtitle">MOIC: {{ '%.2f' | format(forecast_result.levered_returns.moic) }}x</div>
    </div>
    {% else %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Lev. IRR</span></div>
        <div class="stat-value medium" style="color:var(--text-dim);">--</div>
        <div class="stat-subtitle">Set purchase price</div>
    </div>
    {% endif %}
    {% if forecast_result.virtu_levered_returns and forecast_result.virtu_levered_returns.irr is defined %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Virtu IRR</span></div>
        <div class="stat-value" style="font-size:22px;">{{ '%.1f' | format((forecast_result.virtu_levered_returns.irr or 0) * 100) }}%</div>
        <div class="stat-subtitle">MOIC: {{ '%.2f' | format(forecast_result.virtu_levered_returns.moic) }}x</div>
    </div>
    {% endif %}
    <div class="card" style="text-align:center;">
        <div class="card-header"><span class="card-title" style="font-size:11px;">Projected Total</span></div>
        <div class="stat-value" style="font-size:22px;"><span class="data-money" data-raw="{{ forecast_result.aggregate.total_net_incl }}" data-ccy="{{ CCY }}">{{ CSYM }}{{ '{:,.0f}'.format(forecast_result.aggregate.total_net_incl) }}</span></div>
        <div class="stat-subtitle">{{ forecast_result.config.horizon_years }}-yr NE (Incl) &middot; {{ forecast_result.isrc_count }} ISRCs</div>
    </div>
</div>

{# Projection Chart #}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header">
        <span class="card-title">Projected Revenue by Year</span>
        <a href="/deals/{{ forecast_slug }}/forecast/download" class="nav-btn" style="font-size:11px; padding:4px 12px;">Download Excel</a>
    </div>
    <div class="chart-wrap tall">
        <canvas id="forecastChart"></canvas>
    </div>
</div>
</div>

{# ==================== Tab 2: Earnings Waterfall ==================== #}
<div id="fc-waterfall" class="fc-panel" style="display:none;">
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Full Earnings Waterfall</span></div>
    <div style="overflow-x:auto;">
    <table style="font-size:12px; white-space:nowrap;">
        <thead>
        <tr>
            <th style="min-width:180px;">Metric</th>
            <th class="text-right" style="min-width:110px;">LTM</th>
            {% for yt in forecast_result.aggregate.year_totals %}
            <th class="text-right" style="min-width:110px;">Yr {{ yt.year }} ({{ yt.calendar_year }})</th>
            {% endfor %}
        </tr>
        </thead>
        <tbody>
        {% set ltm_wf = forecast_result.ltm_waterfall %}
        {# Gross #}
        <tr>
            <td style="font-weight:600;">Gross Revenue</td>
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.gross) }}</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(yt.gross) }}</td>
            {% endfor %}
        </tr>
        {# Fees #}
        <tr style="color:var(--red);">
            <td style="font-weight:500;">Less: Distribution Fees</td>
            <td class="text-right mono">({{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.fees) }})</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">({{ CSYM }}{{ '{:,.0f}'.format(yt.fees_original) }})</td>
            {% endfor %}
        </tr>
        {# Net Receipts #}
        <tr style="border-top:1px solid var(--border);">
            <td style="font-weight:600;">Net Receipts</td>
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.net_receipts) }}</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(yt.net_receipts_excl) }}</td>
            {% endfor %}
        </tr>
        {# Third Party #}
        <tr style="color:var(--red);">
            <td style="font-weight:500;">Less: Third Party</td>
            <td class="text-right mono">({{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.third_party) }})</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">({{ CSYM }}{{ '{:,.0f}'.format(yt.third_party_excl) }})</td>
            {% endfor %}
        </tr>
        {# NE Excl #}
        <tr style="border-top:1px solid var(--border); background:rgba(59,130,246,0.04);">
            <td style="font-weight:700;">Net Earnings (Excl Synergies)</td>
            <td class="text-right mono" style="font-weight:700;">{{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.net_earnings) }}</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono" style="font-weight:700;">{{ CSYM }}{{ '{:,.0f}'.format(yt.net_earnings_excl) }}</td>
            {% endfor %}
        </tr>
        {# Blank separator #}
        <tr><td colspan="{{ forecast_result.aggregate.year_totals | length + 2 }}" style="height:8px; border:none;"></td></tr>
        {# Fee Savings #}
        <tr style="color:var(--green);">
            <td style="font-weight:500;">Plus: Fee Savings</td>
            <td class="text-right mono">--</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(yt.fee_savings) }}</td>
            {% endfor %}
        </tr>
        {# 3P Savings #}
        <tr style="color:var(--green);">
            <td style="font-weight:500;">Plus: 3P Savings</td>
            <td class="text-right mono">--</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(yt.tp_savings) }}</td>
            {% endfor %}
        </tr>
        {# NE Incl #}
        <tr style="border-top:2px solid var(--accent); background:rgba(34,197,94,0.06);">
            <td style="font-weight:700; color:var(--accent);">Net Earnings (Incl Synergies)</td>
            <td class="text-right mono" style="font-weight:700; color:var(--accent);">{{ CSYM }}{{ '{:,.0f}'.format(ltm_wf.net_earnings) }}</td>
            {% for yt in forecast_result.aggregate.year_totals %}
            <td class="text-right mono" style="font-weight:700; color:var(--accent);">{{ CSYM }}{{ '{:,.0f}'.format(yt.net_earnings_incl) }}</td>
            {% endfor %}
        </tr>
        </tbody>
    </table>
    </div>
</div>
</div>

{# ==================== Tab 3: DCF Analysis ==================== #}
<div id="fc-dcf" class="fc-panel" style="display:none;">
<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
    {% for method_label, method_key in [('Terminal Multiple','terminal_multiple'), ('Perpetuity Growth','perpetuity_growth')] %}
    {% for track_label, track_key in [('Excl Synergies','excl'), ('Incl Synergies','incl')] %}
    {% set d = forecast_result.dcf[method_key][track_key] %}
    <div class="card">
        <div class="card-header"><span class="card-title" style="font-size:12px;">{{ method_label }} — {{ track_label }}</span></div>
        <table style="font-size:12px;">
            <tbody>
            <tr><td>PV of Cash Flows</td><td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(d.pv_cash_flows) }}</td></tr>
            <tr><td>PV of Terminal Value</td><td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(d.pv_terminal_value) }}</td></tr>
            <tr style="border-top:2px solid var(--border);"><td style="font-weight:700;">Implied Valuation</td><td class="text-right mono" style="font-weight:700; color:var(--accent); font-size:14px;">{{ CSYM }}{{ '{:,.0f}'.format(d.implied_valuation) }}</td></tr>
            <tr><td style="color:var(--text-dim);">Implied LTM Multiple</td><td class="text-right mono" style="color:var(--text-dim);">{{ '%.1f' | format(d.implied_ltm_multiple) }}x</td></tr>
            </tbody>
        </table>
    </div>
    {% endfor %}
    {% endfor %}
</div>
</div>

{# ==================== Tab 4: Sensitivity ==================== #}
<div id="fc-sensitivity" class="fc-panel" style="display:none;">
<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
    {# Terminal Multiple Sensitivity #}
    {% set tm_sens = forecast_result.sensitivity.terminal_multiple %}
    <div class="card">
        <div class="card-header"><span class="card-title">Terminal Multiple (WACC × Exit Mult) — Incl</span></div>
        <table style="font-size:12px;">
            <thead>
            <tr>
                <th>WACC \ Exit Mult</th>
                {% for em in tm_sens.exit_mult_values %}
                <th class="text-right">{{ '%.1f' | format(em) }}x</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for wi in range(tm_sens.wacc_values | length) %}
            <tr>
                <td style="font-weight:600;">{{ '%.2f' | format(tm_sens.wacc_values[wi]) }}%</td>
                {% for ci in range(tm_sens.exit_mult_values | length) %}
                <td class="text-right mono" style="{% if wi == 1 and ci == 1 %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}">{{ CSYM }}{{ '{:,.0f}'.format(tm_sens.matrix_incl[wi][ci]) }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    {# Perpetuity Growth Sensitivity #}
    {% set pg_sens = forecast_result.sensitivity.perpetuity_growth %}
    <div class="card">
        <div class="card-header"><span class="card-title">Perpetuity Growth (WACC × TGR) — Incl</span></div>
        <table style="font-size:12px;">
            <thead>
            <tr>
                <th>WACC \ TGR</th>
                {% for tg in pg_sens.tgr_values %}
                <th class="text-right">{{ '%.2f' | format(tg) }}%</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for wi in range(pg_sens.wacc_values | length) %}
            <tr>
                <td style="font-weight:600;">{{ '%.2f' | format(pg_sens.wacc_values[wi]) }}%</td>
                {% for ci in range(pg_sens.tgr_values | length) %}
                <td class="text-right mono" style="{% if wi == 1 and ci == 1 %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}">{{ CSYM }}{{ '{:,.0f}'.format(pg_sens.matrix_incl[wi][ci]) }}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
</div>

{# ---- IRR/MOIC Sensitivity Grids (Purchase Price × Exit Multiple) ---- #}
{% if forecast_result.irr_sensitivity and forecast_result.irr_sensitivity.purchase_prices is defined %}
{% set irrs = forecast_result.irr_sensitivity %}
<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
    {# Unlevered IRR Grid #}
    <div class="card">
        <div class="card-header"><span class="card-title">Unlevered IRR (Price × Exit Multiple)</span></div>
        <div style="overflow-x:auto;">
        <table style="font-size:11px; width:100%;">
            <thead>
            <tr>
                <th style="font-size:10px;">Price \ Exit</th>
                {% for em in irrs.exit_multiples %}
                <th class="text-right" style="font-size:10px;">{{ '%.0f' | format(em) }}x</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for pi in range(irrs.purchase_prices | length) %}
            <tr>
                <td style="font-weight:600; white-space:nowrap;">{{ CSYM }}{{ '{:,.0f}'.format(irrs.purchase_prices[pi]) }}{% if irrs.xntm_values[pi] %} <span style="color:var(--text-dim); font-weight:400;">({{ irrs.xntm_values[pi] }}x)</span>{% endif %}</td>
                {% for ci in range(irrs.exit_multiples | length) %}
                {% set v = irrs.irr_matrix[pi][ci] %}
                <td class="text-right mono" style="{% if pi == 2 and ci == (irrs.exit_multiples | length - 1) %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}{% if v is not none and v > 0 %}color:var(--green);{% elif v is not none and v < 0 %}color:var(--red);{% endif %}">{% if v is not none %}{{ '%.1f' | format(v * 100) }}%{% else %}&mdash;{% endif %}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
    </div>
    {# Unlevered MOIC Grid #}
    <div class="card">
        <div class="card-header"><span class="card-title">Unlevered MOIC (Price × Exit Multiple)</span></div>
        <div style="overflow-x:auto;">
        <table style="font-size:11px; width:100%;">
            <thead>
            <tr>
                <th style="font-size:10px;">Price \ Exit</th>
                {% for em in irrs.exit_multiples %}
                <th class="text-right" style="font-size:10px;">{{ '%.0f' | format(em) }}x</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for pi in range(irrs.purchase_prices | length) %}
            <tr>
                <td style="font-weight:600; white-space:nowrap;">{{ CSYM }}{{ '{:,.0f}'.format(irrs.purchase_prices[pi]) }}</td>
                {% for ci in range(irrs.exit_multiples | length) %}
                {% set v = irrs.moic_matrix[pi][ci] %}
                <td class="text-right mono" style="{% if pi == 2 and ci == (irrs.exit_multiples | length - 1) %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}{% if v is not none and v >= 2.0 %}color:var(--green);{% elif v is not none and v < 1.0 %}color:var(--red);{% endif %}">{% if v is not none %}{{ '%.2f' | format(v) }}x{% else %}&mdash;{% endif %}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
    </div>
</div>
{% endif %}

{# ---- Levered IRR/MOIC Sensitivity ---- #}
{% if forecast_result.levered_sensitivity and forecast_result.levered_sensitivity.purchase_prices is defined %}
{% set lsens = forecast_result.levered_sensitivity %}
<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
    {# Levered IRR Grid #}
    <div class="card">
        <div class="card-header"><span class="card-title">Levered IRR (Price × Exit Multiple) — {{ '%.0f' | format(forecast_result.config.ltv * 100) }}% LTV</span></div>
        <div style="overflow-x:auto;">
        <table style="font-size:11px; width:100%;">
            <thead>
            <tr>
                <th style="font-size:10px;">Price \ Exit</th>
                {% for em in lsens.exit_multiples %}
                <th class="text-right" style="font-size:10px;">{{ '%.0f' | format(em) }}x</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for pi in range(lsens.purchase_prices | length) %}
            <tr>
                <td style="font-weight:600; white-space:nowrap;">{{ CSYM }}{{ '{:,.0f}'.format(lsens.purchase_prices[pi]) }}</td>
                {% for ci in range(lsens.exit_multiples | length) %}
                {% set v = lsens.irr_matrix[pi][ci] %}
                <td class="text-right mono" style="{% if pi == 2 and ci == (lsens.exit_multiples | length - 1) %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}{% if v and v > 0 %}color:var(--green);{% elif v and v < 0 %}color:var(--red);{% endif %}">{% if v is not none %}{{ '%.1f' | format(v * 100) }}%{% else %}—{% endif %}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
    </div>
    {# Levered MOIC Grid #}
    <div class="card">
        <div class="card-header"><span class="card-title">Levered MOIC (Price × Exit Multiple) — {{ '%.0f' | format(forecast_result.config.ltv * 100) }}% LTV</span></div>
        <div style="overflow-x:auto;">
        <table style="font-size:11px; width:100%;">
            <thead>
            <tr>
                <th style="font-size:10px;">Price \ Exit</th>
                {% for em in lsens.exit_multiples %}
                <th class="text-right" style="font-size:10px;">{{ '%.0f' | format(em) }}x</th>
                {% endfor %}
            </tr>
            </thead>
            <tbody>
            {% for pi in range(lsens.purchase_prices | length) %}
            <tr>
                <td style="font-weight:600; white-space:nowrap;">{{ CSYM }}{{ '{:,.0f}'.format(lsens.purchase_prices[pi]) }}</td>
                {% for ci in range(lsens.exit_multiples | length) %}
                {% set v = lsens.moic_matrix[pi][ci] %}
                <td class="text-right mono" style="{% if pi == 2 and ci == (lsens.exit_multiples | length - 1) %}background:rgba(251,191,36,0.15); font-weight:700;{% endif %}{% if v and v >= 2.0 %}color:var(--green);{% elif v and v < 1.0 %}color:var(--red);{% endif %}">{% if v is not none %}{{ '%.2f' | format(v) }}x{% else %}—{% endif %}</td>
                {% endfor %}
            </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
    </div>
</div>
{% endif %}

</div>

{# ==================== Tab 5: Returns ==================== #}
<div id="fc-returns" class="fc-panel" style="display:none;">
{% if forecast_result.unlevered_returns and forecast_result.unlevered_returns.schedule is defined %}
<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
    {# Unlevered Returns #}
    <div class="card">
        <div class="card-header"><span class="card-title">Unlevered Returns</span></div>
        <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:12px; padding:8px; background:var(--bg-inset); border-radius:6px;">
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">IRR</div><div style="font-size:16px; font-weight:700;">{{ '%.1f' | format((forecast_result.unlevered_returns.irr or 0) * 100) }}%</div></div>
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">MOIC</div><div style="font-size:16px; font-weight:700;">{{ '%.2f' | format(forecast_result.unlevered_returns.moic) }}x</div></div>
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">Exit EV</div><div style="font-size:16px; font-weight:700;">{{ CSYM }}{{ '{:,.0f}'.format(forecast_result.unlevered_returns.exit_ev) }}</div></div>
        </div>
        <table style="font-size:11px;">
            <thead><tr><th>Year</th><th class="text-right">UFCF</th><th class="text-right">Exit</th><th class="text-right">Total</th></tr></thead>
            <tbody>
            {% for s in forecast_result.unlevered_returns.schedule %}
            <tr>
                <td>Yr {{ s.year }} ({{ s.calendar_year }})</td>
                <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(s.ufcf) }}</td>
                <td class="text-right mono" style="color:{% if s.exit_proceeds > 0 %}var(--green){% else %}var(--text-dim){% endif %};">{{ CSYM }}{{ '{:,.0f}'.format(s.exit_proceeds) }}</td>
                <td class="text-right mono" style="font-weight:600;">{{ CSYM }}{{ '{:,.0f}'.format(s.total) }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    {# Levered Returns #}
    {% if forecast_result.levered_returns and forecast_result.levered_returns.debt_schedule is defined %}
    <div class="card">
        <div class="card-header"><span class="card-title">Levered Returns</span></div>
        <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:12px; padding:8px; background:var(--bg-inset); border-radius:6px;">
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">IRR</div><div style="font-size:16px; font-weight:700;">{{ '%.1f' | format((forecast_result.levered_returns.irr or 0) * 100) }}%</div></div>
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">MOIC</div><div style="font-size:16px; font-weight:700;">{{ '%.2f' | format(forecast_result.levered_returns.moic) }}x</div></div>
            <div style="text-align:center;"><div style="font-size:10px; color:var(--text-dim);">Exit Equity</div><div style="font-size:16px; font-weight:700;">{{ CSYM }}{{ '{:,.0f}'.format(forecast_result.levered_returns.exit_equity) }}</div></div>
        </div>
        <div style="font-size:11px; color:var(--text-dim); margin-bottom:8px;">
            Equity: {{ CSYM }}{{ '{:,.0f}'.format(forecast_result.levered_returns.equity) }} &middot;
            Debt: {{ CSYM }}{{ '{:,.0f}'.format(forecast_result.levered_returns.debt_initial) }} &middot;
            Rate: {{ '%.2f' | format(forecast_result.levered_returns.interest_rate * 100) }}%
        </div>
        <table style="font-size:11px;">
            <thead><tr><th>Year</th><th class="text-right">UFCF</th><th class="text-right">Interest</th><th class="text-right">Principal</th><th class="text-right">LFCF</th><th class="text-right">Debt Bal.</th></tr></thead>
            <tbody>
            {% for ds in forecast_result.levered_returns.debt_schedule %}
            <tr>
                <td>Yr {{ ds.year }}</td>
                <td class="text-right mono">{{ CSYM }}{{ '{:,.0f}'.format(ds.ufcf) }}</td>
                <td class="text-right mono" style="color:var(--red);">({{ CSYM }}{{ '{:,.0f}'.format(ds.interest) }})</td>
                <td class="text-right mono" style="color:var(--red);">({{ CSYM }}{{ '{:,.0f}'.format(ds.principal) }})</td>
                <td class="text-right mono" style="font-weight:600;">{{ CSYM }}{{ '{:,.0f}'.format(ds.lfcf) }}</td>
                <td class="text-right mono" style="color:var(--text-dim);">{{ CSYM }}{{ '{:,.0f}'.format(ds.closing_balance) }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    {% endif %}
</div>
{% else %}
<div class="card" style="margin-bottom:16px; text-align:center; padding:40px;">
    <div style="font-size:14px; color:var(--text-dim);">Set a Purchase Price in the config above to see returns analysis.</div>
</div>
{% endif %}
</div>

{# ==================== Tab 6: Top ISRCs ==================== #}
<div id="fc-isrcs" class="fc-panel" style="display:none;">
{% if forecast_result.top_isrcs %}
<div class="card" style="margin-bottom:16px;">
    <div class="card-header"><span class="card-title">Top Projected ISRCs</span></div>
    <table>
        <thead><tr><th>#</th><th>Artist</th><th>Title</th><th>Genre</th><th class="text-right">LTM Gross</th><th class="text-right">Projected Total</th></tr></thead>
        <tbody>
        {% for ti in forecast_result.top_isrcs[:30] %}
        <tr>
            <td><span class="rank">{{ loop.index }}</span></td>
            <td style="font-size:12px;">{{ ti.artist }}</td>
            <td style="font-weight:500; color:var(--text-primary); font-size:12px;">{{ ti.title }}</td>
            <td style="font-size:11px; color:var(--text-dim);">{{ ti.genre }}</td>
            <td class="text-right mono" style="font-size:12px;"><span class="data-money" data-raw="{{ ti.ltm_gross }}" data-ccy="{{ CCY }}">{{ CSYM }}{{ '{:,.2f}'.format(ti.ltm_gross) }}</span></td>
            <td class="text-right mono" style="font-size:12px;"><span class="data-money" data-raw="{{ ti.projected_total }}" data-ccy="{{ CCY }}">{{ CSYM }}{{ '{:,.2f}'.format(ti.projected_total) }}</span></td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
</div>
{% endif %}
</div>

{# ---- Forecast Chart JS ---- #}
<script>
(function() {
    const YT = {{ forecast_result.aggregate.year_totals | tojson }};
    const labels = ['LTM'].concat(YT.map(d => 'Yr ' + d.year));
    const grossData = [{{ forecast_result.summary.ltm_gross }}].concat(YT.map(d => d.gross));
    const neExclData = [{{ forecast_result.ltm_waterfall.net_earnings }}].concat(YT.map(d => d.net_earnings_excl));
    const neInclData = [{{ forecast_result.ltm_waterfall.net_earnings }}].concat(YT.map(d => d.net_earnings_incl));
    const CSYM = '{{ results.currency_symbol | default("$") }}';

    new Chart(document.getElementById('forecastChart'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Gross Revenue',
                    data: grossData,
                    backgroundColor: 'rgba(59,130,246,0.5)',
                    borderRadius: 3,
                    barPercentage: 0.5,
                },
                {
                    label: 'NE (Excl)',
                    type: 'line',
                    data: neExclData,
                    borderColor: '#94a3b8',
                    borderDash: [4,3],
                    backgroundColor: 'transparent',
                    pointRadius: 3,
                    pointBackgroundColor: '#94a3b8',
                    fill: false,
                    tension: 0.3,
                },
                {
                    label: 'NE (Incl Synergies)',
                    type: 'line',
                    data: neInclData,
                    borderColor: '#22d3ee',
                    backgroundColor: 'rgba(34,211,238,0.1)',
                    pointRadius: 4,
                    pointBackgroundColor: '#22d3ee',
                    fill: true,
                    tension: 0.3,
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } },
                tooltip: {
                    callbacks: { label: ctx => ctx.dataset.label + ': ' + CSYM + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:0}) }
                }
            },
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => CSYM + (v >= 1000000 ? (v/1000000).toFixed(1) + 'M' : (v/1000).toFixed(0) + 'k') } }
            }
        }
    });
})();
</script>
{% endif %}

{% elif page == 'dashboard' and results %}
{# ==================== DASHBOARD ==================== #}
<div class="page-header" style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:8px;">
    <div>
        <h1>{% if deal_name %}{{ deal_name }}{% else %}Royalty Analytics{% endif %}</h1>
        <p>{{ results.period_range }} &middot; {{ results.total_files }} files &middot; {{ results.isrc_count }} ISRCs</p>
    </div>
    <div style="display:flex; align-items:center; gap:8px; margin-top:4px;">
        <label for="currencyToggle" style="font-size:11px; color:var(--text-muted); white-space:nowrap;">Currency:</label>
        <select id="currencyToggle" class="form-input" style="width:auto; font-size:12px; padding:4px 10px;">
            <option value="original">Original</option>
            <option value="USD">USD ($)</option>
            <option value="EUR">EUR (&euro;)</option>
            <option value="GBP">GBP (&pound;)</option>
            <option value="CAD">CAD (C$)</option>
            <option value="AUD">AUD (A$)</option>
            <option value="JPY">JPY (&yen;)</option>
        </select>
        <span id="currencyStatus" style="font-size:10px; color:var(--text-dim);"></span>
    </div>
</div>

{# ---- B1: Period Gap Alert Banner ---- #}
{% set total_missing = [] %}
{% for ps in results.get('payor_summaries', []) %}
  {% if ps.get('missing_count', 0) > 0 %}
    {% if total_missing.append({'name': ps.name, 'count': ps.missing_count}) %}{% endif %}
  {% endif %}
{% endfor %}
{% if total_missing %}
{% set max_missing = total_missing | map(attribute='count') | max %}
{% set severity = 'red' if max_missing >= 3 else 'yellow' %}
<div id="sec-overview" style="margin-bottom:12px; padding:10px 16px; border-radius:8px; border:1px solid var(--{{ severity }}); background:{% if severity == 'red' %}rgba(248,113,113,0.08){% else %}rgba(251,191,36,0.08){% endif %}; display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
    <span style="font-size:14px;">{% if severity == 'red' %}&#9888;{% else %}&#9888;{% endif %}</span>
    <span style="font-size:12px; color:var(--{{ severity }}); font-weight:600;">Period Gap Alert</span>
    {% for mg in total_missing %}
    <span style="font-size:12px; color:var(--{{ severity }});">{{ mg.name }}: {{ mg.count }} missing month{{ 's' if mg.count != 1 }}</span>
    {% if not loop.last %}<span style="color:var(--text-dim);">&middot;</span>{% endif %}
    {% endfor %}
    <span style="font-size:11px; color:var(--text-dim); margin-left:auto;">LTM may be understated</span>
</div>
{% else %}
<div id="sec-overview"></div>
{% endif %}

{# ---- Delta Report Banner (after re-run) ---- #}
{% if delta_summary is defined and delta_summary %}
<div style="margin-bottom:12px; padding:10px 16px; border-radius:8px; border:1px solid var(--accent); background:rgba(59,130,246,0.06); display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
    <span style="font-size:13px; color:var(--accent); font-weight:600;">Re-run Delta:</span>
    <span style="font-size:12px; color:var(--text-secondary);">{{ delta_summary.summary }}</span>
    {% if delta_summary.deal_slug %}
    <a href="/deals/{{ delta_summary.deal_slug }}/delta" style="margin-left:auto; font-size:11px; color:var(--accent); text-decoration:none; border:1px solid var(--accent); padding:3px 10px; border-radius:4px;">View Full Report</a>
    {% endif %}
</div>
{% endif %}

{# ---- B2: Quick Stats Sticky Bar ---- #}
<div style="position:sticky; top:56px; z-index:99; background:var(--bg-inset); border-bottom:1px solid var(--border); margin:-16px -24px 16px -24px; padding:8px 24px; display:flex; align-items:center; gap:24px; flex-wrap:wrap;">
    <div style="display:flex; align-items:baseline; gap:4px;">
        <span style="font-size:10px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">LTM Gross</span>
        <span class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);"><span class="data-money" data-raw="{{ results.ltm_gross_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ results.ltm_gross_total_fmt }}</span></span>
    </div>
    <div style="display:flex; align-items:baseline; gap:4px;">
        <span style="font-size:10px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">LTM Net</span>
        <span class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);"><span class="data-money" data-raw="{{ results.ltm_net_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ results.ltm_net_total_fmt }}</span></span>
    </div>
    <div style="display:flex; align-items:baseline; gap:4px;">
        <span style="font-size:10px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">ISRCs</span>
        <span class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);">{{ results.isrc_count }}</span>
    </div>
    {% if results.get('weighted_avg_age') %}
    <div style="display:flex; align-items:baseline; gap:4px;">
        <span style="font-size:10px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">WAA</span>
        <span class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);">{{ results.weighted_avg_age.waa_display }}</span>
    </div>
    {% endif %}
    <div style="display:flex; align-items:baseline; gap:4px;">
        <span style="font-size:10px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">Period</span>
        <span class="mono" style="font-size:12px; color:var(--text-secondary);">{{ results.period_range }}</span>
    </div>
    {% if results.ltm_yoy_pct is defined %}
    <div style="margin-left:auto;">
        <span class="stat-change {{ results.ltm_yoy_direction }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(results.ltm_yoy_pct) }}% YoY</span>
    </div>
    {% endif %}
</div>

{# ---- B3: Section Navigation Tabs ---- #}
<div style="margin-bottom:16px; display:flex; gap:4px; flex-wrap:wrap; border-bottom:1px solid var(--border); padding-bottom:8px;" id="dashNav">
    <button class="pill-tab active" onclick="scrollToSection('sec-overview', this)">Overview</button>
    <button class="pill-tab" onclick="scrollToSection('sec-charts', this)">Charts</button>
    <button class="pill-tab" onclick="scrollToSection('sec-songs', this)">Songs</button>
    <button class="pill-tab" onclick="scrollToSection('sec-payors', this)">Payors</button>
    {% if results.get('cohort_analysis') and results.cohort_analysis.get('cohorts') %}
    <button class="pill-tab" onclick="scrollToSection('sec-cohorts', this)">Cohorts</button>
    {% endif %}
    <button class="pill-tab" onclick="scrollToSection('sec-coverage', this)">Coverage</button>
    <button class="pill-tab" onclick="scrollToSection('sec-valuation', this)">Valuation</button>
</div>

{# ---- ROW 1: Hero stats ---- #}
<div class="grid grid-hero" style="margin-bottom:16px;">

    {# -- LTM Revenue (big number + chart) -- #}
    <div class="card">
        <div class="card-header">
            <span class="card-title">LTM Revenue</span>
            <div class="card-icon">&#8364;</div>
        </div>
        <div class="stat-value"><span class="data-money" data-raw="{{ results.ltm_gross_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ results.ltm_gross_total_fmt | default(results.total_gross) }}</span></div>
        <div class="stat-subtitle">Net: <span class="data-money" data-raw="{{ results.ltm_net_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ results.ltm_net_total_fmt | default(results.total_net) }}</span> &middot; last 12 months</div>
        {% if results.ltm_yoy_pct is defined %}
        <div class="stat-change {{ results.ltm_yoy_direction }}">
            {{ '%+.1f' | format(results.ltm_yoy_pct) }}% YoY
        </div>
        {% endif %}
        <div class="chart-wrap short">
            <canvas id="monthlyMiniChart"></canvas>
        </div>
    </div>

    {# -- LTM by Payor (list style) -- #}
    <div class="card">
        <div class="card-header">
            <span class="card-title">LTM Earnings by Payor</span>
        </div>
        {% set ltm_total = results.ltm_by_payor | map(attribute='ltm_gross') | sum %}
        <div class="stat-value medium"><span class="data-money" data-raw="{{ ltm_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ "{:,.2f}".format(ltm_total) }}</span></div>
        <div class="stat-subtitle">Last 12 months total</div>
        <ul class="payor-list" style="margin-top:16px;">
            {% for lp in results.ltm_by_payor %}
            <li class="payor-item">
                <span class="payor-name">{{ lp.name }}</span>
                <span class="payor-value"><span class="data-money" data-raw="{{ lp.ltm_gross }}" data-ccy="{{ lp.currency_code | default(results.currency_code) | default('USD') }}">{{ lp.currency_symbol | default(results.currency_symbol) | default('$') }}{{ lp.ltm_gross_fmt }}</span></span>
            </li>
            {% endfor %}
        </ul>
        <div class="payor-extra">
            <span style="font-size:12px; color:var(--text-dim);">{{ results.ltm_by_payor | length }} payors</span>
        </div>
    </div>

    {# -- Top Songs Quick View -- #}
    <div class="card">
        <div class="pill-tabs">
            <button class="pill-tab active" onclick="showDashTab('top')">Top Songs</button>
            <button class="pill-tab" onclick="showDashTab('annual')">Annual</button>
            <button class="pill-tab" onclick="showDashTab('decay')">YoY Decay</button>
            <button class="pill-tab" onclick="showDashTab('dist')">LTM Stores</button>
            <button class="pill-tab" onclick="showDashTab('types')">LTM Media Types</button>
        </div>

        <div class="tab-content active" id="dtab-top">
            <table>
                <thead><tr><th>#</th><th>Artist</th><th>Title</th><th class="text-right">LTM Gross</th><th class="text-right">YoY</th></tr></thead>
                <tbody>
                {% for song in results.top_songs[:8] %}
                <tr>
                    <td><span class="rank">{{ loop.index }}</span></td>
                    <td>{{ song.artist }}</td>
                    <td style="color:var(--text-primary); font-weight:500;">{{ song.title }}</td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ song._ltm_gross_raw | default(song.gross_raw) | default(0) }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ song.ltm_gross | default(song.gross) }}</span></td>
                    <td class="text-right">
                        {% if song.get('yoy') %}
                        <span class="stat-change {{ song.yoy[-1].direction }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(song.yoy[-1].pct) }}%</span>
                        {% else %}
                        <span style="color:var(--text-dim); font-size:11px;">&mdash;</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="tab-content" id="dtab-annual">
            <table>
                <thead><tr><th>Year</th><th class="text-right">Gross</th><th class="text-right">Net</th></tr></thead>
                <tbody>
                {% for ae in results.annual_earnings %}
                <tr>
                    <td style="font-weight:600; color:var(--text-primary);">{{ ae.year }}</td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ ae.gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ ae.gross }}</span></td>
                    <td class="text-right mono" style="color:var(--text-muted);"><span class="data-money" data-raw="{{ ae.net_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ ae.net }}</span></td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="tab-content" id="dtab-decay">
            <table>
                <thead><tr><th>Period</th><th class="text-right">Prior</th><th class="text-right">Current</th><th class="text-right">Change</th></tr></thead>
                <tbody>
                {% for d in results.yoy_decay %}
                <tr>
                    <td>{{ d.period }}</td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ d.prev_gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ d.prev_gross }}</span></td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ d.curr_gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ d.curr_gross }}</span></td>
                    <td class="text-right mono {{ 'text-red' if '-' in d.change_pct else 'text-green' }}">{{ d.change_pct }}</td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="tab-content" id="dtab-dist">
            {% for d in (results.ltm_stores | default([]))[:8] %}
            <div class="dist-bar-wrap">
                <div class="dist-bar-label">
                    <span class="name">{{ d.name }}</span>
                    <span class="val"><span class="data-money" data-raw="{{ d.gross }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ d.gross_fmt }}</span></span>
                </div>
                <div class="dist-bar-track">
                    <div class="dist-bar-fill" style="width: {{ (d.gross / (results.ltm_stores | default([{}]))[0].gross * 100) | round(1) if results.ltm_stores is defined and results.ltm_stores and results.ltm_stores[0].gross else 0 }}%"></div>
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="tab-content" id="dtab-types">
            {% for d in (results.ltm_media_types | default([]))[:8] %}
            <div class="dist-bar-wrap">
                <div class="dist-bar-label">
                    <span class="name">{{ d.name }}</span>
                    <span class="val"><span class="data-money" data-raw="{{ d.gross }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ d.gross_fmt }}</span></span>
                </div>
                <div class="dist-bar-track">
                    <div class="dist-bar-fill" style="width: {{ (d.gross / (results.ltm_media_types | default([{}]))[0].gross * 100) | round(1) if results.ltm_media_types is defined and results.ltm_media_types and results.ltm_media_types[0].gross else 0 }}%"></div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
</div>

{# ---- B8: Contract Term Summary Card ---- #}
{% set all_contracts = [] %}
{% for ps in results.get('payor_summaries', []) %}
  {% if ps.get('contract_summary') %}
    {% if all_contracts.append(ps) %}{% endif %}
  {% endif %}
{% endfor %}
{% if all_contracts %}
<div class="grid" style="margin-bottom:16px;">
    <div class="card span-full">
        <div class="card-header"><span class="card-title">Contract Term Summary</span></div>
        <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th>Payor</th>
                        <th>Term</th>
                        <th>Matching Right</th>
                        <th>Fee %</th>
                        <th>Split %</th>
                        <th>Assignment</th>
                    </tr>
                </thead>
                <tbody>
                {% for ps in all_contracts %}
                {% set cs = ps.contract_summary %}
                <tr>
                    <td style="font-weight:500; color:var(--text-primary);">{{ ps.name }}</td>
                    <td class="mono" style="font-size:12px;">{{ cs.get('license_term', '--') }}</td>
                    <td>
                        {% if cs.get('matching_right') is not none %}
                        <span style="color:{% if cs.matching_right %}var(--red){% else %}var(--green){% endif %}; font-size:12px;">{{ 'Yes' if cs.matching_right else 'No' }}</span>
                        {% else %}
                        <span style="color:var(--text-dim);">--</span>
                        {% endif %}
                    </td>
                    <td class="mono text-right" style="font-size:12px;">{% if cs.get('distro_fee') is not none %}{{ cs.distro_fee }}%{% else %}--{% endif %}</td>
                    <td class="mono text-right" style="font-size:12px;">{% if cs.get('split_pct') is not none %}{{ cs.split_pct }}%{% else %}--{% endif %}</td>
                    <td>
                        {% if cs.get('assignment_language') is not none %}
                        <span style="color:{% if cs.assignment_language %}var(--yellow){% else %}var(--green){% endif %}; font-size:12px;">{{ 'Yes' if cs.assignment_language else 'No' }}</span>
                        {% else %}
                        <span style="color:var(--text-dim);">--</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>
{% endif %}

{# ---- ROW 2: Charts ---- #}
<div id="sec-charts" class="grid grid-2" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">Monthly Revenue by Payor</span></div>
        <div class="chart-wrap tall">
            <canvas id="monthlyPayorChart"></canvas>
        </div>
    </div>
    <div class="card">
        <div class="card-header"><span class="card-title">Annual Gross by Payor</span></div>
        <div class="chart-wrap tall">
            <canvas id="annualPayorChart"></canvas>
        </div>
    </div>
</div>

{# ---- ROW 3: Top Songs + Per Payor Breakdown ---- #}
<div id="sec-songs" class="grid grid-wide" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header" style="display:flex; justify-content:space-between; align-items:center;">
            <span class="card-title">Top Songs</span>
            <div class="pill-tabs" style="margin:0;">
                <button class="pill-tab active" onclick="showSongTab('alltime')">All-Time</button>
                <button class="pill-tab" onclick="showSongTab('ltm')">LTM</button>
            </div>
        </div>

        {# -- All-Time Top 20 -- #}
        {% set top20 = results.top_songs[:20] %}
        <div class="song-tab-content active" id="stab-alltime">
            <table>
                <thead>
                    <tr><th style="width:36px;">#</th><th>Title</th><th>Artist</th><th class="text-right">Gross</th><th class="text-right" style="width:60px;">%</th><th style="width:180px;"></th></tr>
                </thead>
                <tbody>
                {% set max_pct = top20[0].get('pct_of_total', 0) if top20 else 1 %}
                {% for song in top20 %}
                {% set pct = song.get('pct_of_total', 0) %}
                <tr>
                    <td><span class="rank">{{ loop.index }}</span></td>
                    <td style="color:var(--text-primary); font-weight:500;">{{ song.title }}</td>
                    <td style="color:var(--text-secondary); font-size:12px;">{{ song.artist }}</td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ song.gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ song.gross }}</span></td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);">{{ pct }}%</td>
                    <td>
                        <div style="background:var(--bg-inset); border-radius:4px; height:8px; overflow:hidden;">
                            <div style="height:100%; border-radius:4px; width:{{ (pct / max_pct * 100) | round(1) if max_pct else 0 }}%; background:linear-gradient(90deg, {% if loop.index <= 2 %}var(--yellow), #f97316{% else %}var(--accent), #818cf8{% endif %});"></div>
                        </div>
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
            {% if top20 %}
            {% set total_pct = namespace(v=0) %}
            {% for s in top20 %}{% set total_pct.v = total_pct.v + s.get('pct_of_total', 0) %}{% endfor %}
            <div style="padding:8px 16px; font-size:11px; color:var(--text-dim); text-align:right; border-top:1px solid var(--border);">
                Top {{ top20 | length }} = {{ '%.1f' | format(total_pct.v) }}% of catalog gross
            </div>
            {% endif %}
        </div>

        {# -- LTM Top 20 -- #}
        <div class="song-tab-content" id="stab-ltm" style="display:none;">
            <table>
                <thead>
                    <tr><th style="width:36px;">#</th><th>Title</th><th>Artist</th><th>ISRC</th><th class="text-right">LTM Gross</th><th class="text-right" style="width:60px;">%</th><th style="width:180px;"></th></tr>
                </thead>
                <tbody>
                {% set ltm_max_pct = results.ltm_songs[0].get('pct_of_total', 0) if results.ltm_songs else 1 %}
                {% for song in results.ltm_songs %}
                {% set lpct = song.get('pct_of_total', 0) %}
                <tr>
                    <td><span class="rank">{{ loop.index }}</span></td>
                    <td style="color:var(--text-primary); font-weight:500;">{{ song.title }}</td>
                    <td style="color:var(--text-secondary); font-size:12px;">{{ song.artist }}</td>
                    <td class="mono" style="font-size:11px; color:var(--text-dim);">{{ song.isrc }}</td>
                    <td class="text-right mono"><span class="data-money" data-raw="{{ song.gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ song.gross }}</span></td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);">{{ lpct }}%</td>
                    <td>
                        <div style="background:var(--bg-inset); border-radius:4px; height:8px; overflow:hidden;">
                            <div style="height:100%; border-radius:4px; width:{{ (lpct / ltm_max_pct * 100) | round(1) if ltm_max_pct else 0 }}%; background:linear-gradient(90deg, {% if loop.index <= 2 %}var(--yellow), #f97316{% else %}var(--accent), #818cf8{% endif %});"></div>
                        </div>
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
            {% if results.ltm_songs %}
            {% set ltm_total_pct = namespace(v=0) %}
            {% for s in results.ltm_songs %}{% set ltm_total_pct.v = ltm_total_pct.v + s.get('pct_of_total', 0) %}{% endfor %}
            <div style="padding:8px 16px; font-size:11px; color:var(--text-dim); text-align:right; border-top:1px solid var(--border);">
                Top {{ results.ltm_songs | length }} = {{ '%.1f' | format(ltm_total_pct.v) }}% of LTM gross
            </div>
            {% endif %}
        </div>
    </div>

    <div id="sec-payors" class="card">
        <div class="card-header"><span class="card-title">Per-Payor Summary</span></div>
        {% for ps in results.payor_summaries %}
        <div style="padding:12px 0; border-bottom:1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <div style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ ps.name }}</div>
                    <div style="font-size:11px; color:var(--text-dim);">{{ ps.code }} &middot; {{ ps.statement_type }} &middot; {{ ps.files }} files &middot; {{ ps.isrcs }} ISRCs &middot; fee {{ ps.fee }} &middot; <span style="color:var(--yellow);">{{ ps.detected_currency }}</span></div>
                </div>
                <div style="display:flex; align-items:center; gap:12px;">
                    <a href="/download/payor/{{ ps.code }}" style="font-size:11px; color:var(--accent); text-decoration:none; border:1px solid var(--accent); padding:3px 10px; border-radius:4px;">Export</a>
                    <div class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);"><span class="data-money" data-raw="{{ ps.total_gross_raw }}" data-ccy="{{ ps.currency_code | default(results.currency_code) | default('USD') }}">{{ ps.currency_symbol | default(results.currency_symbol) | default('$') }}{{ ps.total_gross }}</span></div>
                </div>
            </div>
            {# Latest statement & missing months #}
            <div style="margin-top:6px; display:flex; flex-wrap:wrap; gap:8px; font-size:11px; align-items:center;">
                {% if ps.get('latest_statement') %}
                <span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--text-secondary);">Latest: {{ ps.latest_statement }}</span>
                {% endif %}
                {% if ps.get('expected_range') %}
                <span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:{% if ps.range_source == 'manual' %}var(--purple){% else %}var(--text-dim){% endif %};">Range: {{ ps.expected_range }}{% if ps.range_source == 'manual' %} (manual){% endif %}</span>
                {% endif %}
                {% if ps.get('missing_count', 0) > 0 %}
                <span style="background:rgba(251,191,36,0.15); padding:2px 8px; border-radius:4px; color:var(--yellow);">{{ ps.missing_count }} missing month{{ 's' if ps.missing_count != 1 }}:</span>
                {% for mm in ps.missing_months[:6] %}
                <span style="background:var(--bg-inset); border:1px solid rgba(251,191,36,0.3); padding:1px 6px; border-radius:3px; color:var(--yellow); font-size:10px;">{{ mm }}</span>
                {% endfor %}
                {% if ps.missing_months | length > 6 %}
                <span style="color:var(--yellow); font-size:10px;">+{{ ps.missing_months | length - 6 }} more</span>
                {% endif %}
                {% endif %}
                {# Per-payor YoY pills #}
                {% if ps.get('yoy_changes') %}
                {% for yoy in ps.yoy_changes %}
                <span class="stat-change {{ yoy.direction }}" style="margin:0; font-size:10px; padding:1px 6px;">{{ yoy.period }}: {{ '%+.1f' | format(yoy.pct) }}%</span>
                {% endfor %}
                {% endif %}
            </div>
            {% if ps.get('deal_type') or ps.artist_split is not none or ps.territory %}
            <div style="margin-top:6px; display:flex; flex-wrap:wrap; gap:8px; font-size:11px;">
                {% if ps.get('deal_type') %}
                <span style="background:{% if ps.deal_type == 'artist' %}rgba(139,92,246,0.15){% else %}rgba(59,130,246,0.15){% endif %}; padding:2px 8px; border-radius:4px; color:{% if ps.deal_type == 'artist' %}var(--purple){% else %}var(--accent){% endif %};">{{ 'Artist Deal' if ps.deal_type == 'artist' else 'Label Deal' }}</span>
                {% endif %}
                {% if ps.artist_split is not none %}
                <span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--text-secondary);">Split: {{ ps.artist_split }}%</span>
                {% endif %}
                {% if ps.territory %}
                <span style="background:var(--bg-inset); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--text-secondary);">{{ ps.territory }}</span>
                {% endif %}
            </div>
            {% endif %}
            {% if ps.get('contract_summary') %}
            <div style="margin-top:8px; background:var(--bg-inset); border:1px solid var(--border); border-radius:6px; padding:8px 12px;">
                <div style="font-size:11px; font-weight:600; color:var(--text-primary); margin-bottom:4px;">Contract Summary</div>
                {% if ps.contract_summary.get('summary') %}
                <div style="font-size:11px; color:var(--text-secondary); margin-bottom:6px;">{{ ps.contract_summary.summary }}</div>
                {% endif %}
                <div style="display:flex; flex-wrap:wrap; gap:6px; font-size:10px;">
                    {% if ps.contract_summary.get('license_term') %}
                    <span style="background:var(--bg-card); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--text-secondary);">Term: {{ ps.contract_summary.license_term }}</span>
                    {% endif %}
                    {% if ps.contract_summary.get('matching_right') is not none and ps.contract_summary.get('matching_right') is not undefined %}
                    <span style="background:{% if ps.contract_summary.matching_right %}var(--red-dim){% else %}var(--green-dim){% endif %}; padding:2px 8px; border-radius:4px; color:{% if ps.contract_summary.matching_right %}var(--red){% else %}var(--green){% endif %};">Matching: {{ 'Yes' if ps.contract_summary.matching_right else 'No' }}</span>
                    {% endif %}
                    {% if ps.contract_summary.get('assignment_language') is not none and ps.contract_summary.get('assignment_language') is not undefined %}
                    <span style="background:{% if ps.contract_summary.assignment_language %}rgba(251,191,36,0.15){% else %}var(--green-dim){% endif %}; padding:2px 8px; border-radius:4px; color:{% if ps.contract_summary.assignment_language %}var(--yellow){% else %}var(--green){% endif %};">Assignment: {{ 'Yes' if ps.contract_summary.assignment_language else 'No' }}</span>
                    {% endif %}
                    {% if ps.contract_summary.get('distro_fee') is not none %}
                    <span style="background:var(--bg-card); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--cyan);">Fee: {{ ps.contract_summary.distro_fee }}%</span>
                    {% endif %}
                    {% if ps.contract_summary.get('split_pct') is not none %}
                    <span style="background:var(--bg-card); border:1px solid var(--border); padding:2px 8px; border-radius:4px; color:var(--accent);">Split: {{ ps.contract_summary.split_pct }}%</span>
                    {% endif %}
                </div>
            </div>
            {% endif %}
        </div>
        {% endfor %}

        <div style="margin-top:20px;">
            <div class="card-title" style="margin-bottom:12px;">Downloads</div>
            <a href="/download/consolidated" class="dl-link">
                <span class="name">All Payors Combined</span>
                <span class="badge">.xlsx</span>
            </a>
            <a href="/download/csv" class="dl-link">
                <span class="name">All Payors Combined</span>
                <span class="badge">.csv</span>
            </a>
            {% for ps in results.payor_summaries %}
            <a href="/download/payor/{{ ps.code }}" class="dl-link">
                <span class="name">{{ ps.name }}</span>
                <span class="badge">.xlsx</span>
            </a>
            {% endfor %}
        </div>
    </div>
</div>

{# ---- ROW 4: Earnings by Payor & Year ---- #}
{% if results.earnings_matrix and results.earnings_years %}
<div class="grid" style="margin-bottom:16px;">
    <div class="card span-full">
        <div class="card-header"><span class="card-title">Earnings by Payor &amp; Year</span></div>
        <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th>Payor</th>
                        {% for year in results.earnings_years %}
                        <th class="text-right">{{ year }}</th>
                        {% endfor %}
                        <th class="text-right" style="color:var(--text-primary);">Total</th>
                    </tr>
                </thead>
                <tbody>
                {% for row in results.earnings_matrix %}
                <tr>
                    <td style="font-weight:500; color:var(--text-primary);">{{ row.name }}</td>
                    {% for year in results.earnings_years %}
                    {% set ydata = row.years.get(year, row.years.get(year|string, {})) %}
                    <td class="text-right mono">
                        <span style="color:var(--text-primary);"><span class="data-money" data-raw="{{ ydata.get('gross', 0) }}" data-ccy="{{ row.currency_code | default(results.currency_code) | default('USD') }}">{{ row.currency_symbol | default(results.currency_symbol) | default('$') }}{{ ydata.get('gross_fmt', '0.00') }}</span></span>
                        <br><span style="color:var(--text-dim); font-size:10px;">net <span class="data-money" data-raw="{{ ydata.get('net', 0) }}" data-ccy="{{ row.currency_code | default(results.currency_code) | default('USD') }}">{{ ydata.get('net_fmt', '0.00') }}</span></span>
                    </td>
                    {% endfor %}
                    <td class="text-right mono" style="font-weight:700; color:var(--text-primary);">
                        <span class="data-money" data-raw="{{ row.total_gross }}" data-ccy="{{ row.currency_code | default(results.currency_code) | default('USD') }}">{{ row.currency_symbol | default(results.currency_symbol) | default('$') }}{{ row.total_gross_fmt }}</span>
                        <br><span style="color:var(--text-dim); font-size:10px;">net <span class="data-money" data-raw="{{ row.total_net }}" data-ccy="{{ row.currency_code | default(results.currency_code) | default('USD') }}">{{ row.total_net_fmt }}</span></span>
                    </td>
                </tr>
                {% endfor %}
                <tr style="border-top:2px solid var(--border);">
                    <td style="font-weight:700; color:var(--text-primary);">Total</td>
                    {% for year in results.earnings_years %}
                    {% set ytotal = results.earnings_year_totals.get(year, results.earnings_year_totals.get(year|string, {})) %}
                    <td class="text-right mono" style="font-weight:700; color:var(--text-primary);"><span class="data-money data-money-mixed" data-raw="{{ ytotal.get('gross', 0) }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ ytotal.get('gross_fmt', '0.00') }}</span></td>
                    {% endfor %}
                    <td class="text-right mono" style="font-weight:800; color:var(--accent);"><span class="data-money data-money-mixed" data-raw="{{ results.earnings_grand_total }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ results.earnings_grand_total_fmt }}</span></td>
                </tr>
                </tbody>
            </table>
        </div>
    </div>
</div>
{% endif %}

{# ---- ROW 5: Earnings Waterfall, WAA, Source Breakdown ---- #}
{% if results.get('waterfall') and results.waterfall.get('overall') %}
<div class="grid grid-3" style="margin-bottom:16px;">

    {# -- Earnings Waterfall Chart -- #}
    <div class="card span-2">
        <div class="card-header">
            <span class="card-title">Earnings Waterfall</span>
            <select id="waterfallPayorSelect" class="form-input" style="width:auto; font-size:11px; padding:4px 8px;" onchange="updateWaterfall()">
                <option value="overall">All Payors</option>
                {% for code, wf in results.waterfall.per_payor.items() %}
                <option value="{{ code }}">{{ wf.name }}</option>
                {% endfor %}
            </select>
        </div>
        <div class="chart-wrap tall">
            <canvas id="waterfallChart"></canvas>
        </div>
    </div>

    {# -- Weighted Average Age + Source Breakdown -- #}
    <div style="display:flex; flex-direction:column; gap:16px;">
        {% if results.get('weighted_avg_age') %}
        <div class="card">
            <div class="card-header"><span class="card-title">Weighted Avg. Age</span></div>
            <div class="stat-value medium">{{ results.weighted_avg_age.waa_display }}</div>
            <div class="stat-subtitle">
                {{ results.weighted_avg_age.tracks_with_dates }} / {{ results.weighted_avg_age.tracks_with_dates + results.weighted_avg_age.tracks_without_dates }} tracks with dates ({{ results.weighted_avg_age.pct_coverage }}%)
            </div>
            {% if results.weighted_avg_age.tracks_without_dates > 0 %}
            <div style="margin-top:8px; font-size:11px; padding:4px 8px; background:rgba(251,191,36,0.1); border:1px solid rgba(251,191,36,0.25); border-radius:4px; color:var(--yellow);">
                {{ results.weighted_avg_age.tracks_without_dates }} tracks missing release dates
            </div>
            {% endif %}
        </div>
        {% endif %}

        {% if results.get('source_breakdown') and results.source_breakdown.get('rows') %}
        <div class="card">
            <div class="card-header"><span class="card-title">Release Date Sources</span></div>
            <table>
                <thead><tr><th>Source</th><th class="text-right">Count</th><th class="text-right">%</th></tr></thead>
                <tbody>
                {% for row in results.source_breakdown.rows %}
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">{{ row.label }}</td>
                    <td class="text-right mono" style="font-size:12px;">{{ row.count }}</td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);">{{ row.pct }}%</td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}
    </div>
</div>
{% endif %}

{# ---- B4: Cohort/Vintage Analysis Card ---- #}
{% if results.get('cohort_analysis') and results.cohort_analysis.get('cohorts') %}
<div id="sec-cohorts" class="grid" style="margin-bottom:16px;">
    <div class="card span-full">
        <div class="card-header">
            <span class="card-title">Cohort / Vintage Analysis</span>
            <div style="display:flex; gap:8px; align-items:center;">
                <button class="pill-tab active" id="cohortAbsBtn" onclick="setCohortMode('abs')">Absolute</button>
                <button class="pill-tab" id="cohortPctBtn" onclick="setCohortMode('pct')">% Decay</button>
            </div>
        </div>
        <div class="chart-wrap" style="height:320px;">
            <canvas id="cohortChart"></canvas>
        </div>
        <div style="margin-top:8px; font-size:11px; color:var(--text-dim);">
            Shows how each release-year vintage earns across calendar years. Toggle to see decay from peak.
        </div>
    </div>
</div>
{% endif %}

{# ---- B5: Revenue Concentration + B6: Age Distribution + B7: LTM Comparison ---- #}
{% set has_concentration = results.get('revenue_concentration') and results.revenue_concentration.get('total_isrcs', 0) > 0 %}
{% set has_age_dist = results.get('catalog_age_distribution') and results.catalog_age_distribution.get('buckets') %}
{% set has_ltm_cmp = results.get('ltm_comparison') and results.ltm_comparison.get('prior_ltm') and results.ltm_comparison.prior_ltm.gross > 0 %}

<div id="sec-valuation" class="grid grid-3" style="margin-bottom:16px;">

    {# -- B5: Revenue Concentration -- #}
    {% if has_concentration %}
    <div class="card">
        <div class="card-header"><span class="card-title">Revenue Concentration</span></div>
        {% set rc = results.revenue_concentration %}
        <div style="padding:8px 0;">
            {% for label, val in [('Top 1%', rc.top_1_pct), ('Top 5%', rc.top_5_pct), ('Top 10%', rc.top_10_pct), ('Top 20%', rc.top_20_pct), ('Top 50%', rc.top_50_pct)] %}
            <div style="margin-bottom:8px;">
                <div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:3px;">
                    <span style="color:var(--text-secondary);">{{ label }} tracks</span>
                    <span class="mono" style="color:var(--text-primary); font-weight:600;">{{ val }}%</span>
                </div>
                <div style="height:6px; background:var(--bg-inset); border-radius:3px; overflow:hidden;">
                    <div style="height:100%; width:{{ val }}%; background:var(--accent); border-radius:3px; transition:width 0.3s;"></div>
                </div>
            </div>
            {% endfor %}
        </div>
        <div style="margin-top:8px; padding-top:8px; border-top:1px solid var(--border);">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span style="font-size:11px; color:var(--text-dim);">Herfindahl Index</span>
                <span class="mono" style="font-size:14px; font-weight:600; color:var(--text-primary);">{{ rc.herfindahl_index }}</span>
            </div>
            <div style="font-size:10px; color:var(--text-dim); margin-top:2px;">
                {% if rc.herfindahl_index < 1500 %}Low concentration (diversified){% elif rc.herfindahl_index < 2500 %}Moderate concentration{% else %}High concentration{% endif %}
                &middot; {{ rc.total_isrcs }} ISRCs
            </div>
        </div>
    </div>
    {% endif %}

    {# -- B6: Catalog Age Distribution -- #}
    {% if has_age_dist %}
    <div class="card">
        <div class="card-header"><span class="card-title">Catalog Age Distribution</span></div>
        <div class="chart-wrap" style="height:220px;">
            <canvas id="ageDistChart"></canvas>
        </div>
    </div>
    {% endif %}

    {# -- B7: LTM vs Prior-Year-LTM -- #}
    {% if has_ltm_cmp %}
    {% set cmp = results.ltm_comparison %}
    <div class="card">
        <div class="card-header"><span class="card-title">LTM vs Prior-Year-LTM</span></div>
        <table>
            <thead><tr><th>Metric</th><th class="text-right">LTM</th><th class="text-right">Prior LTM</th><th class="text-right">Change</th></tr></thead>
            <tbody>
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">Gross</td>
                    <td class="text-right mono" style="font-size:12px;"><span class="data-money" data-raw="{{ cmp.ltm.gross }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.ltm.gross) }}</span></td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);"><span class="data-money" data-raw="{{ cmp.prior_ltm.gross }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.prior_ltm.gross) }}</span></td>
                    <td class="text-right"><span class="stat-change {{ 'up' if cmp.changes.gross_pct >= 0 else 'down' }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(cmp.changes.gross_pct) }}%</span></td>
                </tr>
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">Net</td>
                    <td class="text-right mono" style="font-size:12px;"><span class="data-money" data-raw="{{ cmp.ltm.net }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.ltm.net) }}</span></td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);"><span class="data-money" data-raw="{{ cmp.prior_ltm.net }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.prior_ltm.net) }}</span></td>
                    <td class="text-right"><span class="stat-change {{ 'up' if cmp.changes.net_pct >= 0 else 'down' }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(cmp.changes.net_pct) }}%</span></td>
                </tr>
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);"># ISRCs</td>
                    <td class="text-right mono" style="font-size:12px;">{{ '{:,}'.format(cmp.ltm.isrc_count) }}</td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);">{{ '{:,}'.format(cmp.prior_ltm.isrc_count) }}</td>
                    <td class="text-right"><span class="stat-change {{ 'up' if cmp.changes.isrc_pct >= 0 else 'down' }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(cmp.changes.isrc_pct) }}%</span></td>
                </tr>
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">Avg/ISRC</td>
                    <td class="text-right mono" style="font-size:12px;"><span class="data-money" data-raw="{{ cmp.ltm.avg_per_isrc }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.ltm.avg_per_isrc) }}</span></td>
                    <td class="text-right mono" style="font-size:12px; color:var(--text-muted);"><span class="data-money" data-raw="{{ cmp.prior_ltm.avg_per_isrc }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ '{:,.2f}'.format(cmp.prior_ltm.avg_per_isrc) }}</span></td>
                    <td class="text-right"><span class="stat-change {{ 'up' if cmp.changes.avg_per_isrc_pct >= 0 else 'down' }}" style="margin:0; font-size:11px;">{{ '%+.1f' | format(cmp.changes.avg_per_isrc_pct) }}%</span></td>
                </tr>
            </tbody>
        </table>
    </div>
    {% endif %}

    {# -- Offer Multiple Calculator -- #}
    <div class="card">
        <div class="card-header"><span class="card-title">Offer Multiple</span></div>
        <div style="padding:8px 0;">
            <label style="font-size:11px; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.04em;">Offer Price</label>
            <div style="position:relative; margin-top:4px; margin-bottom:12px;">
                <span style="position:absolute; left:10px; top:50%; transform:translateY(-50%); font-size:13px; color:var(--text-muted);">{{ results.currency_symbol | default('$') }}</span>
                <input type="text" id="offerPriceInput" class="form-input" placeholder="e.g. 500,000"
                    style="padding-left:24px; font-family:'SF Mono',monospace; font-size:14px; width:100%;"
                    oninput="calcOfferMultiple()">
            </div>
            <div id="offerResults" style="display:none;">
                <div style="display:flex; justify-content:space-between; align-items:baseline; padding:8px 0; border-top:1px solid var(--border);">
                    <span style="font-size:12px; color:var(--text-secondary);">Net Multiple</span>
                    <span id="offerNetMultiple" class="mono" style="font-size:20px; font-weight:700; color:var(--accent);"></span>
                </div>
                <div style="display:flex; justify-content:space-between; align-items:baseline; padding:8px 0; border-top:1px solid var(--border);">
                    <span style="font-size:12px; color:var(--text-secondary);">Gross Multiple</span>
                    <span id="offerGrossMultiple" class="mono" style="font-size:16px; font-weight:600; color:var(--text-primary);"></span>
                </div>
                <div style="padding:8px 0; border-top:1px solid var(--border);">
                    <div style="display:flex; justify-content:space-between; font-size:11px; color:var(--text-dim);">
                        <span>LTM Net</span>
                        <span class="mono">{{ results.currency_symbol | default('$') }}{{ results.ltm_net_total_fmt }}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:11px; color:var(--text-dim); margin-top:2px;">
                        <span>LTM Gross</span>
                        <span class="mono">{{ results.currency_symbol | default('$') }}{{ results.ltm_gross_total_fmt }}</span>
                    </div>
                </div>
            </div>
            <div id="offerHint" style="font-size:11px; color:var(--text-dim); margin-top:4px;">Enter an offer price to see the implied multiple</div>
        </div>
    </div>
</div>

<script>
var _ltmNet = {{ results.ltm_net_total | default(0) }};
var _ltmGross = {{ results.ltm_gross_total | default(0) }};
function calcOfferMultiple() {
    var raw = document.getElementById('offerPriceInput').value.replace(/[^0-9.]/g, '');
    var price = parseFloat(raw);
    var results = document.getElementById('offerResults');
    var hint = document.getElementById('offerHint');
    if (!price || price <= 0) {
        results.style.display = 'none';
        hint.style.display = 'block';
        return;
    }
    results.style.display = 'block';
    hint.style.display = 'none';
    var netMult = _ltmNet > 0 ? (price / _ltmNet) : 0;
    var grossMult = _ltmGross > 0 ? (price / _ltmGross) : 0;
    document.getElementById('offerNetMultiple').textContent = netMult.toFixed(1) + 'x';
    document.getElementById('offerGrossMultiple').textContent = grossMult.toFixed(1) + 'x';
}
</script>

{# ---- Audit Trail Card ---- #}
{% if results.get('audit_summary') %}
<div class="grid" style="margin-bottom:16px;">
    <div class="card span-full">
        <div class="card-header">
            <span class="card-title">Audit Trail</span>
            <span style="font-size:11px; color:var(--text-dim);">Column mapping decisions</span>
        </div>
        {% set asumm = results.audit_summary %}
        <div style="display:flex; gap:24px; padding:8px 16px 4px; flex-wrap:wrap;">
            <div style="text-align:center;">
                <div class="mono" style="font-size:20px; font-weight:600; color:var(--green);">{{ asumm.auto_ingested_files }}</div>
                <div style="font-size:10px; color:var(--text-muted); text-transform:uppercase;">Auto-ingested</div>
            </div>
            <div style="text-align:center;">
                <div class="mono" style="font-size:20px; font-weight:600; color:var(--blue);">{{ asumm.manually_mapped_files }}</div>
                <div style="font-size:10px; color:var(--text-muted); text-transform:uppercase;">Manually mapped</div>
            </div>
            <div style="text-align:center;">
                <div class="mono" style="font-size:20px; font-weight:600; color:var(--text-primary);">{{ asumm.total_columns_mapped }}</div>
                <div style="font-size:10px; color:var(--text-muted); text-transform:uppercase;">Columns mapped</div>
            </div>
            <div style="text-align:center;">
                <div class="mono" style="font-size:20px; font-weight:600; color:var(--green);">{{ asumm.auto_accepted_columns }}</div>
                <div style="font-size:10px; color:var(--text-muted); text-transform:uppercase;">Auto-accepted</div>
            </div>
            <div style="text-align:center;">
                <div class="mono" style="font-size:20px; font-weight:600; color:var(--yellow);">{{ asumm.user_corrected_columns }}</div>
                <div style="font-size:10px; color:var(--text-muted); text-transform:uppercase;">User-corrected</div>
            </div>
        </div>
        {% if asumm.get('per_payor') %}
        <table style="margin-top:8px;">
            <thead>
                <tr>
                    <th>Payor</th>
                    <th class="text-right">Files</th>
                    <th class="text-right">Auto-mapped</th>
                    <th class="text-right">Corrected</th>
                    <th class="text-right">Avg Confidence</th>
                </tr>
            </thead>
            <tbody>
            {% for row in asumm.per_payor %}
                <tr>
                    <td style="font-size:12px; color:var(--text-secondary);">{{ row.name }}</td>
                    <td class="text-right mono" style="font-size:12px;">{{ row.files }}</td>
                    <td class="text-right mono" style="font-size:12px; color:var(--green);">{{ row.auto_mapped }}</td>
                    <td class="text-right mono" style="font-size:12px; color:var(--yellow);">{{ row.user_corrected }}</td>
                    <td class="text-right mono" style="font-size:12px;">{{ '%.0f' | format(row.avg_confidence * 100) }}%</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
        {% endif %}
    </div>
</div>
{% endif %}

{# ---- ROW 6: Statement Coverage Grid ---- #}
{% if results.get('coverage_rows') and results.get('coverage_months') %}
<div id="sec-coverage" class="grid" style="margin-bottom:16px;">
    <div class="card span-full">
        <div class="card-header">
            <span class="card-title">Statement Coverage</span>
            <span style="font-size:11px; color:var(--text-dim);">{{ results.coverage_months | length }} months across {{ results.coverage_rows | length }} payors</span>
        </div>
        <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th style="position:sticky; left:0; background:var(--bg-card); z-index:1;">Payor</th>
                        {% for cm in results.coverage_months %}
                        <th style="text-align:center; font-size:9px; padding:4px 2px; min-width:38px; white-space:nowrap;">{{ cm.short }}</th>
                        {% endfor %}
                        <th class="text-right" style="position:sticky; right:0; background:var(--bg-card); z-index:1;">Missing</th>
                    </tr>
                </thead>
                <tbody>
                {% for row in results.coverage_rows %}
                <tr>
                    <td style="font-weight:500; color:var(--text-primary); white-space:nowrap; position:sticky; left:0; background:var(--bg-card); z-index:1;">{{ row.name }}</td>
                    {% for cell in row.cells %}
                    <td style="text-align:center; padding:4px 2px;">
                        {% if cell.has %}
                        <span style="display:inline-block; width:12px; height:12px; border-radius:3px; background:var(--green); opacity:0.7;"></span>
                        {% else %}
                        <span style="display:inline-block; width:12px; height:12px; border-radius:3px; background:var(--red); opacity:0.5;" title="Missing: {{ results.coverage_months[loop.index0].label }}"></span>
                        {% endif %}
                    </td>
                    {% endfor %}
                    <td class="text-right mono" style="position:sticky; right:0; background:var(--bg-card); z-index:1;">
                        {% if row.missing_count > 0 %}
                        <span style="color:var(--yellow); font-weight:600;">{{ row.missing_count }}</span>
                        {% else %}
                        <span style="color:var(--green);">0</span>
                        {% endif %}
                    </td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>
        {# Detailed missing months list #}
        {% set any_missing = results.coverage_rows | selectattr('missing_count', 'gt', 0) | list %}
        {% if any_missing %}
        <div style="margin-top:16px; padding-top:16px; border-top:1px solid var(--border);">
            <div style="font-size:11px; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.04em; margin-bottom:10px;">Missing Months Detail</div>
            {% for row in any_missing %}
            <div style="margin-bottom:10px;">
                <div style="font-size:12px; font-weight:500; color:var(--text-primary); margin-bottom:4px;">{{ row.name }} <span style="color:var(--yellow); font-size:11px;">({{ row.missing_count }} missing)</span></div>
                <div style="display:flex; flex-wrap:wrap; gap:4px;">
                    {% for mm in row.missing_list %}
                    <span style="font-size:10px; background:rgba(251,191,36,0.1); border:1px solid rgba(251,191,36,0.25); color:var(--yellow); padding:2px 6px; border-radius:3px;">{{ mm }}</span>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
        </div>
        {% endif %}

        {# File inventory per payor #}
        {% set has_inventory = [] %}
        {% for ps in results.payor_summaries %}
            {% if ps.get('file_inventory') %}
                {% if has_inventory.append(ps) %}{% endif %}
            {% endif %}
        {% endfor %}
        {% if has_inventory %}
        <div style="margin-top:16px; padding-top:16px; border-top:1px solid var(--border);">
            <div style="font-size:11px; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.04em; margin-bottom:10px;">File Inventory</div>
            {% for ps in has_inventory %}
            <details style="margin-bottom:8px;">
                <summary style="cursor:pointer; font-size:12px; font-weight:500; color:var(--text-primary); padding:6px 0; user-select:none;">
                    {{ ps.name }}
                    <span style="color:var(--text-dim); font-weight:400;">{{ ps.file_inventory | length }} file{{ 's' if ps.file_inventory | length != 1 }}</span>
                    {% set skipped = ps.file_inventory | selectattr('status', 'eq', 'skipped') | list %}
                    {% set dupes = ps.file_inventory | selectattr('status', 'eq', 'duplicate') | list %}
                    {% if skipped %}
                    <span style="color:var(--red); font-size:10px;">({{ skipped | length }} skipped)</span>
                    {% endif %}
                    {% if dupes %}
                    <span style="color:var(--yellow); font-size:10px;">({{ dupes | length }} duplicate{{ 's' if dupes | length != 1 }})</span>
                    {% endif %}
                </summary>
                <div style="overflow-x:auto; margin-top:4px; margin-bottom:8px;">
                    <table>
                        <thead>
                            <tr>
                                <th>File</th>
                                <th>Folder</th>
                                <th>Period</th>
                                <th>Source</th>
                                <th class="text-right">Rows</th>
                                <th class="text-right">Gross</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                        {% for fi in ps.file_inventory %}
                        <tr>
                            <td style="color:var(--text-primary); font-size:12px; max-width:250px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{{ fi.filename }}</td>
                            <td style="font-size:11px; color:var(--text-dim); max-width:150px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{{ fi.folder or '/' }}</td>
                            <td class="mono" style="font-size:11px;">
                                {% if fi.get('periods') and fi.periods | length > 1 %}
                                {{ fi.periods | length }} periods
                                {% elif fi.period %}
                                {{ fi.period }}
                                {% else %}
                                <span style="color:var(--red);">unknown</span>
                                {% endif %}
                            </td>
                            <td style="font-size:10px;">
                                {% if fi.period_source == 'data' %}
                                <span style="background:var(--green-dim); color:var(--green); padding:1px 6px; border-radius:3px;">data</span>
                                {% elif fi.period_source and fi.period_source.startswith('folder:') %}
                                <span style="background:rgba(162,139,250,0.15); color:var(--purple); padding:1px 6px; border-radius:3px;">{{ fi.period_source }}</span>
                                {% elif fi.period_source == 'filename' %}
                                <span style="background:rgba(59,130,246,0.15); color:var(--accent); padding:1px 6px; border-radius:3px;">filename</span>
                                {% else %}
                                <span style="background:var(--bg-inset); color:var(--text-dim); padding:1px 6px; border-radius:3px;">none</span>
                                {% endif %}
                            </td>
                            <td class="text-right mono" style="font-size:11px;">{{ fi.rows }}</td>
                            <td class="text-right mono" style="font-size:11px;"><span class="data-money" data-raw="{{ fi.gross }}" data-ccy="{{ ps.currency_code | default(results.currency_code) | default('USD') }}">{{ ps.currency_symbol | default(results.currency_symbol) | default('$') }}{{ '{:,.2f}'.format(fi.gross) }}</span></td>
                            <td>
                                {% if fi.status == 'ok' %}
                                <span style="font-size:10px; color:var(--green);">OK</span>
                                {% elif fi.status == 'duplicate' %}
                                <span style="font-size:10px; color:var(--yellow);" title="Duplicate of {{ fi.get('duplicate_of', '?') }}">DUP</span>
                                {% else %}
                                <span style="font-size:10px; color:var(--red);">SKIP</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}
                        </tbody>
                    </table>
                </div>
            </details>
            {% endfor %}
        </div>
        {% endif %}

        {# Quality warnings #}
        {% set all_warnings = [] %}
        {% for ps in results.payor_summaries %}
            {% for w in ps.get('quality_warnings', []) %}
                {% if all_warnings.append({'payor': ps.name, 'msg': w}) %}{% endif %}
            {% endfor %}
        {% endfor %}
        {% if all_warnings %}
        <div style="margin-top:16px; padding-top:16px; border-top:1px solid var(--border);">
            <div style="font-size:11px; font-weight:600; color:var(--yellow); text-transform:uppercase; letter-spacing:0.04em; margin-bottom:10px;">Data Quality Warnings</div>
            {% for w in all_warnings %}
            <div style="font-size:12px; color:var(--text-secondary); padding:4px 0; border-bottom:1px solid var(--border-subtle);">
                <span style="color:var(--yellow); font-weight:500;">{{ w.payor }}:</span> {{ w.msg }}
            </div>
            {% endfor %}
        </div>
        {% endif %}
    </div>
</div>
{% endif %}

{# ---- Chart.js Scripts ---- #}
<script>
const CHART_COLORS = ['#3b82f6', '#a78bfa', '#22d3ee', '#fbbf24', '#f87171'];
const PAYOR_NAMES = {{ payor_names | tojson }};
const PAYOR_CODES = {{ payor_codes | tojson }};
const CSYM = '{{ results.currency_symbol | default("$") }}';

/* ---- Currency data for conversion engine ---- */
const CURRENCY_DATA = {
    defaultCode: '{{ results.currency_code | default("USD") }}',
    defaultSymbol: CSYM,
    payorCurrencies: {{ results.payor_currencies | default({}) | tojson }},
    symbols: {'USD':'$','EUR':'\u20ac','GBP':'\u00a3','CAD':'C$','AUD':'A$','JPY':'\u00a5'},
    /* Raw chart data (for re-converting chart datasets) */
    monthlyTrend: {{ results.monthly_trend | tojson }},
    monthlyByPayor: {{ results.monthly_by_payor | tojson }},
    annualByPayor: {{ results.annual_by_payor | tojson }},
    waterfall: {{ results.waterfall | default({}) | tojson }},
};

/* ---- CurrencyConverter module ---- */
const CurrencyConverter = (function() {
    let _rates = null;
    let _target = 'original';

    function symbolFor(code) {
        return CURRENCY_DATA.symbols[code] || code + ' ';
    }

    function currentSymbol() {
        if (_target === 'original') return null;
        return symbolFor(_target);
    }

    async function fetchRates() {
        if (_rates) return _rates;
        try {
            const resp = await fetch('/api/exchange-rates');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const data = await resp.json();
            if (data.error) throw new Error(data.error);
            _rates = data.rates;
            _rates['USD'] = 1;
            return _rates;
        } catch (e) {
            console.warn('Exchange rate fetch failed:', e);
            return null;
        }
    }

    function convert(amount, sourceCcy, targetCcy) {
        if (!_rates || sourceCcy === targetCcy) return amount;
        const srcRate = _rates[sourceCcy] || 1;
        const tgtRate = _rates[targetCcy] || 1;
        return amount / srcRate * tgtRate;
    }

    function formatMoney(value, ccy) {
        const sym = symbolFor(ccy);
        const decimals = ccy === 'JPY' ? 0 : 2;
        const formatted = Math.abs(value).toLocaleString(undefined, {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        });
        return (value < 0 ? '-' : '') + sym + formatted;
    }

    function csym() {
        return _target === 'original' ? CSYM : symbolFor(_target);
    }

    /* Reverse-lookup: symbol → ISO code */
    const _symToCode = {};
    Object.entries(CURRENCY_DATA.symbols).forEach(([code, sym]) => { _symToCode[sym] = code; });

    function _parseOriginal(el) {
        /* On first touch, cache the original text and parse raw value + source currency from it */
        if (el.dataset._origDone) return;
        el.dataset._origDone = '1';
        el.dataset.origText = el.textContent.trim();
        /* If data-raw is empty, parse numeric value from displayed text */
        if (!el.dataset.raw || el.dataset.raw === '') {
            const numStr = el.dataset.origText.replace(/[^0-9.\-]/g, '');
            const parsed = parseFloat(numStr);
            if (!isNaN(parsed)) el.dataset.raw = String(parsed);
        }
        /* Detect source currency from the leading symbol in original text */
        const txt = el.dataset.origText;
        for (const [sym, code] of Object.entries(_symToCode)) {
            if (txt.startsWith(sym)) { el.dataset.ccy = code; return; }
        }
    }

    function updateDOMValues() {
        document.querySelectorAll('.data-money').forEach(el => {
            _parseOriginal(el);
            const raw = parseFloat(el.dataset.raw);
            if (isNaN(raw)) return;
            const srcCcy = el.dataset.ccy || CURRENCY_DATA.defaultCode;
            if (_target === 'original') {
                el.textContent = el.dataset.origText;
            } else {
                const converted = convert(raw, srcCcy, _target);
                el.textContent = formatMoney(converted, _target);
            }
        });
    }

    function _convertChartData(rawData, srcCcy) {
        if (_target === 'original' || !_rates) return rawData;
        return rawData.map(v => convert(v, srcCcy, _target));
    }

    function updateCharts() {
        const sym = csym();
        const fmtTip = (val) => sym + val.toLocaleString(undefined, {minimumFractionDigits:2});
        const fmtAxis = (v) => sym + (Math.abs(v)/1000).toFixed(0) + 'k';

        /* Mini monthly chart */
        if (window.CHARTS.miniMonthly) {
            const ch = window.CHARTS.miniMonthly;
            const data = CURRENCY_DATA.monthlyTrend.slice(-24);
            const ccy = CURRENCY_DATA.defaultCode;
            ch.data.datasets[0].data = data.map(d => _target === 'original' ? d.gross : convert(d.gross, ccy, _target));
            ch.options.plugins.tooltip.callbacks.label = ctx => fmtTip(ctx.parsed.y);
            ch.update('none');
        }

        /* Monthly by payor */
        if (window.CHARTS.monthlyPayor) {
            const ch = window.CHARTS.monthlyPayor;
            const allPeriods = CURRENCY_DATA.monthlyTrend.slice(-36);
            const periodKeys = allPeriods.map(d => d.period);
            PAYOR_CODES.forEach((code, i) => {
                if (!ch.data.datasets[i]) return;
                const pdata = CURRENCY_DATA.monthlyByPayor[code] || [];
                const lookup = {};
                pdata.forEach(d => { lookup[d.period] = d.gross; });
                const ccy = CURRENCY_DATA.payorCurrencies[code] || CURRENCY_DATA.defaultCode;
                ch.data.datasets[i].data = periodKeys.map(p => {
                    const v = lookup[p] || 0;
                    return _target === 'original' ? v : convert(v, ccy, _target);
                });
            });
            ch.options.plugins.tooltip.callbacks.label = ctx => ctx.dataset.label + ': ' + fmtTip(ctx.parsed.y);
            ch.options.scales.y.ticks.callback = fmtAxis;
            ch.update('none');
        }

        /* Annual by payor */
        if (window.CHARTS.annualPayor) {
            const ch = window.CHARTS.annualPayor;
            const byPayor = CURRENCY_DATA.annualByPayor;
            const allYears = [...new Set(Object.values(byPayor).flat().map(d => d.year))].sort();
            PAYOR_CODES.forEach((code, i) => {
                if (!ch.data.datasets[i]) return;
                const pdata = byPayor[code] || [];
                const lookup = {};
                pdata.forEach(d => { lookup[d.year] = d.gross; });
                const ccy = CURRENCY_DATA.payorCurrencies[code] || CURRENCY_DATA.defaultCode;
                ch.data.datasets[i].data = allYears.map(y => {
                    const v = lookup[y] || 0;
                    return _target === 'original' ? v : convert(v, ccy, _target);
                });
            });
            ch.options.plugins.tooltip.callbacks.label = ctx => ctx.dataset.label + ': ' + fmtTip(ctx.parsed.y);
            ch.options.scales.y.ticks.callback = fmtAxis;
            ch.update('none');
        }

        /* Waterfall chart */
        if (window.CHARTS.waterfall && CURRENCY_DATA.waterfall.overall) {
            const sel = document.getElementById('waterfallPayorSelect');
            const val = sel ? sel.value : 'overall';
            const srcData = val === 'overall' ? CURRENCY_DATA.waterfall.overall : (CURRENCY_DATA.waterfall.per_payor || {})[val];
            if (srcData) {
                const ccy = val === 'overall' ? CURRENCY_DATA.defaultCode : (CURRENCY_DATA.payorCurrencies[val] || CURRENCY_DATA.defaultCode);
                const cv = (v) => _target === 'original' ? v : convert(v, ccy, _target);
                const values = [cv(srcData.gross), -Math.abs(cv(srcData.fees)), cv(srcData.net_receipts), cv(srcData.payable), -Math.abs(cv(srcData.third_party)), cv(srcData.net_earnings)];
                const colors = values.map(v => v >= 0 ? 'rgba(74, 222, 128, 0.7)' : 'rgba(248, 113, 113, 0.7)');
                const ch = window.CHARTS.waterfall;
                ch.data.datasets[0].data = values;
                ch.data.datasets[0].backgroundColor = colors;
                ch.options.plugins.tooltip.callbacks.label = ctx => sym + Math.abs(ctx.parsed.y).toLocaleString(undefined, {minimumFractionDigits:2});
                ch.options.scales.y.ticks.callback = v => sym + (Math.abs(v)/1000).toFixed(0) + 'k';
                ch.update('none');
            }
        }
    }

    async function setTargetCurrency(ccy) {
        _target = ccy;
        const status = document.getElementById('currencyStatus');
        if (ccy === 'original') {
            updateDOMValues();
            updateCharts();
            if (status) status.textContent = '';
            return;
        }
        if (!_rates) {
            if (status) status.textContent = 'Loading rates...';
            const r = await fetchRates();
            if (!r) {
                if (status) { status.textContent = 'Rate fetch failed'; status.style.color = 'var(--red)'; }
                _target = 'original';
                const sel = document.getElementById('currencyToggle');
                if (sel) sel.value = 'original';
                return;
            }
            if (status) { status.textContent = ''; status.style.color = 'var(--text-dim)'; }
        }
        updateDOMValues();
        updateCharts();
    }

    return { fetchRates, convert, formatMoney, setTargetCurrency, csym, symbolFor, currentSymbol };
})();

/* Wire up dropdown */
document.getElementById('currencyToggle').addEventListener('change', function() {
    CurrencyConverter.setTargetCurrency(this.value);
});

Chart.defaults.color = '#52525b';
Chart.defaults.borderColor = 'rgba(30,30,34,0.6)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;

/* Store chart instances for currency updates */
window.CHARTS = {};

/* ---- Mini monthly chart (top-left card) ---- */
(function() {
    const data = CURRENCY_DATA.monthlyTrend;
    const last24 = data.slice(-24);
    window.CHARTS.miniMonthly = new Chart(document.getElementById('monthlyMiniChart'), {
        type: 'bar',
        data: {
            labels: last24.map(d => d.label),
            datasets: [{
                data: last24.map(d => d.gross),
                backgroundColor: 'rgba(59,130,246,0.6)',
                borderRadius: 3,
                barPercentage: 0.7,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: {
                callbacks: { label: ctx => CSYM + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
            }},
            scales: {
                x: { display: false },
                y: { display: false }
            }
        }
    });
})();

/* ---- Monthly revenue by payor (stacked bar) ---- */
(function() {
    const byPayor = CURRENCY_DATA.monthlyByPayor;
    const allPeriods = CURRENCY_DATA.monthlyTrend;
    const last36 = allPeriods.slice(-36);
    const labels = last36.map(d => d.label);
    const periodKeys = last36.map(d => d.period);

    const datasets = PAYOR_CODES.map((code, i) => {
        const pdata = byPayor[code] || [];
        const lookup = {};
        pdata.forEach(d => { lookup[d.period] = d.gross; });
        return {
            label: PAYOR_NAMES[i],
            data: periodKeys.map(p => lookup[p] || 0),
            backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
            borderRadius: 2,
            barPercentage: 0.7,
        };
    });

    window.CHARTS.monthlyPayor = new Chart(document.getElementById('monthlyPayorChart'), {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } },
                tooltip: { mode: 'index', intersect: false,
                    callbacks: { label: ctx => ctx.dataset.label + ': ' + CSYM + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
                },
            },
            scales: {
                x: { stacked: true, grid: { display: false }, ticks: { maxRotation: 45, font: { size: 10 } } },
                y: { stacked: true, ticks: { callback: v => CSYM + (v/1000).toFixed(0) + 'k' } }
            }
        }
    });
})();

/* ---- Annual gross by payor (grouped bar) ---- */
(function() {
    const byPayor = CURRENCY_DATA.annualByPayor;
    const allYears = [...new Set(Object.values(byPayor).flat().map(d => d.year))].sort();
    const labels = allYears.map(String);

    const datasets = PAYOR_CODES.map((code, i) => {
        const pdata = byPayor[code] || [];
        const lookup = {};
        pdata.forEach(d => { lookup[d.year] = d.gross; });
        return {
            label: PAYOR_NAMES[i],
            data: allYears.map(y => lookup[y] || 0),
            backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
            borderRadius: 4,
            barPercentage: 0.6,
        };
    });

    window.CHARTS.annualPayor = new Chart(document.getElementById('annualPayorChart'), {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } },
                tooltip: {
                    callbacks: { label: ctx => ctx.dataset.label + ': ' + CSYM + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
                },
            },
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => CSYM + (v/1000).toFixed(0) + 'k' } }
            }
        }
    });
})();

/* ---- Earnings Waterfall Chart ---- */
{% if results.get('waterfall') and results.waterfall.get('overall') %}
(function() {
    const waterfallData = CURRENCY_DATA.waterfall;

    function renderWaterfall(data) {
        const labels = ['Gross Earnings', 'Fees', 'Net Receipts', 'Payable Share', '3P Share', 'Net Earnings'];
        const values = [data.gross, -Math.abs(data.fees), data.net_receipts, data.payable, -Math.abs(data.third_party), data.net_earnings];
        const colors = values.map(v => v >= 0 ? 'rgba(74, 222, 128, 0.7)' : 'rgba(248, 113, 113, 0.7)');

        const ctx = document.getElementById('waterfallChart');
        if (window.CHARTS.waterfall) window.CHARTS.waterfall.destroy();

        window.CHARTS.waterfall = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors,
                    borderRadius: 4,
                    barPercentage: 0.6,
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: ctx => CSYM + Math.abs(ctx.parsed.y).toLocaleString(undefined, {minimumFractionDigits: 2})
                        }
                    }
                },
                scales: {
                    x: { grid: { display: false } },
                    y: { ticks: { callback: v => CSYM + (Math.abs(v)/1000).toFixed(0) + 'k' } }
                }
            }
        });
    }

    window.updateWaterfall = function() {
        const sel = document.getElementById('waterfallPayorSelect');
        if (!sel) return;
        const val = sel.value;
        if (val === 'overall') {
            renderWaterfall(waterfallData.overall);
        } else {
            const payor = waterfallData.per_payor[val];
            if (payor) renderWaterfall(payor);
        }
        /* Re-apply currency conversion if active */
        const currSel = document.getElementById('currencyToggle');
        if (currSel && currSel.value !== 'original') {
            CurrencyConverter.setTargetCurrency(currSel.value);
        }
    };

    renderWaterfall(waterfallData.overall);
})();
{% endif %}

/* ---- B3: Section navigation smooth scroll ---- */
window.scrollToSection = function(id, btn) {
    const el = document.getElementById(id);
    if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    /* Update active tab */
    document.querySelectorAll('#dashNav .pill-tab').forEach(t => t.classList.remove('active'));
    if (btn) btn.classList.add('active');
};

/* ---- Theme toggle ---- */
window.toggleTheme = function() {
    const html = document.documentElement;
    const current = html.getAttribute('data-theme');
    const next = current === 'light' ? 'dark' : 'light';
    if (next === 'dark') {
        html.removeAttribute('data-theme');
    } else {
        html.setAttribute('data-theme', 'light');
    }
    localStorage.setItem('rc-theme', next);
    _updateThemeIcon();
    _updateChartColors();
};
function _updateThemeIcon() {
    const icon = document.getElementById('themeIcon');
    if (!icon) return;
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    icon.innerHTML = isLight ? '&#9728;' : '&#9790;';
    icon.parentElement.title = isLight ? 'Switch to dark mode' : 'Switch to light mode';
}
function _updateChartColors() {
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    const gridColor = isLight ? 'rgba(0,0,0,0.08)' : 'rgba(255,255,255,0.06)';
    const tickColor = isLight ? '#4b5563' : '#a1a1aa';
    Object.values(Chart.instances || {}).forEach(function(chart) {
        if (!chart.options || !chart.options.scales) return;
        Object.values(chart.options.scales).forEach(function(scale) {
            if (scale.grid) scale.grid.color = gridColor;
            if (scale.ticks) scale.ticks.color = tickColor;
        });
        chart.update('none');
    });
}
/* Set icon on page load */
document.addEventListener('DOMContentLoaded', _updateThemeIcon);

/* ---- B4: Cohort/Vintage chart ---- */
{% if results.get('cohort_analysis') and results.cohort_analysis.get('cohorts') %}
(function() {
    const COHORT_DATA = {{ results.cohort_analysis | tojson }};
    const cohortColors = ['#3b82f6', '#a78bfa', '#22d3ee', '#fbbf24', '#f87171', '#34d399', '#fb923c', '#e879f9', '#94a3b8', '#6ee7b7'];
    let cohortMode = 'abs';
    let cohortChart = null;

    function buildCohortChart(mode) {
        const ctx = document.getElementById('cohortChart');
        if (cohortChart) cohortChart.destroy();

        const years = COHORT_DATA.years;
        const datasets = COHORT_DATA.cohorts.map((c, i) => {
            let data;
            if (mode === 'pct') {
                const peak = Math.max(...Object.values(c.revenue_by_year), 0.01);
                data = years.map(y => {
                    const v = c.revenue_by_year[y] || 0;
                    return Math.round(v / peak * 100 * 10) / 10;
                });
            } else {
                data = years.map(y => c.revenue_by_year[y] || 0);
            }
            return {
                label: c.release_year + ' (' + c.track_count + ')',
                data: data,
                backgroundColor: cohortColors[i % cohortColors.length],
                borderRadius: 2,
                barPercentage: 0.7,
            };
        });

        cohortChart = new Chart(ctx, {
            type: 'bar',
            data: { labels: years.map(String), datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top', labels: { boxWidth: 10, padding: 8, font: { size: 10 } } },
                    tooltip: {
                        callbacks: {
                            label: ctx => {
                                const v = ctx.parsed.y;
                                return ctx.dataset.label + ': ' + (mode === 'pct' ? v + '%' : CSYM + v.toLocaleString(undefined, {minimumFractionDigits:2}));
                            }
                        }
                    },
                },
                scales: {
                    x: { grid: { display: false } },
                    y: { ticks: { callback: v => mode === 'pct' ? v + '%' : CSYM + (v/1000).toFixed(0) + 'k' } }
                }
            }
        });
        window.CHARTS.cohort = cohortChart;
    }

    window.setCohortMode = function(mode) {
        cohortMode = mode;
        document.getElementById('cohortAbsBtn').classList.toggle('active', mode === 'abs');
        document.getElementById('cohortPctBtn').classList.toggle('active', mode === 'pct');
        buildCohortChart(mode);
    };

    buildCohortChart('abs');
})();
{% endif %}

/* ---- B6: Catalog Age Distribution chart ---- */
{% if results.get('catalog_age_distribution') and results.catalog_age_distribution.get('buckets') %}
(function() {
    const AGE_DATA = {{ results.catalog_age_distribution | tojson }};
    const buckets = AGE_DATA.buckets;
    if (!buckets.length) return;

    const labels = buckets.map(b => String(b.year));
    const trackCounts = buckets.map(b => b.track_count);
    const ltmGross = buckets.map(b => b.ltm_gross);

    window.CHARTS.ageDist = new Chart(document.getElementById('ageDistChart'), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'LTM Revenue',
                    data: ltmGross,
                    backgroundColor: 'rgba(59,130,246,0.6)',
                    borderRadius: 3,
                    barPercentage: 0.6,
                    yAxisID: 'y',
                },
                {
                    label: 'Track Count',
                    data: trackCounts,
                    type: 'line',
                    borderColor: '#fbbf24',
                    backgroundColor: 'rgba(251,191,36,0.2)',
                    pointRadius: 3,
                    pointBackgroundColor: '#fbbf24',
                    fill: false,
                    tension: 0.3,
                    yAxisID: 'y1',
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 10, padding: 8, font: { size: 10 } } },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            if (ctx.datasetIndex === 0) return 'LTM: ' + CSYM + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2});
                            return 'Tracks: ' + ctx.parsed.y;
                        }
                    }
                }
            },
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 10 } } },
                y: { position: 'left', ticks: { callback: v => CSYM + (v/1000).toFixed(0) + 'k', font: { size: 10 } } },
                y1: { position: 'right', grid: { display: false }, ticks: { font: { size: 10 } } }
            }
        }
    });
})();
{% endif %}
</script>

{% else %}
{# ==================== NO DATA ==================== #}
<div class="page-header">
    <h1>Royalty Analytics</h1>
    <p>No data loaded yet. Run a consolidation to see the dashboard.</p>
</div>
<div class="card" style="text-align:center; padding:60px;">
    <div style="font-size:48px; color:var(--text-dim); margin-bottom:16px;">&#9835;</div>
    <p style="color:var(--text-muted); margin-bottom:24px;">Run the consolidation from your local directories to populate the dashboard.</p>
    <form method="POST" action="/run-default" style="display:inline;">
        <button type="submit" class="btn-submit" style="width:auto; padding:12px 32px;" onclick="showLoading()">
            Consolidate Now
        </button>
    </form>
    <div style="margin-top:12px;">
        <a href="/upload" style="font-size:13px; color:var(--text-muted);">or configure manually</a>
        <span style="color:var(--text-dim); margin:0 8px;">&middot;</span>
        <a href="/deals" style="font-size:13px; color:var(--text-muted);">load a saved deal</a>
    </div>
</div>
{% endif %}

</div>

{# Loading overlay #}
<div class="loading-overlay" id="loadingOverlay">
    <div class="loading-ring"></div>
    <div class="loading-text" id="loadingText">Processing statements across all payors...</div>
</div>

<script>
function showLoading() {
    document.getElementById('loadingOverlay').classList.add('active');
    pollStatus();
}

function pollStatus() {
    const interval = setInterval(function() {
        fetch('/api/status')
            .then(r => r.json())
            .then(data => {
                if (data.progress) {
                    document.getElementById('loadingText').textContent = data.progress;
                }
                if (data.done) {
                    clearInterval(interval);
                    window.location.href = '/?loaded=1';
                }
                if (data.error) {
                    clearInterval(interval);
                    window.location.href = '/';
                }
            })
            .catch(() => {});
    }, 2000);
}

function showDashTab(name) {
    document.querySelectorAll('[id^="dtab-"]').forEach(el => el.classList.remove('active'));
    document.getElementById('dtab-' + name).classList.add('active');
    const btn = event.target;
    btn.parentElement.querySelectorAll('.pill-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
}
function showSongTab(name) {
    document.querySelectorAll('.song-tab-content').forEach(el => { el.classList.remove('active'); el.style.display = 'none'; });
    const tab = document.getElementById('stab-' + name);
    if (tab) { tab.classList.add('active'); tab.style.display = ''; }
    const btn = event.target;
    btn.parentElement.querySelectorAll('.pill-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
}

// Auto-show loading overlay if processing is running
{% if processing is defined and processing and processing.running %}
(function() {
    document.getElementById('loadingOverlay').classList.add('active');
    document.getElementById('loadingText').textContent = '{{ processing.progress }}';
    pollStatus();
})();
{% endif %}

// Toast notification on load
(function() {
    const params = new URLSearchParams(window.location.search);
    if (params.get('loaded') === '1') {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.textContent = 'Dashboard ready!';
        document.body.appendChild(toast);
        // Browser notification if available
        if ('Notification' in window && Notification.permission === 'granted') {
            new Notification('Royalty Consolidator', { body: 'Dashboard ready!' });
        } else if ('Notification' in window && Notification.permission !== 'denied') {
            Notification.requestPermission().then(function(perm) {
                if (perm === 'granted') {
                    new Notification('Royalty Consolidator', { body: 'Dashboard ready!' });
                }
            });
        }
        setTimeout(function() {
            toast.classList.add('fade-out');
            setTimeout(function() { toast.remove(); }, 400);
        }, 4000);
        // Clean URL
        window.history.replaceState({}, '', '/');
    }
})();
</script>

<script>
/* Global theme toggle (works on all pages) */
if (typeof window.toggleTheme === 'undefined') {
    window.toggleTheme = function() {
        var html = document.documentElement;
        var current = html.getAttribute('data-theme');
        var next = current === 'light' ? 'dark' : 'light';
        if (next === 'dark') { html.removeAttribute('data-theme'); }
        else { html.setAttribute('data-theme', 'light'); }
        localStorage.setItem('rc-theme', next);
        _gUpdateIcon();
    };
    function _gUpdateIcon() {
        var icon = document.getElementById('themeIcon');
        if (!icon) return;
        var isLight = document.documentElement.getAttribute('data-theme') === 'light';
        icon.innerHTML = isLight ? '&#9728;' : '&#9790;';
        icon.parentElement.title = isLight ? 'Switch to dark mode' : 'Switch to light mode';
    }
    document.addEventListener('DOMContentLoaded', _gUpdateIcon);
}
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def run_consolidation(payor_configs, output_dir=None, deal_name=None, file_dates=None):
    """Run the consolidation pipeline and return (payor_results, analytics, consolidated_path)."""
    global _cached_results, _cached_analytics, _cached_deal_name

    payor_results = load_all_payors(payor_configs, file_dates=file_dates)
    if not payor_results:
        return None, None, None

    # If deal_name is set and no explicit output_dir, route exports to deals dir
    if deal_name and output_dir is None:
        slug = _make_slug(deal_name)
        output_dir = os.path.join(DEALS_DIR, slug, 'exports')
    elif output_dir is None:
        output_dir = WORK_DIR

    os.makedirs(output_dir, exist_ok=True)

    consolidated_xlsx = os.path.join(output_dir, 'Consolidated_All_Payors.xlsx')
    consolidated_csv = os.path.join(output_dir, 'Consolidated_All_Payors.csv')
    write_consolidated_excel(payor_results, consolidated_xlsx, deal_name=deal_name or '')
    write_consolidated_csv(payor_results, consolidated_csv, deal_name=deal_name or '')

    # Per-payor individual exports
    per_payor_dir = os.path.join(output_dir, 'per_payor')
    per_payor_paths = write_per_payor_exports(payor_results, per_payor_dir, deal_name=deal_name or '')

    analytics = compute_analytics(payor_results)

    with _state_lock:
        _cached_results = payor_results
        _cached_analytics = analytics
        _cached_deal_name = deal_name or ''
    app.config['CONSOLIDATED_PATH'] = consolidated_xlsx
    app.config['CONSOLIDATED_CSV_PATH'] = consolidated_csv
    app.config['PER_PAYOR_PATHS'] = per_payor_paths

    # Auto-save as a deal if deal_name is provided
    if deal_name:
        slug = _make_slug(deal_name)
        save_deal(slug, deal_name, payor_results, analytics,
                  consolidated_xlsx, consolidated_csv, per_payor_paths)

    return payor_results, analytics, consolidated_xlsx


def _run_in_background(payor_configs, output_dir=None, deal_name=None, file_dates=None):
    """Background worker for consolidation."""
    global _processing_status
    log.info("Background consolidation started: %d payor(s), deal=%s", len(payor_configs), deal_name)
    _log_memory()
    with _state_lock:
        _processing_status = {'running': True, 'progress': 'Loading payor data...', 'done': False, 'error': None}
    try:
        with _state_lock:
            _processing_status['progress'] = f'Processing {len(payor_configs)} payor(s)...'
        payor_results, analytics, consolidated_path = run_consolidation(
            payor_configs, output_dir=output_dir, deal_name=deal_name, file_dates=file_dates)
        with _state_lock:
            if not payor_results:
                log.warning("Background consolidation finished with no data")
                _processing_status.update({'running': False, 'done': True, 'error': 'No data found.'})
            else:
                log.info("Background consolidation done: %s files, %s ISRCs",
                         analytics["total_files"], analytics["isrc_count"])
                _log_memory()
                _processing_status.update({
                    'running': False,
                    'progress': f'Done: {analytics["total_files"]} files, {analytics["isrc_count"]} ISRCs.',
                    'done': True,
                    'error': None,
                })
    except Exception as e:
        log.error("Background consolidation FAILED: %s", e, exc_info=True)
        with _state_lock:
            _processing_status.update({'running': False, 'done': True, 'error': str(e)})


@app.route('/')
def index():
    with _state_lock:
        analytics_copy = dict(_cached_analytics) if _cached_analytics else None
        payor_names = [pr.config.name for pr in _cached_results.values()] if _cached_results else []
        payor_codes = list(_cached_results.keys()) if _cached_results else []
        deal_name_copy = _cached_deal_name
        processing_copy = dict(_processing_status)

        # Inject per-payor currency symbols into cached analytics
        if analytics_copy and _cached_results:
            from consolidator import _payor_currency_symbols
            pcsyms = _payor_currency_symbols(_cached_results)
            for item in analytics_copy.get('ltm_by_payor', []):
                if 'currency_symbol' not in item:
                    item['currency_symbol'] = pcsyms.get(item.get('code'), '$')
            for item in analytics_copy.get('payor_summaries', []):
                if 'currency_symbol' not in item:
                    item['currency_symbol'] = pcsyms.get(item.get('code'), '$')
            for item in analytics_copy.get('earnings_matrix', []):
                if 'currency_symbol' not in item:
                    item['currency_symbol'] = pcsyms.get(item.get('code'), '$')

    # Check for recent delta report
    delta_summary = None
    if deal_name_copy:
        slug = _make_slug(deal_name_copy)
        deal_dir = os.path.join(DEALS_DIR, slug)
        dr = delta_engine.load_delta_from_disk(deal_dir, slug)
        if dr:
            delta_summary = dr.to_dict()

    return render_template_string(
        DASHBOARD_HTML,
        page='dashboard',
        results=analytics_copy,
        payor_names=payor_names,
        payor_codes=payor_codes,
        default_payors=[],
        deal_name=deal_name_copy,
        processing=processing_copy,
        delta_summary=delta_summary,
    )


@app.route('/upload')
def upload_page():
    configs = []  # Presets - empty for now, add entries here as needed
    history = mapper.get_import_history(20)
    formats = mapper.get_saved_formats()
    with _state_lock:
        deal_name_copy = _cached_deal_name
    demo_autofill = request.args.get('demo') == '1'
    if demo_autofill:
        # Clear saved fingerprints so Preview+Map steps are shown
        mapper.clear_all_fingerprints()
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        ingest_step=None,
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=configs,
        deal_name=deal_name_copy,
        import_history=history,
        saved_formats=formats,
        demo_autofill=demo_autofill,
        demo_data_dir=DEMO_DATA_DIR.replace(chr(92), chr(92)*2) if demo_autofill else '',
        gcs_available=storage.is_available(),
    )


@app.route('/run-default', methods=['POST'])
def run_default():
    """Run consolidation using the default local directories in background."""
    global _processing_status
    with _state_lock:
        if _processing_status.get('running'):
            flash('A consolidation is already running.', 'error')
            return redirect(url_for('index'))
        _processing_status = {'running': True, 'progress': 'Starting...', 'done': False, 'error': None}
    t = threading.Thread(target=_run_in_background, args=(DEFAULT_PAYORS,), daemon=True)
    t.start()
    return redirect(url_for('index'))


@app.route('/run-custom', methods=['POST'])
def run_custom():
    """Run consolidation from uploaded files with dynamic payor configs."""
    global _cached_deal_name, _processing_status
    try:
        with _state_lock:
            if _processing_status.get('running'):
                flash('A consolidation is already running.', 'error')
                return redirect(url_for('index'))

        _cached_deal_name = request.form.get('deal_name', '').strip()

        work_dir = os.path.join(WORK_DIR, 'custom')
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir)
        os.makedirs(work_dir, exist_ok=True)

        payor_configs = []
        idx = 0
        while True:
            code = request.form.get(f'payor_code_{idx}')
            if code is None:
                break

            name = request.form.get(f'payor_name_{idx}', code)
            fmt = request.form.get(f'payor_fmt_{idx}', 'auto')
            fee_raw = request.form.get(f'payor_fee_{idx}', '').strip()
            fee = 0.0 if (not fee_raw or fee_raw.upper() == 'N/A') else float(fee_raw) / 100.0
            source_currency = request.form.get(f'payor_source_currency_{idx}', 'auto')
            statement_type = request.form.get(f'payor_stype_{idx}', 'masters')

            # Share calculation toggles
            calc_payable = request.form.get(f'payor_calc_payable_{idx}') is not None
            payable_pct_raw = request.form.get(f'payor_payable_pct_{idx}', '0').strip()
            payable_pct = float(payable_pct_raw) if payable_pct_raw else 0.0
            calc_third_party = request.form.get(f'payor_calc_third_party_{idx}') is not None
            tp_pct_raw = request.form.get(f'payor_third_party_pct_{idx}', '0').strip()
            third_party_pct = float(tp_pct_raw) if tp_pct_raw else 0.0

            # Deal term fields
            deal_type = request.form.get(f'payor_deal_type_{idx}', 'artist').strip()
            split_raw = request.form.get(f'payor_split_{idx}', '').strip()
            artist_split = float(split_raw) if split_raw else None
            territory = request.form.get(f'payor_territory_{idx}', '').strip() or None

            # Contract summary from Gemini analysis
            contract_summary = None
            contract_summary_raw = request.form.get(f'payor_contract_summary_{idx}', '').strip()
            if contract_summary_raw:
                try:
                    contract_summary = json.loads(contract_summary_raw)
                except (json.JSONDecodeError, ValueError):
                    pass

            # Contract PDF uploads (multiple)
            contract_pdf_path = None
            contract_files = request.files.getlist(f'payor_contract_{idx}')
            saved_contracts = []
            for cf in contract_files:
                if cf and cf.filename and cf.filename.lower().endswith('.pdf'):
                    contracts_dir = os.path.join(work_dir, 'contracts')
                    os.makedirs(contracts_dir, exist_ok=True)
                    pdf_filename = f"contract_{code.strip()}_{len(saved_contracts)}.pdf"
                    pdf_path = os.path.join(contracts_dir, pdf_filename)
                    cf.save(pdf_path)
                    saved_contracts.append(pdf_path)
            if saved_contracts:
                contract_pdf_path = saved_contracts[0]  # primary path for backwards compat

            # Expected period range (YYYYMM)
            period_start_raw = request.form.get(f'payor_period_start_{idx}', '').strip()
            expected_start = int(period_start_raw) if period_start_raw and period_start_raw.isdigit() and len(period_start_raw) == 6 else None
            period_end_raw = request.form.get(f'payor_period_end_{idx}', '').strip()
            expected_end = int(period_end_raw) if period_end_raw and period_end_raw.isdigit() and len(period_end_raw) == 6 else None

            # Check for local directory path first
            local_dir = request.form.get(f'payor_dir_{idx}', '').strip()

            if local_dir and os.path.isdir(local_dir):
                payor_dir = local_dir
            else:
                payor_dir = os.path.join(work_dir, f'statements_{code.strip()}')
                os.makedirs(payor_dir, exist_ok=True)

                files = request.files.getlist(f'payor_files_{idx}')
                has_files = False
                for f in files:
                    if not f.filename:
                        continue
                    has_files = True
                    if f.filename.endswith('.zip'):
                        zip_path = os.path.join(payor_dir, f.filename)
                        f.save(zip_path)
                        with zipfile.ZipFile(zip_path, 'r') as zf:
                            zf.extractall(payor_dir)
                    else:
                        f.save(os.path.join(payor_dir, f.filename))

                # GCS files: store paths for streaming (no download to /tmp)
                gcs_files_list = []
                gcs_json = request.form.get(f'gcs_files_{idx}', '').strip()
                if gcs_json:
                    try:
                        gcs_files_list = json.loads(gcs_json)
                        if gcs_files_list:
                            has_files = True
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass

                if not has_files and not local_dir:
                    idx += 1
                    continue

            payor_configs.append(PayorConfig(
                code=code.strip(),
                name=name.strip(),
                fmt=fmt,
                fee=fee,
                source_currency=source_currency,
                statements_dir=payor_dir,
                statement_type=statement_type,
                deal_type=deal_type,
                artist_split=artist_split,
                calc_payable=calc_payable,
                payable_pct=payable_pct,
                calc_third_party=calc_third_party,
                third_party_pct=third_party_pct,
                territory=territory,
                contract_pdf_path=contract_pdf_path,
                contract_summary=contract_summary,
                expected_start=expected_start,
                expected_end=expected_end,
                gcs_files=gcs_files_list if gcs_files_list else None,
            ))
            idx += 1

        if not payor_configs:
            flash('No payors configured. Add at least one payor.', 'error')
            return redirect(url_for('upload_page'))

        # Parse file_dates from the date extraction modal
        file_dates = {}
        file_dates_raw = request.form.get('file_dates_json', '').strip()
        if file_dates_raw:
            try:
                file_dates = json.loads(file_dates_raw)
            except (json.JSONDecodeError, ValueError):
                pass

        # Second pass: fill in missing dates from filenames/folders/content
        from consolidator import parse_period_from_filename, period_to_end_of_month, peek_statement_date
        for cfg in payor_configs:
            src_dir = cfg.statements_dir
            if not src_dir or not os.path.isdir(src_dir):
                continue
            for root, _, fnames in os.walk(src_dir):
                for fn in fnames:
                    if fn in file_dates and file_dates[fn]:
                        continue
                    period = parse_period_from_filename(fn)
                    if not period:
                        rel = os.path.relpath(os.path.join(root, fn), src_dir)
                        parts_list = os.path.normpath(rel).split(os.sep)
                        for part in reversed(parts_list[:-1]):
                            period = parse_period_from_filename(part)
                            if period:
                                break
                    if not period:
                        try:
                            period = peek_statement_date(os.path.join(root, fn), fn)
                        except Exception as e:
                            log.debug("peek_statement_date failed for %s: %s", fn, e)
                    if period:
                        eom = period_to_end_of_month(period)
                        date_parts = eom.split('/')
                        file_dates[fn] = f"{date_parts[0]}/{date_parts[1]}/{date_parts[2][2:]}"

        with _state_lock:
            _processing_status = {'running': True, 'progress': 'Starting...', 'done': False, 'error': None}
        deal_name = _cached_deal_name if _cached_deal_name else None
        out_dir = None if deal_name else work_dir
        t = threading.Thread(
            target=_run_in_background,
            args=(payor_configs,),
            kwargs={'output_dir': out_dir, 'deal_name': deal_name, 'file_dates': file_dates},
            daemon=True,
        )
        t.start()

    except Exception as e:
        log.error("run_custom failed: %s", e, exc_info=True)
        flash(f'Error: {str(e)}', 'error')

    return redirect(url_for('index'))


# ---------------------------------------------------------------------------
# Phase 2: Custom multi-step flow routes
# ---------------------------------------------------------------------------

CUSTOM_TEMP = os.path.join(tempfile.gettempdir(), 'royalty_consolidator', 'custom_flow')
os.makedirs(CUSTOM_TEMP, exist_ok=True)


def _download_gcs_files_to_dir(payor_dir: str, gcs_json: str) -> list:
    """Download GCS-uploaded files to a local payor directory.
    gcs_json is a JSON string: [{name, gcs_path}, ...]
    Handles zip extraction. Cleans up GCS blobs after download.
    Returns list of saved filenames.
    """
    try:
        gcs_files = json.loads(gcs_json)
    except (json.JSONDecodeError, ValueError, TypeError):
        return []
    os.makedirs(payor_dir, exist_ok=True)
    saved = []
    for entry in gcs_files:
        gcs_path = entry.get('gcs_path', '')
        fname = entry.get('name', os.path.basename(gcs_path))
        if not gcs_path:
            continue
        local_path = os.path.join(payor_dir, fname)
        try:
            storage.download_to_file(gcs_path, local_path)
            if fname.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(local_path, 'r') as zf:
                        zf.extractall(payor_dir)
                        saved.extend(n for n in zf.namelist() if not n.endswith('/'))
                    os.remove(local_path)
                except zipfile.BadZipFile:
                    saved.append(fname)
            else:
                saved.append(fname)
            # Clean up GCS temp blob
            storage.delete_blob(gcs_path)
        except Exception as e:
            log.error("GCS download failed for %s: %s", gcs_path, e)
    return saved

DEMO_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'demo_data')

_SUPPORTED_EXT = ('.csv', '.xlsx', '.xls', '.xlsb')


@app.route('/demo')
def demo_shortcut():
    """One-click demo: redirect to upload page with demo data pre-filled."""
    global _cached_deal_name
    _cached_deal_name = 'Demo Catalog'
    return redirect(url_for('upload_page', demo='1'))


def _find_first_file(directory):
    """Recursively find the first supported statement file in a directory tree."""
    if not directory or not os.path.isdir(directory):
        return None
    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for f in sorted(files):
            if f.startswith('~$'):
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext in _SUPPORTED_EXT:
                return os.path.join(root, f)
    return None


def _scan_file_structures(directory):
    """Scan all files in directory, group by unique column structure.

    Returns list of dicts sorted by group size (largest first):
      [{'fingerprint': str, 'files': [filename, ...], 'sample_path': str,
        'headers': [str, ...], 'header_row': int}, ...]
    """
    if not directory or not os.path.isdir(directory):
        return []

    # Collect all supported files
    all_files = []
    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for f in sorted(files):
            if f.startswith('~$'):
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext in _SUPPORTED_EXT:
                all_files.append((f, os.path.join(root, f)))

    if not all_files:
        return []

    # Group by fingerprint
    groups = {}  # fingerprint -> {files, sample_path, headers, header_row}
    for filename, filepath in all_files:
        try:
            detection = mapper.detect_headers(filepath)
            headers = detection['headers']
            header_row = detection['header_row']
            fp = mapper.compute_fingerprint(headers)

            if fp not in groups:
                groups[fp] = {
                    'fingerprint': fp,
                    'files': [],
                    'sample_path': filepath,
                    'headers': headers,
                    'header_row': header_row,
                }
            groups[fp]['files'].append(filename)
        except Exception as e:
            log.warning("Skipping unreadable file %s: %s", filename, e)
            continue

    # Sort by group size (largest first)
    structures = sorted(groups.values(), key=lambda g: len(g['files']), reverse=True)
    return structures


def _scan_file_structures_gcs(gcs_files):
    """Scan GCS files for column structures by downloading a few samples.

    Downloads up to MAX_SAMPLES files to detect unique column structures,
    then assigns remaining files to matching structures by extension.
    Returns same format as _scan_file_structures().
    """
    import tempfile
    if not gcs_files:
        return []

    MAX_SAMPLES = 5  # only download this many for header detection
    valid_files = []
    for entry in gcs_files:
        fname = entry.get('name', '')
        gcs_path = entry.get('gcs_path', '')
        if not fname or not gcs_path or fname.startswith('~$'):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext in _SUPPORTED_EXT:
            valid_files.append((fname, gcs_path, ext))

    if not valid_files:
        return []

    groups = {}  # fingerprint -> {files, sample_path, headers, header_row}
    sampled_exts = set()  # extensions we've already sampled
    tmp_files = []  # track temp files for cleanup

    for fname, gcs_path, ext in valid_files:
        # Only download samples for new extensions (most payors have one format)
        if ext in sampled_exts and len(groups) > 0:
            # Assign to the first group with matching extension
            for g in groups.values():
                g['files'].append(fname)
                break
            continue

        if len(tmp_files) >= MAX_SAMPLES:
            # Hit sample limit — assign remaining to largest group
            largest = max(groups.values(), key=lambda g: len(g['files']))
            largest['files'].append(fname)
            continue

        fd, tmp = tempfile.mkstemp(suffix=ext)
        os.close(fd)
        try:
            storage.download_to_file(gcs_path, tmp)
            detection = mapper.detect_headers(tmp)
            headers = detection['headers']
            header_row = detection['header_row']
            fp = mapper.compute_fingerprint(headers)

            if fp not in groups:
                groups[fp] = {
                    'fingerprint': fp,
                    'files': [],
                    'sample_path': tmp,
                    'headers': headers,
                    'header_row': header_row,
                }
                tmp_files.append(tmp)
            else:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            groups[fp]['files'].append(fname)
            sampled_exts.add(ext)
        except Exception as e:
            log.warning("GCS header scan failed for %s: %s", fname, e)
            try:
                os.remove(tmp)
            except OSError:
                pass

    structures = sorted(groups.values(), key=lambda g: len(g['files']), reverse=True)
    log.info("GCS structure scan: %d files → %d unique structure(s), sampled %d files",
             len(valid_files), len(structures), len(tmp_files))
    return structures


def _get_source_dir(payor):
    """Determine the source directory for a payor (uploaded files or local dir)."""
    payor_dir = payor.get('payor_dir', '')
    local_dir = payor.get('local_dir', '')
    if payor_dir and os.path.isdir(payor_dir) and os.listdir(payor_dir):
        return payor_dir
    if local_dir and os.path.isdir(local_dir):
        return local_dir
    return None


@app.route('/custom/upload', methods=['POST'])
def custom_upload():
    """Phase 2 entry: save files + configs to session, redirect to preview/0."""
    global _cached_deal_name
    try:
        _cached_deal_name = request.form.get('deal_name', '').strip()
        log.info("custom_upload: deal='%s'", _cached_deal_name)

        # Always start with a fresh session to avoid stale files from prior runs
        sid = str(uuid.uuid4())
        _ingest_sessions[sid] = {'custom_flow': {}}
        sess = _ingest_sessions[sid]['custom_flow']

        work_dir = os.path.join(CUSTOM_TEMP, sid)
        os.makedirs(work_dir, exist_ok=True)

        # Parse file_dates from hidden field
        file_dates = {}
        file_dates_raw = request.form.get('file_dates_json', '').strip()
        if file_dates_raw:
            try:
                file_dates = json.loads(file_dates_raw)
            except (json.JSONDecodeError, ValueError):
                pass

        # Parse payor configs and save files
        payors = []
        idx = 0
        while True:
            code = request.form.get(f'payor_code_{idx}')
            if code is None:
                break

            name = request.form.get(f'payor_name_{idx}', code)
            fmt = request.form.get(f'payor_fmt_{idx}', 'auto')
            fee_raw = request.form.get(f'payor_fee_{idx}', '').strip()
            fee = 0.0 if (not fee_raw or fee_raw.upper() == 'N/A') else float(fee_raw) / 100.0
            source_currency = request.form.get(f'payor_source_currency_{idx}', 'auto')
            statement_type = request.form.get(f'payor_stype_{idx}', 'masters')
            split_raw = request.form.get(f'payor_split_{idx}', '').strip()
            artist_split = float(split_raw) if split_raw else None
            territory = request.form.get(f'payor_territory_{idx}', '').strip() or None

            # Share calculation toggles
            calc_payable = request.form.get(f'payor_calc_payable_{idx}') is not None
            payable_pct_raw = request.form.get(f'payor_payable_pct_{idx}', '0').strip()
            payable_pct = float(payable_pct_raw) if payable_pct_raw else 0.0
            calc_third_party = request.form.get(f'payor_calc_third_party_{idx}') is not None
            tp_pct_raw = request.form.get(f'payor_third_party_pct_{idx}', '0').strip()
            third_party_pct = float(tp_pct_raw) if tp_pct_raw else 0.0

            # Save uploaded files
            payor_dir = os.path.join(work_dir, code)
            os.makedirs(payor_dir, exist_ok=True)
            files = request.files.getlist(f'payor_files_{idx}')
            saved_files = []
            for f in files:
                if f.filename:
                    safe_name = f.filename
                    fpath = os.path.join(payor_dir, safe_name)
                    f.save(fpath)
                    if safe_name.lower().endswith('.zip'):
                        try:
                            with zipfile.ZipFile(fpath, 'r') as zf:
                                zf.extractall(payor_dir)
                                saved_files.extend(
                                    n for n in zf.namelist() if not n.endswith('/')
                                )
                            os.remove(fpath)
                        except zipfile.BadZipFile:
                            saved_files.append(safe_name)
                    else:
                        saved_files.append(safe_name)

            # GCS files: store paths for streaming during processing (no download to /tmp)
            gcs_files_list = []
            gcs_json = request.form.get(f'gcs_files_{idx}', '').strip()
            if gcs_json:
                try:
                    gcs_files_list = json.loads(gcs_json)
                    saved_files.extend(e.get('name', '') for e in gcs_files_list)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

            # Also check local dir
            local_dir = request.form.get(f'payor_dir_{idx}', '').strip()

            payors.append({
                'code': code,
                'name': name,
                'fmt': fmt,
                'fee': fee,
                'source_currency': source_currency,
                'statement_type': statement_type,
                'artist_split': artist_split,
                'calc_payable': calc_payable,
                'payable_pct': payable_pct,
                'calc_third_party': calc_third_party,
                'third_party_pct': third_party_pct,
                'territory': territory,
                'payor_dir': payor_dir,
                'local_dir': local_dir,
                'saved_files': saved_files,
                'gcs_files': gcs_files_list,
            })

            idx += 1

        if not payors:
            flash('No payors configured.', 'error')
            return redirect(url_for('upload_page'))

        # Second pass: for any file without a date, try extracting from
        # filename and file contents now that files are saved to disk
        from consolidator import parse_period_from_filename, period_to_end_of_month, peek_statement_date
        for p in payors:
            # GCS-mode payors: detect periods from filenames only (no local files to peek)
            if p.get('gcs_files'):
                for entry in p['gcs_files']:
                    fn = entry.get('name', '')
                    if not fn or (fn in file_dates and file_dates[fn]):
                        continue
                    period = parse_period_from_filename(fn)
                    if period:
                        eom = period_to_end_of_month(period)
                        parts = eom.split('/')
                        file_dates[fn] = f"{parts[0]}/{parts[1]}/{parts[2][2:]}"
                continue

            src_dir = p['payor_dir'] if os.path.isdir(p['payor_dir']) else p.get('local_dir', '')
            if not src_dir or not os.path.isdir(src_dir):
                continue
            for root, _, fnames in os.walk(src_dir):
                for fn in fnames:
                    if fn in file_dates and file_dates[fn]:
                        continue
                    # Try filename
                    period = parse_period_from_filename(fn)
                    # Try parent folder names
                    if not period:
                        rel = os.path.relpath(os.path.join(root, fn), src_dir)
                        parts = os.path.normpath(rel).split(os.sep)
                        for part in reversed(parts[:-1]):
                            period = parse_period_from_filename(part)
                            if period:
                                break
                    # Try peeking inside file content
                    if not period:
                        try:
                            period = peek_statement_date(os.path.join(root, fn), fn)
                        except Exception as e:
                            log.debug("peek_statement_date failed for %s: %s", fn, e)
                    if period:
                        eom = period_to_end_of_month(period)
                        parts = eom.split('/')
                        file_dates[fn] = f"{parts[0]}/{parts[1]}/{parts[2][2:]}"

        sess['payors'] = payors
        sess['deal_name'] = _cached_deal_name
        sess['work_dir'] = work_dir
        sess['file_dates'] = file_dates
        sess['column_mappings'] = {}  # {payor_code: {filename: mapping_info}}
        sess['formulas'] = {}
        sess['cleaning'] = {}  # {payor_code: {remove_top, remove_bottom, sheet, header_row}}

        resp = redirect(url_for('custom_preview', payor_idx=0, struct_idx=0))
        resp.set_cookie('session_id', sid)
        return resp

    except Exception as e:
        log.error("custom_upload failed: %s", e, exc_info=True)
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('upload_page'))


@app.route('/custom/preview/<int:payor_idx>/<int:struct_idx>', methods=['GET', 'POST'])
@app.route('/custom/preview/<int:payor_idx>', methods=['GET', 'POST'], defaults={'struct_idx': 0})
def custom_preview(payor_idx, struct_idx):
    """Phase 2: Preview/clean step for one payor (per file structure)."""
    sess, sid = _get_custom_session()
    payors = sess.get('payors', [])
    if payor_idx >= len(payors):
        flash('Invalid payor index.', 'error')
        return redirect(url_for('upload_page'))

    payor = payors[payor_idx]
    source_dir = _get_source_dir(payor)

    # Scan file structures for this payor (cached in session)
    structures = sess.get('structures', {}).get(payor['code'])
    if structures is None:
        structures = []
        if source_dir:
            structures = _scan_file_structures(source_dir)
        elif payor.get('gcs_files') and storage.is_available():
            structures = _scan_file_structures_gcs(payor['gcs_files'])
        sess.setdefault('structures', {})[payor['code']] = structures

    struct_count = len(structures)
    if struct_idx >= struct_count and struct_count > 0:
        flash('Invalid structure index.', 'error')
        return redirect(url_for('upload_page'))

    current_struct = structures[struct_idx] if structures else None

    # --- Quick Ingest: check if this structure's fingerprint has a saved mapping ---
    if request.method == 'GET' and current_struct:
        saved_mapping = mapper.get_fingerprint_mapping(current_struct['headers'])
        if saved_mapping:
            fingerprint = current_struct['fingerprint']
            cleaning = sess.get('cleaning', {}).get(payor['code'], {}).get(fingerprint, {})
            file_mappings = sess.get('column_mappings', {}).get(payor['code'], {})
            for filename in current_struct['files']:
                file_mappings[filename] = {
                    'mapping': saved_mapping,
                    'remove_top': cleaning.get('remove_top', 0),
                    'remove_bottom': cleaning.get('remove_bottom', 0),
                    'header_row': current_struct.get('header_row', 0),
                    'sheet': cleaning.get('sheet'),
                    'keep_columns': [],
                }
            sess.setdefault('column_mappings', {})[payor['code']] = file_mappings
            mapper.increment_fingerprint_use(fingerprint)

            # Build audit entry: all mapped columns auto-accepted at confidence 1.0
            audit_columns = []
            for col in current_struct['headers']:
                canonical = saved_mapping.get(col, '')
                audit_columns.append({
                    'source_col': col,
                    'auto_proposed': canonical,
                    'auto_confidence': 1.0 if canonical else 0.0,
                    'final_mapping': canonical,
                    'decision': 'auto_accepted' if canonical else 'unmapped',
                })
            mapped_count = sum(1 for c in audit_columns if c['decision'] == 'auto_accepted')
            unmapped_count = sum(1 for c in audit_columns if c['decision'] == 'unmapped')
            audit_entry = {
                'fingerprint': fingerprint,
                'mapping_source': 'fingerprint',
                'columns': audit_columns,
                'summary': {
                    'total_columns': len(audit_columns),
                    'auto_accepted': mapped_count,
                    'user_corrected': 0,
                    'user_added': 0,
                    'unmapped': unmapped_count,
                },
            }
            sess.setdefault('audit_trail', {}).setdefault(payor['code'], {})[fingerprint] = audit_entry

            flash(f'Recognized format for {payor["name"]} — auto-applied saved mapping.', 'success')

            # Advance to next structure/payor/process
            if struct_idx + 1 < struct_count:
                return redirect(url_for('custom_preview', payor_idx=payor_idx, struct_idx=struct_idx + 1))
            elif payor_idx + 1 < len(payors):
                return redirect(url_for('custom_preview', payor_idx=payor_idx + 1, struct_idx=0))
            else:
                return redirect(url_for('custom_process'))

    if request.method == 'POST':
        action = request.form.get('action', 'clean')
        if action == 'skip':
            # Pre-Set payor: skip all structures for this payor
            if payor_idx + 1 < len(payors):
                return redirect(url_for('custom_preview', payor_idx=payor_idx + 1, struct_idx=0))
            else:
                return redirect(url_for('custom_process'))

        # Save cleaning params per structure fingerprint
        remove_top = int(request.form.get('remove_top', 0))
        remove_bottom = int(request.form.get('remove_bottom', 0))
        sheet = request.form.get('sheet', '')

        # Detect header row from the structure's sample file
        filepath = current_struct['sample_path'] if current_struct else None
        detected_header_row = 0
        if filepath:
            detection = mapper.detect_headers(filepath, sheet=sheet or None)
            detected_header_row = detection['header_row']

        fingerprint = current_struct['fingerprint'] if current_struct else 'default'
        sess.setdefault('cleaning', {}).setdefault(payor['code'], {})[fingerprint] = {
            'remove_top': remove_top,
            'remove_bottom': remove_bottom,
            'sheet': sheet,
            'header_row': detected_header_row,
        }

        return redirect(url_for('custom_map', payor_idx=payor_idx, struct_idx=struct_idx))

    # GET: Show preview using structure's sample file
    filepath = current_struct['sample_path'] if current_struct else None

    headers, preview_rows, total_rows, sheets, sheet = [], [], 0, None, None
    if filepath:
        fingerprint = current_struct['fingerprint'] if current_struct else 'default'
        cleaning = sess.get('cleaning', {}).get(payor['code'], {})
        # Try per-fingerprint cleaning first, fall back to legacy flat dict
        if isinstance(cleaning, dict) and fingerprint in cleaning:
            clean_params = cleaning[fingerprint]
        elif isinstance(cleaning, dict) and 'remove_top' in cleaning:
            clean_params = cleaning  # legacy flat format
        else:
            clean_params = {}

        detection = mapper.detect_headers(filepath, sheet=clean_params.get('sheet'))
        headers = detection['headers']
        sheets = detection.get('sheets')
        sheet = clean_params.get('sheet', '')

        clean_result = mapper.apply_cleaning(
            filepath,
            remove_top=clean_params.get('remove_top', 0),
            remove_bottom=clean_params.get('remove_bottom', 0),
            sheet=sheet or None,
            header_row=detection['header_row'],
        )
        headers = clean_result['headers']
        preview_rows = clean_result['preview_rows']
        total_rows = clean_result['total_rows']

    struct_files = current_struct['files'] if current_struct else []

    resp = render_template_string(
        DASHBOARD_HTML,
        page='upload',
        custom_step='preview',
        custom_payor_idx=payor_idx,
        custom_payor_count=len(payors),
        custom_payor_name=payor['name'],
        custom_is_preset=False,
        custom_headers=headers,
        custom_preview_rows=preview_rows,
        custom_total_rows=total_rows,
        custom_sheets=sheets,
        custom_sheet=sheet,
        custom_remove_top=clean_params.get('remove_top', 0) if filepath else 0,
        custom_remove_bottom=clean_params.get('remove_bottom', 0) if filepath else 0,
        custom_struct_idx=struct_idx,
        custom_struct_count=struct_count,
        custom_struct_files=struct_files,
        results=None, payor_names=[], payor_codes=[], default_payors=[],
        deal_name=sess.get('deal_name', ''),
    )
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/custom/map/<int:payor_idx>/<int:struct_idx>', methods=['GET', 'POST'])
@app.route('/custom/map/<int:payor_idx>', methods=['GET', 'POST'], defaults={'struct_idx': 0})
def custom_map(payor_idx, struct_idx):
    """Phase 2: Column mapping step for one payor (per file structure)."""
    sess, sid = _get_custom_session()
    payors = sess.get('payors', [])
    if payor_idx >= len(payors):
        flash('Invalid payor index.', 'error')
        return redirect(url_for('upload_page'))

    payor = payors[payor_idx]
    structures = sess.get('structures', {}).get(payor['code'], [])
    struct_count = len(structures)
    current_struct = structures[struct_idx] if struct_idx < struct_count else None

    if request.method == 'POST':
        # Save the mapping
        headers_json = request.form.get('headers_json', '[]')
        try:
            headers = json.loads(headers_json)
        except (json.JSONDecodeError, ValueError):
            headers = []

        col_mapping = {}
        keep_cols = []
        for i, h in enumerate(headers):
            mapped_to = request.form.get(f'map_{i}', '')
            if mapped_to:
                col_mapping[h] = mapped_to
            keep_val = request.form.get(f'keep_{i}')
            if keep_val:
                keep_cols.append(h)

        # Get cleaning params for this structure's fingerprint
        fingerprint = current_struct['fingerprint'] if current_struct else 'default'
        cleaning_by_fp = sess.get('cleaning', {}).get(payor['code'], {})
        if isinstance(cleaning_by_fp, dict) and fingerprint in cleaning_by_fp:
            cleaning = cleaning_by_fp[fingerprint]
        elif isinstance(cleaning_by_fp, dict) and 'remove_top' in cleaning_by_fp:
            cleaning = cleaning_by_fp  # legacy flat format
        else:
            cleaning = {}

        # Save mapping only for files in THIS structure's group
        file_mappings = sess.get('column_mappings', {}).get(payor['code'], {})
        if current_struct:
            for filename in current_struct['files']:
                file_mappings[filename] = {
                    'mapping': col_mapping,
                    'remove_top': cleaning.get('remove_top', 0),
                    'remove_bottom': cleaning.get('remove_bottom', 0),
                    'header_row': cleaning.get('header_row'),
                    'sheet': cleaning.get('sheet'),
                    'keep_columns': keep_cols,
                }
        else:
            # Fallback: apply to all files (legacy behavior)
            source_dir = _get_source_dir(payor)
            if source_dir:
                for root, dirs, files in os.walk(source_dir):
                    for f in files:
                        if f.startswith('~$'):
                            continue
                        ext = os.path.splitext(f)[1].lower()
                        if ext in ('.csv', '.xlsx', '.xls', '.xlsb', '.pdf'):
                            file_mappings[f] = {
                                'mapping': col_mapping,
                                'remove_top': cleaning.get('remove_top', 0),
                                'remove_bottom': cleaning.get('remove_bottom', 0),
                                'header_row': cleaning.get('header_row'),
                                'sheet': cleaning.get('sheet'),
                                'keep_columns': keep_cols,
                            }

        sess.setdefault('column_mappings', {})[payor['code']] = file_mappings

        # --- Audit Trail: capture proposals BEFORE saving to DB ---
        proposed = mapper.propose_mapping(headers)

        # Persist fingerprint mapping to DB so future runs can Quick Ingest
        if current_struct:
            fingerprint = current_struct['fingerprint']
            mapper.save_mapping(fingerprint, headers, col_mapping, source_label=payor['name'])
            mapper.save_synonyms(col_mapping)
        audit_columns = []
        for col in headers:
            prop = proposed.get(col, {})
            auto_canonical = prop.get('canonical', '')
            auto_conf = prop.get('confidence', 0.0)
            final = col_mapping.get(col, '')
            if not final and not auto_canonical:
                decision = 'unmapped'
            elif not auto_canonical and final:
                decision = 'user_added'
            elif auto_canonical == final:
                decision = 'auto_accepted'
            else:
                decision = 'user_corrected'
            audit_columns.append({
                'source_col': col,
                'auto_proposed': auto_canonical,
                'auto_confidence': auto_conf,
                'final_mapping': final,
                'decision': decision,
            })
        ac_count = sum(1 for c in audit_columns if c['decision'] == 'auto_accepted')
        uc_count = sum(1 for c in audit_columns if c['decision'] == 'user_corrected')
        ua_count = sum(1 for c in audit_columns if c['decision'] == 'user_added')
        um_count = sum(1 for c in audit_columns if c['decision'] == 'unmapped')
        audit_entry = {
            'fingerprint': current_struct['fingerprint'] if current_struct else 'default',
            'mapping_source': 'user',
            'columns': audit_columns,
            'summary': {
                'total_columns': len(audit_columns),
                'auto_accepted': ac_count,
                'user_corrected': uc_count,
                'user_added': ua_count,
                'unmapped': um_count,
            },
        }
        fp_key = current_struct['fingerprint'] if current_struct else 'default'
        sess.setdefault('audit_trail', {}).setdefault(payor['code'], {})[fp_key] = audit_entry

        # Next structure, next payor, or process
        if struct_idx + 1 < struct_count:
            return redirect(url_for('custom_preview', payor_idx=payor_idx, struct_idx=struct_idx + 1))
        elif payor_idx + 1 < len(payors):
            return redirect(url_for('custom_preview', payor_idx=payor_idx + 1, struct_idx=0))
        else:
            return redirect(url_for('custom_process'))

    # GET: Show mapping grid using structure's sample file
    filepath = current_struct['sample_path'] if current_struct else None
    if not filepath:
        source_dir = _get_source_dir(payor)
        filepath = _find_first_file(source_dir) if source_dir else None

    headers, preview_rows, proposed = [], [], {}
    if filepath:
        fingerprint = current_struct['fingerprint'] if current_struct else 'default'
        cleaning_by_fp = sess.get('cleaning', {}).get(payor['code'], {})
        if isinstance(cleaning_by_fp, dict) and fingerprint in cleaning_by_fp:
            cleaning = cleaning_by_fp[fingerprint]
        elif isinstance(cleaning_by_fp, dict) and 'remove_top' in cleaning_by_fp:
            cleaning = cleaning_by_fp
        else:
            cleaning = {}

        clean_result = mapper.apply_cleaning(
            filepath,
            remove_top=cleaning.get('remove_top', 0),
            remove_bottom=cleaning.get('remove_bottom', 0),
            sheet=cleaning.get('sheet') or None,
        )
        headers = clean_result['headers']
        preview_rows = clean_result['preview_rows']
        proposed = mapper.propose_mapping(headers)

    struct_files = current_struct['files'] if current_struct else []

    resp = render_template_string(
        DASHBOARD_HTML,
        page='upload',
        custom_step='map',
        custom_payor_idx=payor_idx,
        custom_payor_count=len(payors),
        custom_payor_name=payor['name'],
        custom_headers=headers,
        custom_preview_rows=preview_rows,
        custom_proposed=proposed,
        mapping_options=mapper.PHASE2_MAPPING_OPTIONS,
        custom_struct_idx=struct_idx,
        custom_struct_count=struct_count,
        custom_struct_files=struct_files,
        results=None, payor_names=[], payor_codes=[], default_payors=[],
        deal_name=sess.get('deal_name', ''),
    )
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/custom/process', methods=['GET', 'POST'])
def custom_process():
    """Phase 2: Parse all files with user mappings asynchronously."""
    try:
        sess, sid = _get_custom_session()

        # If already done, redirect to validation
        cp_status = sess.get('_cp_status', {})
        if cp_status.get('done') and not cp_status.get('error'):
            return redirect(url_for('custom_validate'))

        # If already running, show the loading page
        if cp_status.get('running'):
            return _render_processing_page(sid)

        # Start background processing
        # Count total files across all payors for ETA
        import time as _time
        total_files = 0
        for p in sess.get('payors', []):
            pdir = p.get('payor_dir', p.get('local_dir', ''))
            if os.path.isdir(pdir):
                for _r, _d, _f in os.walk(pdir):
                    total_files += sum(1 for fn in _f if os.path.splitext(fn)[1].lower() in ('.csv', '.xlsx', '.xls', '.xlsb', '.pdf'))

        sess['_cp_status'] = {
            'running': True, 'done': False, 'error': None,
            'progress': 'Starting...', 'started_at': _time.time(),
            'total_files': total_files, 'files_done': 0,
        }

        def _bg_process():
            try:
                _do_custom_process_bg(sess, sid)
                sess['_cp_status']['done'] = True
                sess['_cp_status']['running'] = False
                sess['_cp_status']['progress'] = 'Complete'
            except Exception as e:
                log.error("custom_process bg failed: %s", e, exc_info=True)
                sess['_cp_status']['done'] = True
                sess['_cp_status']['running'] = False
                sess['_cp_status']['error'] = str(e)

        t = threading.Thread(target=_bg_process, daemon=True)
        t.start()

        return _render_processing_page(sid)

    except Exception as e:
        log.error("custom_process failed: %s", e, exc_info=True)
        flash(f'Error processing files: {str(e)}', 'error')
        return redirect(url_for('upload_page'))


@app.route('/api/custom/process-status')
def custom_process_status():
    """Poll endpoint for custom_process background status."""
    try:
        import time as _time
        sess, sid = _get_custom_session()
        cp = sess.get('_cp_status', {})
        files_done = cp.get('files_done', 0)
        total_files = cp.get('total_files', 0)
        started_at = cp.get('started_at', 0)
        elapsed = _time.time() - started_at if started_at else 0

        # ETA calculation
        eta_seconds = None
        if files_done > 0 and total_files > files_done and elapsed > 0:
            per_file = elapsed / files_done
            eta_seconds = int(per_file * (total_files - files_done))

        return jsonify({
            'running': cp.get('running', False),
            'done': cp.get('done', False),
            'error': cp.get('error'),
            'progress': cp.get('progress', ''),
            'files_done': files_done,
            'total_files': total_files,
            'elapsed': int(elapsed),
            'eta_seconds': eta_seconds,
        })
    except Exception:
        return jsonify({'running': False, 'done': False, 'error': 'No session'})


def _render_processing_page(sid):
    """Return a simple loading page that polls process-status with progress bar and ETA."""
    html = """<!DOCTYPE html>
<html><head><title>Processing...</title>
<style>
  body { background:#0f1117; color:#e4e4e7; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; display:flex; justify-content:center; align-items:center; min-height:100vh; margin:0; }
  .box { text-align:center; width:400px; }
  .spinner { width:40px; height:40px; border:3px solid #27272a; border-top-color:#6366f1; border-radius:50%; animation:spin 0.8s linear infinite; margin:0 auto 20px; }
  @keyframes spin { to { transform:rotate(360deg); } }
  .msg { font-size:16px; color:#e4e4e7; font-weight:500; }
  .progress { font-size:13px; color:#6366f1; margin-top:8px; }
  .bar-wrap { background:#27272a; border-radius:6px; height:8px; margin-top:16px; overflow:hidden; }
  .bar-fill { height:100%; border-radius:6px; background:linear-gradient(90deg,#6366f1,#8b5cf6); transition:width 0.5s ease; width:0%; }
  .stats { display:flex; justify-content:space-between; margin-top:8px; font-size:12px; color:#71717a; }
  .eta { font-size:14px; color:#a1a1aa; margin-top:12px; }
</style></head><body>
<div class="box">
  <div class="spinner"></div>
  <div class="msg">Processing statements...</div>
  <div class="progress" id="prog">Starting...</div>
  <div class="bar-wrap"><div class="bar-fill" id="bar"></div></div>
  <div class="stats"><span id="counter"></span><span id="elapsed"></span></div>
  <div class="eta" id="eta"></div>
</div>
<script>
function fmtTime(s) {
  if (!s || s < 0) return '';
  if (s < 60) return s + 's';
  var m = Math.floor(s/60), sec = s%60;
  return m + 'm ' + (sec < 10 ? '0' : '') + sec + 's';
}
(function poll() {
  fetch('/api/custom/process-status')
    .then(r => r.json())
    .then(d => {
      document.getElementById('prog').textContent = d.progress || '';
      var pct = 0;
      if (d.total_files > 0) {
        pct = Math.min(100, Math.round(d.files_done / d.total_files * 100));
        document.getElementById('counter').textContent = d.files_done + ' / ' + d.total_files + ' files';
      }
      document.getElementById('bar').style.width = pct + '%';
      if (d.elapsed > 0) document.getElementById('elapsed').textContent = fmtTime(d.elapsed) + ' elapsed';
      if (d.eta_seconds != null) {
        document.getElementById('eta').textContent = '~' + fmtTime(d.eta_seconds) + ' remaining';
      } else if (d.files_done === 0 && d.running) {
        document.getElementById('eta').textContent = 'Estimating...';
      }
      if (d.done) {
        document.getElementById('bar').style.width = '100%';
        if (d.error) {
          document.querySelector('.msg').textContent = 'Error';
          document.getElementById('prog').textContent = d.error;
          document.querySelector('.spinner').style.display = 'none';
          document.getElementById('eta').textContent = '';
        } else {
          document.querySelector('.msg').textContent = 'Done!';
          document.getElementById('eta').textContent = 'Redirecting...';
          setTimeout(function(){ window.location.href = '/custom/validate'; }, 500);
        }
      } else {
        setTimeout(poll, 2000);
      }
    })
    .catch(() => setTimeout(poll, 3000));
})();
</script></body></html>"""
    r = make_response(html)
    r.set_cookie('session_id', sid)
    return r


def _do_custom_process_bg(sess, sid):
    """Actual processing logic (runs in background thread)."""
    payors = sess.get('payors', [])
    log.info("custom_process: %d payor(s)", len(payors))
    _log_memory()
    file_dates = sess.get('file_dates', {})
    column_mappings_by_payor = sess.get('column_mappings', {})

    sess['_cp_status']['progress'] = f'Building configs for {len(payors)} payor(s)...'

    # Build PayorConfig objects
    configs = []
    for p in payors:
        payor_dir = p['payor_dir']
        local_dir = p.get('local_dir', '')
        statements_dir = payor_dir if os.path.isdir(payor_dir) and os.listdir(payor_dir) else local_dir

        # Merge: copy any files from local_dir missing in payor_dir
        if local_dir and os.path.isdir(local_dir) and os.path.isdir(payor_dir):
            existing = {f.lower() for f in os.listdir(payor_dir)}
            for root, dirs, files in os.walk(local_dir):
                for f in files:
                    if f.startswith('~$'):
                        continue
                    ext = os.path.splitext(f)[1].lower()
                    if ext in _SUPPORTED_EXT and f.lower() not in existing:
                        try:
                            shutil.copy2(os.path.join(root, f), os.path.join(payor_dir, f))
                            existing.add(f.lower())
                            log.info("  Copied missing file from local_dir: %s", f)
                        except Exception as e:
                            log.warning("Failed to copy %s: %s", f, e)

        gcs_files = p.get('gcs_files') or None
        has_local = statements_dir and os.path.isdir(statements_dir)

        if not has_local and not gcs_files:
            continue

        cfg = PayorConfig(
            code=p['code'],
            name=p['name'],
            statements_dir=statements_dir or '',
            fmt=p['fmt'],
            fee=p['fee'],
            source_currency=p.get('source_currency', p.get('fx_currency', 'auto')),
            statement_type=p['statement_type'],
            artist_split=p.get('artist_split'),
            calc_payable=p.get('calc_payable', False),
            payable_pct=p.get('payable_pct', 0.0),
            calc_third_party=p.get('calc_third_party', False),
            third_party_pct=p.get('third_party_pct', 0.0),
            territory=p.get('territory'),
            gcs_files=gcs_files,
        )
        configs.append(cfg)

    if not configs:
        raise RuntimeError('No valid payor configurations.')

    sess['_cp_status']['progress'] = f'Processing {len(configs)} payor(s)...'

    def _file_progress(files_done, current_file):
        sess['_cp_status']['files_done'] = files_done
        sess['_cp_status']['progress'] = f'Parsing {current_file}...'

    payor_results = load_all_payors(
        configs,
        file_dates=file_dates,
        column_mappings_by_payor=column_mappings_by_payor if column_mappings_by_payor else None,
        progress_cb=_file_progress,
    )
    sess['payor_results_keys'] = list(payor_results.keys())

    sess['_cp_status']['progress'] = 'Computing analytics & storing results...'

    # Store results temporarily (in-memory for this session)
    sess_data = _ingest_sessions.get(sid, {})
    sess_data['_phase2_results'] = payor_results
    sess_data['_phase2_configs'] = configs

    # Collect all file paths for validation
    all_file_paths = []
    for cfg in configs:
        if os.path.isdir(cfg.statements_dir):
            for root, dirs, files in os.walk(cfg.statements_dir):
                for f in files:
                    all_file_paths.append(os.path.join(root, f))
    sess['all_file_paths'] = all_file_paths


@app.route('/custom/validate', methods=['GET', 'POST'])
def custom_validate():
    """Phase 2: Show validation issues."""
    sess, sid = _get_custom_session()
    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})

    if request.method == 'POST':
        action = request.form.get('action', 'continue')
        if action == 'continue':
            return redirect(url_for('custom_calc'))
        elif action == 'remove_rerun':
            # TODO: implement remove flagged and re-run
            flash('Flagged items removed. Re-processing...', 'info')
            return redirect(url_for('custom_process'))

    # Run validation
    all_file_paths = sess.get('all_file_paths', [])
    validation_result = validator.run_validation(payor_results, file_paths=all_file_paths)

    resp = render_template_string(
        DASHBOARD_HTML,
        page='upload',
        custom_step='validate',
        validation_result=validation_result,
        results=None, payor_names=[], payor_codes=[], default_payors=[],
        deal_name=sess.get('deal_name', ''),
    )
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/custom/calc', methods=['GET', 'POST'])
def custom_calc():
    """Phase 2: Earnings waterfall auto-calc and formula entry."""
    sess, sid = _get_custom_session()
    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})

    if request.method == 'POST':
        # Collect formulas from form
        formulas = {}
        for key, val in request.form.items():
            if key.startswith('formula_') and val.strip():
                field_name = key[len('formula_'):]
                formulas[field_name] = val.strip()
        sess['formulas'] = formulas
        return redirect(url_for('custom_enrich'))

    # Detect truly present waterfall fields from raw source data (before fabrication)
    # Scan raw pr.detail columns to find what actually came from the source
    source_fields = set()
    raw_to_canonical = [
        ('gross', 'Gross Earnings'), ('fees', 'Fees'), ('net', 'Net Receipts'),
    ]
    for code, pr in payor_results.items():
        d = pr.detail
        for src_col, canonical in raw_to_canonical:
            if src_col in d.columns and pd.to_numeric(d[src_col], errors='coerce').fillna(0).abs().sum() > 0:
                source_fields.add(canonical)

    if payor_results:
        # Determine derivable fields using WATERFALL_RELATIONSHIPS
        derivable = set()
        for target, func, required in formula_engine.WATERFALL_RELATIONSHIPS:
            if target not in source_fields and required.issubset(source_fields | derivable):
                derivable.add(target)
        # Iterate once more to catch transitive derivations
        for target, func, required in formula_engine.WATERFALL_RELATIONSHIPS:
            if target not in source_fields and target not in derivable and required.issubset(source_fields | derivable):
                derivable.add(target)

        # Build field status list for template
        waterfall_fields = []
        for field in formula_engine.WATERFALL_FIELDS:
            if field in source_fields:
                status = 'present'
            elif field in derivable:
                status = 'auto_calc'
            else:
                status = 'needs_formula'
            waterfall_fields.append({
                'name': field,
                'status': status,
                'formula': sess.get('formulas', {}).get(field, ''),
            })
    else:
        waterfall_fields = [{'name': f, 'status': 'needs_formula', 'formula': ''} for f in formula_engine.WATERFALL_FIELDS]

    resp = render_template_string(
        DASHBOARD_HTML,
        page='upload',
        custom_step='calc',
        waterfall_fields=waterfall_fields,
        results=None, payor_names=[], payor_codes=[], default_payors=[],
        deal_name=sess.get('deal_name', ''),
    )
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/custom/enrich', methods=['GET', 'POST'])
def custom_enrich():
    """Phase 3: Release date enrichment step."""
    log.info("custom_enrich: method=%s", request.method)
    sess, sid = _get_custom_session()
    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})

    if request.method == 'POST':
        action = request.form.get('action', 'enrich')

        if action == 'skip':
            # Skip enrichment, go to export
            return redirect(url_for('custom_export'))

        if action == 'continue':
            # Enrichment is done, go to export
            return redirect(url_for('custom_export'))

        if action == 'enrich':
            # Guard: don't restart if already running or done
            existing_status = sess.get('enrichment_status', {})
            if existing_status.get('running'):
                return redirect(url_for('custom_enrich'))
            if existing_status.get('done'):
                return redirect(url_for('custom_enrich'))

            # Start enrichment in background — keys from env/session, toggles from form
            genius_token = sess.get('genius_token') or os.getenv('GENIUS_TOKEN', '')
            gemini_key = sess.get('gemini_key') or os.getenv('GEMINI_API_KEY', '')
            # Respect user's toggle choices
            if not request.form.get('use_genius'):
                genius_token = ''
            if not request.form.get('use_gemini'):
                gemini_key = ''
            sess['genius_token'] = genius_token
            sess['gemini_key'] = gemini_key

            # Build combined detail DF from all payors
            from consolidator import _build_detail_23col
            all_details = []
            for code, pr in payor_results.items():
                detail_23 = _build_detail_23col(pr, deal_name=sess.get('deal_name', ''),
                                                 formulas=sess.get('formulas'))
                all_details.append(detail_23)

            if not all_details:
                flash('No data to enrich.', 'error')
                return redirect(url_for('custom_export'))

            combined_detail = pd.concat(all_details, ignore_index=True)

            # Count unique ISRCs for ETA estimate (~1.5s per track for MusicBrainz)
            unique_isrcs = set()
            for _, row in combined_detail.iterrows():
                isrc = str(row.get('ISRC', '')).strip()
                title = str(row.get('Title', '')).strip()
                artist = str(row.get('Artist', '')).strip()
                if isrc and isrc not in ('', 'nan', 'None'):
                    unique_isrcs.add(isrc.upper())
                elif title and artist:
                    unique_isrcs.add(f"{title}::{artist}")
            estimated_seconds = int(len(unique_isrcs) * 1.5)

            # Initialize enrichment status
            import time as _etime
            sess['enrichment_status'] = {
                'running': True, 'done': False, 'error': None,
                'phase': 'starting', 'current': 0, 'total': 0, 'message': 'Starting...',
                '_started_at': _etime.monotonic(),
                'eta_seconds': estimated_seconds, 'need_lookup': len(unique_isrcs),
            }

            def _enrichment_thread():
                try:
                    def _progress(info):
                        sess['enrichment_status'].update(info)
                        # Update need_lookup when enrichment engine reports actual count after cache check
                        if 'need_lookup' in info:
                            sess['enrichment_status']['need_lookup'] = info['need_lookup']
                            sess['enrichment_status']['eta_seconds'] = int(info['need_lookup'] * 1.5)
                        # Compute live ETA from elapsed time and processed count
                        elapsed = info.get('elapsed', 0)
                        processed = info.get('processed_count', 0)
                        need = sess['enrichment_status'].get('need_lookup', 0)
                        if processed > 0 and need > 0:
                            rate = elapsed / processed
                            remaining = max(0, int(rate * (need - processed)))
                            sess['enrichment_status']['eta_seconds'] = remaining
                        elif info.get('phase') == 'done':
                            sess['enrichment_status']['eta_seconds'] = 0

                    result = enrichment.enrich_release_dates(
                        combined_detail,
                        genius_token=genius_token,
                        gemini_api_key=gemini_key,
                        progress_callback=_progress,
                    )
                    sess['enrichment_result'] = {
                        'lookups': result.lookups,
                        'stats': result.stats,
                        'tracks_without_dates': result.tracks_without_dates,
                    }
                    sess['enrichment_status'] = {
                        'running': False, 'done': True, 'error': None,
                        'phase': 'done', 'current': result.stats.get('total', 0),
                        'total': result.stats.get('total', 0),
                        'message': 'Enrichment complete!',
                        'eta_seconds': 0,
                    }
                except Exception as e:
                    log.error("Enrichment thread failed: %s", e, exc_info=True)
                    sess['enrichment_status'] = {
                        'running': False, 'done': False, 'error': str(e),
                        'phase': 'error', 'current': 0, 'total': 0, 'message': str(e),
                    }

            t = threading.Thread(target=_enrichment_thread, daemon=True)
            t.start()

            # Re-render with running state
            resp = render_template_string(
                DASHBOARD_HTML,
                page='upload',
                custom_step='enrich',
                enrichment_running=True,
                enrichment_done=False,
                results=None, payor_names=[], payor_codes=[], default_payors=[],
                deal_name=sess.get('deal_name', ''),
            )
            r = make_response(resp)
            r.set_cookie('session_id', sid)
            return r

    # GET: Check enrichment state
    enrich_status = sess.get('enrichment_status', {})
    enrich_result = sess.get('enrichment_result')
    enrichment_running = enrich_status.get('running', False)
    enrichment_done = enrich_status.get('done', False)

    # Check API key availability (don't expose actual keys to template)
    genius_token = sess.get('genius_token') or os.getenv('GENIUS_TOKEN', '')
    gemini_key = sess.get('gemini_key') or os.getenv('GEMINI_API_KEY', '')

    template_vars = {
        'page': 'upload',
        'custom_step': 'enrich',
        'enrichment_running': enrichment_running,
        'enrichment_done': enrichment_done,
        'enrichment_stats': enrich_result.get('stats', {}) if enrich_result else {},
        'enrichment_no_dates': enrich_result.get('tracks_without_dates', []) if enrich_result else [],
        'genius_available': bool(genius_token),
        'gemini_available': bool(gemini_key),
        'results': None, 'payor_names': [], 'payor_codes': [], 'default_payors': [],
        'deal_name': sess.get('deal_name', ''),
    }

    resp = render_template_string(DASHBOARD_HTML, **template_vars)
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/custom/export', methods=['GET', 'POST'])
def custom_export():
    """Phase 3: Export options step."""
    sess, sid = _get_custom_session()
    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})

    if request.method == 'POST':
        export_options = {
            'combined_csv': bool(request.form.get('combined_csv')),
            'per_payor_csv': bool(request.form.get('per_payor_csv')),
            'combined_excel': bool(request.form.get('combined_excel')),
            'per_payor_excel': bool(request.form.get('per_payor_excel')),
            'aggregate': bool(request.form.get('aggregate')),
            'aggregate_by': request.form.getlist('aggregate_by'),
        }
        sess['export_options'] = export_options
        return redirect(url_for('custom_finalize'))

    payor_names = [pr.config.name for pr in payor_results.values()]

    # Determine which canonical group-by fields have actual mapped data
    # Map raw detail columns to canonical aggregation field names
    raw_to_agg = [
        ('statement_date', 'Statement Date'), ('identifier', 'ISRC'),
        ('title', 'Title'), ('artist', 'Artist'), ('store', 'Source'),
        ('media_type', 'Media Type'), ('country', 'Territory'),
    ]
    # Payor and Royalty Type are always available (set by config, not mapping)
    available_agg_fields = ['Statement Date', 'Royalty Type', 'Payor']
    for code, pr in payor_results.items():
        d = pr.detail
        for raw_col, canon in raw_to_agg:
            if canon not in available_agg_fields and raw_col in d.columns:
                vals = d[raw_col].astype(str).str.strip()
                if vals.ne('').any() and vals.ne('nan').any() and vals.ne('None').any():
                    available_agg_fields.append(canon)
    # Default checked: all available non-type fields
    default_agg_checked = ['Statement Date', 'Payor', 'ISRC', 'Title', 'Artist']

    resp = render_template_string(
        DASHBOARD_HTML,
        page='upload',
        custom_step='export',
        export_payor_names=payor_names,
        available_agg_fields=available_agg_fields,
        default_agg_checked=default_agg_checked,
        results=None, payor_names=[], payor_codes=[], default_payors=[],
        deal_name=sess.get('deal_name', ''),
    )
    r = make_response(resp)
    r.set_cookie('session_id', sid)
    return r


@app.route('/api/enrichment-status')
def api_enrichment_status():
    """AJAX: Return enrichment progress for polling."""
    import time as _etime
    sess, sid = _get_custom_session()
    status = sess.get('enrichment_status', {})
    # Detect stale enrichment (thread died or instance replaced)
    if status.get('running') and status.get('_started_at'):
        age = _etime.monotonic() - status['_started_at']
        if age > _STALE_TIMEOUT:
            status = {'running': False, 'done': False,
                      'error': 'Enrichment timed out. The background task may have crashed. You can skip this step.',
                      'phase': 'error', 'current': 0, 'total': 0, 'message': 'Timed out'}
            sess['enrichment_status'] = status
    return jsonify({
        'running': status.get('running', False),
        'done': status.get('done', False),
        'error': status.get('error'),
        'phase': status.get('phase', ''),
        'current': status.get('current', 0),
        'total': status.get('total', 0),
        'message': status.get('message', ''),
        'eta_seconds': status.get('eta_seconds', 0),
        'need_lookup': status.get('need_lookup', 0),
    })


@app.route('/custom/finalize', methods=['GET', 'POST'])
def custom_finalize():
    """Phase 2/3: Launch background consolidation thread with all mappings+formulas+enrichment."""
    global _cached_deal_name, _processing_status
    log.info("custom_finalize: method=%s", request.method)
    sess, sid = _get_custom_session()
    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})
    configs = sess_data.get('_phase2_configs', [])
    formulas = sess.get('formulas', {})
    deal_name = sess.get('deal_name', '') or _cached_deal_name

    if not payor_results:
        flash('No processed data found. Please start over.', 'error')
        return redirect(url_for('upload_page'))

    with _state_lock:
        if _processing_status.get('running'):
            flash('A consolidation is already running.', 'error')
            return redirect(url_for('index'))
        _set_processing_status(running=True, progress='Finalizing...', done=False, error=None)

    _cached_deal_name = deal_name

    # Get enrichment and export options from session
    enrich_result = sess.get('enrichment_result')
    export_options = sess.get('export_options', {
        'combined_csv': True, 'per_payor_csv': True,
        'combined_excel': True, 'per_payor_excel': False,
    })
    enrichment_stats = enrich_result.get('stats') if enrich_result else None

    def _finalize_thread():
        global _cached_results, _cached_analytics, _processing_status
        try:
            with _state_lock:
                _set_processing_status(progress='Applying enrichment...')

            # Apply enrichment to raw PayorResult.detail DataFrames
            if enrich_result and enrich_result.get('lookups'):
                lookups = enrich_result['lookups']
                for code, pr in payor_results.items():
                    pr.detail = apply_enrichment_to_raw_detail(pr.detail, lookups)

            with _state_lock:
                _set_processing_status(progress='Writing output files...')
                _cached_results = payor_results

            # Determine output dir
            if deal_name:
                deal_slug = re.sub(r'[^A-Za-z0-9_-]', '_', deal_name).upper()
                deal_dir = os.path.join(DEALS_DIR, deal_slug)
                os.makedirs(deal_dir, exist_ok=True)
                exports_dir = os.path.join(deal_dir, 'exports')
                os.makedirs(exports_dir, exist_ok=True)
                xlsx_path = os.path.join(exports_dir, f'{deal_slug}_consolidated.xlsx')
                csv_path = os.path.join(exports_dir, f'{deal_slug}_consolidated.csv')
            else:
                work_dir = sess.get('work_dir', CUSTOM_TEMP)
                xlsx_path = os.path.join(work_dir, 'consolidated.xlsx')
                csv_path = os.path.join(work_dir, 'consolidated.csv')
                exports_dir = work_dir

            per_payor_paths = {}
            agg_by = export_options.get('aggregate_by') if export_options.get('aggregate') else None

            # Write outputs based on export options
            if export_options.get('combined_excel', True):
                write_consolidated_excel(payor_results, xlsx_path, deal_name=deal_name, formulas=formulas or None, aggregate_by=agg_by)
            if export_options.get('combined_csv', True):
                write_consolidated_csv(payor_results, csv_path, deal_name=deal_name, formulas=formulas or None, aggregate_by=agg_by)
            if export_options.get('per_payor_excel', False):
                per_payor_paths = write_per_payor_exports(payor_results, exports_dir, deal_name=deal_name, formulas=formulas or None, aggregate_by=agg_by)
            if export_options.get('per_payor_csv', True):
                pp_csv_dir = os.path.join(exports_dir, 'per_payor_csv')
                pp_csv_paths = write_per_payor_csv_exports(payor_results, pp_csv_dir, deal_name=deal_name, formulas=formulas or None, aggregate_by=agg_by)
                per_payor_paths.update(pp_csv_paths)

            analytics = compute_analytics(payor_results, formulas=formulas or None,
                                          enrichment_stats=enrichment_stats)

            # --- Inject audit trail into analytics for dashboard rendering ---
            audit_trail = sess.get('audit_trail', {})
            if audit_trail:
                # Compute cross-payor audit summary
                total_files = 0
                auto_ingested = 0
                manually_mapped = 0
                total_cols_mapped = 0
                auto_accepted_cols = 0
                user_corrected_cols = 0
                # Build payor-name lookup from payor_results
                payor_name_map = {pr.config.code: pr.config.name for pr in payor_results.values()}
                per_payor_rows = []
                for payor_code, fp_entries in audit_trail.items():
                    p_files = 0
                    p_auto = 0
                    p_corrected = 0
                    p_conf_sum = 0.0
                    p_conf_count = 0
                    for fp, entry in fp_entries.items():
                        total_files += 1
                        p_files += 1
                        src = entry.get('mapping_source', 'user')
                        if src == 'fingerprint':
                            auto_ingested += 1
                        else:
                            manually_mapped += 1
                        s = entry.get('summary', {})
                        total_cols_mapped += s.get('auto_accepted', 0) + s.get('user_corrected', 0) + s.get('user_added', 0)
                        auto_accepted_cols += s.get('auto_accepted', 0)
                        user_corrected_cols += s.get('user_corrected', 0)
                        p_auto += s.get('auto_accepted', 0)
                        p_corrected += s.get('user_corrected', 0)
                        for col_entry in entry.get('columns', []):
                            if col_entry.get('auto_confidence', 0) > 0:
                                p_conf_sum += col_entry['auto_confidence']
                                p_conf_count += 1
                    per_payor_rows.append({
                        'name': payor_name_map.get(payor_code, payor_code),
                        'files': p_files,
                        'auto_mapped': p_auto,
                        'user_corrected': p_corrected,
                        'avg_confidence': round(p_conf_sum / p_conf_count, 2) if p_conf_count else 0.0,
                    })
                analytics['audit_trail'] = audit_trail
                analytics['audit_summary'] = {
                    'total_files': total_files,
                    'auto_ingested_files': auto_ingested,
                    'manually_mapped_files': manually_mapped,
                    'total_columns_mapped': total_cols_mapped,
                    'auto_accepted_columns': auto_accepted_cols,
                    'user_corrected_columns': user_corrected_cols,
                    'per_payor': per_payor_rows,
                }

            with _state_lock:
                _cached_analytics = analytics
                if export_options.get('combined_excel', True):
                    app.config['CONSOLIDATED_PATH'] = xlsx_path
                if export_options.get('combined_csv', True):
                    app.config['CONSOLIDATED_CSV_PATH'] = csv_path
                app.config['PER_PAYOR_PATHS'] = per_payor_paths

            # Save deal if named
            if deal_name:
                deal_meta = {
                    'name': deal_name,
                    'slug': deal_slug,
                    'timestamp': datetime.now().isoformat(),
                    'payor_codes': [pr.config.code for pr in payor_results.values()],
                    'payor_names': [pr.config.name for pr in payor_results.values()],
                    'total_gross': analytics.get('total_gross', '0'),
                    'isrc_count': analytics.get('isrc_count', '0'),
                    'total_files': analytics.get('total_files', 0),
                    'currency_symbol': analytics.get('currency_symbol', '$'),
                    'ltm_gross': analytics.get('ltm_gross_total_fmt', '0'),
                    'ltm_net': analytics.get('ltm_net_total_fmt', '0'),
                }
                # Include audit trail in deal metadata
                if audit_trail:
                    deal_meta['audit_trail'] = audit_trail
                    deal_meta['audit_summary'] = analytics.get('audit_summary', {})
                with open(os.path.join(deal_dir, 'deal_meta.json'), 'w') as f:
                    json.dump(deal_meta, f, indent=2)
                with open(os.path.join(deal_dir, 'analytics.json'), 'w') as f:
                    json.dump(analytics, f, indent=2, default=str)
                with open(os.path.join(deal_dir, 'payor_results.pkl'), 'wb') as f:
                    pickle.dump(payor_results, f)

            with _state_lock:
                _set_processing_status(running=False, progress='Done!', done=True, error=None)

        except Exception as e:
            log.error("Finalize thread failed: %s", e, exc_info=True)
            with _state_lock:
                _set_processing_status(running=False, progress='', done=False, error=str(e))

    t = threading.Thread(target=_finalize_thread, daemon=True)
    t.start()

    # Clean up session
    _clear_custom_session()

    # Render a processing page with loading overlay (instead of redirect)
    # This ensures the user sees the spinner and polls for completion.
    return render_template_string(
        DASHBOARD_HTML,
        page='dashboard',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name,
        processing={'running': True, 'progress': 'Finalizing...', 'done': False, 'error': None},
    )


# ---------------------------------------------------------------------------
# Phase 2: API endpoints
# ---------------------------------------------------------------------------

@app.route('/api/custom/preview', methods=['POST'])
def api_custom_preview():
    """AJAX: Live cleaning preview (returns headers + rows after row removal)."""
    data = request.get_json(silent=True) or {}
    sess, sid = _get_custom_session()
    payors = sess.get('payors', [])
    payor_idx = data.get('payor_idx', 0)
    struct_idx = data.get('struct_idx', 0)

    if payor_idx >= len(payors):
        return jsonify({'error': 'Invalid payor index'})

    payor = payors[payor_idx]

    # Use structure's sample file if available
    structures = sess.get('structures', {}).get(payor['code'], [])
    if structures and struct_idx < len(structures):
        filepath = structures[struct_idx]['sample_path']
    else:
        source_dir = _get_source_dir(payor)
        filepath = _find_first_file(source_dir) if source_dir else None

    if not filepath:
        return jsonify({'error': 'No files found', 'headers': [], 'rows': [], 'total_rows': 0})

    remove_top = data.get('remove_top', 0)
    remove_bottom = data.get('remove_bottom', 0)
    sheet = data.get('sheet', '') or None

    result = mapper.apply_cleaning(filepath, remove_top=remove_top,
                                   remove_bottom=remove_bottom, sheet=sheet)
    return jsonify({
        'headers': result['headers'],
        'rows': result['preview_rows'],
        'total_rows': result['total_rows'],
    })


@app.route('/api/custom/formula-preview', methods=['POST'])
def api_custom_formula_preview():
    """AJAX: Formula computation preview (first 10 rows)."""
    data = request.get_json(silent=True) or {}
    formulas = data.get('formulas', {})

    if not formulas:
        return jsonify({'error': 'No formulas provided'})

    sess_data, _ = _get_ingest_session()
    payor_results = sess_data.get('_phase2_results', {})

    if not payor_results:
        return jsonify({'error': 'No processed data available'})

    # Build combined detail for preview
    all_details = []
    for code, pr in payor_results.items():
        from consolidator import _build_detail_23col
        detail_23 = _build_detail_23col(pr)
        all_details.append(detail_23)

    if not all_details:
        return jsonify({'error': 'No detail data'})

    combined = pd.concat(all_details, ignore_index=True)
    result = formula_engine.preview_formulas(combined, formulas, n_rows=10)
    return jsonify(result)


@app.route('/api/custom/validate-formula', methods=['POST'])
def api_custom_validate_formula():
    """AJAX: Single formula validation."""
    data = request.get_json(silent=True) or {}
    formula_str = data.get('formula', '')

    available = formula_engine.WATERFALL_FIELDS + formula_engine.PERCENT_FIELDS
    result = formula_engine.validate_formula(formula_str, available)
    return jsonify(result)


@app.route('/refresh')
def refresh():
    """Re-run consolidation with default payors."""
    try:
        payor_results, analytics, consolidated_path = run_consolidation(DEFAULT_PAYORS)
        if payor_results:
            flash(f'Refreshed: {analytics["total_files"]} files, {analytics["isrc_count"]} ISRCs.', 'success')
        else:
            flash('No data found.', 'error')
    except Exception as e:
        log.error("Refresh failed: %s", e, exc_info=True)
        flash(f'Error: {str(e)}', 'error')
    return redirect(url_for('index'))


@app.route('/download/<filetype>')
def download(filetype):
    if filetype == 'consolidated':
        path = app.config.get('CONSOLIDATED_PATH')
    elif filetype == 'csv':
        path = app.config.get('CONSOLIDATED_CSV_PATH')
    elif filetype == 'model':
        path = app.config.get('MODEL_PATH')
    else:
        return 'Not found', 404

    if path and os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    return 'File not found. Run consolidation first.', 404


@app.route('/download/payor/<code>')
def download_payor(code):
    """Download a single payor's consolidated export."""
    per_payor = app.config.get('PER_PAYOR_PATHS', {})
    path = per_payor.get(code)
    if path and os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    return 'File not found. Run consolidation first.', 404


@app.route('/download/contract/<code>')
def download_contract(code):
    """Download the contract PDF for a specific payor."""
    if _cached_results:
        for payor_code, pr in _cached_results.items():
            if payor_code == code and pr.config.contract_pdf_path:
                path = pr.config.contract_pdf_path
                if os.path.exists(path):
                    return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    return 'Contract not found.', 404


_exchange_rate_cache = {'rates': None, 'ts': 0}

@app.route('/api/exchange-rates')
def api_exchange_rates():
    """Proxy exchange rates (USD-based) with 1-hour cache."""
    import time, urllib.request, urllib.error
    now = time.time()
    if _exchange_rate_cache['rates'] and now - _exchange_rate_cache['ts'] < 3600:
        return jsonify(_exchange_rate_cache['rates'])

    urls = [
        'https://open.er-api.com/v6/latest/USD',
        'https://api.frankfurter.app/latest?from=USD',
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'RoyaltyConsolidator/1.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode())
            rates = data.get('rates', {})
            if rates:
                result = {'base': 'USD', 'rates': rates}
                _exchange_rate_cache['rates'] = result
                _exchange_rate_cache['ts'] = now
                return jsonify(result)
        except Exception as e:
            log.warning('Exchange rate fetch failed (%s): %s', url, e)
            continue
    return jsonify({'error': 'Could not fetch exchange rates'}), 502


@app.route('/api/analytics')
def api_analytics():
    """Return analytics as JSON (for future AJAX refresh)."""
    with _state_lock:
        if _cached_analytics:
            return jsonify(dict(_cached_analytics))
    return jsonify({'error': 'No data loaded'}), 404


@app.route('/api/status')
def api_status():
    """Return background processing status as JSON."""
    import time as _time
    with _state_lock:
        status = dict(_processing_status)
        # Detect stale/dead background threads (e.g. Cloud Run instance replaced)
        if status.get('running') and status.get('_updated_at'):
            age = _time.monotonic() - status['_updated_at']
            if age > _STALE_TIMEOUT:
                _processing_status.update({'running': False, 'done': True,
                    'error': 'Processing timed out. The background task may have crashed. Please try again.'})
                status = dict(_processing_status)
        status.pop('_updated_at', None)
        return jsonify(status)


@app.route('/api/list-dir-files', methods=['POST'])
def api_list_dir_files():
    """List statement files from local directory paths.
    POST JSON: {dirs: [str, ...]}
    Returns JSON: {files: [filename, ...]}
    """
    data = request.get_json(silent=True) or {}
    dirs = data.get('dirs', [])
    files = []
    for d in dirs:
        if d and os.path.isdir(d):
            for root, _, fnames in os.walk(d):
                for fn in sorted(fnames):
                    ext = os.path.splitext(fn)[1].lower()
                    if ext in ('.csv', '.xlsx', '.xls', '.xlsb') and not fn.startswith('~$'):
                        files.append(fn)
    return jsonify({'files': files})


@app.route('/api/upload-urls', methods=['POST'])
def api_upload_urls():
    """Return resumable GCS upload session URLs for large files.
    POST JSON: {files: [{name, size, payor_idx, content_type}]}
    Returns JSON: {urls: [{name, payor_idx, upload_url, gcs_path}]}
    """
    if not storage.is_available():
        return jsonify({'error': 'GCS not configured'}), 503
    data = request.get_json(silent=True) or {}
    files = data.get('files', [])
    origin = request.headers.get('Origin', request.host_url.rstrip('/'))
    batch_id = str(uuid.uuid4())
    urls = []
    for f in files:
        fname = f.get('name', 'file')
        payor_idx = f.get('payor_idx', 0)
        ct = f.get('content_type', 'application/octet-stream')
        gcs_path = f"tmp_uploads/{batch_id}/{payor_idx}/{fname}"
        try:
            session_info = storage.create_upload_session(gcs_path, content_type=ct, origin=origin)
            urls.append({
                'name': fname,
                'payor_idx': payor_idx,
                'upload_url': session_info['upload_url'],
                'gcs_path': session_info['gcs_path'],
            })
        except Exception as e:
            log.error("create_upload_session failed for %s: %s", fname, e)
            return jsonify({'error': f'Failed to create upload session for {fname}: {e}'}), 500
    return jsonify({'urls': urls})


@app.route('/api/extract-dates', methods=['POST'])
def api_extract_dates():
    """Extract statement dates from filenames and file contents.
    POST JSON: {filenames: [str, ...], dirs: [str, ...] (optional)}
    Returns JSON: {dates: {filename: "MM/DD/YY" or ""}, sources: {filename: "filename"|"content"|""}}
    """
    from consolidator import parse_period_from_filename, period_to_end_of_month, peek_statement_date
    data = request.get_json(silent=True) or {}
    filenames = data.get('filenames', [])
    dirs = data.get('dirs', [])
    dates = {}
    sources = {}

    # Build a lookup of filename -> full filepath for content peeking
    file_paths = {}
    for d in dirs:
        if d and os.path.isdir(d):
            for root, _, fnames in os.walk(d):
                for fn in fnames:
                    file_paths[fn] = os.path.join(root, fn)

    for fn in filenames:
        # Try filename first
        period = parse_period_from_filename(fn)
        if period:
            eom = period_to_end_of_month(period)
            parts = eom.split('/')
            dates[fn] = f"{parts[0]}/{parts[1]}/{parts[2][2:]}"
            sources[fn] = 'filename'
            continue

        # Try peeking inside file content
        fpath = file_paths.get(fn)
        if fpath:
            period = peek_statement_date(fpath, fn)
            if period:
                eom = period_to_end_of_month(period)
                parts = eom.split('/')
                dates[fn] = f"{parts[0]}/{parts[1]}/{parts[2][2:]}"
                sources[fn] = 'content'
                continue

        dates[fn] = ''
        sources[fn] = ''
    return jsonify({'dates': dates, 'sources': sources})


@app.route('/api/analyze-contract', methods=['POST'])
def api_analyze_contract():
    """Analyze one or more contract PDFs using Gemini to extract key deal terms.
    Accepts multiple files via 'contract_pdfs' field.
    Returns JSON: {license_term, matching_right, assignment_language, distro_fee, split_pct, summary}
    """
    contract_files = request.files.getlist('contract_pdfs')
    if not contract_files or not any(f.filename for f in contract_files):
        return jsonify({'error': 'No PDFs uploaded'}), 400

    # Check for Gemini API key
    api_key = request.form.get('gemini_key', '').strip() or _gemini_api_key
    if not api_key:
        return jsonify({'error': 'Gemini API key required. Set GEMINI_API_KEY env var or provide in settings.'}), 400

    try:
        import tempfile as _tf
        tmp_paths = []
        uploaded_files = []

        _genai = _get_genai()
        _client = _genai.Client(api_key=api_key)

        # Save and upload each PDF
        for cf in contract_files:
            if not cf.filename or not cf.filename.lower().endswith('.pdf'):
                continue
            tmp = _tf.NamedTemporaryFile(delete=False, suffix='.pdf')
            cf.save(tmp.name)
            tmp.close()
            tmp_paths.append(tmp.name)
            uploaded_files.append(_client.files.upload(file=tmp.name))

        if not uploaded_files:
            return jsonify({'error': 'No valid PDF files found'}), 400
        n_docs = len(uploaded_files)
        prompt = f"""Analyze {'this music industry contract' if n_docs == 1 else 'these ' + str(n_docs) + ' music industry contract documents together as parts of the same deal'} and extract the following deal terms.
Return a single JSON object with exactly these keys (use null if not found):

{{
  "license_term": "string describing the license/contract term, e.g. '3 years', 'Life of copyright', '5 years with auto-renewal'",
  "matching_right": true/false or null if not mentioned,
  "assignment_language": true/false - whether the contract contains assignment/transfer of rights language (vs pure license),
  "distro_fee": number - the distribution fee percentage (e.g. 15 for 15%), or null,
  "split_pct": number - the artist/label split percentage for the party receiving statements (e.g. 50 for 50/50), or null,
  "deal_type": "artist" or "label" - whose perspective the deal is from based on who the contracting party is,
  "summary": "2-3 sentence plain English summary of the key commercial terms{' across all documents' if n_docs > 1 else ''}"
}}

Only return valid JSON, no markdown fences or extra text."""

        response = _client.models.generate_content(
            model='gemini-2.0-flash',
            contents=[*uploaded_files, prompt],
        )

        # Clean up temp files
        for p in tmp_paths:
            try:
                os.unlink(p)
            except OSError:
                pass

        # Parse JSON from response
        text = response.text.strip()
        # Strip markdown fences if present
        if text.startswith('```'):
            text = text.split('\n', 1)[1] if '\n' in text else text[3:]
        if text.endswith('```'):
            text = text[:-3]
        if text.startswith('json'):
            text = text[4:]
        result = json.loads(text.strip())
        return jsonify(result)

    except json.JSONDecodeError:
        return jsonify({'error': 'Could not parse Gemini response', 'raw': response.text[:500]}), 500
    except Exception as e:
        log.error("Contract analysis failed: %s", e, exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/deals')
def deals_page():
    """List all saved deals."""
    deals = list_deals()
    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='deals',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        deals=deals,
    )


@app.route('/deals/<slug>/load')
def load_deal_route(slug):
    """Load a saved deal and redirect to dashboard."""
    global _cached_results, _cached_analytics, _cached_deal_name
    try:
        deal_name, payor_results, analytics, xlsx_path, csv_path, per_payor_paths = load_deal(slug)
        with _state_lock:
            _cached_results = payor_results
            _cached_analytics = analytics
            _cached_deal_name = deal_name
        app.config['CONSOLIDATED_PATH'] = xlsx_path
        app.config['CONSOLIDATED_CSV_PATH'] = csv_path
        app.config['PER_PAYOR_PATHS'] = per_payor_paths
        flash(f'Loaded deal "{deal_name}".', 'success')
    except Exception as e:
        log.error("load_deal_route failed for %s: %s", slug, e, exc_info=True)
        flash(f'Error loading deal: {str(e)}', 'error')
        return redirect(url_for('deals_page'))
    return redirect(url_for('index'))


@app.route('/deals/<slug>/delete', methods=['POST'])
def delete_deal_route(slug):
    """Delete a saved deal from DB + GCS + local disk."""
    deleted_any = False
    # Delete from PostgreSQL (CASCADE removes all child rows)
    if db.is_available():
        try:
            if db.delete_deal_from_db(slug):
                deleted_any = True
        except Exception as e:
            log.error("DB delete failed for %s: %s", slug, e)
    # Delete GCS files
    if storage.is_available():
        try:
            storage.delete_deal_files(slug)
        except Exception as e:
            log.error("GCS cleanup failed for %s: %s", slug, e)
    # Delete local files
    deal_dir = os.path.join(DEALS_DIR, slug)
    if os.path.isdir(deal_dir):
        shutil.rmtree(deal_dir)
        deleted_any = True
    if deleted_any:
        flash(f'Deal "{slug}" deleted.', 'success')
    else:
        flash(f'Deal "{slug}" not found.', 'error')
    return redirect(url_for('deals_page'))


@app.route('/deals/<slug>/edit')
def edit_deal_route(slug):
    """Show edit form for a saved deal."""
    try:
        deal_name, payor_results, analytics, xlsx_path, csv_path, per_payor_paths = load_deal(slug)
        # Build payor config dicts for the template
        edit_payors = []
        for code, pr in payor_results.items():
            c = pr.config
            edit_payors.append({
                'code': c.code,
                'name': c.name,
                'fmt': c.fmt,
                'fee': c.fee,
                'source_currency': getattr(c, 'source_currency', getattr(c, 'fx_currency', 'USD')),
                'statement_type': c.statement_type,
                'deal_type': getattr(c, 'deal_type', 'artist'),
                'artist_split': c.artist_split,
                'territory': c.territory,
                'expected_start': c.expected_start,
                'expected_end': c.expected_end,
                'statements_dir': c.statements_dir,
                'file_count': pr.file_count,
            })
        with _state_lock:
            deal_name_copy = _cached_deal_name
        return render_template_string(
            DASHBOARD_HTML,
            page='edit_deal',
            results=None,
            payor_names=[],
            payor_codes=[],
            default_payors=[],
            deal_name=deal_name_copy,
            edit_deal_name=deal_name,
            edit_slug=slug,
            edit_payors=edit_payors,
            edit_analytics=analytics,
        )
    except Exception as e:
        log.error("edit_deal GET failed for %s: %s", slug, e, exc_info=True)
        flash(f'Error loading deal for editing: {str(e)}', 'error')
        return redirect(url_for('deals_page'))


@app.route('/deals/<slug>/edit', methods=['POST'])
def edit_deal_post(slug):
    log.info("edit_deal_post: slug=%s", slug)
    """Handle edit deal form submission."""
    global _cached_results, _cached_analytics, _cached_deal_name, _processing_status
    try:
        action = request.form.get('action', 'rerun')
        new_deal_name = request.form.get('deal_name', '').strip()
        if not new_deal_name:
            flash('Deal name is required.', 'error')
            return redirect(url_for('edit_deal_route', slug=slug))

        # Load existing deal to get current data
        old_deal_name, old_payor_results, old_analytics, old_xlsx, old_csv, old_pp = load_deal(slug)

        # Parse payor configs from form
        payor_configs = []
        existing_dirs = {}  # map code -> existing statements_dir
        for code, pr in old_payor_results.items():
            existing_dirs[code] = pr.config.statements_dir

        idx = 0
        while True:
            code = request.form.get(f'payor_code_{idx}')
            if code is None:
                break

            name = request.form.get(f'payor_name_{idx}', code)
            fmt = request.form.get(f'payor_fmt_{idx}', 'auto')
            fee_raw = request.form.get(f'payor_fee_{idx}', '').strip()
            fee = 0.0 if (not fee_raw or fee_raw.upper() == 'N/A') else float(fee_raw) / 100.0
            source_currency = request.form.get(f'payor_source_currency_{idx}', 'auto')
            statement_type = request.form.get(f'payor_stype_{idx}', 'masters')

            # Share calculation toggles
            calc_payable = request.form.get(f'payor_calc_payable_{idx}') is not None
            payable_pct_raw = request.form.get(f'payor_payable_pct_{idx}', '0').strip()
            payable_pct = float(payable_pct_raw) if payable_pct_raw else 0.0
            calc_third_party = request.form.get(f'payor_calc_third_party_{idx}') is not None
            tp_pct_raw = request.form.get(f'payor_third_party_pct_{idx}', '0').strip()
            third_party_pct = float(tp_pct_raw) if tp_pct_raw else 0.0

            deal_type = request.form.get(f'payor_deal_type_{idx}', 'artist').strip()
            split_raw = request.form.get(f'payor_split_{idx}', '').strip()
            artist_split = float(split_raw) if split_raw else None
            territory = request.form.get(f'payor_territory_{idx}', '').strip() or None

            # Preserve existing contract summary & path from saved deal
            contract_summary = None
            contract_pdf_path = None
            if code.strip() in old_payor_results:
                old_cfg = old_payor_results[code.strip()].config
                contract_summary = getattr(old_cfg, 'contract_summary', None)
                contract_pdf_path = getattr(old_cfg, 'contract_pdf_path', None)

            period_start_raw = request.form.get(f'payor_period_start_{idx}', '').strip()
            expected_start = int(period_start_raw) if period_start_raw and period_start_raw.isdigit() and len(period_start_raw) == 6 else None
            period_end_raw = request.form.get(f'payor_period_end_{idx}', '').strip()
            expected_end = int(period_end_raw) if period_end_raw and period_end_raw.isdigit() and len(period_end_raw) == 6 else None

            # Determine statements directory: reuse existing if available
            payor_dir = existing_dirs.get(code.strip())
            if not payor_dir or not os.path.isdir(payor_dir):
                # For new payors, create a dir inside the deal
                payor_dir = os.path.join(DEALS_DIR, slug, f'statements_{code.strip()}')
                os.makedirs(payor_dir, exist_ok=True)

            # Handle new file uploads (append to existing dir)
            files = request.files.getlist(f'payor_files_{idx}')
            for f in files:
                if not f.filename:
                    continue
                if f.filename.endswith('.zip'):
                    zip_path = os.path.join(payor_dir, f.filename)
                    f.save(zip_path)
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        zf.extractall(payor_dir)
                else:
                    f.save(os.path.join(payor_dir, f.filename))

            # GCS files: store paths for streaming (no download to /tmp)
            gcs_files_list = None
            gcs_json = request.form.get(f'gcs_files_{idx}', '').strip()
            if gcs_json:
                try:
                    gcs_files_list = json.loads(gcs_json) or None
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

            payor_configs.append(PayorConfig(
                code=code.strip(),
                name=name.strip(),
                fmt=fmt,
                fee=fee,
                source_currency=source_currency,
                statements_dir=payor_dir,
                statement_type=statement_type,
                deal_type=deal_type,
                artist_split=artist_split,
                calc_payable=calc_payable,
                payable_pct=payable_pct,
                calc_third_party=calc_third_party,
                third_party_pct=third_party_pct,
                territory=territory,
                contract_pdf_path=contract_pdf_path,
                contract_summary=contract_summary,
                expected_start=expected_start,
                expected_end=expected_end,
                gcs_files=gcs_files_list,
            ))
            idx += 1

        if not payor_configs:
            flash('No payors configured. Add at least one payor.', 'error')
            return redirect(url_for('edit_deal_route', slug=slug))

        new_slug = _make_slug(new_deal_name)

        if action == 'rerun':
            # Full re-run consolidation
            with _state_lock:
                if _processing_status.get('running'):
                    flash('A consolidation is already running.', 'error')
                    return redirect(url_for('edit_deal_route', slug=slug))
                _cached_deal_name = new_deal_name
                _processing_status = {'running': True, 'progress': 'Starting...', 'done': False, 'error': None}
            t = threading.Thread(
                target=_run_in_background,
                args=(payor_configs,),
                kwargs={'output_dir': None, 'deal_name': new_deal_name},
                daemon=True,
            )
            t.start()
            flash(f'Re-running consolidation for "{new_deal_name}"...', 'success')
            return redirect(url_for('index'))

        else:
            # Save config only — update payor configs in pickled results without re-running
            updated_results = {}
            for pc in payor_configs:
                if pc.code in old_payor_results:
                    pr = old_payor_results[pc.code]
                    pr.config = pc
                    updated_results[pc.code] = pr
                else:
                    # New payor added without re-run — skip (no data to attach)
                    pass

            # Recompute analytics if we still have data
            if updated_results:
                analytics = compute_analytics(updated_results)
            else:
                analytics = old_analytics

            # Build export paths from existing deal
            exports_dir = os.path.join(DEALS_DIR, new_slug, 'exports')
            xlsx_path = os.path.join(exports_dir, 'Consolidated_All_Payors.xlsx')
            csv_path = os.path.join(exports_dir, 'Consolidated_All_Payors.csv')
            pp_paths = {}
            pp_dir = os.path.join(exports_dir, 'per_payor')
            if os.path.isdir(pp_dir):
                for fname in os.listdir(pp_dir):
                    for code in updated_results.keys():
                        if code in fname:
                            pp_paths[code] = os.path.join(pp_dir, fname)
                            break

            # If slug changed, move the deal directory
            if new_slug != slug:
                old_dir = os.path.join(DEALS_DIR, slug)
                new_dir = os.path.join(DEALS_DIR, new_slug)
                if os.path.isdir(old_dir):
                    shutil.move(old_dir, new_dir)

            save_deal(new_slug, new_deal_name, updated_results, analytics,
                      xlsx_path, csv_path, pp_paths)
            flash(f'Deal "{new_deal_name}" config updated.', 'success')
            return redirect(url_for('deals_page'))

    except Exception as e:
        log.error("edit_deal POST failed for %s: %s", slug, e, exc_info=True)
        flash(f'Error editing deal: {str(e)}', 'error')
        return redirect(url_for('deals_page'))


# ---------------------------------------------------------------------------
# C1: Quick Re-run
# ---------------------------------------------------------------------------

@app.route('/deals/<slug>/rerun-quick', methods=['POST'])
def rerun_quick_route(slug):
    """One-click re-run: load deal config, re-run consolidation with same PayorConfigs."""
    global _cached_deal_name, _processing_status
    try:
        deal_name, payor_results, analytics, _, _, _ = load_deal(slug)
        payor_configs = [pr.config for pr in payor_results.values()]

        # On Cloud Run, payor_results is empty (no pickle). Reconstruct from DB.
        if not payor_configs and db.is_available():
            try:
                pc_dicts = db.load_payor_configs_from_db(slug)
                for p in pc_dicts:
                    cfg = PayorConfig(
                        code=p['code'],
                        name=p['name'],
                        statements_dir='',
                        fmt=p['fmt'],
                        fee=p['fee'],
                        source_currency=p.get('source_currency', 'auto'),
                        statement_type=p['statement_type'],
                        artist_split=p.get('artist_split'),
                        calc_payable=p.get('calc_payable', False),
                        payable_pct=p.get('payable_pct', 0.0),
                        calc_third_party=p.get('calc_third_party', False),
                        third_party_pct=p.get('third_party_pct', 0.0),
                        territory=p.get('territory'),
                        gcs_files=p.get('gcs_files'),
                    )
                    payor_configs.append(cfg)
                log.info("Reconstructed %d PayorConfigs from DB for rerun of %s", len(payor_configs), slug)
            except Exception as e:
                log.error("Failed to load payor configs from DB for %s: %s", slug, e)

        if not payor_configs:
            flash('No payor configs found for rerun. Upload files first.', 'error')
            return redirect(url_for('deals_page'))

        # Verify GCS files exist (at least spot-check first payor)
        gcs_mode = any(c.gcs_files for c in payor_configs)
        if gcs_mode:
            try:
                first_gcs = next((c for c in payor_configs if c.gcs_files), None)
                if first_gcs and first_gcs.gcs_files:
                    sample = first_gcs.gcs_files[0]
                    if not storage.blob_exists(sample.get('gcs_path', '')):
                        flash('GCS files expired. Please re-upload.', 'error')
                        return redirect(url_for('deals_page'))
            except Exception:
                pass  # If check fails, still attempt rerun

        with _state_lock:
            if _processing_status.get('running'):
                flash('A consolidation is already running.', 'error')
                return redirect(url_for('deals_page'))
            _cached_deal_name = deal_name
            _processing_status = {'running': True, 'progress': 'Starting quick re-run...', 'done': False, 'error': None}

        # Snapshot analytics before re-run for delta report
        deal_dir = os.path.join(DEALS_DIR, slug)
        delta_engine.snapshot_analytics(deal_dir)

        t = threading.Thread(
            target=_run_in_background_with_delta,
            args=(payor_configs, slug, deal_name),
            daemon=True,
        )
        t.start()
        flash(f'Quick re-running "{deal_name}"...', 'success')
        return redirect(url_for('index'))
    except Exception as e:
        log.error("rerun_quick failed for %s: %s", slug, e, exc_info=True)
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('deals_page'))


def _run_in_background_with_delta(payor_configs, slug, deal_name):
    """Background worker that runs consolidation then computes delta."""
    global _processing_status
    log.info("Quick re-run started: slug=%s, deal=%s", slug, deal_name)
    _log_memory()
    with _state_lock:
        _processing_status = {'running': True, 'progress': 'Re-running consolidation...', 'done': False, 'error': None}
    try:
        payor_results, analytics, consolidated_path = run_consolidation(
            payor_configs, deal_name=deal_name)

        if not payor_results:
            with _state_lock:
                _processing_status.update({'running': False, 'done': True, 'error': 'No data found.'})
            return

        # Compute delta report if previous analytics exist
        deal_dir = os.path.join(DEALS_DIR, slug)
        prev_path = os.path.join(deal_dir, 'analytics_prev.json')
        if os.path.isfile(prev_path):
            try:
                with open(prev_path, 'r') as f:
                    old_analytics = json.load(f)
                report = delta_engine.compute_delta(old_analytics, analytics, slug)
                delta_engine.save_delta_to_disk(deal_dir, report)
                # Save to DB if available
                if db.is_available():
                    deal_id = db.get_deal_id_by_slug(slug)
                    if deal_id:
                        db.save_delta_report(deal_id, report.to_dict())
                log.info("Delta report generated: %s", report.summary)
            except Exception as e:
                log.warning("Delta report failed: %s", e)

        with _state_lock:
            _processing_status.update({
                'running': False,
                'progress': f'Done: {analytics["total_files"]} files, {analytics["isrc_count"]} ISRCs.',
                'done': True, 'error': None,
            })
    except Exception as e:
        log.error("Quick re-run FAILED: %s", e, exc_info=True)
        with _state_lock:
            _processing_status.update({'running': False, 'done': True, 'error': str(e)})


# ---------------------------------------------------------------------------
# C2: Deal Templates
# ---------------------------------------------------------------------------

@app.route('/deals/<slug>/save-template', methods=['POST'])
def save_template_route(slug):
    """Save a deal's payor config as a named template."""
    try:
        deal_name, payor_results, _, _, _, _ = load_deal(slug)
        template_name = request.form.get('template_name', '').strip()
        if not template_name:
            template_name = f"Template from {deal_name}"

        payor_configs = []
        for code, pr in payor_results.items():
            c = pr.config
            payor_configs.append({
                'code': c.code,
                'name': c.name,
                'fmt': c.fmt,
                'fee': c.fee,
                'source_currency': getattr(c, 'source_currency', 'USD'),
                'statement_type': c.statement_type,
                'deal_type': getattr(c, 'deal_type', 'artist'),
                'artist_split': c.artist_split,
                'territory': c.territory,
                'calc_payable': getattr(c, 'calc_payable', False),
                'payable_pct': getattr(c, 'payable_pct', 0),
                'calc_third_party': getattr(c, 'calc_third_party', False),
                'third_party_pct': getattr(c, 'third_party_pct', 0),
            })

        settings = {'source_deal': deal_name, 'source_slug': slug}

        # Save to DB
        if db.is_available():
            db.save_template(template_name, payor_configs, settings)

        # Also save to local JSON
        templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates_data')
        os.makedirs(templates_dir, exist_ok=True)
        tpl_path = os.path.join(templates_dir, f"{_make_slug(template_name)}.json")
        with open(tpl_path, 'w') as f:
            json.dump({'name': template_name, 'payor_configs': payor_configs, 'settings': settings}, f, indent=2)

        flash(f'Template "{template_name}" saved.', 'success')
    except Exception as e:
        log.error("save_template failed: %s", e, exc_info=True)
        flash(f'Error saving template: {str(e)}', 'error')
    return redirect(url_for('deals_page'))


@app.route('/templates')
def templates_page():
    """List all saved deal templates."""
    templates = []
    # Load from DB
    if db.is_available():
        try:
            templates = db.list_templates()
        except Exception:
            pass
    # Fallback: load from local JSON
    if not templates:
        templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates_data')
        if os.path.isdir(templates_dir):
            for fname in sorted(os.listdir(templates_dir)):
                if fname.endswith('.json'):
                    try:
                        with open(os.path.join(templates_dir, fname), 'r') as f:
                            tpl = json.load(f)
                        templates.append(tpl)
                    except Exception:
                        pass
    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='templates',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        templates=templates,
    )


@app.route('/templates/<name>/apply', methods=['POST'])
def apply_template_route(name):
    """Apply a template to pre-populate the upload form."""
    template = None
    if db.is_available():
        try:
            template = db.get_template(name)
        except Exception:
            pass
    if not template:
        tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'templates_data', f"{_make_slug(name)}.json")
        if os.path.isfile(tpl_path):
            with open(tpl_path, 'r') as f:
                template = json.load(f)
    if not template:
        flash(f'Template "{name}" not found.', 'error')
        return redirect(url_for('templates_page'))

    # Store template config in session-like flash data
    flash(json.dumps(template.get('payor_configs', [])), 'template_config')
    flash(f'Template "{name}" applied. Configure upload directories below.', 'success')
    return redirect(url_for('upload_page'))


@app.route('/templates/<name>/delete', methods=['POST'])
def delete_template_route(name):
    """Delete a deal template."""
    deleted = False
    if db.is_available():
        try:
            deleted = db.delete_template(name)
        except Exception:
            pass
    tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'templates_data', f"{_make_slug(name)}.json")
    if os.path.isfile(tpl_path):
        os.remove(tpl_path)
        deleted = True
    if deleted:
        flash(f'Template "{name}" deleted.', 'success')
    else:
        flash(f'Template "{name}" not found.', 'error')
    return redirect(url_for('templates_page'))


# ---------------------------------------------------------------------------
# C3: Delta Reports
# ---------------------------------------------------------------------------

@app.route('/deals/<slug>/delta')
def delta_report_page(slug):
    """Show the delta report for a deal after re-run."""
    deal_dir = os.path.join(DEALS_DIR, slug)
    report = delta_engine.load_delta_from_disk(deal_dir, slug)
    if not report:
        flash('No delta report found. Run a re-run first.', 'error')
        return redirect(url_for('deals_page'))
    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='delta',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        delta_report=report.to_dict(),
        delta_slug=slug,
    )


# ---------------------------------------------------------------------------
# D2: Forecast Routes
# ---------------------------------------------------------------------------

@app.route('/api/parse-sofr-excel', methods=['POST'])
def api_parse_sofr_excel():
    """Parse SOFR forward curve from uploaded Excel file."""
    f = request.files.get('sofr_file')
    if not f or not f.filename:
        return jsonify({'error': 'No file uploaded'}), 400

    import tempfile
    tmp = None
    try:
        suffix = os.path.splitext(f.filename)[1] or '.xlsx'
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        f.save(tmp.name)
        tmp.close()
        curve = forecast_engine.parse_sofr_from_excel(tmp.name)
        return jsonify({'curve': curve, 'count': len(curve)})
    except Exception as e:
        log.error("parse_sofr_from_excel failed: %s", e, exc_info=True)
        return jsonify({'error': str(e)}), 400
    finally:
        if tmp and os.path.isfile(tmp.name):
            os.unlink(tmp.name)


FORECAST_BETA_PASSWORD = os.getenv('FORECAST_BETA_PASSWORD', 'virtu2025')

_FORECAST_GATE_HTML = '''<!DOCTYPE html>
<html><head><title>Forecast — Beta Access</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f172a; color: #e2e8f0; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
.gate-card { background: #1e293b; border-radius: 12px; padding: 40px; max-width: 380px; width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,0.3); }
.gate-card h2 { margin: 0 0 8px; font-size: 20px; }
.gate-card p { color: #94a3b8; font-size: 13px; margin: 0 0 24px; }
.gate-card input[type=password] { width: 100%; padding: 10px 12px; border-radius: 6px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 14px; box-sizing: border-box; margin-bottom: 12px; }
.gate-card button { width: 100%; padding: 10px; border-radius: 6px; border: none; background: #22d3ee; color: #0f172a; font-weight: 600; font-size: 14px; cursor: pointer; }
.gate-card button:hover { background: #06b6d4; }
.gate-error { color: #f87171; font-size: 12px; margin-bottom: 12px; }
.gate-card a { color: #94a3b8; font-size: 12px; text-decoration: none; display: block; text-align: center; margin-top: 16px; }
</style></head><body>
<div class="gate-card">
    <h2>Forecast Beta</h2>
    <p>This feature is in beta testing. Enter the access password to continue.</p>
    {% if error %}<div class="gate-error">{{ error }}</div>{% endif %}
    <form method="POST">
        <input type="password" name="beta_password" placeholder="Password" autofocus>
        <button type="submit">Unlock</button>
    </form>
    <a href="/deals">Back to Deals</a>
</div></body></html>'''


def _check_forecast_gate(slug):
    """Check if forecast beta gate is unlocked. Returns None if OK, or a Response to return."""
    if session.get('forecast_unlocked'):
        return None
    if request.method == 'POST' and 'beta_password' in request.form:
        if request.form['beta_password'] == FORECAST_BETA_PASSWORD:
            session['forecast_unlocked'] = True
            return redirect(url_for('forecast_page', slug=slug))
        return render_template_string(_FORECAST_GATE_HTML, error='Incorrect password.')
    return render_template_string(_FORECAST_GATE_HTML, error=None)


@app.route('/deals/<slug>/forecast', methods=['GET', 'POST'])
def forecast_page(slug):
    """Show forecast configuration form (GET) or handle beta gate password (POST without forecast fields)."""
    # Beta gate check
    if not session.get('forecast_unlocked'):
        if request.method == 'POST' and 'beta_password' in request.form:
            if request.form['beta_password'] == FORECAST_BETA_PASSWORD:
                session['forecast_unlocked'] = True
                return redirect(url_for('forecast_page', slug=slug))
            return render_template_string(_FORECAST_GATE_HTML, error='Incorrect password.')
        if request.method == 'GET' or (request.method == 'POST' and 'beta_password' not in request.form):
            return render_template_string(_FORECAST_GATE_HTML, error=None)

    # POST with forecast form data → delegate to forecast_run
    if request.method == 'POST' and 'genre_default' in request.form:
        return forecast_run(slug)

    try:
        deal_name, payor_results, analytics, _, _, _ = load_deal(slug)
        with _state_lock:
            deal_name_copy = _cached_deal_name
        return render_template_string(
            DASHBOARD_HTML,
            page='forecast',
            results=analytics,
            payor_names=[pr.config.name for pr in payor_results.values()],
            payor_codes=list(payor_results.keys()),
            default_payors=[],
            deal_name=deal_name_copy,
            forecast_slug=slug,
            forecast_deal_name=deal_name,
            forecast_result=None,
            genre_choices=forecast_engine.GENRE_CHOICES,
        )
    except Exception as e:
        log.error("forecast_page failed for %s: %s", slug, e, exc_info=True)
        flash(f'Error loading deal for forecast: {str(e)}', 'error')
        return redirect(url_for('deals_page'))


def forecast_run(slug):
    """Run forecast projection (called from forecast_page POST handler)."""
    try:
        deal_name, payor_results, analytics, _, _, _ = load_deal(slug)

        # Parse config from form
        purchase_price = float(request.form.get('purchase_price', 0) or 0)
        exit_multiple = float(request.form.get('exit_multiple', 15) or 15)
        ltv = float(request.form.get('ltv', 55) or 55) / 100
        sofr_rate = float(request.form.get('sofr_rate', 4.5) or 4.5) / 100
        sofr_floor = float(request.form.get('sofr_floor', 2.0) or 2.0) / 100
        sofr_spread = float(request.form.get('sofr_spread', 275) or 275) / 10000
        cash_flow_sweep = float(request.form.get('cash_flow_sweep', 100) or 100) / 100
        synergy_ramp_months = int(request.form.get('synergy_ramp_months', 12) or 12)

        # New fields: Virtu WACC
        virtu_wacc_raw = request.form.get('virtu_wacc', '').strip()
        virtu_wacc = float(virtu_wacc_raw) / 100 if virtu_wacc_raw else None

        # New fields: Purchase structure
        holdback = float(request.form.get('holdback', 0) or 0)
        pcdpcdr = float(request.form.get('pcdpcdr', 0) or 0)
        cash_date = request.form.get('cash_date', '').strip() or None
        close_date = request.form.get('close_date', '').strip() or None

        # New fields: Deal metadata
        opportunity_name = request.form.get('opportunity_name', deal_name).strip()
        rights_included = request.form.get('rights_included', 'Masters').strip()
        deal_type = request.form.get('deal_type', 'Catalog').strip()

        # New fields: SOFR curve (JSON textarea)
        sofr_curve = []
        sofr_curve_raw = request.form.get('sofr_curve_json', '').strip()
        if sofr_curve_raw:
            try:
                sofr_curve = json.loads(sofr_curve_raw)
                if not isinstance(sofr_curve, list):
                    sofr_curve = []
            except (json.JSONDecodeError, TypeError):
                sofr_curve = []

        # New fields: FX rates (JSON)
        fx_rates = {}
        fx_rates_raw = request.form.get('fx_rates_json', '').strip()
        if fx_rates_raw:
            try:
                fx_rates = json.loads(fx_rates_raw)
                if not isinstance(fx_rates, dict):
                    fx_rates = {}
            except (json.JSONDecodeError, TypeError):
                fx_rates = {}

        # New fields: Per-payor configs (JSON)
        payor_configs = {}
        payor_configs_raw = request.form.get('payor_configs_json', '').strip()
        if payor_configs_raw:
            try:
                payor_configs = json.loads(payor_configs_raw)
                if not isinstance(payor_configs, dict):
                    payor_configs = {}
            except (json.JSONDecodeError, TypeError):
                payor_configs = {}

        config = forecast_engine.ForecastConfig(
            genre_default=request.form.get('genre_default', 'default'),
            horizon_years=int(request.form.get('horizon_years', 5)),
            discount_rate=float(request.form.get('discount_rate', 9.375) or 9.375) / 100,
            terminal_growth=float(request.form.get('terminal_growth', 1)) / 100 if request.form.get('terminal_growth') else None,
            purchase_price=purchase_price,
            exit_multiple=exit_multiple,
            ltv=ltv,
            sofr_rate=sofr_rate,
            sofr_floor=sofr_floor,
            sofr_spread=sofr_spread,
            cash_flow_sweep=cash_flow_sweep,
            synergy_ramp_months=synergy_ramp_months,
            virtu_wacc=virtu_wacc,
            holdback=holdback,
            pcdpcdr=pcdpcdr,
            cash_date=cash_date,
            close_date=close_date,
            opportunity_name=opportunity_name,
            rights_included=rights_included,
            deal_type=deal_type,
            sofr_curve=sofr_curve,
            fx_rates=fx_rates,
            payor_configs=payor_configs,
        )

        # Parse new fee rate synergy
        new_fee_raw = request.form.get('new_fee_rate', '').strip()
        if new_fee_raw:
            config.new_fee_rate = float(new_fee_raw) / 100
            config.synergy_start_year = int(request.form.get('synergy_start_year', 1))

        # Parse third party synergy rate
        tp_syn_raw = request.form.get('third_party_synergy_rate', '').strip()
        if tp_syn_raw:
            config.third_party_synergy_rate = float(tp_syn_raw) / 100

        # Run forecast
        result = forecast_engine.run_forecast(payor_results, analytics, config)

        # Save to DB
        if db.is_available():
            deal_id = db.get_deal_id_by_slug(slug)
            if deal_id:
                db.save_forecast(deal_id, config.to_dict(), result.get('summary'))
                db.update_deal_forecast_config(slug, config.to_dict())

        # Save result to disk
        deal_dir = os.path.join(DEALS_DIR, slug)
        os.makedirs(deal_dir, exist_ok=True)
        forecast_path = os.path.join(deal_dir, 'forecast_result.json')
        with open(forecast_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)

        with _state_lock:
            deal_name_copy = _cached_deal_name

        return render_template_string(
            DASHBOARD_HTML,
            page='forecast',
            results=analytics,
            payor_names=[pr.config.name for pr in payor_results.values()],
            payor_codes=list(payor_results.keys()),
            default_payors=[],
            deal_name=deal_name_copy,
            forecast_slug=slug,
            forecast_deal_name=deal_name,
            forecast_result=result,
            genre_choices=forecast_engine.GENRE_CHOICES,
        )
    except Exception as e:
        log.error("forecast_run failed for %s: %s", slug, e, exc_info=True)
        flash(f'Forecast error: {str(e)}', 'error')
        return redirect(url_for('forecast_page', slug=slug))


@app.route('/deals/<slug>/forecast/download')
def forecast_download(slug):
    """Download forecast Excel file."""
    if not session.get('forecast_unlocked'):
        return redirect(url_for('forecast_page', slug=slug))
    try:
        deal_name, payor_results, analytics, _, _, _ = load_deal(slug)
        deal_dir = os.path.join(DEALS_DIR, slug)
        forecast_path = os.path.join(deal_dir, 'forecast_result.json')

        if not os.path.isfile(forecast_path):
            flash('No forecast result found. Run a forecast first.', 'error')
            return redirect(url_for('forecast_page', slug=slug))

        with open(forecast_path, 'r') as f:
            result = json.load(f)

        exports_dir = os.path.join(deal_dir, 'exports')
        os.makedirs(exports_dir, exist_ok=True)
        xlsx_path = os.path.join(exports_dir, f'{slug}_Forecast_Model.xlsx')
        forecast_engine.export_forecast_excel(result, xlsx_path, deal_name)

        return send_file(xlsx_path, as_attachment=True,
                         download_name=f'{deal_name}_Forecast_Model.xlsx')
    except Exception as e:
        log.error("forecast_download failed for %s: %s", slug, e, exc_info=True)
        flash(f'Download error: {str(e)}', 'error')
        return redirect(url_for('forecast_page', slug=slug))


@app.route('/api/forecast-preview', methods=['POST'])
def api_forecast_preview():
    """AJAX single-ISRC forecast preview."""
    try:
        data = request.get_json(silent=True) or {}
        isrc = data.get('isrc', '')
        genre = data.get('genre', 'default')
        ltm_gross = float(data.get('ltm_gross', 0))
        release_date = data.get('release_date')
        horizon = int(data.get('horizon_years', 5))
        discount_rate = float(data.get('discount_rate', 10)) / 100

        if ltm_gross <= 0:
            return jsonify({'error': 'LTM gross must be > 0'}), 400

        config = forecast_engine.ForecastConfig(
            genre_default=genre,
            horizon_years=horizon,
            discount_rate=discount_rate,
        )

        baseline = {
            'isrc': isrc,
            'gross': ltm_gross,
            'fees': ltm_gross * 0.15,
            'net_receipts': ltm_gross * 0.85,
            'net_earnings': ltm_gross * 0.70,
            'release_date': release_date,
        }

        projections = forecast_engine.project_isrc(baseline, release_date, config, genre)
        total_projected = sum(p['gross'] for p in projections)

        return jsonify({
            'projections': projections,
            'total_projected': round(total_projected, 2),
            'baseline_gross': ltm_gross,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Chat Routes
# ---------------------------------------------------------------------------

@app.route('/chat')
def chat_page():
    with _state_lock:
        analytics_copy = dict(_cached_analytics) if _cached_analytics else None
        deal_name_copy = _cached_deal_name
        processing_copy = dict(_processing_status)
    return render_template_string(
        DASHBOARD_HTML,
        page='chat',
        results=analytics_copy,
        deal_name=deal_name_copy,
        processing=processing_copy,
    )


@app.route('/api/chat', methods=['POST'])
def api_chat():
    """Handle chat messages via Gemini API."""
    global _chat_histories

    # Check API key
    if not _gemini_api_key:
        return jsonify({
            'reply': 'Gemini API key is not configured. Please add your API key to the `.env` file as `GEMINI_API_KEY=your_key_here` and restart the app.',
            'session_id': request.json.get('session_id', ''),
        })

    # Check data
    with _state_lock:
        if not _cached_analytics:
            return jsonify({
                'reply': 'No royalty data is loaded. Please upload statements and run a consolidation first, or load a saved deal from the Deals tab.',
                'session_id': request.json.get('session_id', ''),
            })

    data = request.json or {}
    user_message = data.get('message', '').strip()
    session_id = data.get('session_id', str(uuid.uuid4()))

    if not user_message:
        return jsonify({'reply': 'Please enter a message.', 'session_id': session_id})

    # Build system prompt with data context
    context = _build_chat_context()
    system_prompt = CHATBOT_SYSTEM_PROMPT + "\n\n" + context

    # Get or create conversation history
    if session_id not in _chat_histories:
        _chat_histories[session_id] = []

    history = _chat_histories[session_id]

    try:
        _genai = _get_genai()
        from google.genai import types as _genai_types
        _client = _genai.Client(api_key=_gemini_api_key or os.getenv('GEMINI_API_KEY', ''))

        # Build contents from history + new message
        contents = []
        for msg in history:
            contents.append({'role': msg['role'], 'parts': [{'text': msg['text']}]})
        contents.append({'role': 'user', 'parts': [{'text': user_message}]})

        response = _client.models.generate_content(
            model='gemini-2.0-flash',
            contents=contents,
            config=_genai_types.GenerateContentConfig(system_instruction=system_prompt),
        )
        reply = response.text

        # Store in history
        history.append({'role': 'user', 'text': user_message})
        history.append({'role': 'model', 'text': reply})

        # Cap history at 50 messages to prevent unbounded growth
        if len(history) > 50:
            history[:] = history[-50:]

        return jsonify({'reply': reply, 'session_id': session_id})

    except Exception as e:
        log.error("Chat API failed: %s", e, exc_info=True)
        error_msg = str(e)
        if 'API_KEY' in error_msg.upper() or 'AUTHENTICATION' in error_msg.upper():
            reply = 'Invalid Gemini API key. Please check your `.env` file and restart the app.'
        else:
            reply = f'An error occurred while processing your request: {error_msg}'
        return jsonify({'reply': reply, 'session_id': session_id})


# ---------------------------------------------------------------------------
# Ingest Wizard Routes
# ---------------------------------------------------------------------------

@app.route('/ingest')
def ingest_page():
    """Redirect to unified upload page."""
    return redirect(url_for('upload_page'))


@app.route('/ingest/upload', methods=['POST'])
def ingest_upload():
    """Save file, detect headers, check fingerprint. If match → skip to QC."""
    _ingest_session, sid = _get_ingest_session()
    _ingest_session.clear()

    # Check for GCS-uploaded file first
    gcs_file_path = request.form.get('gcs_file_path', '').strip()
    gcs_file_name = request.form.get('gcs_file_name', '').strip()

    f = request.files.get('statement_file')

    if gcs_file_path and gcs_file_name:
        # File was uploaded directly to GCS — download it locally
        filename = gcs_file_name
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ('.xlsx', '.xls', '.xlsb', '.csv'):
            flash('Unsupported file type. Please upload XLSX, XLS, XLSB, or CSV.', 'error')
            return redirect(url_for('upload_page'))
        filepath = os.path.join(INGEST_TEMP, filename)
        try:
            storage.download_to_file(gcs_file_path, filepath)
            storage.delete_blob(gcs_file_path)
        except Exception as e:
            log.error("GCS download failed for ingest: %s", e)
            flash(f'Failed to download file from storage: {e}', 'error')
            return redirect(url_for('upload_page'))
    elif f and f.filename:
        filename = f.filename
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ('.xlsx', '.xls', '.xlsb', '.csv'):
            flash('Unsupported file type. Please upload XLSX, XLS, XLSB, or CSV.', 'error')
            return redirect(url_for('upload_page'))
        filepath = os.path.join(INGEST_TEMP, filename)
        f.save(filepath)
    else:
        flash('Please select a file to upload.', 'error')
        return redirect(url_for('upload_page'))

    # Detect headers
    detection = mapper.detect_headers(filepath)
    headers = detection['headers']
    fingerprint = mapper.compute_fingerprint(headers) if headers else ''

    _ingest_session.update({
        'filepath': filepath,
        'filename': filename,
        'sheet_name': detection['sheets'][0] if detection.get('sheets') else None,
        'detection': detection,
        'fingerprint': fingerprint,
        'proposed_mapping': {},
        'confirmed_mapping': {},
        'mapped_df': None,
        'qc_result': None,
        'step': 'detect',
        'export_path': '',
    })

    # Check fingerprint for existing mapping
    if headers and fingerprint:
        saved = mapper.get_fingerprint_mapping(headers)
        if saved:
            # Auto-apply saved mapping and skip to QC
            _ingest_session['confirmed_mapping'] = saved
            _ingest_session['step'] = 'qc'

            sheet = _ingest_session['sheet_name']
            header_row = detection['header_row']
            mapped_df = mapper.apply_mapping(filepath, saved, header_row, sheet)
            _ingest_session['mapped_df'] = mapped_df
            _ingest_session['qc_result'] = mapper.run_qc(mapped_df)

            mapper.increment_fingerprint_use(fingerprint)
            flash('Recognized format — auto-applied saved mapping.', 'success')
            resp = redirect(url_for('ingest_qc'))
            resp.set_cookie('session_id', sid)
            return resp

    resp = redirect(url_for('ingest_detect'))
    resp.set_cookie('session_id', sid)
    return resp


@app.route('/ingest/detect', methods=['GET', 'POST'])
def ingest_detect():
    """Checkpoint 1: show raw rows, user confirms header row + sheet."""
    _ingest_session, sid = _get_ingest_session()

    if not _ingest_session.get('filepath'):
        flash('No file uploaded. Please start over.', 'error')
        return redirect(url_for('upload_page'))

    if request.method == 'POST':
        # User confirmed header row and sheet
        header_row = int(request.form.get('header_row', 0))
        sheet_name = request.form.get('sheet_name', '') or None

        # Re-detect with updated sheet if changed
        filepath = _ingest_session['filepath']
        if sheet_name != _ingest_session.get('sheet_name'):
            detection = mapper.detect_headers(filepath, sheet_name)
            _ingest_session['detection'] = detection
            _ingest_session['sheet_name'] = sheet_name
        else:
            detection = _ingest_session['detection']

        # Use user-selected header row
        raw_rows = detection.get('preview_rows', [])
        if 0 <= header_row < len(raw_rows):
            headers = [str(c).strip() for c in raw_rows[header_row]]
        else:
            headers = detection.get('headers', [])

        detection['header_row'] = header_row
        detection['headers'] = headers
        _ingest_session['detection'] = detection
        _ingest_session['fingerprint'] = mapper.compute_fingerprint(headers)

        # Propose mapping
        proposed = mapper.propose_mapping(headers)
        _ingest_session['proposed_mapping'] = proposed
        _ingest_session['step'] = 'map'

        return redirect(url_for('ingest_map'))

    # GET — show detection UI
    detection = _ingest_session.get('detection', {})
    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        ingest_step='detect',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        ingest=_ingest_session,
        detection=detection,
        canonical_fields=mapper.CANONICAL_FIELDS,
    )


@app.route('/ingest/map', methods=['GET', 'POST'])
def ingest_map():
    """Checkpoint 2: mapping form with confidence badges, user confirms."""
    _ingest_session, sid = _get_ingest_session()

    if not _ingest_session.get('filepath'):
        flash('No file uploaded. Please start over.', 'error')
        return redirect(url_for('upload_page'))

    if request.method == 'POST':
        # Build confirmed mapping from form
        detection = _ingest_session.get('detection', {})
        headers = detection.get('headers', [])
        confirmed = {}
        for col in headers:
            val = request.form.get(f'map_{col}', '')
            if val and val in mapper.CANONICAL_FIELDS:
                confirmed[col] = val

        _ingest_session['confirmed_mapping'] = confirmed

        # Save mapping and synonyms
        fingerprint = _ingest_session.get('fingerprint', '')
        if fingerprint:
            mapper.save_mapping(fingerprint, headers, confirmed,
                                source_label=_ingest_session.get('filename', ''))
            mapper.save_synonyms(confirmed)

        # Apply mapping
        filepath = _ingest_session['filepath']
        sheet = _ingest_session.get('sheet_name')
        header_row = detection.get('header_row', 0)
        mapped_df = mapper.apply_mapping(filepath, confirmed, header_row, sheet)
        _ingest_session['mapped_df'] = mapped_df

        # Run QC
        _ingest_session['qc_result'] = mapper.run_qc(mapped_df)
        _ingest_session['step'] = 'qc'

        return redirect(url_for('ingest_qc'))

    # GET — show mapping UI
    detection = _ingest_session.get('detection', {})
    proposed = _ingest_session.get('proposed_mapping', {})

    # Read a preview of data rows (below header)
    header_row = detection.get('header_row', 0)
    preview_rows = detection.get('preview_rows', [])
    data_preview = preview_rows[header_row + 1:header_row + 6] if preview_rows else []

    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        ingest_step='map',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        ingest=_ingest_session,
        detection=detection,
        proposed=proposed,
        canonical_fields=mapper.CANONICAL_FIELDS,
        required_fields=list(mapper.REQUIRED_FIELDS),
        data_preview=data_preview,
    )


@app.route('/ingest/qc')
def ingest_qc():
    """Checkpoint 3: QC report with stats, issues table, data preview."""
    _ingest_session, sid = _get_ingest_session()

    if _ingest_session.get('mapped_df') is None:
        flash('No mapped data. Please start over.', 'error')
        return redirect(url_for('upload_page'))

    mapped_df = _ingest_session.get('mapped_df')
    qc_result = _ingest_session.get('qc_result')

    # Build preview (first 20 rows)
    preview_df = mapped_df.head(20) if mapped_df is not None else pd.DataFrame()
    preview_cols = list(preview_df.columns)
    preview_data = preview_df.values.tolist()

    # Aggregate stats
    stats = {}
    if mapped_df is not None and not mapped_df.empty:
        for col in ('gross', 'net', 'sales'):
            if col in mapped_df.columns:
                stats[f'{col}_sum'] = f"{mapped_df[col].sum():,.2f}"
                stats[f'{col}_mean'] = f"{mapped_df[col].mean():,.2f}"
        if 'identifier' in mapped_df.columns:
            stats['unique_ids'] = mapped_df['identifier'].nunique()
        if 'period' in mapped_df.columns:
            periods = mapped_df['period'].unique()
            stats['unique_periods'] = len(periods)

    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        ingest_step='qc',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        ingest=_ingest_session,
        qc=qc_result,
        preview_cols=preview_cols,
        preview_data=preview_data,
        stats=stats,
        canonical_fields=mapper.CANONICAL_FIELDS,
    )


@app.route('/ingest/approve', methods=['POST'])
def ingest_approve():
    """Checkpoint 4: export clean file, log import, flash success."""
    _ingest_session, sid = _get_ingest_session()

    if _ingest_session.get('mapped_df') is None:
        flash('No mapped data. Please start over.', 'error')
        return redirect(url_for('upload_page'))

    mapped_df = _ingest_session['mapped_df']
    qc_result = _ingest_session.get('qc_result')
    filename = _ingest_session.get('filename', 'export')

    export_fmt = request.form.get('export_format', 'xlsx')
    base = os.path.splitext(filename)[0]
    ext = '.csv' if export_fmt == 'csv' else '.xlsx'
    export_name = f"{base}_mapped{ext}"
    export_path = os.path.join(INGEST_TEMP, export_name)

    mapper.export_clean(mapped_df, export_path, fmt=export_fmt)
    _ingest_session['export_path'] = export_path
    _ingest_session['step'] = 'done'

    # Log import
    mapper.log_import(
        filename=filename,
        fingerprint=_ingest_session.get('fingerprint', ''),
        mapping=_ingest_session.get('confirmed_mapping', {}),
        row_count=len(mapped_df),
        qc_warnings=qc_result.warning_count if qc_result else 0,
        qc_errors=qc_result.error_count if qc_result else 0,
        status='approved',
    )

    flash(f'Export complete: {len(mapped_df):,} rows written to {export_name}', 'success')
    return redirect(url_for('ingest_done'))


@app.route('/ingest/done')
def ingest_done():
    """Success page with download link."""
    _ingest_session, sid = _get_ingest_session()
    with _state_lock:
        deal_name_copy = _cached_deal_name
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        ingest_step='done',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=[],
        deal_name=deal_name_copy,
        ingest=_ingest_session,
    )


@app.route('/ingest/download')
def ingest_download():
    """Serve the exported file."""
    _ingest_session, sid = _get_ingest_session()
    path = _ingest_session.get('export_path', '')
    if path and os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    flash('No export file found. Please run the wizard again.', 'error')
    return redirect(url_for('upload_page'))


@app.route('/ingest/reset')
def ingest_reset():
    """Clear session, start over."""
    _clear_ingest_session()
    flash('Ingest wizard reset.', 'success')
    return redirect(url_for('upload_page'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    log.info(f"Royalty Consolidator starting on http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
