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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(_log_dir, 'app.log'), encoding='utf-8'),
    ],
)
log = logging.getLogger('royalty')

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template_string, request, send_file, flash, redirect, url_for, jsonify, make_response
import pandas as pd

# Lazy Gemini import to avoid ~2-3s delay from deprecation warnings on startup
genai = None

_gemini_api_key = os.getenv('GEMINI_API_KEY', '')

def _get_genai():
    """Lazy-load google.generativeai on first use."""
    global genai
    if genai is None:
        import google.generativeai as _genai
        genai = _genai
        if _gemini_api_key:
            genai.configure(api_key=_gemini_api_key)
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

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2 GB


@app.after_request
def _no_cache(response):
    """Prevent browser from caching HTML pages (stale JS/CSS)."""
    if 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response

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
}

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
    """Load a saved deal from disk. Returns (deal_name, payor_results, analytics) or raises."""
    deal_dir = os.path.join(DEALS_DIR, slug)

    with open(os.path.join(deal_dir, 'deal_meta.json'), 'r') as f:
        meta = json.load(f)

    with open(os.path.join(deal_dir, 'payor_results.pkl'), 'rb') as f:
        payor_results = pickle.load(f)

    # Recompute analytics from pickle if LTM fields are missing (older saves)
    analytics_path = os.path.join(deal_dir, 'analytics.json')
    with open(analytics_path, 'r') as f:
        analytics = json.load(f)

    if 'ltm_stores' not in analytics or 'ltm_media_types' not in analytics:
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
            --radius: 12px;
            --radius-sm: 8px;
            --radius-xs: 6px;
        }

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
        <form method="POST" action="/ingest/upload" enctype="multipart/form-data">
            <div class="form-group">
                <label class="form-label">Statement File</label>
                <input class="form-input" type="file" name="statement_file" accept=".xlsx,.xls,.xlsb,.csv" required>
            </div>
            <button type="submit" class="btn-submit">Upload &amp; Detect</button>
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
                <div style="color:var(--text-dim); font-size:11px; margin-top:2px;">{{ fmt.column_names | length }} columns &middot; {{ fmt.updated_at[:10] }}</div>
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
            <td style="color:var(--text-dim); font-size:11px;">{{ log.created_at[:16].replace('T', ' ') }}</td>
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

function _submitViaFetch() {
    const form = document.getElementById('uploadForm');
    const formData = new FormData();

    // Add all non-file form fields
    const inputs = form.querySelectorAll('input:not([type=file]), select, textarea');
    inputs.forEach(inp => {
        if (inp.name) formData.append(inp.name, inp.value);
    });

    // Add files from _dzFiles or native inputs
    const fileInputs = form.querySelectorAll('input[type=file][name^="payor_files_"]');
    fileInputs.forEach(inp => {
        const dzList = _dzFiles[inp.name];
        if (dzList && dzList.length) {
            dzList.forEach(f => formData.append(inp.name, f, f.name));
        } else {
            for (const f of inp.files) formData.append(inp.name, f, f.name);
        }
    });

    // Show uploading state
    const btn = document.getElementById('submitBtn');
    const origText = btn.textContent;
    btn.textContent = 'Uploading...';
    btn.disabled = true;

    fetch(form.action || '/custom/upload', {
        method: 'POST',
        body: formData,
        redirect: 'follow',
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
<div class="page-header">
    <h1>Saved Deals</h1>
    <p>Load a previous consolidation run or delete old ones.</p>
</div>

{% if deals %}
<div class="grid grid-3">
    {% for deal in deals %}
    <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:12px;">
            <div>
                <div style="font-size:16px; font-weight:700; color:var(--text-primary); letter-spacing:-0.01em;">{{ deal.name }}</div>
                <div style="font-size:11px; color:var(--text-dim); margin-top:2px;">{{ deal.timestamp[:16].replace('T', ' ') }}</div>
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
        <div style="display:flex; gap:8px;">
            <a href="/deals/{{ deal.slug }}/load" class="nav-btn primary" style="flex:1; text-align:center; padding:9px 0; font-size:13px;">Load</a>
            <a href="/deals/{{ deal.slug }}/edit" class="nav-btn" style="padding:9px 14px; font-size:13px;">Edit</a>
            <form method="POST" action="/deals/{{ deal.slug }}/delete" style="margin:0;" onsubmit="return confirm('Delete deal {{ deal.name }}?');">
                <button type="submit" class="nav-btn" style="padding:9px 14px; font-size:13px; color:var(--red); border-color:var(--red-dim);">Delete</button>
            </form>
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

{# ---- ROW 2: Charts ---- #}
<div class="grid grid-2" style="margin-bottom:16px;">
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

{# ---- ROW 3: LTM Top Songs + Per Payor Breakdown ---- #}
<div class="grid grid-wide" style="margin-bottom:16px;">
    <div class="card">
        <div class="card-header"><span class="card-title">LTM Top 20 Songs</span></div>
        <table>
            <thead>
                <tr><th>#</th><th>Artist</th><th>Title</th><th>ISRC</th><th class="text-right">LTM Gross</th></tr>
            </thead>
            <tbody>
            {% for song in results.ltm_songs %}
            <tr>
                <td><span class="rank">{{ loop.index }}</span></td>
                <td>{{ song.artist }}</td>
                <td style="color:var(--text-primary); font-weight:500;">{{ song.title }}</td>
                <td class="mono" style="font-size:11px; color:var(--text-dim);">{{ song.isrc }}</td>
                <td class="text-right mono"><span class="data-money" data-raw="{{ song.gross_raw }}" data-ccy="{{ results.currency_code | default('USD') }}">{{ results.currency_symbol | default('$') }}{{ song.gross }}</span></td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    <div class="card">
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
<div class="grid" style="margin-bottom:16px;">
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

    return render_template_string(
        DASHBOARD_HTML,
        page='dashboard',
        results=analytics_copy,
        payor_names=payor_names,
        payor_codes=payor_codes,
        default_payors=[],
        deal_name=deal_name_copy,
        processing=processing_copy,
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
            })

            idx += 1

        if not payors:
            flash('No payors configured.', 'error')
            return redirect(url_for('upload_page'))

        # Second pass: for any file without a date, try extracting from
        # filename and file contents now that files are saved to disk
        from consolidator import parse_period_from_filename, period_to_end_of_month, peek_statement_date
        for p in payors:
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
    """Phase 2: Parse all files with user mappings, then redirect to validation."""
    try:
        sess, sid = _get_custom_session()

        # ── Prevent duplicate concurrent processing runs ──
        with _processing_locks_guard:
            if sid not in _processing_locks:
                _processing_locks[sid] = threading.Lock()
            lock = _processing_locks[sid]

        if not lock.acquire(blocking=False):
            # Another run is already in progress – wait for it to finish
            log.warning("custom_process: duplicate run blocked for sid=%s", sid)
            lock.acquire()          # block until the first run is done
            lock.release()
            # First run already stored results – just redirect to validation
            return redirect(url_for('custom_validate'))

        try:
            return _do_custom_process(sess, sid)
        finally:
            lock.release()
    except Exception as e:
        log.error("custom_process failed: %s", e, exc_info=True)
        flash(f'Error processing files: {str(e)}', 'error')
        return redirect(url_for('upload_page'))


def _do_custom_process(sess, sid):
    """Actual processing logic (called under lock)."""
    try:
        payors = sess.get('payors', [])
        log.info("custom_process: %d payor(s)", len(payors))
        _log_memory()
        file_dates = sess.get('file_dates', {})
        column_mappings_by_payor = sess.get('column_mappings', {})

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

            if not statements_dir or not os.path.isdir(statements_dir):
                continue

            cfg = PayorConfig(
                code=p['code'],
                name=p['name'],
                statements_dir=statements_dir,
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
            )
            configs.append(cfg)

        if not configs:
            flash('No valid payor configurations.', 'error')
            return redirect(url_for('upload_page'))

        payor_results = load_all_payors(
            configs,
            file_dates=file_dates,
            column_mappings_by_payor=column_mappings_by_payor if column_mappings_by_payor else None,
        )
        sess['payor_results_keys'] = list(payor_results.keys())

        # Store results temporarily (in-memory for this session)
        sess_data, _ = _get_ingest_session()
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

    except Exception as e:
        log.error("_do_custom_process failed: %s", e, exc_info=True)
        flash(f'Error processing files: {str(e)}', 'error')
        return redirect(url_for('upload_page'))

    return redirect(url_for('custom_validate'))


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
            sess['enrichment_status'] = {
                'running': True, 'done': False, 'error': None,
                'phase': 'starting', 'current': 0, 'total': 0, 'message': 'Starting...',
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
    sess, sid = _get_custom_session()
    status = sess.get('enrichment_status', {})
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
        _processing_status = {'running': True, 'progress': 'Finalizing...', 'done': False, 'error': None}

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
                _processing_status['progress'] = 'Applying enrichment...'

            # Apply enrichment to raw PayorResult.detail DataFrames
            if enrich_result and enrich_result.get('lookups'):
                lookups = enrich_result['lookups']
                for code, pr in payor_results.items():
                    pr.detail = apply_enrichment_to_raw_detail(pr.detail, lookups)

            with _state_lock:
                _processing_status['progress'] = 'Writing output files...'
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
                _processing_status = {'running': False, 'progress': 'Done!', 'done': True, 'error': None}

        except Exception as e:
            log.error("Finalize thread failed: %s", e, exc_info=True)
            with _state_lock:
                _processing_status = {'running': False, 'progress': '', 'done': False, 'error': str(e)}

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
    with _state_lock:
        return jsonify(dict(_processing_status))


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
        _genai.configure(api_key=api_key)

        # Save and upload each PDF
        for cf in contract_files:
            if not cf.filename or not cf.filename.lower().endswith('.pdf'):
                continue
            tmp = _tf.NamedTemporaryFile(delete=False, suffix='.pdf')
            cf.save(tmp.name)
            tmp.close()
            tmp_paths.append(tmp.name)
            uploaded_files.append(_genai.upload_file(tmp.name, mime_type='application/pdf'))

        if not uploaded_files:
            return jsonify({'error': 'No valid PDF files found'}), 400

        model = _genai.GenerativeModel('gemini-2.0-flash')
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

        response = model.generate_content([*uploaded_files, prompt])

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
        model = _genai.GenerativeModel(
            'gemini-2.0-flash',
            system_instruction=system_prompt,
        )

        # Build contents from history + new message
        contents = []
        for msg in history:
            contents.append({'role': msg['role'], 'parts': [msg['text']]})
        contents.append({'role': 'user', 'parts': [user_message]})

        response = model.generate_content(contents)
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

    f = request.files.get('statement_file')
    if not f or not f.filename:
        flash('Please select a file to upload.', 'error')
        return redirect(url_for('upload_page'))

    filename = f.filename
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ('.xlsx', '.xls', '.xlsb', '.csv'):
        flash('Unsupported file type. Please upload XLSX, XLS, XLSB, or CSV.', 'error')
        return redirect(url_for('upload_page'))

    filepath = os.path.join(INGEST_TEMP, filename)
    f.save(filepath)

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
    log.info("Royalty Consolidator starting on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
