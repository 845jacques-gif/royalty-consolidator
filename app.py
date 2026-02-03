"""
Royalty Statement Consolidator - Web Dashboard
Flask app with auto-consolidation from local dirs, polished dark UI, and Chart.js visuals.
"""

import json
import os
import shutil
import tempfile
import traceback
import zipfile

from flask import Flask, render_template_string, request, send_file, flash, redirect, url_for, jsonify

from consolidator import (
    PayorConfig, load_all_payors, write_consolidated_excel,
    populate_template, load_supplemental_metadata, compute_analytics,
    DEFAULT_PAYORS,
)

app = Flask(__name__)
app.secret_key = 'royalty-consolidator-2026'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

WORK_DIR = os.path.join(tempfile.gettempdir(), 'royalty_consolidator', 'current')
os.makedirs(WORK_DIR, exist_ok=True)

# Cache results in memory so we don't re-parse on every page load
_cached_results = {}
_cached_analytics = {}

# ---------------------------------------------------------------------------
# HTML Dashboard Template
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PLYGRND Royalty Dashboard</title>
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
        <div class="nav-logo">P</div>
        <span class="nav-title">PLYGRND</span>
        <div class="nav-links">
            <a href="/" class="{{ 'active' if page == 'dashboard' }}">Dashboard</a>
            <a href="/upload" class="{{ 'active' if page == 'upload' }}">Upload</a>
        </div>
    </div>
    <div class="nav-right">
        {% if results %}
        <a href="/refresh" class="nav-btn">Refresh Data</a>
        <a href="/download/consolidated" class="nav-btn primary">Export .xlsx</a>
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
<div class="page-header">
    <h1>Upload Statements</h1>
    <p>Add any number of payors, configure each one, and upload their statement files.</p>
</div>

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
<form method="POST" action="/run-custom" enctype="multipart/form-data" id="uploadForm">
    <div class="card" style="margin-bottom:16px;">
        <div class="card-header">
            <span class="card-title">Payors</span>
            <button type="button" class="nav-btn" onclick="addPayor()">+ Add Payor</button>
        </div>
        <p style="font-size:12px; color:var(--text-dim); margin-bottom:20px;">
            Configure each payor with a code, name, format, fee, and upload their statement files (.zip, .xlsx, or .csv).
        </p>

        <div id="payorList">
            {# Default first payor #}
            <div class="payor-block" data-idx="0">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                    <span style="font-size:14px; font-weight:600; color:var(--text-primary);">Payor 1</span>
                </div>
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
                            <option value="believe">Believe (Excel)</option>
                            <option value="recordjet">RecordJet (CSV)</option>
                        </select>
                    </div>
                </div>
                <div class="form-row" style="margin-bottom:10px;">
                    <div class="form-group">
                        <label class="form-label">Fee %</label>
                        <input class="form-input" type="number" name="payor_fee_0" value="15" min="0" max="100" step="0.1">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Currency</label>
                        <select class="form-input" name="payor_fx_0">
                            <option value="EUR">EUR</option>
                            <option value="USD">USD</option>
                            <option value="GBP">GBP</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">FX Rate</label>
                        <input class="form-input" type="number" name="payor_fxrate_0" value="1.0" min="0" step="0.0001">
                        <span style="font-size:10px; color:var(--text-dim);">1.0 = no conversion</span>
                    </div>
                </div>
                <div class="form-group" style="margin-bottom:0;">
                    <label class="form-label">Statement Files</label>
                    <input class="form-input" type="file" name="payor_files_0" multiple accept=".zip,.xlsx,.xls,.csv" required>
                </div>
            </div>
        </div>
    </div>

    <button type="submit" class="btn-submit" id="submitBtn" onclick="showLoading()">
        Process All Payors
    </button>
</form>

<script>
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
                    <option value="believe">Believe (Excel)</option>
                    <option value="recordjet">RecordJet (CSV)</option>
                </select>
            </div>
        </div>
        <div class="form-row" style="margin-bottom:10px;">
            <div class="form-group">
                <label class="form-label">Fee %</label>
                <input class="form-input" type="number" name="payor_fee_${n}" value="15" min="0" max="100" step="0.1">
            </div>
            <div class="form-group">
                <label class="form-label">Currency</label>
                <select class="form-input" name="payor_fx_${n}">
                    <option value="EUR">EUR</option>
                    <option value="USD">USD</option>
                    <option value="GBP">GBP</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">FX Rate</label>
                <input class="form-input" type="number" name="payor_fxrate_${n}" value="1.0" min="0" step="0.0001">
            </div>
        </div>
        <div class="form-group" style="margin-bottom:0;">
            <label class="form-label">Statement Files</label>
            <input class="form-input" type="file" name="payor_files_${n}" multiple accept=".zip,.xlsx,.xls,.csv" required>
        </div>
    </div>`;
    document.getElementById('payorList').insertAdjacentHTML('beforeend', html);
}
</script>

{% elif page == 'dashboard' and results %}
{# ==================== DASHBOARD ==================== #}
<div class="page-header">
    <h1>Royalty Analytics</h1>
    <p>{{ results.period_range }} &middot; {{ results.total_files }} files &middot; {{ results.isrc_count }} ISRCs</p>
</div>

{# ---- ROW 1: Hero stats ---- #}
<div class="grid grid-hero" style="margin-bottom:16px;">

    {# -- Total Gross Revenue (big number + chart) -- #}
    <div class="card">
        <div class="card-header">
            <span class="card-title">Total Gross Revenue</span>
            <div class="card-icon">&#8364;</div>
        </div>
        <div class="stat-value">&euro;{{ results.total_gross }}</div>
        <div class="stat-subtitle">All payors combined &middot; lifetime</div>
        {% if results.yoy_decay %}
        {% set last_decay = results.yoy_decay[-1] %}
        <div class="stat-change {{ 'up' if '+' in last_decay.change_pct else 'down' }}">
            {{ last_decay.change_pct }} YoY
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
        <div class="stat-value medium">&euro;{{ "{:,.2f}".format(ltm_total) }}</div>
        <div class="stat-subtitle">Last 12 months total</div>
        <ul class="payor-list" style="margin-top:16px;">
            {% for lp in results.ltm_by_payor %}
            <li class="payor-item">
                <span class="payor-name">{{ lp.name }}</span>
                <span class="payor-value">&euro;{{ lp.ltm_gross_fmt }}</span>
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
            <button class="pill-tab" onclick="showDashTab('dist')">Distributors</button>
        </div>

        <div class="tab-content active" id="dtab-top">
            <table>
                <thead><tr><th>#</th><th>Artist</th><th>Title</th><th class="text-right">Gross</th></tr></thead>
                <tbody>
                {% for song in results.top_songs[:8] %}
                <tr>
                    <td><span class="rank">{{ loop.index }}</span></td>
                    <td>{{ song.artist }}</td>
                    <td style="color:var(--text-primary); font-weight:500;">{{ song.title }}</td>
                    <td class="text-right mono">&euro;{{ song.gross }}</td>
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
                    <td class="text-right mono">&euro;{{ ae.gross }}</td>
                    <td class="text-right mono" style="color:var(--text-muted);">&euro;{{ ae.net }}</td>
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
                    <td class="text-right mono">&euro;{{ d.prev_gross }}</td>
                    <td class="text-right mono">&euro;{{ d.curr_gross }}</td>
                    <td class="text-right mono {{ 'text-red' if '-' in d.change_pct else 'text-green' }}">{{ d.change_pct }}</td>
                </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="tab-content" id="dtab-dist">
            {% for d in results.top_distributors[:8] %}
            <div class="dist-bar-wrap">
                <div class="dist-bar-label">
                    <span class="name">{{ d.name }}</span>
                    <span class="val">&euro;{{ d.gross_fmt }}</span>
                </div>
                <div class="dist-bar-track">
                    <div class="dist-bar-fill" style="width: {{ (d.gross / results.top_distributors[0].gross * 100) | round(1) }}%"></div>
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
                <td class="text-right mono">&euro;{{ song.gross }}</td>
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
                    <div style="font-size:11px; color:var(--text-dim);">{{ ps.code }} &middot; {{ ps.files }} files &middot; {{ ps.isrcs }} ISRCs &middot; fee {{ ps.fee }}</div>
                </div>
                <div class="mono" style="font-size:14px; font-weight:700; color:var(--text-primary);">&euro;{{ ps.total_gross }}</div>
            </div>
        </div>
        {% endfor %}

        <div style="margin-top:20px;">
            <div class="card-title" style="margin-bottom:12px;">Downloads</div>
            <a href="/download/consolidated" class="dl-link">
                <span class="name">Consolidated Statements</span>
                <span class="badge">.xlsx</span>
            </a>
        </div>
    </div>
</div>

{# ---- Chart.js Scripts ---- #}
<script>
const CHART_COLORS = ['#3b82f6', '#a78bfa', '#22d3ee', '#fbbf24', '#f87171'];
const PAYOR_NAMES = {{ payor_names | tojson }};
const PAYOR_CODES = {{ payor_codes | tojson }};

Chart.defaults.color = '#52525b';
Chart.defaults.borderColor = 'rgba(30,30,34,0.6)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;

/* ---- Mini monthly chart (top-left card) ---- */
(function() {
    const data = {{ results.monthly_trend | tojson }};
    const last24 = data.slice(-24);
    new Chart(document.getElementById('monthlyMiniChart'), {
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
                callbacks: { label: ctx => '\u20ac' + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
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
    const byPayor = {{ results.monthly_by_payor | tojson }};
    const allPeriods = {{ results.monthly_trend | tojson }};
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

    new Chart(document.getElementById('monthlyPayorChart'), {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } },
                tooltip: { mode: 'index', intersect: false,
                    callbacks: { label: ctx => ctx.dataset.label + ': \u20ac' + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
                },
            },
            scales: {
                x: { stacked: true, grid: { display: false }, ticks: { maxRotation: 45, font: { size: 10 } } },
                y: { stacked: true, ticks: { callback: v => '\u20ac' + (v/1000).toFixed(0) + 'k' } }
            }
        }
    });
})();

/* ---- Annual gross by payor (grouped bar) ---- */
(function() {
    const byPayor = {{ results.annual_by_payor | tojson }};
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

    new Chart(document.getElementById('annualPayorChart'), {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } },
                tooltip: {
                    callbacks: { label: ctx => ctx.dataset.label + ': \u20ac' + ctx.parsed.y.toLocaleString(undefined, {minimumFractionDigits:2}) }
                },
            },
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => '\u20ac' + (v/1000).toFixed(0) + 'k' } }
            }
        }
    });
})();
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
    </div>
</div>
{% endif %}

</div>

{# Loading overlay #}
<div class="loading-overlay" id="loadingOverlay">
    <div class="loading-ring"></div>
    <div class="loading-text">Processing statements across all payors...</div>
</div>

<script>
function showLoading() {
    document.getElementById('loadingOverlay').classList.add('active');
}

function showDashTab(name) {
    document.querySelectorAll('[id^="dtab-"]').forEach(el => el.classList.remove('active'));
    document.getElementById('dtab-' + name).classList.add('active');
    const btn = event.target;
    btn.parentElement.querySelectorAll('.pill-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
}
</script>

</body>
</html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def run_consolidation(payor_configs, output_dir=None):
    """Run the consolidation pipeline and return (payor_results, analytics, consolidated_path)."""
    global _cached_results, _cached_analytics

    payor_results = load_all_payors(payor_configs)
    if not payor_results:
        return None, None, None

    if output_dir is None:
        output_dir = WORK_DIR

    consolidated_path = os.path.join(output_dir, 'Consolidated_All_Payors.xlsx')
    write_consolidated_excel(payor_results, consolidated_path)

    analytics = compute_analytics(payor_results)

    _cached_results = payor_results
    _cached_analytics = analytics
    app.config['CONSOLIDATED_PATH'] = consolidated_path

    return payor_results, analytics, consolidated_path


@app.route('/')
def index():
    return render_template_string(
        DASHBOARD_HTML,
        page='dashboard',
        results=_cached_analytics if _cached_analytics else None,
        payor_names=[pr.config.name for pr in _cached_results.values()] if _cached_results else [],
        payor_codes=list(_cached_results.keys()) if _cached_results else [],
        default_payors=[],
    )


@app.route('/upload')
def upload_page():
    configs = []
    for c in DEFAULT_PAYORS:
        configs.append({'name': c.name, 'code': c.code, 'fmt': c.fmt, 'fee': int(c.fee * 100)})
    return render_template_string(
        DASHBOARD_HTML,
        page='upload',
        results=None,
        payor_names=[],
        payor_codes=[],
        default_payors=configs,
    )


@app.route('/run-default', methods=['POST'])
def run_default():
    """Run consolidation using the default local directories."""
    try:
        payor_results, analytics, consolidated_path = run_consolidation(DEFAULT_PAYORS)
        if not payor_results:
            flash('No data found in default directories.', 'error')
            return redirect(url_for('upload_page'))
        flash(f'Consolidated {analytics["total_files"]} files, {analytics["isrc_count"]} ISRCs.', 'success')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        traceback.print_exc()
    return redirect(url_for('index'))


@app.route('/run-custom', methods=['POST'])
def run_custom():
    """Run consolidation from uploaded files with dynamic payor configs."""
    try:
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
            fmt = request.form.get(f'payor_fmt_{idx}', 'believe')
            fee = float(request.form.get(f'payor_fee_{idx}', 15)) / 100.0
            fx_currency = request.form.get(f'payor_fx_{idx}', 'EUR')
            fx_rate = float(request.form.get(f'payor_fxrate_{idx}', 1.0))

            payor_dir = os.path.join(work_dir, f'statements_{code.strip()}')
            os.makedirs(payor_dir, exist_ok=True)

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
                fx_currency=fx_currency,
                fx_rate=fx_rate,
                statements_dir=payor_dir,
            ))
            idx += 1

        if not payor_configs:
            flash('No payors configured. Add at least one payor.', 'error')
            return redirect(url_for('upload_page'))

        payor_results, analytics, consolidated_path = run_consolidation(payor_configs, work_dir)
        if not payor_results:
            flash('No data found in uploaded files.', 'error')
            return redirect(url_for('upload_page'))

        flash(f'Consolidated {analytics["total_files"]} files across {len(payor_configs)} payors, {analytics["isrc_count"]} ISRCs.', 'success')

    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        traceback.print_exc()

    return redirect(url_for('index'))


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
        flash(f'Error: {str(e)}', 'error')
        traceback.print_exc()
    return redirect(url_for('index'))


@app.route('/download/<filetype>')
def download(filetype):
    if filetype == 'consolidated':
        path = app.config.get('CONSOLIDATED_PATH')
    elif filetype == 'model':
        path = app.config.get('MODEL_PATH')
    else:
        return 'Not found', 404

    if path and os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
    return 'File not found. Run consolidation first.', 404


@app.route('/api/analytics')
def api_analytics():
    """Return analytics as JSON (for future AJAX refresh)."""
    if _cached_analytics:
        return jsonify(_cached_analytics)
    return jsonify({'error': 'No data loaded'}), 404


if __name__ == '__main__':
    print("\n  PLYGRND Royalty Dashboard")
    print("  Open in your browser: http://localhost:5000")
    print("  Press Ctrl+C to stop.\n")
    app.run(host='0.0.0.0', port=5000, debug=False)
