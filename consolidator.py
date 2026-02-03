"""
Royalty Statement Consolidator - Multi-Payor Edition
Ingests royalty statements from any payor (PDF, CSV, Excel),
auto-detects columns, consolidates, and populates a multi-tab financial model.
"""

import argparse
import calendar
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import openpyxl

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PayorConfig:
    """Configuration for a single payor."""
    code: str              # Short code: B1, B2, RJ — maps to {code}_Model tab
    name: str              # Display name: "Believe 15%", "RecordJet"
    statements_dir: str    # Directory containing statement files
    fmt: str               # 'auto', 'believe', or 'recordjet'
    fee: float             # Distribution fee as decimal (0.15 = 15%)
    fx_currency: str = 'USD'
    fx_rate: float = 1.0   # Multiply local currency amounts by this to get USD


@dataclass
class PayorResult:
    """Aggregated data for one payor."""
    config: PayorConfig
    isrc_meta: pd.DataFrame    # Unique ISRCs with title, artist, total_gross
    monthly: pd.DataFrame      # ISRC × period with gross, net, sales
    detail: pd.DataFrame       # ISRC × period × distributor × download_type
    pivot_gross: pd.DataFrame  # Pivot: rows=ISRC, cols=period, vals=gross
    by_distributor: pd.DataFrame
    file_count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def period_to_end_of_month(period):
    """Convert a YYYYMM int to the last day of that month as a date."""
    year = int(str(period)[:4])
    month = int(str(period)[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def month_to_column(year, month):
    """Convert calendar year+month to the model tab column index.

    Monthly columns start at column 29 (AC) = Jan 2020.
    """
    offset = (year - 2020) * 12 + (month - 1)
    return 29 + offset


def parse_period_from_filename(filename):
    """Extract YYYYMM period from filename."""
    # Match patterns like: "PLYGRND 202201", "2022-01", "2022_01", "202201"
    m = re.search(r'(\d{4})[\s._-]?(\d{2})', filename)
    if m:
        return int(f"{m.group(1)}{m.group(2)}")
    return None


# ---------------------------------------------------------------------------
# Universal column auto-detection
# ---------------------------------------------------------------------------

# Each key is the standard schema field; the list has lowercase substrings to
# match against the source column name (checked in order — first match wins).
COLUMN_PATTERNS = {
    'identifier': ['isrc', 'identifier', 'upc', 'product code', 'catalog', 'track id',
                    'asset id', 'recording id', 'track code'],
    'title': ['title', 'track', 'song name', 'song', 'track name'],
    'artist': ['artist', 'performer', 'act', 'band', 'creator'],
    'product_title': ['product title', 'album', 'release', 'product', 'bundle',
                      'album name', 'release title', 'ean'],
    'distributor': ['distributor', 'store', 'platform', 'dsp', 'service',
                    'retailer', 'partner', 'channel', 'source'],
    'sales': ['sales', 'quantity', 'units', 'streams', 'plays', 'count',
              'qty', 'volume', 'number of', 'total units'],
    'download_type': ['download type', 'usage type', 'transaction type',
                      'content type', 'sale type', 'revenue type', 'type'],
    'gross': ['gross', 'ppu total', 'revenue', 'earning gross', 'total revenue',
              'gross revenue', 'earnings', 'amount', 'total amount',
              'earning_gross', 'price', 'retail'],
    'net': ['net', 'royalty', 'earning net', 'payout', 'net revenue', 'your share',
            'payable', 'earning_net', 'net amount', 'royalties'],
    'period': ['period', 'reporting period', 'statement period', 'sale period',
               'accounting period', 'royalty period', 'month'],
    'country': ['country', 'territory', 'region', 'market'],
}


def _fuzzy_match_columns(df_columns):
    """Match source column names to our standard schema.

    Returns a dict mapping standard field -> source column name.
    """
    mapped = {}
    used = set()
    lower_cols = {c: c.strip().lower() for c in df_columns}

    # Sort patterns so more specific matches come first
    for field, patterns in COLUMN_PATTERNS.items():
        for pattern in patterns:
            for orig, low in lower_cols.items():
                if orig in used:
                    continue
                if pattern == low or pattern in low:
                    mapped[field] = orig
                    used.add(orig)
                    break
            if field in mapped:
                break

    return mapped


def _read_raw_dataframe(filepath, filename):
    """Read any file (PDF, CSV, Excel) into a raw DataFrame."""
    ext = os.path.splitext(filename)[1].lower()

    if ext == '.csv':
        return pd.read_csv(filepath)

    elif ext in ('.xlsx', '.xls'):
        return pd.read_excel(filepath, sheet_name=0)

    elif ext == '.pdf':
        if not HAS_PDF:
            print(f"    WARNING: pdfplumber not installed, skipping {filename}", flush=True)
            return None
        frames = []
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    header = [str(c).strip() if c else f'col_{i}' for i, c in enumerate(table[0])]
                    rows = table[1:]
                    frames.append(pd.DataFrame(rows, columns=header))
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    return None


def parse_file_universal(filepath, filename, fmt='auto'):
    """Parse any statement file into the standard schema.

    fmt='auto' uses column auto-detection.
    fmt='believe' / 'recordjet' use the legacy column mappings as a hint
    but still fall through to auto-detect if needed.
    """
    df = _read_raw_dataframe(filepath, filename)
    if df is None or df.empty:
        return None

    df.columns = [str(c).strip() for c in df.columns]

    # --- Legacy format shortcuts (exact column names we know) ---
    if fmt == 'believe':
        lc = {c.strip().lower(): c for c in df.columns}
        if 'identifier' in lc and 'ppu total' in lc:
            return _apply_believe_mapping(df, filename)

    if fmt == 'recordjet':
        lc = {c.strip().lower(): c for c in df.columns}
        if 'isrc' in lc and ('earning gross eur' in lc or 'earning_gross_eur' in lc):
            return _apply_recordjet_mapping(df, filename)

    # --- Auto-detect columns ---
    col_map = _fuzzy_match_columns(df.columns)

    if 'identifier' not in col_map and 'gross' not in col_map:
        # Not enough columns recognized — try legacy mappings as fallback
        lc = {c.strip().lower(): c for c in df.columns}
        if 'identifier' in lc and 'ppu total' in lc:
            return _apply_believe_mapping(df, filename)
        if 'isrc' in lc and ('earning gross eur' in lc or 'earning_gross_eur' in lc):
            return _apply_recordjet_mapping(df, filename)

        cols_found = list(df.columns)
        print(f"    WARNING: Could not detect columns in {filename}. "
              f"Found: {cols_found[:15]}", flush=True)
        return None

    # Derive period
    if 'period' in col_map:
        raw_period = df[col_map['period']]
        # Try to parse YYYYMM from the period column
        period_vals = raw_period.astype(str).str.replace(r'[^0-9]', '', regex=True)
        # Keep only 6-digit periods; for longer strings take first 6 chars
        period_vals = period_vals.apply(
            lambda x: int(x[:6]) if len(x) >= 6 and x[:6].isdigit() else 0
        )
        df['_period'] = period_vals
    else:
        period = parse_period_from_filename(filename)
        if period is None:
            # Try to find a date-like column
            for c in df.columns:
                sample = str(df[c].dropna().iloc[0]) if len(df[c].dropna()) > 0 else ''
                m = re.search(r'(\d{4})[\s._/-]?(\d{2})', sample)
                if m:
                    period = int(f"{m.group(1)}{m.group(2)}")
                    break
        if period is None:
            print(f"    WARNING: No period found for {filename}, skipping.", flush=True)
            return None
        df['_period'] = period

    n = len(df)

    def _get(field, default_val=''):
        if field in col_map:
            return df[col_map[field]]
        return pd.Series([default_val] * n)

    def _get_numeric(field):
        if field in col_map:
            return pd.to_numeric(df[col_map[field]], errors='coerce').fillna(0)
        return pd.Series([0.0] * n)

    result = pd.DataFrame({
        'identifier': _get('identifier'),
        'title': _get('title'),
        'artist': _get('artist'),
        'product_title': _get('product_title'),
        'distributor': _get('distributor'),
        'download_type': _get('download_type'),
        'period': df['_period'],
        'gross': _get_numeric('gross'),
        'net': _get_numeric('net'),
        'sales': _get_numeric('sales'),
    })

    # If net is all zeros but gross isn't, net might be in a different column we missed
    # or the file only has one revenue column — that's fine, leave it.
    return result


# ---------------------------------------------------------------------------
# Legacy mapping helpers (kept for exact-match Believe / RecordJet files)
# ---------------------------------------------------------------------------

def _apply_believe_mapping(df, filename):
    """Apply the known Believe column mapping."""
    df.columns = [c.strip().lower() for c in df.columns]
    if 'period' in df.columns:
        df['period'] = df['period'].astype(int)
    else:
        period = parse_period_from_filename(filename)
        if period is None:
            return None
        df['period'] = period

    return pd.DataFrame({
        'identifier': df['identifier'],
        'title': df['title'],
        'artist': df['artist'],
        'product_title': df.get('product title', pd.Series([''] * len(df))),
        'distributor': df.get('distributor', pd.Series([''] * len(df))),
        'download_type': df.get('download type', pd.Series([''] * len(df))),
        'period': df['period'],
        'gross': pd.to_numeric(df['ppu total'], errors='coerce').fillna(0),
        'net': pd.to_numeric(df['royalty'], errors='coerce').fillna(0),
        'sales': pd.to_numeric(df.get('sales', pd.Series([0] * len(df))), errors='coerce').fillna(0),
    })


def _apply_recordjet_mapping(df, filename):
    """Apply the known RecordJet column mapping."""
    col_map = {c.strip().lower(): c for c in df.columns}
    m = re.search(r'(\d{4})[\s._-]?(\d{2})', filename)
    period = int(f"{m.group(1)}{m.group(2)}") if m else parse_period_from_filename(filename)
    if period is None:
        return None

    gross_col = col_map.get('earning gross eur', col_map.get('earning_gross_eur'))
    net_col = col_map.get('earning net eur', col_map.get('earning_net_eur'))
    isrc_col = col_map.get('isrc')
    title_col = col_map.get('title')
    artist_col = col_map.get('artist')
    ean_col = col_map.get('ean')
    store_col = col_map.get('store')
    type_col = col_map.get('type', col_map.get('channel'))
    qty_col = col_map.get('quantity')

    if not gross_col or not isrc_col:
        return None

    return pd.DataFrame({
        'identifier': df[isrc_col] if isrc_col else '',
        'title': df[title_col] if title_col else '',
        'artist': df[artist_col] if artist_col else '',
        'product_title': df[ean_col].astype(str) if ean_col else '',
        'distributor': df[store_col] if store_col else '',
        'download_type': df[type_col] if type_col else '',
        'period': period,
        'gross': pd.to_numeric(df[gross_col], errors='coerce').fillna(0),
        'net': pd.to_numeric(df[net_col], errors='coerce').fillna(0) if net_col else 0,
        'sales': pd.to_numeric(df[qty_col], errors='coerce').fillna(0) if qty_col else 0,
    })


# ---------------------------------------------------------------------------
# Per-payor loading and aggregation
# ---------------------------------------------------------------------------

def load_payor_statements(config: PayorConfig) -> Optional[PayorResult]:
    """Load and aggregate all statement files for one payor."""
    print(f"\n  [{config.code}] Loading {config.name} from: {config.statements_dir}", flush=True)

    chunks = []
    detail_chunks = []
    meta_chunks = []
    dist_chunks = []
    file_count = 0

    SUPPORTED_EXT = ('.csv', '.xlsx', '.xls', '.pdf')

    for root, dirs, files in os.walk(config.statements_dir):
        for f in sorted(files):
            if f.startswith('~$'):
                continue
            ext = os.path.splitext(f)[1].lower()
            if ext not in SUPPORTED_EXT:
                continue
            filepath = os.path.join(root, f)
            df = parse_file_universal(filepath, f, fmt=config.fmt)

            if df is None:
                print(f"    WARNING: Could not parse {f}, skipping.", flush=True)
                continue

            # Apply FX conversion
            if config.fx_rate != 1.0:
                df['gross'] = df['gross'] * config.fx_rate
                df['net'] = df['net'] * config.fx_rate

            # Drop rows with missing ISRC
            df['identifier'] = df['identifier'].astype(str).str.strip()
            df = df[df['identifier'].ne('') & df['identifier'].ne('nan') & df['identifier'].notna()]

            print(f"    {f} ({len(df):,} rows)", flush=True)

            # Metadata per ISRC (first occurrence)
            meta = (
                df.groupby('identifier')
                .agg({'title': 'first', 'artist': 'first', 'product_title': 'first'})
                .reset_index()
            )
            meta_chunks.append(meta)

            # ISRC + period aggregate
            agg = (
                df.groupby(['identifier', 'period'])
                .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
                .reset_index()
            )
            chunks.append(agg)

            # Detail: ISRC + period + distributor + download_type
            detail_agg = (
                df.groupby(['identifier', 'period', 'distributor', 'download_type'])
                .agg({
                    'title': 'first', 'artist': 'first', 'product_title': 'first',
                    'gross': 'sum', 'net': 'sum', 'sales': 'sum',
                })
                .reset_index()
            )
            detail_chunks.append(detail_agg)

            # Distributor aggregate
            dist_agg = (
                df.groupby(['distributor', 'period'])
                .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
                .reset_index()
            )
            dist_chunks.append(dist_agg)

            file_count += 1
            del df

    if not chunks:
        print(f"    No files found for {config.name}.", flush=True)
        return None

    print(f"    Aggregating {file_count} files...", flush=True)

    # Combine and re-aggregate across files
    monthly = pd.concat(chunks, ignore_index=True)
    del chunks
    monthly = (
        monthly.groupby(['identifier', 'period'])
        .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
        .reset_index()
    )
    monthly['statement_date'] = monthly['period'].apply(period_to_end_of_month)

    detail = pd.concat(detail_chunks, ignore_index=True)
    del detail_chunks
    detail = (
        detail.groupby(['identifier', 'period', 'distributor', 'download_type'])
        .agg({
            'title': 'first', 'artist': 'first', 'product_title': 'first',
            'gross': 'sum', 'net': 'sum', 'sales': 'sum',
        })
        .reset_index()
    )
    detail['statement_date'] = detail['period'].apply(period_to_end_of_month)

    all_meta = pd.concat(meta_chunks, ignore_index=True)
    del meta_chunks
    isrc_meta = all_meta.drop_duplicates('identifier', keep='first').reset_index(drop=True)
    del all_meta

    dist_all = pd.concat(dist_chunks, ignore_index=True)
    del dist_chunks
    by_distributor = (
        dist_all.groupby('distributor')
        .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
        .reset_index()
        .sort_values('gross', ascending=False)
    )
    by_distributor.columns = ['Distributor', 'Total Gross', 'Total Net', 'Total Sales']
    del dist_all

    # Pivot: rows=ISRC, cols=period, vals=gross
    pivot_gross = monthly.pivot_table(
        index='identifier', columns='period', values='gross', aggfunc='sum', fill_value=0
    )

    # Total gross per ISRC
    isrc_meta['total_gross'] = isrc_meta['identifier'].map(pivot_gross.sum(axis=1))
    isrc_meta = isrc_meta.sort_values('total_gross', ascending=False).reset_index(drop=True)

    print(f"    {config.name}: {file_count} files, {len(isrc_meta):,} ISRCs, "
          f"${isrc_meta['total_gross'].sum():,.2f} total gross", flush=True)

    return PayorResult(
        config=config,
        isrc_meta=isrc_meta,
        monthly=monthly,
        detail=detail,
        pivot_gross=pivot_gross,
        by_distributor=by_distributor,
        file_count=file_count,
    )


def load_all_payors(configs: List[PayorConfig]) -> Dict[str, PayorResult]:
    """Load statements for all payors."""
    results = {}
    for cfg in configs:
        result = load_payor_statements(cfg)
        if result is not None:
            results[cfg.code] = result
    return results


# ---------------------------------------------------------------------------
# Consolidated Excel output (all payors combined)
# ---------------------------------------------------------------------------

def write_consolidated_excel(payor_results: Dict[str, PayorResult], output_path):
    """Write a single consolidated Excel with data from all payors."""
    print(f"\n  Writing consolidated data to: {output_path}", flush=True)

    all_clean = []
    all_summary = []
    all_monthly = []
    all_isrc_meta = []

    for code, pr in payor_results.items():
        payor_name = pr.config.name

        # Clean detail export
        clean = pr.detail[['statement_date', 'identifier', 'artist', 'title', 'product_title',
                           'distributor', 'download_type', 'gross', 'net']].copy()
        clean.columns = ['Statement Date', 'ISRC', 'Artist', 'Title', 'Product Title',
                         'Distributor', 'Download Type', 'Gross Royalties', 'Net Royalties']
        clean.insert(0, 'Payor', payor_name)
        all_clean.append(clean)

        # ISRC + month summary
        summary = pr.monthly.merge(
            pr.isrc_meta[['identifier', 'title', 'artist']], on='identifier', how='left'
        )
        summary = summary[['identifier', 'title', 'artist', 'period', 'statement_date', 'gross', 'net', 'sales']]
        summary.columns = ['ISRC', 'Title', 'Artist', 'Period', 'Statement Date', 'Gross', 'Net', 'Sales']
        summary.insert(0, 'Payor', payor_name)
        all_summary.append(summary)

        # Monthly totals
        mt = (
            pr.monthly.groupby(['period', 'statement_date'])
            .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
            .reset_index()
        )
        mt.columns = ['Period', 'Statement Date', 'Gross', 'Net', 'Sales']
        mt.insert(0, 'Payor', payor_name)
        all_monthly.append(mt)

        # ISRC metadata
        meta = pr.isrc_meta[['identifier', 'title', 'artist', 'product_title', 'total_gross']].copy()
        meta.insert(0, 'Payor', payor_name)
        all_isrc_meta.append(meta)

    combined_clean = pd.concat(all_clean, ignore_index=True).sort_values(['Statement Date', 'Payor', 'ISRC'])
    combined_summary = pd.concat(all_summary, ignore_index=True).sort_values(['Payor', 'ISRC', 'Period'])
    combined_monthly = pd.concat(all_monthly, ignore_index=True).sort_values(['Period', 'Payor'])
    combined_meta = pd.concat(all_isrc_meta, ignore_index=True)

    # Cross-payor top songs (deduped by ISRC, summed across payors)
    cross_payor = (
        combined_meta.groupby('identifier')
        .agg({'title': 'first', 'artist': 'first', 'total_gross': 'sum'})
        .reset_index()
        .sort_values('total_gross', ascending=False)
    )

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        combined_clean.to_excel(writer, sheet_name='Consolidated', index=False)
        combined_summary.to_excel(writer, sheet_name='By ISRC-Month', index=False)
        combined_monthly.to_excel(writer, sheet_name='Monthly Totals', index=False)

        # Per-payor distributor breakdown
        for code, pr in payor_results.items():
            sheet_name = f'Distributors_{code}'
            pr.by_distributor.to_excel(writer, sheet_name=sheet_name, index=False)

        # Top songs across all payors
        top = cross_payor.head(50).copy()
        top.columns = ['ISRC', 'Title', 'Artist', 'Total Gross']
        top.insert(0, 'Rank', range(1, len(top) + 1))
        top.to_excel(writer, sheet_name='Top 50 Songs', index=False)

    print(f"  Done. {len(combined_clean):,} rows in 'Consolidated'.", flush=True)
    return combined_clean


def write_consolidated_csv(payor_results: Dict[str, PayorResult], output_path):
    """Write consolidated detail as a single CSV file (standard schema)."""
    print(f"\n  Writing consolidated CSV to: {output_path}", flush=True)

    all_clean = []
    for code, pr in payor_results.items():
        clean = pr.detail[['statement_date', 'identifier', 'artist', 'title', 'product_title',
                           'distributor', 'download_type', 'gross', 'net']].copy()
        clean.columns = ['Statement Date', 'ISRC', 'Artist', 'Title', 'Product Title',
                         'Distributor', 'Download Type', 'Gross Royalties', 'Net Royalties']
        clean.insert(0, 'Payor', pr.config.name)
        all_clean.append(clean)

    combined = pd.concat(all_clean, ignore_index=True).sort_values(['Statement Date', 'Payor', 'ISRC'])
    combined.to_csv(output_path, index=False)
    print(f"  Done. {len(combined):,} rows.", flush=True)
    return combined


# ---------------------------------------------------------------------------
# Populate the financial model template
# ---------------------------------------------------------------------------

def clear_data_rows(ws, start_row, max_col=120):
    """Clear data from start_row to the last used row."""
    for row in range(start_row, ws.max_row + 1):
        for col in range(2, max_col + 1):
            ws.cell(row=row, column=col).value = None


def populate_model_tab(ws, pr: PayorResult, supplemental_meta=None):
    """Populate a single payor model tab with data.

    ws: the worksheet for this payor's model tab
    pr: PayorResult with aggregated data
    supplemental_meta: optional DataFrame with ISRC, release_date, artist_share,
                       third_party_share, reversion_date, license_term
    """
    config = pr.config
    isrc_meta = pr.isrc_meta
    pivot_gross = pr.pivot_gross

    # Set config cells
    ws['D25'] = config.name
    ws['D32'] = config.fee

    # Clear existing data rows (52+)
    clear_data_rows(ws, start_row=52, max_col=120)

    isrcs = isrc_meta['identifier'].tolist()
    periods = sorted(pivot_gross.columns.tolist())

    # Pre-compute column mapping
    col_map = {}
    for period in periods:
        year = int(str(period)[:4])
        month = int(str(period)[4:6])
        col_map[period] = month_to_column(year, month)

    # Build supplemental lookup
    supp = {}
    if supplemental_meta is not None and not supplemental_meta.empty:
        for _, row in supplemental_meta.iterrows():
            isrc = str(row.get('identifier', row.get('isrc', row.get('ISRC', '')))).strip()
            if isrc:
                supp[isrc] = row

    meta_dict = isrc_meta.set_index('identifier').to_dict('index')
    start_row = 52

    for i, isrc in enumerate(isrcs):
        row = start_row + i
        meta = meta_dict[isrc]

        # B: ISRC, C: Title, D: Artist
        ws.cell(row=row, column=2, value=isrc)
        ws.cell(row=row, column=3, value=meta['title'])
        ws.cell(row=row, column=4, value=meta['artist'])

        # Supplemental: G: Release Date, I: Reversion, J: Artist Share, K: 3P Share
        if isrc in supp:
            s = supp[isrc]
            release = s.get('release_date', s.get('Release Date'))
            if pd.notna(release):
                ws.cell(row=row, column=7, value=release)
            reversion = s.get('reversion_date', s.get('Reversion Date', s.get('reversion')))
            if pd.notna(reversion):
                ws.cell(row=row, column=9, value=reversion)
            artist_share = s.get('artist_share', s.get('Artist Share', s.get('Share PLYGRND')))
            if pd.notna(artist_share):
                ws.cell(row=row, column=10, value=float(artist_share))
            tp_share = s.get('third_party_share', s.get('Third Party Splits', s.get('3P Share')))
            if pd.notna(tp_share):
                ws.cell(row=row, column=11, value=float(tp_share))

        # Monthly gross earnings
        if isrc in pivot_gross.index:
            isrc_data = pivot_gross.loc[isrc]
            for period in periods:
                val = isrc_data.get(period, 0)
                if val and val != 0:
                    ws.cell(row=row, column=col_map[period], value=round(float(val), 6))

        if (i + 1) % 200 == 0:
            print(f"      {i + 1}/{len(isrcs)} ISRCs...", flush=True)

    print(f"    {config.code}_Model: {len(isrcs)} ISRCs, "
          f"{len(periods)} months ({min(periods)}-{max(periods)})", flush=True)


def populate_metadata_sheet(ws, payor_results: Dict[str, PayorResult], supplemental_meta=None):
    """Populate the combined Metadata sheet with all unique ISRCs across payors."""
    # Gather all ISRC metadata, deduped
    all_meta = []
    for code, pr in payor_results.items():
        meta = pr.isrc_meta[['identifier', 'title', 'artist', 'product_title']].copy()
        meta['total_gross'] = pr.isrc_meta['total_gross']
        all_meta.append(meta)

    combined = pd.concat(all_meta, ignore_index=True)
    # Keep the version with the highest total_gross for each ISRC
    combined = combined.sort_values('total_gross', ascending=False)
    combined = combined.drop_duplicates('identifier', keep='first').reset_index(drop=True)

    # Build supplemental lookup
    supp = {}
    if supplemental_meta is not None and not supplemental_meta.empty:
        for _, row in supplemental_meta.iterrows():
            isrc = str(row.get('identifier', row.get('isrc', row.get('ISRC', '')))).strip()
            if isrc:
                supp[isrc] = row

    # Clear existing data (row 5+)
    clear_data_rows(ws, start_row=5, max_col=12)

    for i, (_, row) in enumerate(combined.iterrows()):
        r = 5 + i
        isrc = row['identifier']
        ws.cell(row=r, column=2, value=isrc)                   # B: ISRC
        ws.cell(row=r, column=3, value=row['title'])            # C: Track Title
        ws.cell(row=r, column=4, value=row['artist'])           # D: Artists
        ws.cell(row=r, column=6, value=row['product_title'])    # F: Album Name

        if isrc in supp:
            s = supp[isrc]
            release = s.get('release_date', s.get('Release Date'))
            if pd.notna(release):
                ws.cell(row=r, column=5, value=release)         # E: Release Date
            license_term = s.get('license_term', s.get('License Term', s.get('Term')))
            if pd.notna(license_term):
                ws.cell(row=r, column=7, value=license_term)    # G: License Term
            reversion = s.get('reversion_date', s.get('Reversion Date'))
            if pd.notna(reversion):
                ws.cell(row=r, column=8, value=reversion)       # H: Reversion Date
            tp_share = s.get('third_party_share', s.get('Third Party Splits'))
            if pd.notna(tp_share):
                ws.cell(row=r, column=9, value=tp_share)        # I: Third Party Splits
            artist_share = s.get('artist_share', s.get('Artist/Label Share', s.get('Share PLYGRND')))
            if pd.notna(artist_share):
                ws.cell(row=r, column=10, value=artist_share)   # J: Artist/Label Share

    print(f"    Metadata: {len(combined)} unique ISRCs", flush=True)


def populate_template(template_path, output_path, payor_results: Dict[str, PayorResult],
                      supplemental_meta=None):
    """Copy template and populate all payor model tabs + metadata."""
    print(f"\n  Copying template to: {output_path}", flush=True)
    shutil.copy2(template_path, output_path)

    print("  Opening workbook (this may be slow for large templates)...", flush=True)
    wb = openpyxl.load_workbook(output_path)
    sheet_names = wb.sheetnames

    # Populate each payor's model tab
    for code, pr in payor_results.items():
        tab_name = f"{code}_Model"
        if tab_name in sheet_names:
            print(f"  Populating {tab_name}...", flush=True)
            populate_model_tab(wb[tab_name], pr, supplemental_meta)
        else:
            print(f"  WARNING: Tab '{tab_name}' not found in template. "
                  f"Available: {sheet_names}", flush=True)

    # Populate combined Metadata
    if 'Metadata' in sheet_names:
        print("  Populating Metadata...", flush=True)
        populate_metadata_sheet(wb['Metadata'], payor_results, supplemental_meta)

    print("  Saving workbook...", flush=True)
    wb.save(output_path)
    wb.close()
    print(f"  Saved: {output_path}", flush=True)


# ---------------------------------------------------------------------------
# Supplemental metadata loader
# ---------------------------------------------------------------------------

def load_supplemental_metadata(paths: List[str]) -> Optional[pd.DataFrame]:
    """Load supplemental metadata files (CSV or Excel) and combine them.

    Expected columns (flexible naming):
      ISRC, Release Date, Artist Share, Third Party Splits, Reversion Date, License Term
    """
    if not paths:
        return None

    frames = []
    for p in paths:
        if not os.path.exists(p):
            print(f"  WARNING: Metadata file not found: {p}", flush=True)
            continue
        if p.endswith('.csv'):
            df = pd.read_csv(p)
        elif p.endswith('.xlsx') or p.endswith('.xls'):
            df = pd.read_excel(p, sheet_name=0)
        else:
            continue

        # Normalize column names
        col_remap = {}
        for c in df.columns:
            cl = c.strip().lower()
            if cl in ('isrc', 'identifier'):
                col_remap[c] = 'identifier'
            elif 'release' in cl and 'date' in cl:
                col_remap[c] = 'release_date'
            elif 'artist' in cl and 'share' in cl:
                col_remap[c] = 'artist_share'
            elif 'share' in cl and 'plygrnd' in cl:
                col_remap[c] = 'artist_share'
            elif '3p' in cl or 'third' in cl:
                col_remap[c] = 'third_party_share'
            elif 'reversion' in cl:
                col_remap[c] = 'reversion_date'
            elif 'license' in cl or 'term' in cl:
                col_remap[c] = 'license_term'

        df = df.rename(columns=col_remap)
        frames.append(df)

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    if 'identifier' in combined.columns:
        combined['identifier'] = combined['identifier'].astype(str).str.strip()
        combined = combined.drop_duplicates('identifier', keep='first')
    return combined


# ---------------------------------------------------------------------------
# Analytics helpers (used by both CLI and web app)
# ---------------------------------------------------------------------------

def compute_analytics(payor_results: Dict[str, PayorResult]) -> dict:
    """Compute cross-payor analytics for the web app summary."""
    # Combine monthly data across all payors
    all_monthly = []
    all_meta = []
    total_files = 0

    for code, pr in payor_results.items():
        m = pr.monthly.copy()
        m['payor'] = pr.config.name
        all_monthly.append(m)

        meta = pr.isrc_meta.copy()
        meta['payor'] = pr.config.name
        all_meta.append(meta)

        total_files += pr.file_count

    monthly = pd.concat(all_monthly, ignore_index=True)
    meta = pd.concat(all_meta, ignore_index=True)

    # --- Basic stats ---
    total_gross = monthly['gross'].sum()
    total_net = monthly['net'].sum()
    periods = sorted(monthly['period'].unique().tolist())
    period_range = f"{min(periods)} - {max(periods)}" if periods else "N/A"

    # Unique ISRCs across all payors
    all_isrcs = set()
    for code, pr in payor_results.items():
        all_isrcs.update(pr.isrc_meta['identifier'].tolist())

    # --- Top songs (cross-payor) ---
    cross = (
        meta.groupby('identifier')
        .agg({'title': 'first', 'artist': 'first', 'total_gross': 'sum'})
        .reset_index()
        .sort_values('total_gross', ascending=False)
    )
    top_songs = []
    for _, row in cross.head(10).iterrows():
        top_songs.append({
            'isrc': str(row['identifier']),
            'artist': str(row['artist'])[:30],
            'title': str(row['title'])[:40],
            'gross': f"{row['total_gross']:,.2f}",
        })

    # --- Annual gross earnings ---
    monthly['year'] = monthly['period'].astype(str).str[:4].astype(int)
    annual = (
        monthly.groupby('year')
        .agg({'gross': 'sum', 'net': 'sum'})
        .reset_index()
        .sort_values('year')
    )
    annual_earnings = []
    for _, row in annual.iterrows():
        annual_earnings.append({
            'year': int(row['year']),
            'gross': f"{row['gross']:,.2f}",
            'net': f"{row['net']:,.2f}",
        })

    # --- LTM (Last Twelve Months) earnings by song ---
    max_period = max(periods)
    max_year = int(str(max_period)[:4])
    max_month = int(str(max_period)[4:6])
    # Go back 12 months
    ltm_start_year = max_year - 1 if max_month < 12 else max_year
    ltm_start_month = max_month + 1 if max_month < 12 else 1
    if max_month == 12:
        ltm_start_year = max_year
        ltm_start_month = 1
    else:
        ltm_start_year = max_year - 1
        ltm_start_month = max_month + 1
    ltm_start_period = ltm_start_year * 100 + ltm_start_month

    ltm_monthly = monthly[monthly['period'] >= ltm_start_period]
    ltm_by_song = (
        ltm_monthly.groupby('identifier')
        .agg({'gross': 'sum'})
        .reset_index()
    )
    # Merge with metadata for titles
    ltm_by_song = ltm_by_song.merge(
        cross[['identifier', 'title', 'artist']], on='identifier', how='left'
    )
    ltm_by_song = ltm_by_song.sort_values('gross', ascending=False)

    ltm_songs = []
    for _, row in ltm_by_song.head(20).iterrows():
        ltm_songs.append({
            'isrc': str(row['identifier']),
            'artist': str(row['artist'])[:30],
            'title': str(row['title'])[:40],
            'gross': f"{row['gross']:,.2f}",
        })

    # --- YoY decay analysis ---
    # Calculate year-over-year growth rates for the catalog
    annual_totals = annual.set_index('year')['gross']
    yoy_decay = []
    years_list = sorted(annual_totals.index.tolist())
    for i in range(1, len(years_list)):
        prev_year = years_list[i - 1]
        curr_year = years_list[i]
        prev_val = annual_totals[prev_year]
        curr_val = annual_totals[curr_year]
        if prev_val > 0:
            pct_change = (curr_val - prev_val) / prev_val * 100
        else:
            pct_change = 0
        yoy_decay.append({
            'period': f"{prev_year} → {curr_year}",
            'prev_gross': f"{prev_val:,.2f}",
            'curr_gross': f"{curr_val:,.2f}",
            'change_pct': f"{pct_change:+.1f}%",
        })

    # --- Per-payor summary ---
    payor_summaries = []
    for code, pr in payor_results.items():
        payor_summaries.append({
            'code': code,
            'name': pr.config.name,
            'files': pr.file_count,
            'isrcs': len(pr.isrc_meta),
            'total_gross': f"{pr.isrc_meta['total_gross'].sum():,.2f}",
            'fee': f"{pr.config.fee:.0%}",
            'fx': pr.config.fx_currency,
        })

    # --- Monthly trend (all payors combined, for charts) ---
    monthly_agg = (
        monthly.groupby('period')
        .agg({'gross': 'sum', 'net': 'sum'})
        .reset_index()
        .sort_values('period')
    )
    monthly_trend = []
    for _, row in monthly_agg.iterrows():
        p = int(row['period'])
        yr = int(str(p)[:4])
        mo = int(str(p)[4:6])
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        label = f"{month_names[mo]} {yr}"
        monthly_trend.append({
            'period': p,
            'label': label,
            'gross': round(float(row['gross']), 2),
            'net': round(float(row['net']), 2),
        })

    # --- Monthly trend per payor (for stacked charts) ---
    monthly_by_payor = {}
    for code, pr in payor_results.items():
        payor_monthly = (
            pr.monthly.groupby('period')
            .agg({'gross': 'sum', 'net': 'sum'})
            .reset_index()
            .sort_values('period')
        )
        entries = []
        for _, row in payor_monthly.iterrows():
            entries.append({
                'period': int(row['period']),
                'gross': round(float(row['gross']), 2),
                'net': round(float(row['net']), 2),
            })
        monthly_by_payor[code] = entries

    # --- LTM by payor ---
    ltm_by_payor = []
    for code, pr in payor_results.items():
        payor_ltm = pr.monthly[pr.monthly['period'] >= ltm_start_period]
        ltm_gross = float(payor_ltm['gross'].sum())
        ltm_net = float(payor_ltm['net'].sum())
        ltm_by_payor.append({
            'code': code,
            'name': pr.config.name,
            'ltm_gross': round(ltm_gross, 2),
            'ltm_gross_fmt': f"{ltm_gross:,.2f}",
            'ltm_net': round(ltm_net, 2),
            'ltm_net_fmt': f"{ltm_net:,.2f}",
        })

    # --- Annual by payor (for stacked bar chart) ---
    annual_by_payor = {}
    for code, pr in payor_results.items():
        pm = pr.monthly.copy()
        pm['year'] = pm['period'].astype(str).str[:4].astype(int)
        pa = pm.groupby('year').agg({'gross': 'sum'}).reset_index().sort_values('year')
        annual_by_payor[code] = [
            {'year': int(r['year']), 'gross': round(float(r['gross']), 2)}
            for _, r in pa.iterrows()
        ]

    # --- Top distributors across all payors ---
    all_dist = []
    for code, pr in payor_results.items():
        d = pr.by_distributor.copy()
        d.columns = ['distributor', 'gross', 'net', 'sales']
        all_dist.append(d)
    if all_dist:
        combined_dist = pd.concat(all_dist, ignore_index=True)
        top_distributors = (
            combined_dist.groupby('distributor')
            .agg({'gross': 'sum', 'net': 'sum', 'sales': 'sum'})
            .reset_index()
            .sort_values('gross', ascending=False)
        )
        dist_list = []
        for _, row in top_distributors.head(15).iterrows():
            dist_list.append({
                'name': str(row['distributor']),
                'gross': round(float(row['gross']), 2),
                'gross_fmt': f"{row['gross']:,.2f}",
                'sales': int(row['sales']),
            })
    else:
        dist_list = []

    return {
        'total_files': total_files,
        'isrc_count': f"{len(all_isrcs):,}",
        'isrc_count_raw': len(all_isrcs),
        'total_gross': f"{total_gross:,.2f}",
        'total_gross_raw': round(float(total_gross), 2),
        'total_net': f"{total_net:,.2f}",
        'total_net_raw': round(float(total_net), 2),
        'period_range': period_range,
        'top_songs': top_songs,
        'annual_earnings': annual_earnings,
        'ltm_songs': ltm_songs,
        'yoy_decay': yoy_decay,
        'payor_summaries': payor_summaries,
        'monthly_trend': monthly_trend,
        'monthly_by_payor': monthly_by_payor,
        'ltm_by_payor': ltm_by_payor,
        'annual_by_payor': annual_by_payor,
        'top_distributors': dist_list,
    }


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_summary(payor_results: Dict[str, PayorResult]):
    """Print a summary to the console."""
    analytics = compute_analytics(payor_results)

    print("\n" + "=" * 65)
    print("  CONSOLIDATION SUMMARY")
    print("=" * 65)
    print(f"  Total files processed:  {analytics['total_files']}")
    print(f"  Unique ISRCs (all):     {analytics['isrc_count']}")
    print(f"  Date range:             {analytics['period_range']}")
    print(f"  Total gross (USD):      ${analytics['total_gross']}")
    print(f"  Total net (USD):        ${analytics['total_net']}")

    print("\n  PAYORS:")
    print("  " + "-" * 61)
    for ps in analytics['payor_summaries']:
        print(f"  {ps['name']:<20s}  {ps['files']:>3} files  {ps['isrcs']:>5} ISRCs  "
              f"${ps['total_gross']:>14s}  fee={ps['fee']}  {ps['fx']}")

    print("\n  ANNUAL GROSS EARNINGS:")
    print("  " + "-" * 40)
    for ae in analytics['annual_earnings']:
        print(f"    {ae['year']}:  ${ae['gross']:>14s}")

    print("\n  YoY CATALOG DECAY:")
    print("  " + "-" * 50)
    for d in analytics['yoy_decay']:
        print(f"    {d['period']}:  {d['change_pct']:>8s}  (${d['prev_gross']} → ${d['curr_gross']})")

    print("\n  TOP 10 EARNING SONGS (all payors):")
    print("  " + "-" * 61)
    for idx, song in enumerate(analytics['top_songs']):
        print(f"  {idx+1:>3}. ${song['gross']:>14s}  {song['artist']:<20s} - {song['title']}")
    print("=" * 65)


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

# Default payor configs (can be overridden via CLI or web app)
DEFAULT_PAYORS = [
    PayorConfig(
        code='B1', name='Believe 15%', fmt='believe', fee=0.15,
        fx_currency='EUR', fx_rate=1.0,  # Keep in EUR; model handles FX
        statements_dir=r'C:\Users\jacques\Downloads\Believe_15_extracted',
    ),
    PayorConfig(
        code='B2', name='Believe 20%', fmt='believe', fee=0.20,
        fx_currency='EUR', fx_rate=1.0,
        statements_dir=r'C:\Users\jacques\Downloads\Believe_20_extracted',
    ),
    PayorConfig(
        code='RJ', name='RecordJet', fmt='recordjet', fee=0.07,
        fx_currency='EUR', fx_rate=1.0,
        statements_dir=r'C:\Users\jacques\Downloads\RecordJet_extracted',
    ),
]


def main():
    parser = argparse.ArgumentParser(description='Multi-payor royalty statement consolidator.')
    parser.add_argument('--template', default=r'C:\Users\jacques\Documents\202601_PLYGRND Model_v3.xlsx',
                        help='Path to the financial model template (with payor tabs)')
    parser.add_argument('--output', default=None, help='Path for populated model output')
    parser.add_argument('--consolidated', default=None, help='Path for consolidated data Excel')
    parser.add_argument('--metadata', nargs='*', default=[], help='Supplemental metadata files')

    args = parser.parse_args()

    out_dir = os.path.dirname(args.template)
    if args.output is None:
        args.output = os.path.join(out_dir, 'PLYGRND_Model_populated.xlsx')
    if args.consolidated is None:
        args.consolidated = os.path.join(out_dir, 'Consolidated_All_Payors.xlsx')

    print("\n  ROYALTY STATEMENT CONSOLIDATOR (Multi-Payor)")
    print("  " + "=" * 50)
    for cfg in DEFAULT_PAYORS:
        print(f"  {cfg.code}: {cfg.name} ({cfg.fmt}) fee={cfg.fee:.0%} dir={cfg.statements_dir}")
    print()

    # Step 1: Load all payors
    print("  [1/4] Parsing statement files for all payors...")
    payor_results = load_all_payors(DEFAULT_PAYORS)

    if not payor_results:
        print("  ERROR: No payor data loaded.")
        sys.exit(1)

    # Step 2: Write consolidated Excel
    print("\n  [2/4] Writing consolidated Excel...")
    write_consolidated_excel(payor_results, args.consolidated)

    # Step 3: Load supplemental metadata
    supplemental = None
    if args.metadata:
        print("\n  [2.5] Loading supplemental metadata...")
        supplemental = load_supplemental_metadata(args.metadata)
        if supplemental is not None:
            print(f"    Loaded {len(supplemental)} metadata records.", flush=True)

    # Step 4: Populate template
    print("\n  [3/4] Populating financial model template...")
    populate_template(args.template, args.output, payor_results, supplemental)

    # Step 5: Summary
    print_summary(payor_results)

    print(f"\n  OUTPUT FILES:")
    print(f"    Consolidated: {args.consolidated}")
    print(f"    Model:        {args.output}")
    print()


if __name__ == '__main__':
    main()
