"""
Statement Ingestion Mapper
Detects headers, proposes column mappings, runs QC, and remembers mappings via SQLite.
"""

import csv
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from consolidator import COLUMN_PATTERNS

log = logging.getLogger('royalty')

# Lazy import to avoid circular dependency at module load time
_db_mod = None

def _db():
    """Lazy-load db module and check availability."""
    global _db_mod
    if _db_mod is None:
        try:
            import db as _d
            _db_mod = _d
        except ImportError:
            return None
    return _db_mod if _db_mod.is_available() else None

# Canonical schema fields
CANONICAL_FIELDS = [
    'identifier', 'iswc', 'upc', 'other_identifier',
    'title', 'artist', 'product_title', 'store',
    'media_type', 'period', 'gross', 'net', 'fees', 'sales',
    'country', 'release_date',
]

REQUIRED_FIELDS = {'identifier', 'gross'}

NUMERIC_FIELDS = {'gross', 'net', 'fees', 'sales'}

# Phase 2: Extended mapping options including percent fields for waterfall
PHASE2_MAPPING_OPTIONS = CANONICAL_FIELDS + ['fee_pct']

PERCENT_NUMERIC_FIELDS = set()

ALL_NUMERIC_FIELDS = NUMERIC_FIELDS | PERCENT_NUMERIC_FIELDS

DB_NAME = 'mappings.db'


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _db_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_NAME)


_sqlite_available = False


def _get_conn():
    if not _sqlite_available:
        raise RuntimeError("SQLite not available")
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def init_db():
    """Create tables if they don't exist. Returns True on success."""
    global _sqlite_available
    try:
        conn = sqlite3.connect(_db_path())
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS fingerprints (
                fingerprint TEXT PRIMARY KEY,
                column_names TEXT NOT NULL,
                mapping TEXT NOT NULL,
                source_label TEXT DEFAULT '',
                use_count INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS synonyms (
                raw_name TEXT PRIMARY KEY,
                canonical TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS import_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                fingerprint TEXT,
                mapping_used TEXT,
                row_count INTEGER DEFAULT 0,
                qc_warnings INTEGER DEFAULT 0,
                qc_errors INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL
            );
        """)
        conn.commit()
        conn.close()
        _sqlite_available = True
    except Exception as e:
        import logging
        logging.getLogger('royalty').warning("SQLite unavailable (read-only filesystem?): %s", e)
        _sqlite_available = False
    return _sqlite_available


init_db()


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

def compute_fingerprint(cols: List[str]) -> str:
    """SHA-256 of sorted lowercase column names."""
    normalized = sorted(c.strip().lower() for c in cols if c.strip())
    payload = '|'.join(normalized)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# Header detection
# ---------------------------------------------------------------------------

def _sniff_csv_encoding(filepath: str) -> str:
    """Detect CSV encoding by reading raw bytes once and testing decodes."""
    try:
        with open(filepath, 'rb') as f:
            raw = f.read()
    except IOError:
        return 'utf-8'
    for enc in ('utf-8-sig', 'utf-8'):
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    # latin-1 never fails (all bytes 0-255 are valid), so use it as fallback
    return 'latin-1'


def _pick_best_sheet(sheet_names: List[str]) -> str:
    """Auto-select the most likely line-item data sheet from an Excel workbook.

    Prefers sheets whose names suggest digital royalty detail data.
    Falls back to the first sheet if nothing matches.
    """
    # Keywords that suggest line-item royalty data (ordered by priority)
    PREFER_KW = [
        'digital sales', 'digital', 'streaming', 'detail', 'royalt',
        'line item', 'lineitem', 'transaction', 'sales', 'download',
        'mechanical', 'master',
    ]
    # Keywords that suggest summary / non-data tabs we want to skip
    SKIP_KW = [
        'summary', 'payment', 'invoice', 'cover', 'index', 'totals',
        'instructions', 'notes', 'template', 'info',
    ]

    lower_names = [(s, s.lower().strip()) for s in sheet_names]

    # First pass: pick the highest-priority PREFER keyword match
    for kw in PREFER_KW:
        for original, low in lower_names:
            if kw in low:
                return original

    # Second pass: pick the first sheet that isn't a SKIP sheet
    for original, low in lower_names:
        if not any(sk in low for sk in SKIP_KW):
            return original

    # Fallback: first sheet
    return sheet_names[0]


def detect_headers(filepath: str, sheet: Optional[str] = None) -> dict:
    """Read first 30 rows, find the header row by string-cell ratio.

    Returns dict with keys:
        headers: list of column name strings
        header_row: 0-based index of the detected header row
        preview_rows: list of lists (first 30 rows of raw data)
        sheets: list of sheet names (for Excel files) or None
    """
    ext = os.path.splitext(filepath)[1].lower()

    raw_rows: List[List[Any]] = []
    sheets: Optional[List[str]] = None

    if ext in ('.xlsx', '.xls', '.xlsb'):
        engine = 'pyxlsb' if ext == '.xlsb' else None
        xls = pd.ExcelFile(filepath, engine=engine)
        sheets = xls.sheet_names
        target_sheet = sheet if sheet and sheet in sheets else _pick_best_sheet(sheets)
        df_raw = pd.read_excel(filepath, sheet_name=target_sheet, header=None, nrows=30, dtype=str, engine=engine)
        raw_rows = df_raw.fillna('').values.tolist()
    elif ext == '.csv':
        enc = _sniff_csv_encoding(filepath)
        with open(filepath, encoding=enc, newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 30:
                    break
                raw_rows.append(row)
    else:
        return {'headers': [], 'header_row': 0, 'preview_rows': [], 'sheets': None}

    if not raw_rows:
        return {'headers': [], 'header_row': 0, 'preview_rows': [], 'sheets': sheets}

    # Find the row with the highest ratio of non-empty string cells
    # Use column count as tiebreaker to prefer wider header rows over single-cell titles
    best_row = 0
    best_score = (-1, -1)  # (quality_score, column_count)
    for idx, row in enumerate(raw_rows):
        if not row:
            continue
        total = len(row)
        non_empty_cells = sum(1 for c in row if str(c).strip())
        string_cells = sum(
            1 for c in row
            if str(c).strip() and not _is_numeric(str(c).strip())
        )
        # Prefer rows where most cells are non-empty text (likely header labels)
        ratio = string_cells / max(total, 1)
        non_empty = non_empty_cells / max(total, 1)
        score = ratio * 0.7 + non_empty * 0.3
        # Use (score, column_count) so wider rows win ties
        candidate = (score, non_empty_cells)
        if candidate > best_score:
            best_score = candidate
            best_row = idx

    headers = [str(c).strip() for c in raw_rows[best_row]] if raw_rows else []

    return {
        'headers': headers,
        'header_row': best_row,
        'preview_rows': raw_rows,
        'sheets': sheets,
    }


def _is_numeric(s: str) -> bool:
    """Check if a string looks numeric."""
    s = s.replace(',', '').replace(' ', '')
    try:
        float(s)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Mapping proposal
# ---------------------------------------------------------------------------

def propose_mapping(cols: List[str]) -> Dict[str, dict]:
    """Propose a mapping from source columns to canonical fields.

    Returns dict keyed by source column name, each value is:
        {'canonical': str or '', 'confidence': float}

    Strategy:
        1. DB fingerprint match → confidence 1.0
        2. DB synonyms → confidence 0.9
        3. COLUMN_PATTERNS from consolidator → confidence 0.7
        4. Unmapped → confidence 0.0
    """
    fp = compute_fingerprint(cols)

    result = {}

    # 1. Check fingerprint — try PostgreSQL first, then SQLite
    saved_mapping = None
    dbm = _db()
    if dbm:
        try:
            saved_mapping = dbm.get_fingerprint_mapping_db(fp)
        except Exception as e:
            log.debug("DB fingerprint lookup failed: %s", e)

    if saved_mapping is None and _sqlite_available:
        try:
            conn = _get_conn()
            row = conn.execute(
                'SELECT mapping FROM fingerprints WHERE fingerprint = ?', (fp,)
            ).fetchone()
            if row:
                saved_mapping = json.loads(row['mapping'])
            conn.close()
        except Exception:
            pass

    if saved_mapping:
        for col in cols:
            canonical = saved_mapping.get(col, '')
            result[col] = {'canonical': canonical, 'confidence': 1.0 if canonical else 0.0}
        return result

    # Build a set of already-assigned canonical fields to avoid duplicates
    assigned: set = set()

    # 2. Check synonyms — try PostgreSQL first, then SQLite
    for col in cols:
        raw_lower = col.strip().lower()
        canonical = None
        if dbm:
            try:
                canonical = dbm.get_synonyms_db(raw_lower)
            except Exception as e:
                log.debug("DB synonym lookup failed for '%s': %s", raw_lower, e)
        if canonical is None and _sqlite_available:
            try:
                conn = _get_conn()
                syn_row = conn.execute(
                    'SELECT canonical FROM synonyms WHERE raw_name = ?', (raw_lower,)
                ).fetchone()
                if syn_row:
                    canonical = syn_row['canonical']
                conn.close()
            except Exception:
                pass
        if canonical and canonical not in assigned:
            result[col] = {'canonical': canonical, 'confidence': 0.9}
            assigned.add(canonical)

    # 3. COLUMN_PATTERNS fuzzy match
    # For each column, find all potential matches and pick the longest pattern
    # (most specific). This prevents e.g. "accounting period" matching sales
    # via the short pattern "count" instead of period via "accounting period".
    for col in cols:
        if col in result:
            continue
        col_lower = col.strip().lower()
        best_canonical = None
        best_pattern_len = -1
        for canonical, patterns in COLUMN_PATTERNS.items():
            if canonical in assigned:
                continue
            if canonical not in CANONICAL_FIELDS:
                continue
            for pattern in patterns:
                if pattern == col_lower or pattern in col_lower:
                    if len(pattern) > best_pattern_len:
                        best_pattern_len = len(pattern)
                        best_canonical = canonical
                    break  # first matching pattern per canonical field
        if best_canonical:
            result[col] = {'canonical': best_canonical, 'confidence': 0.7}
            assigned.add(best_canonical)

    # 4. Unmapped
    for col in cols:
        if col not in result:
            result[col] = {'canonical': '', 'confidence': 0.0}

    return result


def get_fingerprint_mapping(cols: List[str]) -> Optional[Dict[str, str]]:
    """Check if a fingerprint match exists. Returns saved mapping dict or None."""
    fp = compute_fingerprint(cols)
    # Try PostgreSQL first
    dbm = _db()
    if dbm:
        try:
            result = dbm.get_fingerprint_mapping_db(fp)
            if result is not None:
                return result
        except Exception as e:
            log.debug("DB fingerprint lookup failed: %s", e)
    # Fall back to SQLite
    if _sqlite_available:
        try:
            conn = _get_conn()
            row = conn.execute(
                'SELECT mapping FROM fingerprints WHERE fingerprint = ?', (fp,)
            ).fetchone()
            conn.close()
            if row:
                return json.loads(row['mapping'])
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Phase 2: Cleaning preview
# ---------------------------------------------------------------------------

def apply_cleaning(filepath: str, remove_top: int = 0, remove_bottom: int = 0,
                   sheet: Optional[str] = None, header_row: Optional[int] = None) -> dict:
    """Read file, optionally drop rows from top/bottom, return preview data.

    Returns dict with keys:
        headers: list of column name strings
        preview_rows: list of lists (first 15 data rows after cleaning)
        total_rows: int (total data rows after cleaning)
        header_row: int (detected or provided header row index)
    """
    # First detect headers if not specified
    if header_row is None:
        detection = detect_headers(filepath, sheet=sheet)
        header_row = detection['header_row']

    ext = os.path.splitext(filepath)[1].lower()
    if ext in ('.xlsx', '.xls', '.xlsb'):
        engine = 'pyxlsb' if ext == '.xlsb' else None
        if not sheet:
            xls = pd.ExcelFile(filepath, engine=engine)
            sheet = _pick_best_sheet(xls.sheet_names)
        df = pd.read_excel(filepath, sheet_name=sheet, header=header_row, dtype=str, engine=engine)
    elif ext == '.csv':
        enc = _sniff_csv_encoding(filepath)
        # Use skiprows to avoid C parser errors from inconsistent field counts
        # in rows before the header (e.g. summary sections in Empire CSVs)
        skip = list(range(header_row)) if header_row and header_row > 0 else None
        try:
            df = pd.read_csv(filepath, header=0 if skip else header_row,
                             skiprows=skip, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            df = pd.read_csv(filepath, header=0 if skip else header_row,
                             skiprows=skip, dtype=str, encoding='latin-1')
        except pd.errors.ParserError:
            try:
                df = pd.read_csv(filepath, header=0 if skip else header_row,
                                 skiprows=skip, dtype=str, encoding=enc, on_bad_lines='skip')
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, header=0 if skip else header_row,
                                 skiprows=skip, dtype=str, encoding='latin-1', on_bad_lines='skip')
    else:
        return {'headers': [], 'preview_rows': [], 'total_rows': 0, 'header_row': 0}

    df.columns = [str(c).strip() for c in df.columns]

    # Drop rows from top
    if remove_top > 0 and remove_top < len(df):
        df = df.iloc[remove_top:]

    # Drop rows from bottom
    if remove_bottom > 0 and remove_bottom < len(df):
        df = df.iloc[:-remove_bottom]

    df = df.reset_index(drop=True)

    headers = list(df.columns)
    preview_rows = df.head(15).fillna('').values.tolist()

    return {
        'headers': headers,
        'preview_rows': preview_rows,
        'total_rows': len(df),
        'header_row': header_row,
    }


# ---------------------------------------------------------------------------
# Apply mapping
# ---------------------------------------------------------------------------

def apply_mapping(filepath: str, mapping: Dict[str, str], header_row: int,
                  sheet: Optional[str] = None) -> pd.DataFrame:
    """Read file starting at header_row, rename columns per mapping, coerce types.

    mapping: {source_col_name: canonical_field_name}
    Returns a DataFrame with canonical column names.
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext in ('.xlsx', '.xls', '.xlsb'):
        engine = 'pyxlsb' if ext == '.xlsb' else None
        if not sheet:
            xls = pd.ExcelFile(filepath, engine=engine)
            sheet = _pick_best_sheet(xls.sheet_names)
        df = pd.read_excel(filepath, sheet_name=sheet, header=header_row, dtype=str, engine=engine)
    elif ext == '.csv':
        enc = _sniff_csv_encoding(filepath)
        skip = list(range(header_row)) if header_row and header_row > 0 else None
        try:
            df = pd.read_csv(filepath, header=0 if skip else header_row,
                             skiprows=skip, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            df = pd.read_csv(filepath, header=0 if skip else header_row,
                             skiprows=skip, dtype=str, encoding='latin-1')
        except pd.errors.ParserError:
            try:
                df = pd.read_csv(filepath, header=0 if skip else header_row,
                                 skiprows=skip, dtype=str, encoding=enc, on_bad_lines='skip')
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, header=0 if skip else header_row,
                                 skiprows=skip, dtype=str, encoding='latin-1', on_bad_lines='skip')
    else:
        df = pd.DataFrame()

    df.columns = [str(c).strip() for c in df.columns]

    # Build rename dict (only mapped columns — including Phase 2 percent fields)
    all_known = set(CANONICAL_FIELDS) | PERCENT_NUMERIC_FIELDS
    rename = {}
    for src, canonical in mapping.items():
        if canonical and canonical in all_known and src in df.columns:
            rename[src] = canonical

    df = df.rename(columns=rename)

    # Keep only canonical + percent columns that exist
    keep = [c for c in CANONICAL_FIELDS if c in df.columns]
    keep += [c for c in PERCENT_NUMERIC_FIELDS if c in df.columns]
    df = df[keep].copy()

    # Add missing canonical columns
    for c in CANONICAL_FIELDS:
        if c not in df.columns:
            df[c] = '' if c not in NUMERIC_FIELDS else 0

    # Coerce numeric fields (including percent fields)
    for c in NUMERIC_FIELDS | PERCENT_NUMERIC_FIELDS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0)

    # Coerce period
    if 'period' in df.columns:
        df['period'] = (
            df['period'].astype(str)
            .str.replace(r'[^0-9]', '', regex=True)
            .apply(lambda x: x[:6] if len(x) >= 6 else x)
        )

    # Strip string fields
    for c in ['identifier', 'iswc', 'upc', 'other_identifier', 'title', 'artist',
              'product_title', 'store', 'media_type', 'country', 'release_date']:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


# ---------------------------------------------------------------------------
# QC
# ---------------------------------------------------------------------------

@dataclass
class QCIssue:
    severity: str       # 'ERROR' or 'WARNING'
    check: str          # short name
    message: str        # human-readable
    row_indices: List[int] = field(default_factory=list)
    count: int = 0


@dataclass
class QCResult:
    total_rows: int = 0
    valid_rows: int = 0
    issues: List[QCIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(i.count for i in self.issues if i.severity == 'ERROR')

    @property
    def warning_count(self) -> int:
        return sum(i.count for i in self.issues if i.severity == 'WARNING')

    @property
    def has_errors(self) -> bool:
        return self.error_count > 0


def run_qc(df: pd.DataFrame) -> QCResult:
    """Run quality checks on the mapped DataFrame."""
    result = QCResult(total_rows=len(df))
    issues = []

    # 1. Missing identifier (ERROR)
    if 'identifier' in df.columns:
        mask = df['identifier'].isin(['', 'nan', 'None', 'NaN']) | df['identifier'].isna()
        bad = mask.sum()
        if bad > 0:
            issues.append(QCIssue(
                severity='ERROR',
                check='missing_identifier',
                message=f'{bad} rows have missing identifier',
                row_indices=df.index[mask].tolist()[:50],
                count=int(bad),
            ))

    # 2. Numeric parse failures (WARNING) for gross/net/sales
    for col in NUMERIC_FIELDS:
        if col in df.columns:
            zeros = (df[col] == 0).sum()
            total = len(df)
            # If original was non-empty but parsed to 0, that's a warning
            # We already coerced, so just check if everything is 0
            if col == 'gross' and zeros == total and total > 0:
                issues.append(QCIssue(
                    severity='WARNING',
                    check=f'{col}_all_zero',
                    message=f'All {col} values are zero — possible parse failure',
                    count=int(total),
                ))

    # 3. Duplicate identifier+period (WARNING)
    if 'identifier' in df.columns and 'period' in df.columns:
        dupes = df.duplicated(subset=['identifier', 'period'], keep=False)
        dupe_count = dupes.sum()
        if dupe_count > 0:
            issues.append(QCIssue(
                severity='WARNING',
                check='duplicate_id_period',
                message=f'{dupe_count} rows have duplicate identifier+period combinations',
                row_indices=df.index[dupes].tolist()[:50],
                count=int(dupe_count),
            ))

    # 4. Invalid period (ERROR) — not valid YYYYMM
    if 'period' in df.columns:
        def _valid_period(p):
            s = str(p).strip()
            if len(s) < 6:
                return False
            s = s[:6]
            if not s.isdigit():
                return False
            year = int(s[:4])
            month = int(s[4:6])
            return 2000 <= year <= 2099 and 1 <= month <= 12

        invalid_mask = ~df['period'].apply(_valid_period)
        # Exclude rows where period is empty (might just be unmapped)
        non_empty_period = df['period'].astype(str).str.strip().ne('')
        invalid_mask = invalid_mask & non_empty_period
        bad = invalid_mask.sum()
        if bad > 0:
            issues.append(QCIssue(
                severity='ERROR',
                check='invalid_period',
                message=f'{bad} rows have invalid period format (expected YYYYMM)',
                row_indices=df.index[invalid_mask].tolist()[:50],
                count=int(bad),
            ))

    # 5. Negative gross/net (WARNING)
    for col in ('gross', 'net'):
        if col in df.columns:
            neg_mask = df[col] < 0
            neg_count = neg_mask.sum()
            if neg_count > 0:
                issues.append(QCIssue(
                    severity='WARNING',
                    check=f'negative_{col}',
                    message=f'{neg_count} rows have negative {col} values',
                    row_indices=df.index[neg_mask].tolist()[:50],
                    count=int(neg_count),
                ))

    # 6. All-zero rows (WARNING)
    if all(c in df.columns for c in NUMERIC_FIELDS):
        zero_mask = (df['gross'] == 0) & (df['net'] == 0) & (df['sales'] == 0)
        zero_count = zero_mask.sum()
        if zero_count > 0:
            issues.append(QCIssue(
                severity='WARNING',
                check='all_zero_rows',
                message=f'{zero_count} rows have all-zero gross/net/sales',
                row_indices=df.index[zero_mask].tolist()[:50],
                count=int(zero_count),
            ))

    # 7. Very few rows (WARNING)
    if len(df) < 10:
        issues.append(QCIssue(
            severity='WARNING',
            check='few_rows',
            message=f'File has only {len(df)} rows — verify this is complete',
            count=len(df),
        ))

    result.issues = issues
    error_rows = set()
    for issue in issues:
        if issue.severity == 'ERROR':
            error_rows.update(issue.row_indices)
    result.valid_rows = len(df) - len(error_rows)

    return result


# ---------------------------------------------------------------------------
# Save / persist
# ---------------------------------------------------------------------------

def save_mapping(fingerprint: str, cols: List[str], mapping: Dict[str, str],
                 source_label: str = ''):
    """Upsert mapping to fingerprints table (PostgreSQL + SQLite)."""
    # Save to PostgreSQL if available
    dbm = _db()
    if dbm:
        try:
            dbm.save_mapping_db(fingerprint, cols, mapping, source_label)
        except Exception as e:
            log.warning("DB save_mapping failed: %s", e)

    # Save to SQLite as fallback
    if _sqlite_available:
        try:
            conn = _get_conn()
            now = datetime.now().isoformat()
            col_json = json.dumps(cols)
            map_json = json.dumps(mapping)

            existing = conn.execute(
                'SELECT use_count FROM fingerprints WHERE fingerprint = ?', (fingerprint,)
            ).fetchone()

            if existing:
                conn.execute("""
                    UPDATE fingerprints
                    SET mapping = ?, column_names = ?, source_label = ?, updated_at = ?,
                        use_count = use_count + 1
                    WHERE fingerprint = ?
                """, (map_json, col_json, source_label, now, fingerprint))
            else:
                conn.execute("""
                    INSERT INTO fingerprints (fingerprint, column_names, mapping, source_label,
                                              use_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                """, (fingerprint, col_json, map_json, source_label, now, now))

            conn.commit()
            conn.close()
        except Exception:
            pass


def save_synonyms(mapping: Dict[str, str]):
    """Extract user corrections into synonyms table (PostgreSQL + SQLite)."""
    # Save to PostgreSQL if available
    dbm = _db()
    if dbm:
        try:
            dbm.save_synonyms_db(mapping)
        except Exception as e:
            log.warning("DB save_synonyms failed: %s", e)

    # Save to SQLite as fallback
    if _sqlite_available:
        try:
            conn = _get_conn()
            for raw_col, canonical in mapping.items():
                if not canonical or canonical not in CANONICAL_FIELDS:
                    continue
                raw_lower = raw_col.strip().lower()
                if not raw_lower:
                    continue
                conn.execute(
                    'INSERT OR REPLACE INTO synonyms (raw_name, canonical) VALUES (?, ?)',
                    (raw_lower, canonical)
                )
            conn.commit()
            conn.close()
        except Exception:
            pass


def increment_fingerprint_use(fingerprint: str):
    """Increment use_count for an existing fingerprint (PostgreSQL + SQLite)."""
    dbm = _db()
    if dbm:
        try:
            dbm.increment_fingerprint_use_db(fingerprint)
        except Exception as e:
            log.debug("DB increment_fingerprint_use failed: %s", e)

    if _sqlite_available:
        try:
            conn = _get_conn()
            conn.execute(
                'UPDATE fingerprints SET use_count = use_count + 1, updated_at = ? WHERE fingerprint = ?',
                (datetime.now().isoformat(), fingerprint)
            )
            conn.commit()
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_clean(df: pd.DataFrame, path: str, fmt: str = 'xlsx'):
    """Write mapped DataFrame to XLSX or CSV."""
    # Ensure canonical column order
    cols = [c for c in CANONICAL_FIELDS if c in df.columns]
    out = df[cols].copy()

    if fmt == 'csv':
        out.to_csv(path, index=False)
    else:
        out.to_excel(path, index=False, engine='openpyxl')


# ---------------------------------------------------------------------------
# Import log
# ---------------------------------------------------------------------------

def log_import(filename: str, fingerprint: str, mapping: Dict[str, str],
               row_count: int, qc_warnings: int, qc_errors: int, status: str = 'approved'):
    """Write to import_log (PostgreSQL + SQLite)."""
    dbm = _db()
    if dbm:
        try:
            dbm.log_import_db(filename, fingerprint, mapping, row_count,
                              qc_warnings, qc_errors, status)
        except Exception as e:
            log.warning("DB log_import failed: %s", e)

    if _sqlite_available:
        try:
            conn = _get_conn()
            conn.execute("""
                INSERT INTO import_log (filename, fingerprint, mapping_used, row_count,
                                        qc_warnings, qc_errors, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (filename, fingerprint, json.dumps(mapping), row_count,
                  qc_warnings, qc_errors, status, datetime.now().isoformat()))
            conn.commit()
            conn.close()
        except Exception:
            pass


def get_import_history(limit: int = 20) -> List[dict]:
    """Return recent import log entries (PostgreSQL preferred, SQLite fallback)."""
    dbm = _db()
    if dbm:
        try:
            return dbm.get_import_history_db(limit)
        except Exception as e:
            log.debug("DB get_import_history failed: %s", e)

    if _sqlite_available:
        try:
            conn = _get_conn()
            rows = conn.execute(
                'SELECT * FROM import_log ORDER BY created_at DESC LIMIT ?', (limit,)
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception:
            pass
    return []


def get_saved_formats() -> List[dict]:
    """Return all saved fingerprint mappings (PostgreSQL preferred, SQLite fallback)."""
    dbm = _db()
    if dbm:
        try:
            return dbm.get_saved_formats_db()
        except Exception as e:
            log.debug("DB get_saved_formats failed: %s", e)

    if _sqlite_available:
        try:
            conn = _get_conn()
            rows = conn.execute(
                'SELECT fingerprint, source_label, column_names, use_count, updated_at '
                'FROM fingerprints ORDER BY use_count DESC'
            ).fetchall()
            conn.close()
            results = []
            for r in rows:
                entry = dict(r)
                entry['column_names'] = json.loads(entry['column_names'])
                results.append(entry)
            return results
        except Exception:
            pass
    return []
