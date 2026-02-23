"""
Earnings Waterfall Formula Engine
Handles bracket-notation formula parsing, waterfall field auto-calculation,
and percent-column derivations for the consolidator Phase 2 pipeline.
"""

import logging
import re
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger('royalty')


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WATERFALL_FIELDS = [
    'Gross Earnings', 'Fees', 'Net Receipts',
    'Payable Share', 'Third Party Share', 'Net Earnings',
]

PERCENT_FIELDS = ['Fee %', 'Payable %', 'Third Party %']

# Known derivation rules: (target, expression_using_others)
# Each rule is (result_field, callable(row_dict) -> value, required_fields)
WATERFALL_RELATIONSHIPS = [
    # Fees = Gross - Net Receipts
    ('Fees', lambda r: r['Gross Earnings'] - r['Net Receipts'], {'Gross Earnings', 'Net Receipts'}),
    # Net Receipts = Gross - Fees
    ('Net Receipts', lambda r: r['Gross Earnings'] - r['Fees'], {'Gross Earnings', 'Fees'}),
    # Gross = Net Receipts + Fees
    ('Gross Earnings', lambda r: r['Net Receipts'] + r['Fees'], {'Net Receipts', 'Fees'}),
    # Net Earnings = Payable Share - Third Party Share
    ('Net Earnings', lambda r: r['Payable Share'] - r['Third Party Share'], {'Payable Share', 'Third Party Share'}),
    # Payable Share = Net Earnings + Third Party Share
    ('Payable Share', lambda r: r['Net Earnings'] + r['Third Party Share'], {'Net Earnings', 'Third Party Share'}),
    # Third Party Share = Payable Share - Net Earnings
    ('Third Party Share', lambda r: r['Payable Share'] - r['Net Earnings'], {'Payable Share', 'Net Earnings'}),
    # Payable Share = Net Receipts (when no third-party split)
    ('Payable Share', lambda r: r['Net Receipts'], {'Net Receipts'}),
    # Net Earnings = Payable Share (when third-party share is 0 or absent)
    ('Net Earnings', lambda r: r['Payable Share'], {'Payable Share'}),
]

# Percent-based derivation rules
PERCENT_RELATIONSHIPS = [
    # Fees = Gross * Fee%
    ('Fees', lambda r: r['Gross Earnings'] * r['Fee %'] / 100.0, {'Gross Earnings', 'Fee %'}),
    # Net Receipts = Gross * (1 - Fee%)
    ('Net Receipts', lambda r: r['Gross Earnings'] * (1 - r['Fee %'] / 100.0), {'Gross Earnings', 'Fee %'}),
    # Payable Share = Net Receipts * Payable%
    ('Payable Share', lambda r: r['Net Receipts'] * r['Payable %'] / 100.0, {'Net Receipts', 'Payable %'}),
    # Third Party Share = Net Receipts * Third Party%
    ('Third Party Share', lambda r: r['Net Receipts'] * r['Third Party %'] / 100.0, {'Net Receipts', 'Third Party %'}),
]

# Safe builtins for formula eval
_SAFE_BUILTINS = {'abs': abs, 'min': min, 'max': max, 'round': round}

# Regex: bracket references like [Gross Earnings]
_BRACKET_RE = re.compile(r'\[([^\]]+)\]')

# Regex: only allow safe characters in formulas
_SAFE_FORMULA_RE = re.compile(r'^[=\s\d\.\+\-\*/\(\)\[\]a-zA-Z_%,]+$')


# ---------------------------------------------------------------------------
# Field detection
# ---------------------------------------------------------------------------

def detect_available_fields(df: pd.DataFrame) -> dict:
    """Check which waterfall fields exist with non-zero data in the DataFrame.

    Returns dict with keys:
        present: list of field names with non-zero data
        missing: list of field names that are absent or all-zero
        percent_cols: list of percent field names with data
    """
    present = []
    missing = []
    percent_cols = []

    for field in WATERFALL_FIELDS:
        if field in df.columns:
            col = pd.to_numeric(df[field], errors='coerce').fillna(0)
            if col.abs().sum() > 0:
                present.append(field)
            else:
                missing.append(field)
        else:
            missing.append(field)

    for field in PERCENT_FIELDS:
        if field in df.columns:
            col = pd.to_numeric(df[field], errors='coerce').fillna(0)
            if col.abs().sum() > 0:
                percent_cols.append(field)

    return {
        'present': present,
        'missing': missing,
        'percent_cols': percent_cols,
    }


# ---------------------------------------------------------------------------
# Auto-calculation
# ---------------------------------------------------------------------------

def auto_calculate(df: pd.DataFrame, available: Optional[dict] = None) -> Tuple[pd.DataFrame, List[str]]:
    """Iteratively resolve missing waterfall fields using known relationships.

    Also handles percent columns (e.g. Fee% -> Fees = Gross * Fee%).
    Returns (updated_df, still_missing_fields).
    """
    if available is None:
        available = detect_available_fields(df)

    df = df.copy()
    resolved = set(available['present'])
    percent_available = set(available['percent_cols'])
    still_missing = list(available['missing'])

    # Ensure numeric types for waterfall columns
    for f in WATERFALL_FIELDS:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors='coerce').fillna(0)

    for f in PERCENT_FIELDS:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors='coerce').fillna(0)

    # Iterative resolution (max 5 passes to handle chained dependencies)
    for _ in range(5):
        made_progress = False

        # Try percent-based derivations first
        for target, func, required in PERCENT_RELATIONSHIPS:
            if target in resolved:
                continue
            # Check if all required fields are available (some may be percent fields)
            reqs_met = all(
                (r in resolved) or (r in percent_available)
                for r in required
            )
            if not reqs_met:
                continue

            try:
                df[target] = df.apply(lambda row, f=func: _safe_row_calc(row, f), axis=1)
                resolved.add(target)
                if target in still_missing:
                    still_missing.remove(target)
                made_progress = True
            except Exception as e:
                log.debug("Percent derivation failed for %s: %s", target, e)

        # Try direct waterfall relationships
        for target, func, required in WATERFALL_RELATIONSHIPS:
            if target in resolved:
                continue
            if not required.issubset(resolved):
                continue

            try:
                df[target] = df.apply(lambda row, f=func: _safe_row_calc(row, f), axis=1)
                resolved.add(target)
                if target in still_missing:
                    still_missing.remove(target)
                made_progress = True
            except Exception as e:
                log.debug("Waterfall derivation failed for %s: %s", target, e)

        if not made_progress:
            break

    return df, still_missing


def _safe_row_calc(row, func):
    """Apply a calculation function to a row, catching division errors."""
    try:
        result = func(row)
        if pd.isna(result) or abs(result) == float('inf'):
            return 0.0
        return result
    except (ZeroDivisionError, KeyError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Formula parsing
# ---------------------------------------------------------------------------

def parse_formula(formula_str: str, available_columns: List[str]) -> Tuple[Optional[Callable], Optional[str]]:
    """Parse a bracket-notation formula like '=[Net Receipts] / 0.8'.

    Validates field references against available_columns.
    Returns (callable, None) on success or (None, error_message) on failure.
    The callable takes a dict of {field_name: value} and returns a float.
    """
    formula_str = formula_str.strip()
    if not formula_str:
        return None, 'Empty formula'

    # Strip leading '=' if present
    if formula_str.startswith('='):
        formula_str = formula_str[1:].strip()

    if not formula_str:
        return None, 'Empty formula after removing ='

    # Validate safe characters
    if not _SAFE_FORMULA_RE.match('=' + formula_str):
        return None, f'Formula contains invalid characters'

    # Extract bracket references
    refs = _BRACKET_RE.findall(formula_str)
    if not refs:
        return None, 'Formula must reference at least one field using [Field Name] syntax'

    # Validate references
    for ref in refs:
        if ref not in available_columns:
            return None, f'Unknown field: [{ref}]. Available: {", ".join(available_columns)}'

    # Build Python expression by replacing [Field Name] with dict lookups
    expr = formula_str
    for ref in sorted(set(refs), key=len, reverse=True):  # longest first to avoid partial replacement
        safe_key = repr(ref)
        expr = expr.replace(f'[{ref}]', f'_vals[{safe_key}]')

    # Compile and wrap
    try:
        code = compile(expr, '<formula>', 'eval')
    except SyntaxError as e:
        return None, f'Syntax error: {e}'

    def _evaluate(_vals):
        try:
            result = eval(code, {'__builtins__': _SAFE_BUILTINS}, {'_vals': _vals})
            if pd.isna(result) or abs(result) == float('inf'):
                return 0.0
            return float(result)
        except ZeroDivisionError:
            return 0.0
        except Exception as e:
            return 0.0

    return _evaluate, None


def validate_formula(formula_str: str, available_columns: List[str]) -> dict:
    """Validate a formula string without executing it.

    Returns {'valid': True} or {'valid': False, 'error': '...'}.
    """
    _, error = parse_formula(formula_str, available_columns)
    if error:
        return {'valid': False, 'error': error}
    return {'valid': True}


# ---------------------------------------------------------------------------
# Apply formulas to DataFrame
# ---------------------------------------------------------------------------

def apply_formulas(df: pd.DataFrame, formulas_dict: Dict[str, str]) -> Tuple[pd.DataFrame, List[str]]:
    """Apply user-defined formulas to a DataFrame.

    formulas_dict: {target_field: "=expression"} e.g. {'Fees': '=[Gross Earnings] * 0.15'}
    Returns (modified_df, list_of_errors).
    """
    df = df.copy()
    errors = []
    available = [c for c in df.columns]

    for target, formula_str in formulas_dict.items():
        func, error = parse_formula(formula_str, available)
        if error:
            errors.append(f'{target}: {error}')
            continue

        def _apply_row(row, f=func):
            vals = {col: float(row[col]) if col in row.index else 0.0
                    for col in available
                    if col in WATERFALL_FIELDS or col in PERCENT_FIELDS}
            return f(vals)

        try:
            df[target] = df.apply(_apply_row, axis=1)
            if target not in available:
                available.append(target)
        except Exception as e:
            errors.append(f'{target}: Execution error: {e}')

    return df, errors


def preview_formulas(df: pd.DataFrame, formulas_dict: Dict[str, str],
                     n_rows: int = 10) -> dict:
    """Preview formula results on first N rows.

    Returns {
        'columns': [...],
        'rows': [[...], ...],
        'errors': [...]
    }
    """
    preview_df = df.head(n_rows).copy()

    # Ensure numeric
    for f in WATERFALL_FIELDS + PERCENT_FIELDS:
        if f in preview_df.columns:
            preview_df[f] = pd.to_numeric(preview_df[f], errors='coerce').fillna(0)

    result_df, errors = apply_formulas(preview_df, formulas_dict)

    # Build output with waterfall fields only
    show_cols = [c for c in WATERFALL_FIELDS + PERCENT_FIELDS + list(formulas_dict.keys())
                 if c in result_df.columns]
    # Deduplicate while preserving order
    seen = set()
    unique_cols = []
    for c in show_cols:
        if c not in seen:
            unique_cols.append(c)
            seen.add(c)

    rows = []
    for _, row in result_df[unique_cols].iterrows():
        rows.append([round(float(v), 4) if pd.notna(v) else 0.0 for v in row])

    return {
        'columns': unique_cols,
        'rows': rows,
        'errors': errors,
    }
