"""
Validation & Issue Flagging Engine
Runs 5 quality checks on consolidated payor results to catch data issues
before final output: duplicate files, non-conforming formats, missing columns,
period gaps, and duplicate rows.
"""

import hashlib
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationIssue:
    """A single validation issue found during checks."""
    check: str              # Check name (e.g. 'duplicate_files', 'period_gaps')
    severity: str           # 'error' or 'warning'
    message: str            # Human-readable description
    affected_files: List[str] = field(default_factory=list)
    affected_rows: List[int] = field(default_factory=list)
    count: int = 0
    payor_code: str = ''


@dataclass
class ValidationResult:
    """Combined result of all validation checks."""
    issues: List[ValidationIssue] = field(default_factory=list)
    total_files: int = 0
    total_rows: int = 0

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == 'error')

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == 'warning')

    @property
    def issue_count(self) -> int:
        return len(self.issues)


# ---------------------------------------------------------------------------
# Check 1: Duplicate files (identical content across batch)
# ---------------------------------------------------------------------------

def check_duplicate_files(file_paths: List[str]) -> List[ValidationIssue]:
    """SHA-256 content hash to flag identical files uploaded in the same batch."""
    issues = []
    hash_to_files: Dict[str, List[str]] = {}

    for fp in file_paths:
        if not os.path.isfile(fp):
            continue
        try:
            h = hashlib.sha256()
            with open(fp, 'rb') as f:
                for chunk in iter(lambda: f.read(65536), b''):
                    h.update(chunk)
            digest = h.hexdigest()
            basename = os.path.basename(fp)
            hash_to_files.setdefault(digest, []).append(basename)
        except (IOError, OSError):
            continue

    for digest, filenames in hash_to_files.items():
        if len(filenames) > 1:
            issues.append(ValidationIssue(
                check='duplicate_files',
                severity='warning',
                message=f'{len(filenames)} files have identical content: {", ".join(filenames)}',
                affected_files=filenames,
                count=len(filenames),
            ))

    return issues


# ---------------------------------------------------------------------------
# Check 2: Non-conforming format (skipped/0-row files)
# ---------------------------------------------------------------------------

def check_non_conforming_format(file_inventory: List[dict], payor_code: str = '') -> List[ValidationIssue]:
    """Flag files with status='skipped' or 0 rows parsed."""
    issues = []
    bad_files = []

    for fi in file_inventory:
        if fi.get('status') == 'skipped' or fi.get('rows', 0) == 0:
            bad_files.append(fi.get('filename', 'unknown'))

    if bad_files:
        issues.append(ValidationIssue(
            check='non_conforming_format',
            severity='error',
            message=f'{len(bad_files)} file(s) could not be parsed: {", ".join(bad_files[:5])}{"..." if len(bad_files) > 5 else ""}',
            affected_files=bad_files,
            count=len(bad_files),
            payor_code=payor_code,
        ))

    return issues


# ---------------------------------------------------------------------------
# Check 3: Missing expected columns (Pre-Set payors only)
# ---------------------------------------------------------------------------

# Expected columns per known format — extendable
EXPECTED_COLUMNS = {}


def check_missing_expected_columns(fmt: str, detected_cols: Set[str],
                                   payor_name: str = '') -> List[ValidationIssue]:
    """For Pre-Set payors, compare detected columns vs expected set."""
    issues = []
    if fmt not in EXPECTED_COLUMNS:
        return issues

    expected = EXPECTED_COLUMNS[fmt]
    detected_lower = {c.lower() for c in detected_cols}
    missing = expected - detected_lower

    if missing:
        issues.append(ValidationIssue(
            check='missing_expected_columns',
            severity='warning',
            message=f'{payor_name}: Missing expected columns: {", ".join(sorted(missing))}',
            count=len(missing),
            payor_code=payor_name,
        ))

    return issues


# ---------------------------------------------------------------------------
# Check 4: Period gaps (missing months)
# ---------------------------------------------------------------------------

def check_period_gaps(monthly_df: pd.DataFrame, payor_code: str = '',
                      expected_start: Optional[int] = None,
                      expected_end: Optional[int] = None) -> List[ValidationIssue]:
    """Find missing months between min/max Statement Date (or expected range).

    monthly_df must have a 'period' column with YYYYMM int values.
    """
    issues = []
    if monthly_df is None or monthly_df.empty or 'period' not in monthly_df.columns:
        return issues

    periods = monthly_df['period'].dropna().astype(int)
    if periods.empty:
        return issues

    actual_periods = set(periods.unique())

    min_period = expected_start if expected_start else int(periods.min())
    max_period = expected_end if expected_end else int(periods.max())

    # Generate all expected months
    expected = set()
    y, m = divmod(min_period, 100)
    # Validate
    if m < 1 or m > 12 or y < 2000 or y > 2099:
        return issues

    end_y, end_m = divmod(max_period, 100)
    if end_m < 1 or end_m > 12:
        return issues

    while y * 100 + m <= end_y * 100 + end_m:
        expected.add(y * 100 + m)
        m += 1
        if m > 12:
            m = 1
            y += 1

    missing = sorted(expected - actual_periods)
    if missing:
        # Format as readable strings
        missing_strs = [f'{p // 100}-{p % 100:02d}' for p in missing[:12]]
        suffix = f' ... and {len(missing) - 12} more' if len(missing) > 12 else ''
        issues.append(ValidationIssue(
            check='period_gaps',
            severity='warning',
            message=f'{payor_code}: {len(missing)} missing month(s): {", ".join(missing_strs)}{suffix}',
            count=len(missing),
            payor_code=payor_code,
        ))

    return issues


# ---------------------------------------------------------------------------
# Check 5: Duplicate data rows
# ---------------------------------------------------------------------------

def check_duplicate_rows(detail_df: pd.DataFrame, payor_code: str = '') -> List[ValidationIssue]:
    """Flag fully identical data rows via df.duplicated()."""
    issues = []
    if detail_df is None or detail_df.empty:
        return issues

    dupes = detail_df.duplicated(keep='first')
    dupe_count = int(dupes.sum())

    if dupe_count > 0:
        dupe_indices = detail_df.index[dupes].tolist()[:100]  # Cap at 100
        issues.append(ValidationIssue(
            check='duplicate_rows',
            severity='warning',
            message=f'{payor_code}: {dupe_count} duplicate data row(s) found',
            affected_rows=dupe_indices,
            count=dupe_count,
            payor_code=payor_code,
        ))

    return issues


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_validation(payor_results: dict, file_paths: Optional[List[str]] = None) -> ValidationResult:
    """Run all 5 validation checks across all payors.

    payor_results: {code: PayorResult} from consolidator
    file_paths: optional list of all file paths for duplicate file check
    """
    result = ValidationResult()
    all_issues = []

    # Check 1: Duplicate files across entire batch
    if file_paths:
        all_issues.extend(check_duplicate_files(file_paths))

    total_files = 0
    total_rows = 0

    for code, pr in payor_results.items():
        cfg = pr.config
        inv = pr.file_inventory if hasattr(pr, 'file_inventory') else []
        total_files += pr.file_count

        # Check 2: Non-conforming format
        all_issues.extend(check_non_conforming_format(inv, payor_code=code))

        # Check 3: Missing expected columns (only for known formats)
        if cfg.fmt in EXPECTED_COLUMNS and hasattr(pr, 'detail') and pr.detail is not None:
            detected = set(pr.detail.columns)
            all_issues.extend(check_missing_expected_columns(cfg.fmt, detected, payor_name=cfg.name))

        # Check 4: Period gaps
        if hasattr(pr, 'monthly') and pr.monthly is not None:
            all_issues.extend(check_period_gaps(
                pr.monthly, payor_code=code,
                expected_start=cfg.expected_start,
                expected_end=cfg.expected_end,
            ))

        # Check 5: Duplicate rows
        if hasattr(pr, 'detail') and pr.detail is not None:
            total_rows += len(pr.detail)
            all_issues.extend(check_duplicate_rows(pr.detail, payor_code=code))

    result.issues = all_issues
    result.total_files = total_files
    result.total_rows = total_rows

    return result


# ---------------------------------------------------------------------------
# Filtering (Remove & Re-run support)
# ---------------------------------------------------------------------------

def filter_excluded(detail_df: pd.DataFrame,
                    exclude_files: Optional[List[str]] = None,
                    exclude_rows: Optional[List[int]] = None) -> pd.DataFrame:
    """Remove flagged files/rows from a detail DataFrame for re-processing.

    exclude_files: list of filenames to remove (matched against a '_source_file' column if present)
    exclude_rows: list of row indices to drop
    """
    if detail_df is None or detail_df.empty:
        return detail_df

    df = detail_df.copy()

    if exclude_files and '_source_file' in df.columns:
        df = df[~df['_source_file'].isin(exclude_files)]

    if exclude_rows:
        df = df.drop(index=[i for i in exclude_rows if i in df.index])

    return df.reset_index(drop=True)
