"""
Compare app consolidation output vs manual consolidated files.
Runs the consolidator on raw data, then compares sums for overlapping date ranges.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from consolidator import PayorConfig, load_all_payors

# --- 1. Run consolidation on raw files ---
configs = [
    PayorConfig(
        code='B1', name='Believe 15%', fmt='auto', fee=0.15,
        fx_currency='EUR', fx_rate=1.0,
        statements_dir=r'C:\Users\jacques\Downloads\Believe_15_extracted',
        statement_type='masters',
    ),
    PayorConfig(
        code='B2', name='Believe 20%', fmt='auto', fee=0.20,
        fx_currency='EUR', fx_rate=1.0,
        statements_dir=r'C:\Users\jacques\Downloads\Believe_20_extracted',
        statement_type='masters',
    ),
    PayorConfig(
        code='RJ', name='RecordJet', fmt='auto', fee=0.07,
        fx_currency='EUR', fx_rate=1.0,
        statements_dir=r'C:\Users\jacques\Downloads\RecordJet_extracted',
        statement_type='masters',
    ),
]

print("=" * 70)
print("  STEP 1: Running consolidator on raw files")
print("=" * 70)
results = load_all_payors(configs)

# --- 2. Load manual files ---
print("\n" + "=" * 70)
print("  STEP 2: Loading manual consolidated files")
print("=" * 70)

manual_b15 = pd.read_excel(r'C:\Users\jacques\Documents\Believe 15% Fee Consolidated Statements.xlsx')
manual_b20 = pd.read_excel(r'C:\Users\jacques\Documents\Believe 20% Fee Consolidated Statements.xlsx')
manual_rj = pd.read_excel(r'C:\Users\jacques\Documents\Record Jet Consolidated.xlsx')

print(f"  Manual B15: {len(manual_b15):,} rows, cols: {list(manual_b15.columns)}")
print(f"  Manual B20: {len(manual_b20):,} rows, cols: {list(manual_b20.columns)}")
print(f"  Manual RJ:  {len(manual_rj):,} rows, cols: {list(manual_rj.columns)}")

# --- 3. Compare for each payor ---
print("\n" + "=" * 70)
print("  STEP 3: Comparing gross/net sums for overlapping date ranges")
print("=" * 70)

def get_period_range(manual_df, date_col='Statement Date'):
    """Get min/max period from manual file."""
    dates = pd.to_datetime(manual_df[date_col])
    min_period = int(dates.min().strftime('%Y%m'))
    max_period = int(dates.max().strftime('%Y%m'))
    return min_period, max_period

def filter_app_by_period(pr, min_p, max_p):
    """Filter app monthly data to a period range."""
    m = pr.monthly
    return m[(m['period'] >= min_p) & (m['period'] <= max_p)]

# --- Believe 15% ---
print("\n--- Believe 15% ---")
if 'B1' in results:
    pr_b1 = results['B1']
    min_p, max_p = get_period_range(manual_b15)
    print(f"  Manual date range: {min_p} to {max_p}")
    print(f"  App date range: {pr_b1.monthly['period'].min()} to {pr_b1.monthly['period'].max()}")

    filtered = filter_app_by_period(pr_b1, min_p, max_p)
    app_gross = filtered['gross'].sum()
    app_net = filtered['net'].sum()

    if 'Gross' in manual_b15.columns:
        man_gross = manual_b15['Gross'].sum()
    else:
        man_gross = 0
    if 'Net Royalty' in manual_b15.columns:
        man_net = manual_b15['Net Royalty'].sum()
    elif 'Net' in manual_b15.columns:
        man_net = manual_b15['Net'].sum()
    else:
        man_net = 0

    print(f"  Manual Gross: {man_gross:>15,.2f}")
    print(f"  App    Gross: {app_gross:>15,.2f}")
    print(f"  Diff   Gross: {app_gross - man_gross:>15,.2f} ({(app_gross - man_gross) / man_gross * 100:.4f}%)")
    print()
    print(f"  Manual Net:   {man_net:>15,.2f}")
    print(f"  App    Net:   {app_net:>15,.2f}")
    print(f"  Diff   Net:   {app_net - man_net:>15,.2f} ({(app_net - man_net) / man_net * 100:.4f}%)")
else:
    print("  B1 not in results!")

# --- Believe 20% ---
print("\n--- Believe 20% ---")
if 'B2' in results:
    pr_b2 = results['B2']
    min_p, max_p = get_period_range(manual_b20)
    print(f"  Manual date range: {min_p} to {max_p}")
    print(f"  App date range: {pr_b2.monthly['period'].min()} to {pr_b2.monthly['period'].max()}")

    filtered = filter_app_by_period(pr_b2, min_p, max_p)
    app_gross = filtered['gross'].sum()
    app_net = filtered['net'].sum()

    # B20 manual uses 'ppu total' for gross and 'royalty' for net
    if 'ppu total' in manual_b20.columns:
        man_gross = pd.to_numeric(manual_b20['ppu total'], errors='coerce').fillna(0).sum()
    elif 'Gross' in manual_b20.columns:
        man_gross = manual_b20['Gross'].sum()
    else:
        man_gross = 0

    if 'royalty' in manual_b20.columns:
        man_net = pd.to_numeric(manual_b20['royalty'], errors='coerce').fillna(0).sum()
    elif 'Net' in manual_b20.columns:
        man_net = manual_b20['Net'].sum()
    else:
        man_net = 0

    print(f"  Manual Gross: {man_gross:>15,.2f}")
    print(f"  App    Gross: {app_gross:>15,.2f}")
    print(f"  Diff   Gross: {app_gross - man_gross:>15,.2f} ({(app_gross - man_gross) / man_gross * 100:.4f}%)")
    print()
    print(f"  Manual Net:   {man_net:>15,.2f}")
    print(f"  App    Net:   {app_net:>15,.2f}")
    print(f"  Diff   Net:   {app_net - man_net:>15,.2f} ({(app_net - man_net) / man_net * 100:.4f}%)")
else:
    print("  B2 not in results!")

# --- RecordJet ---
print("\n--- RecordJet ---")
if 'RJ' in results:
    pr_rj = results['RJ']
    min_p, max_p = get_period_range(manual_rj)
    print(f"  Manual date range: {min_p} to {max_p}")
    print(f"  App date range: {pr_rj.monthly['period'].min()} to {pr_rj.monthly['period'].max()}")

    filtered = filter_app_by_period(pr_rj, min_p, max_p)
    app_gross = filtered['gross'].sum()
    app_net = filtered['net'].sum()

    man_gross = manual_rj['Gross'].sum() if 'Gross' in manual_rj.columns else 0
    man_net = manual_rj['Net'].sum() if 'Net' in manual_rj.columns else 0

    print(f"  Manual Gross: {man_gross:>15,.2f}")
    print(f"  App    Gross: {app_gross:>15,.2f}")
    print(f"  Diff   Gross: {app_gross - man_gross:>15,.2f} ({(app_gross - man_gross) / man_gross * 100:.4f}%)")
    print()
    print(f"  Manual Net:   {man_net:>15,.2f}")
    print(f"  App    Net:   {app_net:>15,.2f}")
    print(f"  Diff   Net:   {app_net - man_net:>15,.2f} ({(app_net - man_net) / man_net * 100:.4f}%)")
else:
    print("  RJ not in results!")

# --- Check statement_date format ---
print("\n" + "=" * 70)
print("  STEP 4: Statement date format check")
print("=" * 70)
for code, pr in results.items():
    sample = pr.detail['statement_date'].iloc[0] if len(pr.detail) > 0 else 'N/A'
    print(f"  {code} sample statement_date: {sample} (type: {type(sample).__name__})")

print("\nDone.")
