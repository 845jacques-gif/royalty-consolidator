"""Test different XIRR date conventions to find the one matching the model."""
from datetime import date
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from forecast import _xirr

# Model's NE values
ne = [2452073, 2085351, 1860486, 1710962, 1626490]
pp = 16_000_000
exit_mult = 15
exit_ev = ne[-1] * exit_mult

# Build unlevered cash flows
cfs = [-pp] + ne[:-1] + [ne[-1] + exit_ev]
print(f"Cash flows: {[round(c/1e6, 2) for c in cfs]}M")
print(f"MOIC: {sum(cfs[1:]) / pp:.3f}x")

# Try different date conventions
forecast_start = date(2025, 11, 28)
close_date = date(2025, 11, 30)

conventions = {
    'Mid-year (Jul 1)': [
        (forecast_start, cfs[0]),
        *[(date(2026 + i, 7, 1), cfs[i + 1]) for i in range(5)]
    ],
    'Year-end (Dec 31)': [
        (forecast_start, cfs[0]),
        *[(date(2026 + i, 12, 31), cfs[i + 1]) for i in range(5)]
    ],
    'Close anniversary (Nov 30)': [
        (close_date, cfs[0]),
        *[(date(2026 + i, 11, 30), cfs[i + 1]) for i in range(5)]
    ],
    'Annual from Y0 (exact years)': [
        (close_date, cfs[0]),
        *[(date(2025 + i + 1, 11, 30), cfs[i + 1]) for i in range(5)]
    ],
    'Standard IRR (Jan 1 each year)': [
        (date(2025, 1, 1), cfs[0]),
        *[(date(2026 + i, 1, 1), cfs[i + 1]) for i in range(5)]
    ],
}

print(f"\n{'Convention':<35} {'IRR':>8}  {'vs model 19.71%':>15}")
print('-' * 65)
for name, flows in conventions.items():
    irr = _xirr(flows)
    gap = (irr - 0.1971) * 100 if irr else 0
    print(f"{name:<35} {irr*100:>7.2f}%  {gap:>+14.2f}pp")

# Also test: what if model uses simple IRR (numpy) not XIRR?
try:
    import numpy as np
    simple_irr = np.irr(cfs) if hasattr(np, 'irr') else None
    if simple_irr is None:
        import numpy_financial as npf
        simple_irr = npf.irr(cfs)
    print(f"\n{'numpy simple IRR':<35} {simple_irr*100:>7.2f}%  {(simple_irr-0.1971)*100:>+14.2f}pp")
except Exception as e:
    # Manual IRR via bisection
    def manual_irr(cfs, lo=0.01, hi=1.0, tol=1e-8):
        for _ in range(200):
            mid = (lo + hi) / 2
            npv = sum(c / (1 + mid)**t for t, c in enumerate(cfs))
            if npv > 0:
                lo = mid
            else:
                hi = mid
            if hi - lo < tol:
                break
        return mid

    sirr = manual_irr(cfs)
    print(f"\n{'Simple IRR (annual periods)':<35} {sirr*100:>7.2f}%  {(sirr-0.1971)*100:>+14.2f}pp")
