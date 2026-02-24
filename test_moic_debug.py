"""Debug MOIC calculation - compare our debt schedule against model's V&R sheet."""
import json
import pickle
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from forecast import ForecastConfig, run_forecast

with open('deals/LUCKI/analytics.json') as f:
    analytics = json.load(f)
with open('deals/LUCKI/payor_results.pkl', 'rb') as f:
    payor_results = pickle.load(f)
with open('deals/LUCKI/forecast_result.json') as f:
    old_result = json.load(f)

cfg_data = old_result['config']
config = ForecastConfig(
    genre_default=cfg_data.get('genre_default', 'rap'),
    horizon_years=cfg_data.get('horizon_years', 5),
    discount_rate=cfg_data.get('discount_rate', 0.09375),
    terminal_growth=cfg_data.get('terminal_growth', 0.01),
    purchase_price=cfg_data.get('purchase_price', 16000000),
    exit_multiple=cfg_data.get('exit_multiple', 15),
    ltv=cfg_data.get('ltv', 0.55),
    sofr_rate=cfg_data.get('sofr_rate', 0.045),
    sofr_floor=cfg_data.get('sofr_floor', 0.02),
    sofr_spread=cfg_data.get('sofr_spread', 0.0275),
    cash_flow_sweep=cfg_data.get('cash_flow_sweep', 1.0),
    synergy_ramp_months=cfg_data.get('synergy_ramp_months', 12),
    virtu_wacc=cfg_data.get('virtu_wacc', 0.09),
    holdback=cfg_data.get('holdback', 1600000),
    pcdpcdr=cfg_data.get('pcdpcdr', 0),
    cash_date=cfg_data.get('cash_date', '2026-01-01'),
    close_date=cfg_data.get('close_date', '2025-11-30'),
    sofr_curve=cfg_data.get('sofr_curve', []),
)

import logging
logging.basicConfig(level=logging.WARNING)

forecast = run_forecast(payor_results, analytics, config)

# === UNLEVERED ===
unlev = forecast.get('unlevered_returns', {})
print("=" * 70)
print("UNLEVERED RETURNS")
print("=" * 70)
print(f"  Purchase: ${config.purchase_price:,.0f}")
print(f"  MOIC: {unlev.get('moic', 0):.3f}x  (model V&R: 2.133x)")
print(f"  IRR:  {unlev.get('irr', 0)*100:.2f}%  (model V&R: 19.71%)")
model_unlev_ne = [2452073, 2085351, 1860486, 1710962, 1626490]
model_exit = 1626490 * 15
print(f"\n  {'Yr':>3} {'Our UFCF':>12} {'Model UFCF':>12} {'Exit':>14}")
for s in unlev.get('schedule', []):
    i = s['year'] - 1
    m = model_unlev_ne[i] if i < len(model_unlev_ne) else 0
    print(f"  {s['year']:>3} {s['ufcf']:>12,.0f} {m:>12,.0f} {s['exit_proceeds']:>14,.0f}")

# Manual MOIC verification
our_total = sum(s['ufcf'] for s in unlev.get('schedule', [])) + unlev.get('schedule', [{}])[-1].get('exit_proceeds', 0)
print(f"\n  Total CFs: ${our_total:,.0f}")
print(f"  Manual MOIC: {our_total / config.purchase_price:.3f}x")

# === CMG LEVERED ===
lev = forecast.get('levered_returns', {})
print("\n" + "=" * 70)
print("CMG LEVERED RETURNS")
print("=" * 70)
print(f"  Equity:     ${lev.get('equity', 0):,.0f}   (model: $7,200,000)")
print(f"  Debt:       ${lev.get('debt_initial', 0):,.0f}   (model: $8,800,000)")
print(f"  MOIC:       {lev.get('moic', 0):.3f}x   (model cached: 3.834x — STALE)")
print(f"  IRR:        {lev.get('irr', 0)*100:.2f}%   (model: 30.82%)")
print(f"  Exit EV:    ${lev.get('exit_ev', 0):,.0f}")
print(f"  Rem Debt:   ${lev.get('remaining_debt', 0):,.0f}")
print(f"  Exit Eq:    ${lev.get('exit_equity', 0):,.0f}")
print(f"  Total LFCF: ${lev.get('total_lfcf', 0):,.0f}")

# Model interest rates from V&R sheet
model_rates = [0.0588, 0.0596, 0.0615, 0.0631, 0.0648]

print(f"\n  {'Yr':>3} {'Opening':>12} {'Rate':>7} {'Interest':>10} {'Principal':>10} {'Closing':>12} {'LFCF':>10}")
print(f"  {'-'*65}")
for ds in lev.get('debt_schedule', []):
    i = ds['year'] - 1
    mr = model_rates[i] if i < len(model_rates) else 0
    print(f"  {ds['year']:>3} {ds['opening_balance']:>12,.0f} {ds['interest_rate']:>6.2%} {ds['interest']:>10,.0f} "
          f"{ds['principal']:>10,.0f} {ds['closing_balance']:>12,.0f} {ds['lfcf']:>10,.0f}")

# Theoretical max MOIC (zero interest)
total_ne = sum(yt['net_earnings_incl'] for yt in forecast['aggregate']['year_totals'])
exit_ev = forecast['aggregate']['year_totals'][-1]['net_earnings_incl'] * 15
max_moic = (total_ne + exit_ev - lev.get('debt_initial', 0)) / lev.get('equity', 1)
print(f"\n  Theoretical max MOIC (zero interest): {max_moic:.3f}x")
print(f"  Model cached MOIC of 3.834x {'EXCEEDS' if 3.834 > max_moic else 'is within'} theoretical max")

# === VIRTU LEVERED ===
virtu = forecast.get('virtu_levered_returns')
if virtu:
    print("\n" + "=" * 70)
    print("VIRTU LEVERED RETURNS")
    print("=" * 70)
    print(f"  Virtu DCF:  ${virtu.get('virtu_dcf_pg_incl', 0):,.0f}   (model: $21,056,615)")
    print(f"  Equity:     ${virtu.get('equity', 0):,.0f}   (model: $4,418,862)")
    print(f"  Debt:       ${virtu.get('debt_initial', 0):,.0f}   (model: $11,581,138)")
    print(f"  MOIC:       {virtu.get('moic', 0):.3f}x   (model cached: 6.596x — STALE)")
    print(f"  IRR:        {virtu.get('irr', 0)*100:.2f}%   (model: 45.80%)")
    print(f"  Exit EV:    ${virtu.get('exit_ev', 0):,.0f}")
    print(f"  Rem Debt:   ${virtu.get('remaining_debt', 0):,.0f}")
    print(f"  Exit Eq:    ${virtu.get('exit_equity', 0):,.0f}")

    print(f"\n  {'Yr':>3} {'Opening':>12} {'Rate':>7} {'Interest':>10} {'Principal':>10} {'Closing':>12} {'LFCF':>10}")
    print(f"  {'-'*65}")
    for ds in virtu.get('debt_schedule', []):
        print(f"  {ds['year']:>3} {ds['opening_balance']:>12,.0f} {ds['interest_rate']:>6.2%} {ds['interest']:>10,.0f} "
              f"{ds['principal']:>10,.0f} {ds['closing_balance']:>12,.0f} {ds['lfcf']:>10,.0f}")

    # Virtu theoretical max
    virtu_max = (total_ne + exit_ev - virtu.get('debt_initial', 0)) / virtu.get('equity', 1)
    print(f"\n  Theoretical max MOIC (zero interest): {virtu_max:.3f}x")

    # What the model SHOULD show (with properly resolved circular ref)
    # Model's NE values
    m_ne = [2452073, 2085351, 1860486, 1710962, 1626490]
    m_exit_ev = 1626490 * 15
    m_virtu_debt = 11581138
    m_virtu_equity = 4418862

    # Simulate model's debt schedule with proper interest
    balance = m_virtu_debt
    total_lfcf = 0
    print(f"\n  --- Model debt schedule (properly resolved, using model NE) ---")
    print(f"  {'Yr':>3} {'Opening':>12} {'Rate':>7} {'Interest':>10} {'Principal':>10} {'Closing':>12} {'LFCF':>10}")
    print(f"  {'-'*65}")
    for yr in range(5):
        opening = balance
        rate = model_rates[yr]
        ne = m_ne[yr]
        # Iterative interest (same as our code)
        est_int = opening * rate
        for _ in range(10):
            avail = max(0, ne - est_int) * 1.0
            princ = min(opening, avail)
            closing = opening - princ
            new_int = ((opening + closing) / 2) * rate
            if abs(new_int - est_int) < 0.01:
                est_int = new_int
                break
            est_int = new_int
        interest = est_int
        avail = max(0, ne - interest) * 1.0
        princ = min(opening, avail)
        closing = opening - princ
        lfcf = ne - interest - princ
        total_lfcf += lfcf
        print(f"  {yr+1:>3} {opening:>12,.0f} {rate:>6.2%} {interest:>10,.0f} "
              f"{princ:>10,.0f} {closing:>12,.0f} {lfcf:>10,.0f}")
        balance = closing

    virtu_remaining_debt = balance
    exit_eq = m_exit_ev - virtu_remaining_debt
    model_moic = (total_lfcf + exit_eq) / m_virtu_equity
    print(f"\n  Model true Virtu MOIC (resolved): {model_moic:.3f}x")
    print(f"  Model true exit equity: ${exit_eq:,.0f}")
    print(f"  Model true remaining debt: ${virtu_remaining_debt:,.0f}")

    # Same for CMG
    balance = 8800000
    total_lfcf = 0
    print(f"\n  --- Model CMG debt schedule (properly resolved) ---")
    print(f"  {'Yr':>3} {'Opening':>12} {'Rate':>7} {'Interest':>10} {'Principal':>10} {'Closing':>12} {'LFCF':>10}")
    print(f"  {'-'*65}")
    for yr in range(5):
        opening = balance
        rate = model_rates[yr]
        ne = m_ne[yr]
        est_int = opening * rate
        for _ in range(10):
            avail = max(0, ne - est_int) * 1.0
            princ = min(opening, avail)
            closing = opening - princ
            new_int = ((opening + closing) / 2) * rate
            if abs(new_int - est_int) < 0.01:
                est_int = new_int
                break
            est_int = new_int
        interest = est_int
        avail = max(0, ne - interest) * 1.0
        princ = min(opening, avail)
        closing = opening - princ
        lfcf = ne - interest - princ
        total_lfcf += lfcf
        print(f"  {yr+1:>3} {opening:>12,.0f} {rate:>6.2%} {interest:>10,.0f} "
              f"{princ:>10,.0f} {closing:>12,.0f} {lfcf:>10,.0f}")
        balance = closing

    exit_eq = m_exit_ev - balance
    cmg_moic = (total_lfcf + exit_eq) / 7200000
    print(f"\n  Model true CMG MOIC (resolved): {cmg_moic:.3f}x")
    print(f"  Model true exit equity: ${exit_eq:,.0f}")
    print(f"  Model true remaining debt: ${balance:,.0f}")

    # Model true Virtu IRR
    from datetime import date
    close_date = date(2025, 11, 30)
    virtu_xirr = [(close_date, -m_virtu_equity)]
    for yr in range(5):
        cf_date = date(close_date.year + yr + 1, close_date.month, close_date.day)
        cf = 0  # LFCF = 0 each year
        if yr == 4:
            cf += m_exit_ev - virtu_remaining_debt  # exit equity
        virtu_xirr.append((cf_date, cf))
    from forecast import _xirr
    model_virtu_irr = _xirr(virtu_xirr)
    print(f"  Model true Virtu IRR (resolved): {model_virtu_irr*100:.2f}%")

    # Model true CMG IRR
    bal2 = 8800000
    cmg_xirr = [(close_date, -7200000.0)]
    for yr in range(5):
        opening = bal2
        rate = model_rates[yr]
        ne_val = m_ne[yr]
        est_int2 = opening * rate
        for _ in range(10):
            avail2 = max(0, ne_val - est_int2) * 1.0
            princ2 = min(opening, avail2)
            closing2 = opening - princ2
            new_int2 = ((opening + closing2) / 2) * rate
            if abs(new_int2 - est_int2) < 0.01:
                est_int2 = new_int2
                break
            est_int2 = new_int2
        avail2 = max(0, ne_val - est_int2)
        princ2 = min(opening, avail2)
        closing2 = opening - princ2
        lfcf2 = ne_val - est_int2 - princ2
        cf_date = date(close_date.year + yr + 1, close_date.month, close_date.day)
        cf = lfcf2
        if yr == 4:
            cf += m_exit_ev - closing2
        cmg_xirr.append((cf_date, cf))
        bal2 = closing2
    model_cmg_irr = _xirr(cmg_xirr)
    print(f"  Model true CMG IRR (resolved): {model_cmg_irr*100:.2f}%")

print("\n" + "=" * 70)
print("SUMMARY — COMPARISON WITH MODEL TRUE VALUES")
print("=" * 70)
print(f"  (Model 'true' = properly resolved circular ref + close-date convention)")
print(f"  (Model cached MOIC/IRR are STALE due to Excel CIRC switch)")
print()
print(f"  {'Metric':<20} {'Ours':>10} {'Model True':>12} {'Gap':>10}")
print(f"  {'-'*55}")
print(f"  {'Unlev MOIC':<20} {unlev.get('moic',0):>9.3f}x {'2.133':>11}x {(unlev.get('moic',0)-2.133):>+9.3f}x")
print(f"  {'Unlev IRR':<20} {unlev.get('irr',0)*100:>9.2f}% {'19.71':>11}% {(unlev.get('irr',0)*100-19.71):>+9.2f}pp")
if lev:
    print(f"  {'CMG MOIC':<20} {lev.get('moic',0):>9.3f}x {'3.335':>11}x {(lev.get('moic',0)-3.335):>+9.3f}x")
    print(f"  {'CMG IRR':<20} {lev.get('irr',0)*100:>9.2f}% {'27.24':>11}% {(lev.get('irr',0)*100-27.24):>+9.2f}pp")
if virtu:
    print(f"  {'Virtu MOIC':<20} {virtu.get('moic',0):>9.3f}x {'4.577':>11}x {(virtu.get('moic',0)-4.577):>+9.3f}x")
    print(f"  {'Virtu IRR':<20} {virtu.get('irr',0)*100:>9.2f}% {'35.56':>11}% {(virtu.get('irr',0)*100-35.56):>+9.2f}pp")
