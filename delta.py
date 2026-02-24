"""
Delta Report Engine — compares analytics before and after a re-run.
Produces a structured DeltaReport with new/removed periods, ISRCs,
and revenue variance by payor + ISRC.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

log = logging.getLogger('royalty')


@dataclass
class DeltaReport:
    """Structured comparison between two analytics snapshots."""
    deal_slug: str
    created_at: str = ''
    # Period changes
    new_periods: List[int] = field(default_factory=list)
    removed_periods: List[int] = field(default_factory=list)
    # ISRC changes
    new_isrcs: List[str] = field(default_factory=list)
    removed_isrcs: List[str] = field(default_factory=list)
    # Revenue changes
    old_ltm_gross: float = 0
    new_ltm_gross: float = 0
    ltm_gross_change_pct: float = 0
    old_ltm_net: float = 0
    new_ltm_net: float = 0
    ltm_net_change_pct: float = 0
    old_isrc_count: int = 0
    new_isrc_count: int = 0
    # Per-payor variance
    payor_variance: List[Dict] = field(default_factory=list)
    # ISRC-level top movers
    top_gainers: List[Dict] = field(default_factory=list)
    top_losers: List[Dict] = field(default_factory=list)
    # Summary text
    summary: str = ''

    def to_dict(self) -> dict:
        return {
            'deal_slug': self.deal_slug,
            'created_at': self.created_at,
            'new_periods': self.new_periods,
            'removed_periods': self.removed_periods,
            'new_isrcs': self.new_isrcs,
            'removed_isrcs': self.removed_isrcs,
            'old_ltm_gross': self.old_ltm_gross,
            'new_ltm_gross': self.new_ltm_gross,
            'ltm_gross_change_pct': self.ltm_gross_change_pct,
            'old_ltm_net': self.old_ltm_net,
            'new_ltm_net': self.new_ltm_net,
            'ltm_net_change_pct': self.ltm_net_change_pct,
            'old_isrc_count': self.old_isrc_count,
            'new_isrc_count': self.new_isrc_count,
            'payor_variance': self.payor_variance,
            'top_gainers': self.top_gainers,
            'top_losers': self.top_losers,
            'summary': self.summary,
        }


def snapshot_analytics(deal_dir: str) -> Optional[str]:
    """Save current analytics.json as analytics_prev.json before re-run.
    Returns the path to the snapshot, or None if no analytics exist."""
    analytics_path = os.path.join(deal_dir, 'analytics.json')
    if not os.path.isfile(analytics_path):
        return None
    prev_path = os.path.join(deal_dir, 'analytics_prev.json')
    try:
        with open(analytics_path, 'r', encoding='utf-8') as f:
            data = f.read()
        with open(prev_path, 'w', encoding='utf-8') as f:
            f.write(data)
        return prev_path
    except Exception as e:
        log.warning("Failed to snapshot analytics: %s", e)
        return None


def compute_delta(old_analytics: dict, new_analytics: dict, deal_slug: str) -> DeltaReport:
    """Compare two analytics snapshots and produce a DeltaReport."""
    report = DeltaReport(
        deal_slug=deal_slug,
        created_at=datetime.utcnow().isoformat() + 'Z',
    )

    # --- Period comparison ---
    old_periods = _extract_periods(old_analytics)
    new_periods = _extract_periods(new_analytics)
    report.new_periods = sorted(list(new_periods - old_periods))
    report.removed_periods = sorted(list(old_periods - new_periods))

    # --- ISRC comparison ---
    old_isrcs = _extract_isrcs(old_analytics)
    new_isrcs = _extract_isrcs(new_analytics)
    report.new_isrcs = sorted(list(new_isrcs - old_isrcs))[:100]  # Cap at 100
    report.removed_isrcs = sorted(list(old_isrcs - new_isrcs))[:100]

    # --- LTM Revenue comparison ---
    report.old_ltm_gross = old_analytics.get('ltm_gross_total', 0)
    report.new_ltm_gross = new_analytics.get('ltm_gross_total', 0)
    if report.old_ltm_gross > 0:
        report.ltm_gross_change_pct = round(
            (report.new_ltm_gross - report.old_ltm_gross) / report.old_ltm_gross * 100, 1)
    report.old_ltm_net = old_analytics.get('ltm_net_total', 0)
    report.new_ltm_net = new_analytics.get('ltm_net_total', 0)
    if report.old_ltm_net > 0:
        report.ltm_net_change_pct = round(
            (report.new_ltm_net - report.old_ltm_net) / report.old_ltm_net * 100, 1)

    report.old_isrc_count = old_analytics.get('isrc_count_raw', 0)
    report.new_isrc_count = new_analytics.get('isrc_count_raw', 0)

    # --- Per-payor variance ---
    old_payors = {ps['code']: ps for ps in old_analytics.get('payor_summaries', [])}
    new_payors = {ps['code']: ps for ps in new_analytics.get('payor_summaries', [])}
    all_codes = sorted(set(list(old_payors.keys()) + list(new_payors.keys())))
    for code in all_codes:
        old_ps = old_payors.get(code, {})
        new_ps = new_payors.get(code, {})
        old_gross = old_ps.get('total_gross_raw', 0)
        new_gross = new_ps.get('total_gross_raw', 0)
        pct = round((new_gross - old_gross) / old_gross * 100, 1) if old_gross > 0 else 0
        report.payor_variance.append({
            'code': code,
            'name': new_ps.get('name', old_ps.get('name', code)),
            'old_gross': round(old_gross, 2),
            'new_gross': round(new_gross, 2),
            'change': round(new_gross - old_gross, 2),
            'change_pct': pct,
        })

    # --- ISRC-level top movers (from ltm_songs) ---
    old_songs = {s['isrc']: s for s in old_analytics.get('ltm_songs', [])}
    new_songs = {s['isrc']: s for s in new_analytics.get('ltm_songs', [])}
    all_song_isrcs = set(list(old_songs.keys()) + list(new_songs.keys()))
    movers = []
    for isrc in all_song_isrcs:
        old_g = old_songs.get(isrc, {}).get('gross_raw', 0)
        new_g = new_songs.get(isrc, {}).get('gross_raw', 0)
        diff = new_g - old_g
        title = new_songs.get(isrc, old_songs.get(isrc, {})).get('title', isrc)
        movers.append({
            'isrc': isrc,
            'title': title,
            'old_gross': round(old_g, 2),
            'new_gross': round(new_g, 2),
            'change': round(diff, 2),
        })
    movers.sort(key=lambda x: x['change'], reverse=True)
    report.top_gainers = movers[:10]
    report.top_losers = list(reversed(movers[-10:]))

    # --- Summary text ---
    parts = []
    if report.new_periods:
        parts.append(f"{len(report.new_periods)} new month{'s' if len(report.new_periods) != 1 else ''} added")
    if report.removed_periods:
        parts.append(f"{len(report.removed_periods)} month{'s' if len(report.removed_periods) != 1 else ''} removed")
    if report.ltm_gross_change_pct != 0:
        direction = 'up' if report.ltm_gross_change_pct > 0 else 'down'
        parts.append(f"gross {direction} {abs(report.ltm_gross_change_pct)}%")
    if report.new_isrcs:
        parts.append(f"{len(report.new_isrcs)} new ISRC{'s' if len(report.new_isrcs) != 1 else ''}")
    report.summary = ', '.join(parts) if parts else 'No significant changes detected'

    return report


def load_delta_from_disk(deal_dir: str, deal_slug: str) -> Optional[DeltaReport]:
    """Load a previously computed delta from disk."""
    delta_path = os.path.join(deal_dir, 'delta_report.json')
    if not os.path.isfile(delta_path):
        return None
    try:
        with open(delta_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        report = DeltaReport(deal_slug=deal_slug)
        for key, val in data.items():
            if hasattr(report, key):
                setattr(report, key, val)
        return report
    except Exception as e:
        log.warning("Failed to load delta report: %s", e)
        return None


def save_delta_to_disk(deal_dir: str, report: DeltaReport):
    """Save delta report to disk."""
    delta_path = os.path.join(deal_dir, 'delta_report.json')
    try:
        with open(delta_path, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, indent=2)
    except Exception as e:
        log.warning("Failed to save delta report: %s", e)


def _extract_periods(analytics: dict) -> set:
    """Extract all unique periods from analytics data."""
    periods = set()
    for ps in analytics.get('payor_summaries', []):
        for ab in ps.get('annual_breakdown', []):
            pass  # Annual doesn't have periods
    # Use coverage_months
    for cm in analytics.get('coverage_months', []):
        periods.add(cm.get('period', 0))
    # Also check monthly_trend
    for mt in analytics.get('monthly_trend', []):
        p = mt.get('period')
        if p:
            periods.add(int(p))
    return periods


def _extract_isrcs(analytics: dict) -> set:
    """Extract all unique ISRCs from analytics data."""
    isrcs = set()
    for song in analytics.get('ltm_songs', []):
        isrc = song.get('isrc')
        if isrc:
            isrcs.add(isrc)
    for song in analytics.get('top_songs', []):
        isrc = song.get('isrc')
        if isrc:
            isrcs.add(isrc)
    return isrcs
