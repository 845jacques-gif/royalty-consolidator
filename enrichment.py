"""
Release Date Enrichment Engine
3-tier lookup (MusicBrainz, Genius, Gemini) with persistent JSON cache,
deduplication, and progress callbacks.
"""

import json
import logging
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import pandas as pd

log = logging.getLogger('royalty')

# Lazy DB module reference
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


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TrackLookupItem:
    """A single track to look up."""
    isrc: str = ''
    title: str = ''
    artist: str = ''
    existing_release_date: str = ''
    row_indices: list = field(default_factory=list)  # indices into detail_df


@dataclass
class EnrichmentResult:
    """Result of the enrichment process."""
    lookups: Dict[str, dict] = field(default_factory=dict)  # key -> {release_date, source, track_name, artist_name, looked_up}
    stats: Dict[str, int] = field(default_factory=dict)
    tracks_without_dates: List[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cache layer — separate from consolidator's isrc_cache.json
# ---------------------------------------------------------------------------

_enrichment_cache = {}
_enrichment_cache_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'release_date_cache.json')


def _load_cache():
    global _enrichment_cache
    # Try DB first
    dbm = _db()
    if dbm:
        try:
            _enrichment_cache = dbm.load_full_enrichment_cache_db()
            if _enrichment_cache:
                return
        except Exception as e:
            log.debug("DB enrichment cache load failed: %s", e)
    # Fall back to JSON file
    if os.path.exists(_enrichment_cache_path):
        try:
            with open(_enrichment_cache_path, 'r', encoding='utf-8') as f:
                _enrichment_cache = json.load(f)
        except (json.JSONDecodeError, IOError):
            _enrichment_cache = {}


def _save_cache():
    # Save to DB if available
    dbm = _db()
    if dbm:
        try:
            dbm.save_enrichment_cache_db(_enrichment_cache)
        except Exception as e:
            log.debug("DB enrichment cache save failed: %s", e)
    # Always save to file as fallback
    try:
        with open(_enrichment_cache_path, 'w', encoding='utf-8') as f:
            json.dump(_enrichment_cache, f, indent=2, ensure_ascii=False)
    except IOError as e:
        log.warning("Failed to save enrichment cache file: %s", e)


def _cache_key_isrc(isrc: str) -> str:
    return isrc.strip().upper()


def _cache_key_title_artist(title: str, artist: str) -> str:
    t = re.sub(r'\s+', ' ', title.strip().upper())
    a = re.sub(r'\s+', ' ', artist.strip().upper())
    return f"{t}::{a}"


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate_tracks(detail_df: pd.DataFrame) -> List[TrackLookupItem]:
    """Group detail rows into unique tracks for lookup.

    Groups by ISRC first; rows without ISRC group by (Title, Artist).
    Tracks that already have a release_date from source data are tagged SRC.
    Vectorized — avoids row-by-row iteration on large DataFrames.
    """
    # Ensure columns exist (normalize names)
    df = detail_df
    for col in ['ISRC', 'Title', 'Artist', 'Release Date']:
        if col not in df.columns:
            for alt in [col.lower(), col.replace(' ', '_').lower()]:
                if alt in df.columns:
                    df = df.rename(columns={alt: col})
                    break
            else:
                df[col] = ''

    # Vectorized column prep
    isrc_col = df['ISRC'].fillna('').astype(str).str.strip().str.upper()
    title_col = df['Title'].fillna('').astype(str).str.strip()
    artist_col = df['Artist'].fillna('').astype(str).str.strip()
    rd_col = df['Release Date'].fillna('').astype(str).str.strip()

    # Mark valid ISRCs
    valid_isrc = isrc_col.ne('') & ~isrc_col.isin(['NAN', 'NONE'])
    _invalid_rd = {'', 'nan', 'None', 'NaT'}

    items = []
    item_by_isrc = {}   # isrc -> item (O(1) lookup instead of linear scan)
    item_by_ta = {}     # ta_key -> item

    # Process rows with valid ISRC — use groupby for speed
    if valid_isrc.any():
        isrc_groups = df.loc[valid_isrc].groupby(isrc_col[valid_isrc])
        for isrc_val, group in isrc_groups:
            first_idx = group.index[0]
            rd_val = rd_col.at[first_idx]
            item = TrackLookupItem(
                isrc=isrc_val,
                title=title_col.at[first_idx],
                artist=artist_col.at[first_idx],
                existing_release_date=rd_val if rd_val not in _invalid_rd else '',
                row_indices=list(group.index),
            )
            items.append(item)
            item_by_isrc[isrc_val] = item

    # Process rows without ISRC — group by title+artist
    no_isrc = ~valid_isrc & title_col.ne('') & artist_col.ne('')
    if no_isrc.any():
        ta_key_col = title_col[no_isrc].str.upper().str.replace(r'\s+', ' ', regex=True) + '::' + \
                     artist_col[no_isrc].str.upper().str.replace(r'\s+', ' ', regex=True)
        ta_groups = df.loc[no_isrc].groupby(ta_key_col)
        for ta_key, group in ta_groups:
            first_idx = group.index[0]
            rd_val = rd_col.at[first_idx]
            item = TrackLookupItem(
                title=title_col.at[first_idx],
                artist=artist_col.at[first_idx],
                existing_release_date=rd_val if rd_val not in _invalid_rd else '',
                row_indices=list(group.index),
            )
            items.append(item)
            item_by_ta[ta_key] = item

    return items


# ---------------------------------------------------------------------------
# Tier 1: MusicBrainz
# ---------------------------------------------------------------------------

def _lookup_musicbrainz_isrc(isrc: str, _retries: int = 3) -> dict:
    """Look up a single ISRC on MusicBrainz. Retries on 503 rate-limit."""
    url = f"https://musicbrainz.org/ws/2/recording?query=isrc:{isrc}&fmt=json"

    result = {'release_date': '', 'track_name': '', 'artist_name': ''}
    for attempt in range(_retries):
        req = Request(url, headers={"User-Agent": "RoyaltyConsolidator/2.0 (contact@example.com)"})
        try:
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            recordings = data.get("recordings", [])
            if recordings:
                best_date = None
                best_rec = recordings[0]
                for rec in recordings:
                    frd = rec.get("first-release-date", "")
                    if frd and (best_date is None or frd < best_date):
                        best_date = frd
                        best_rec = rec

                artist_credit = best_rec.get("artist-credit", [{}])
                artist_name = artist_credit[0].get("name", "") if artist_credit else ""
                result = {
                    'release_date': best_date or '',
                    'track_name': best_rec.get("title", ""),
                    'artist_name': artist_name,
                }
            return result
        except (HTTPError, URLError) as e:
            if hasattr(e, 'code') and e.code == 503:
                time.sleep(1 + attempt)  # backoff: 1s, 2s, 3s
                continue
            log.debug("MB ISRC HTTP error for %s: %s", isrc, e)
            return result
        except Exception as e:
            log.debug("MB ISRC lookup error for %s: %s", isrc, e)
            return result

    return result


def _lookup_musicbrainz_title_artist(title: str, artist: str, _retries: int = 3) -> dict:
    """Search MusicBrainz by title+artist. Retries on 503 rate-limit."""
    query = f'recording:"{title}" AND artist:"{artist}"'
    url = f"https://musicbrainz.org/ws/2/recording?query={quote_plus(query)}&fmt=json&limit=5"

    result = {'release_date': '', 'track_name': '', 'artist_name': ''}
    for attempt in range(_retries):
        req = Request(url, headers={"User-Agent": "RoyaltyConsolidator/2.0 (contact@example.com)"})
        try:
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            recordings = data.get("recordings", [])
            if recordings:
                best_date = None
                best_rec = recordings[0]
                for rec in recordings:
                    frd = rec.get("first-release-date", "")
                    if frd and (best_date is None or frd < best_date):
                        best_date = frd
                        best_rec = rec

                artist_credit = best_rec.get("artist-credit", [{}])
                artist_name = artist_credit[0].get("name", "") if artist_credit else ""
                result = {
                    'release_date': best_date or '',
                    'track_name': best_rec.get("title", ""),
                    'artist_name': artist_name,
                }
            return result
        except (HTTPError, URLError) as e:
            if hasattr(e, 'code') and e.code == 503:
                time.sleep(1 + attempt)
                continue
            log.debug("MB title+artist HTTP error for '%s' by '%s': %s", title, artist, e)
            return result
        except Exception as e:
            log.debug("MB title+artist lookup error for '%s' by '%s': %s", title, artist, e)
            return result

    return result


# ---------------------------------------------------------------------------
# Tier 2: Genius API
# ---------------------------------------------------------------------------

def _lookup_genius(title: str, artist: str, token: str, min_confidence: float = 0.6) -> dict:
    """Search Genius for a track and return release date if found with sufficient confidence."""
    result = {'release_date': '', 'track_name': '', 'artist_name': ''}

    if not token:
        return result

    query = f"{title} {artist}"
    url = f"https://api.genius.com/search?q={quote_plus(query)}"
    req = Request(url, headers={
        "Authorization": f"Bearer {token}",
        "User-Agent": "RoyaltyConsolidator/2.0",
    })

    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        hits = data.get("response", {}).get("hits", [])
        if not hits:
            return result

        # Fuzzy match on title + artist
        best_score = 0.0
        best_hit = None
        target = f"{title} {artist}".lower()

        for hit in hits[:5]:
            song = hit.get("result", {})
            candidate = f"{song.get('title', '')} {song.get('primary_artist', {}).get('name', '')}".lower()
            score = SequenceMatcher(None, target, candidate).ratio()
            if score > best_score:
                best_score = score
                best_hit = song

        if best_hit and best_score >= min_confidence:
            # Genius search results don't always include release_date directly,
            # but the song endpoint does. Fetch it.
            song_id = best_hit.get('id')
            if song_id:
                song_url = f"https://api.genius.com/songs/{song_id}"
                song_req = Request(song_url, headers={
                    "Authorization": f"Bearer {token}",
                    "User-Agent": "RoyaltyConsolidator/2.0",
                })
                try:
                    with urlopen(song_req, timeout=10) as song_resp:
                        song_data = json.loads(song_resp.read())
                    song_info = song_data.get("response", {}).get("song", {})
                    rd = song_info.get("release_date") or ''
                    result = {
                        'release_date': rd,
                        'track_name': song_info.get('title', best_hit.get('title', '')),
                        'artist_name': song_info.get('primary_artist', {}).get('name', ''),
                    }
                except Exception as e:
                    log.debug("Genius song detail fetch failed for '%s': %s", title, e)
                    result = {
                        'release_date': '',
                        'track_name': best_hit.get('title', ''),
                        'artist_name': best_hit.get('primary_artist', {}).get('name', ''),
                    }

    except (HTTPError, URLError) as e:
        log.debug("Genius search HTTP error for '%s': %s", title, e)
    except Exception as e:
        log.debug("Genius search error for '%s': %s", title, e)

    return result


# ---------------------------------------------------------------------------
# Tier 3: Gemini API (batch)
# ---------------------------------------------------------------------------

def _lookup_gemini_batch(items: List[TrackLookupItem], api_key: str,
                          batch_size: int = 20,
                          progress_callback: Optional[Callable] = None,
                          timeout_per_batch: int = 30) -> Dict[str, dict]:
    """Use Gemini to look up release dates in batches. Returns {cache_key: result_dict}.

    progress_callback: optional callable({phase, current, total, message}) for UI updates.
    timeout_per_batch: seconds to wait for each Gemini API call before giving up (default 30).
    """
    results = {}

    if not api_key or not items:
        return results

    try:
        from google import genai
        _client = genai.Client(api_key=api_key)
    except Exception as exc:
        print(f"[enrichment] Gemini init error: {exc}", file=sys.stderr, flush=True)
        return results

    total_batches = (len(items) + batch_size - 1) // batch_size
    executor = ThreadPoolExecutor(max_workers=min(5, total_batches))

    # Process in batches
    for batch_num, batch_start in enumerate(range(0, len(items), batch_size)):
        batch = items[batch_start:batch_start + batch_size]

        if progress_callback:
            progress_callback({
                'phase': 'gemini',
                'current': batch_start,
                'total': len(items),
                'message': f'Gemini: batch {batch_num + 1}/{total_batches} ({batch_start}/{len(items)} tracks)...',
            })

        # Build prompt
        lines = []
        for i, item in enumerate(batch):
            if item.isrc:
                lines.append(f"{i+1}. ISRC: {item.isrc} | Title: {item.title} | Artist: {item.artist}")
            else:
                lines.append(f"{i+1}. Title: {item.title} | Artist: {item.artist}")

        prompt = (
            "For each song below, provide the original release date in YYYY-MM-DD format. "
            "If you don't know the exact date, provide YYYY-MM or YYYY. "
            "If you cannot determine the release date, write 'UNKNOWN'.\n"
            "Respond with ONLY numbered lines in the format: NUMBER. YYYY-MM-DD\n\n"
            + "\n".join(lines)
        )

        try:
            # Run generate_content with a timeout to avoid hanging indefinitely
            future = executor.submit(
                lambda p: _client.models.generate_content(model='gemini-2.0-flash', contents=p),
                prompt,
            )
            response = future.result(timeout=timeout_per_batch)
            response_text = response.text.strip()

            # Parse response line by line
            for line in response_text.split('\n'):
                line = line.strip()
                if not line:
                    continue
                # Match "1. 2022-05-13" or "1. UNKNOWN"
                m = re.match(r'^(\d+)\.\s*(.+)$', line)
                if m:
                    idx = int(m.group(1)) - 1
                    date_str = m.group(2).strip()
                    if idx < len(batch) and date_str.upper() != 'UNKNOWN':
                        # Validate date format
                        if re.match(r'^\d{4}(-\d{2})?(-\d{2})?$', date_str):
                            item = batch[idx]
                            key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)
                            results[key] = {
                                'release_date': date_str,
                                'source': 'GM',
                                'track_name': item.title,
                                'artist_name': item.artist,
                                'looked_up': True,
                            }
        except FuturesTimeoutError:
            print(f"[enrichment] Gemini batch {batch_num + 1}/{total_batches} timed out after {timeout_per_batch}s — skipping",
                  file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[enrichment] Gemini batch {batch_num + 1}/{total_batches} error: {exc}",
                  file=sys.stderr, flush=True)

        # Small delay between batches
        if batch_start + batch_size < len(items):
            time.sleep(1)

    executor.shutdown(wait=False)
    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def enrich_release_dates(
    detail_df: pd.DataFrame,
    genius_token: str = '',
    gemini_api_key: str = '',
    skip_tiers: Optional[List[str]] = None,
    progress_callback: Optional[Callable] = None,
) -> EnrichmentResult:
    """Run 3-tier release date enrichment on a detail DataFrame.

    progress_callback: called with {phase, current, total, message}
    skip_tiers: list of tier codes to skip ('MB', 'GN', 'GM')
    """
    skip_tiers = skip_tiers or []
    _load_cache()

    result = EnrichmentResult()
    stats = {
        'total': 0,
        'from_source': 0,
        'from_cache': 0,
        'mb_found': 0,
        'gn_found': 0,
        'gm_found': 0,
        'not_found': 0,
    }

    # Deduplicate tracks
    items = deduplicate_tracks(detail_df)
    stats['total'] = len(items)

    if progress_callback:
        progress_callback({'phase': 'dedup', 'current': 0, 'total': len(items),
                           'message': f'Found {len(items)} unique tracks to look up'})

    # Separate tracks that already have dates from source
    need_lookup = []
    for item in items:
        key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)

        if item.existing_release_date:
            result.lookups[key] = {
                'release_date': item.existing_release_date,
                'source': 'SRC',
                'track_name': item.title,
                'artist_name': item.artist,
                'looked_up': False,
            }
            stats['from_source'] += 1
        elif key in _enrichment_cache and _enrichment_cache[key].get('release_date'):
            result.lookups[key] = _enrichment_cache[key]
            stats['from_cache'] += 1
        else:
            need_lookup.append(item)

    if progress_callback:
        progress_callback({'phase': 'cache', 'current': stats['from_source'] + stats['from_cache'],
                           'total': len(items),
                           'message': f"{stats['from_source']} from source data, {stats['from_cache']} from cache, {len(need_lookup)} to look up"})

    # Tier 1: MusicBrainz (multi-threaded, 10 workers)
    still_need = []
    if 'MB' not in skip_tiers and need_lookup:
        mb_lock = threading.Lock()
        mb_counter = [0]  # mutable counter for progress

        def _mb_worker(item):
            """Look up a single item on MusicBrainz (runs in thread pool)."""
            if item.isrc:
                mb_result = _lookup_musicbrainz_isrc(item.isrc)
            else:
                mb_result = _lookup_musicbrainz_title_artist(item.title, item.artist)

            key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)

            with mb_lock:
                mb_counter[0] += 1
                if progress_callback:
                    progress_callback({
                        'phase': 'musicbrainz', 'current': mb_counter[0],
                        'total': len(need_lookup),
                        'message': f'MusicBrainz: {mb_counter[0]}/{len(need_lookup)} — {item.title or item.isrc}',
                    })

            return item, key, mb_result

        with ThreadPoolExecutor(max_workers=min(20, len(need_lookup))) as mb_pool:
            futures = [mb_pool.submit(_mb_worker, item) for item in need_lookup]

            for future in as_completed(futures):
                try:
                    item, key, mb_result = future.result()
                except Exception as e:
                    log.debug("MB worker thread error: %s", e)
                    continue

                if mb_result.get('release_date'):
                    entry = {
                        'release_date': mb_result['release_date'],
                        'source': 'MB',
                        'track_name': mb_result.get('track_name', item.title),
                        'artist_name': mb_result.get('artist_name', item.artist),
                        'looked_up': True,
                    }
                    result.lookups[key] = entry
                    _enrichment_cache[key] = entry
                    stats['mb_found'] += 1
                else:
                    _enrichment_cache[key] = {
                        'release_date': '',
                        'source': '',
                        'track_name': item.title,
                        'artist_name': item.artist,
                        'looked_up': True,
                    }
                    still_need.append(item)
    else:
        still_need = need_lookup

    _save_cache()

    # Tier 2: Genius (multi-threaded, 10 workers)
    genius_still_need = []
    if 'GN' not in skip_tiers and genius_token and still_need:
        gn_lock = threading.Lock()
        gn_counter = [0]

        def _gn_worker(item):
            """Look up a single item on Genius (runs in thread pool)."""
            gn_result = _lookup_genius(item.title, item.artist, genius_token)
            key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)

            with gn_lock:
                gn_counter[0] += 1
                if progress_callback:
                    progress_callback({
                        'phase': 'genius', 'current': gn_counter[0],
                        'total': len(still_need),
                        'message': f'Genius: {gn_counter[0]}/{len(still_need)} — {item.title}',
                    })

            return item, key, gn_result

        with ThreadPoolExecutor(max_workers=min(20, len(still_need))) as gn_pool:
            futures = [gn_pool.submit(_gn_worker, item) for item in still_need]

            for future in as_completed(futures):
                try:
                    item, key, gn_result = future.result()
                except Exception as e:
                    log.debug("Genius worker thread error: %s", e)
                    continue

                if gn_result.get('release_date'):
                    entry = {
                        'release_date': gn_result['release_date'],
                        'source': 'GN',
                        'track_name': gn_result.get('track_name', item.title),
                        'artist_name': gn_result.get('artist_name', item.artist),
                        'looked_up': True,
                    }
                    result.lookups[key] = entry
                    _enrichment_cache[key] = entry
                    stats['gn_found'] += 1
                else:
                    genius_still_need.append(item)
    else:
        genius_still_need = still_need

    _save_cache()

    # Tier 3: Gemini
    if 'GM' not in skip_tiers and gemini_api_key and genius_still_need:
        if progress_callback:
            progress_callback({'phase': 'gemini', 'current': 0, 'total': len(genius_still_need),
                               'message': f'Gemini: Processing {len(genius_still_need)} tracks in batches...'})

        gm_results = _lookup_gemini_batch(genius_still_need, gemini_api_key,
                                           progress_callback=progress_callback)

        for key, entry in gm_results.items():
            result.lookups[key] = entry
            _enrichment_cache[key] = entry
            stats['gm_found'] += 1

        # Mark remaining as not found
        for item in genius_still_need:
            key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)
            if key not in result.lookups:
                stats['not_found'] += 1
                result.tracks_without_dates.append({
                    'isrc': item.isrc,
                    'title': item.title,
                    'artist': item.artist,
                })
    else:
        # All remaining from Genius step are not found
        for item in genius_still_need:
            key = _cache_key_isrc(item.isrc) if item.isrc else _cache_key_title_artist(item.title, item.artist)
            if key not in result.lookups:
                stats['not_found'] += 1
                result.tracks_without_dates.append({
                    'isrc': item.isrc,
                    'title': item.title,
                    'artist': item.artist,
                })

    _save_cache()

    result.stats = stats

    if progress_callback:
        progress_callback({'phase': 'done', 'current': stats['total'], 'total': stats['total'],
                           'message': f"Done! SRC:{stats['from_source']} Cache:{stats['from_cache']} MB:{stats['mb_found']} GN:{stats['gn_found']} GM:{stats['gm_found']} Not found:{stats['not_found']}"})

    return result


# ---------------------------------------------------------------------------
# Apply enrichment to detail DataFrame
# ---------------------------------------------------------------------------

def apply_enrichment_to_detail(detail_df: pd.DataFrame, lookups: Dict[str, dict]) -> pd.DataFrame:
    """Apply enrichment lookup results back to the detail DataFrame.

    Adds/updates 'Release Date' and 'Release Date Source' columns.
    Matches by ISRC first, then Title::Artist fallback.
    """
    df = detail_df.copy()

    if 'Release Date Source' not in df.columns:
        df['Release Date Source'] = ''

    for idx, row in df.iterrows():
        isrc = str(row.get('ISRC', '')).strip().upper()
        title = str(row.get('Title', '')).strip()
        artist = str(row.get('Artist', '')).strip()

        # Try ISRC key first
        entry = None
        if isrc and isrc not in ('', 'NAN', 'NONE'):
            key = _cache_key_isrc(isrc)
            entry = lookups.get(key)

        # Fallback to title::artist
        if not entry and title and artist:
            key = _cache_key_title_artist(title, artist)
            entry = lookups.get(key)

        if entry and entry.get('release_date'):
            current_rd = str(row.get('Release Date', '')).strip()
            if not current_rd or current_rd in ('', 'nan', 'None', 'NaT'):
                df.at[idx, 'Release Date'] = entry['release_date']
            df.at[idx, 'Release Date Source'] = entry.get('source', '')

    return df
