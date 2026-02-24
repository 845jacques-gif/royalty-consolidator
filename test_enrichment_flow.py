"""
Test script: full custom ingest flow through to enrichment on the Royalty Consolidator Flask app.

Exercises:
  POST /custom/upload  ->  preview/map per payor  ->  process  ->  validate  ->  calc  ->  enrich
  Polls /api/enrichment-status until done, then GETs /custom/enrich to fetch final stats.
"""

import json
import re
import sys
import time

import requests
from bs4 import BeautifulSoup

BASE = "http://127.0.0.1:5000"
DEMO = r"C:\Users\jacques\Downloads\royalty_consolidator\demo_data"

PAYORS = [
    {"code": "B1",   "name": "Believe Digital", "dir": f"{DEMO}\\statements_B1",   "fmt": "auto", "stype": "masters"},
    {"code": "FUGA", "name": "FUGA",            "dir": f"{DEMO}\\statements_FUGA", "fmt": "auto", "stype": "masters"},
    {"code": "ST",   "name": "Songtrust",       "dir": f"{DEMO}\\statements_ST",   "fmt": "auto", "stype": "publishing"},
    {"code": "EMP",  "name": "Empire",           "dir": f"{DEMO}\\statements_EMP", "fmt": "auto", "stype": "masters"},
]

TIMEOUT = 120  # seconds max for enrichment polling
POLL_INTERVAL = 3  # seconds between polls


def banner(msg):
    print(f"\n{'='*70}")
    print(f"  {msg}")
    print(f"{'='*70}")


def check_ok(resp, step_name, allow_redirect=True):
    """Verify response is OK.  Print summary and return the response."""
    final_url = resp.url
    code = resp.status_code
    ok = code == 200 or (allow_redirect and 200 <= code < 400)
    status = "OK" if ok else "FAIL"
    print(f"  [{status}] {step_name}: HTTP {code}  -> {final_url}")
    if not ok:
        # Try to extract flash messages for debugging
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            flashes = soup.select(".flash, .alert, .toast-body")
            for f in flashes:
                print(f"    Flash: {f.get_text(strip=True)}")
        except Exception:
            pass
        print(f"    Response length: {len(resp.text)} bytes")
    return resp


def extract_flash_messages(html):
    """Pull flash messages from the HTML for diagnostic output."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        msgs = []
        for el in soup.select(".toast-body, .flash, .alert"):
            txt = el.get_text(strip=True)
            if txt:
                msgs.append(txt)
        return msgs
    except Exception:
        return []


def main():
    sess = requests.Session()
    sess.headers["User-Agent"] = "RoyaltyConsolidator-TestBot/1.0"

    # ── 0. Health check ──────────────────────────────────────────────
    banner("Step 0: Health check")
    try:
        r = sess.get(BASE, timeout=10)
        print(f"  App reachable: HTTP {r.status_code}")
    except Exception as e:
        print(f"  FATAL: Cannot reach {BASE}: {e}")
        sys.exit(1)

    # ── 1. POST /custom/upload ───────────────────────────────────────
    banner("Step 1: POST /custom/upload with 4 payors (local dirs)")
    form = {"deal_name": "Test Deal - Enrichment Flow"}
    for i, p in enumerate(PAYORS):
        form[f"payor_code_{i}"] = p["code"]
        form[f"payor_name_{i}"] = p["name"]
        form[f"payor_dir_{i}"]  = p["dir"]
        form[f"payor_fmt_{i}"]  = p["fmt"]
        form[f"payor_stype_{i}"] = p["stype"]
        form[f"payor_fee_{i}"]  = ""
        form[f"payor_fx_{i}"]   = "USD"
        form[f"payor_fxrate_{i}"] = ""
        form[f"payor_split_{i}"] = ""
        form[f"payor_territory_{i}"] = ""

    r = sess.post(f"{BASE}/custom/upload", data=form, allow_redirects=True, timeout=30)
    check_ok(r, "custom/upload -> redirect")

    flashes = extract_flash_messages(r.text)
    for f in flashes:
        print(f"    Flash: {f}")

    # The app should redirect through preview/map for each payor (and each
    # structure within a payor).  If Quick Ingest kicks in (fingerprint match),
    # the GET to /custom/preview will auto-redirect past preview+map.
    #
    # We need to handle both scenarios:
    #   a) Quick Ingest: GET preview -> auto-redirect to next payor or /custom/process
    #   b) No saved mapping: shows preview page, we POST to accept, then map page, POST to accept

    current_url = r.url  # After all redirects

    # ── 2-3. Navigate preview + map for each payor/structure ─────────
    banner("Step 2-3: Preview + Map loop")

    # We'll detect where we are from the current URL after the upload redirect
    MAX_ITERATIONS = 30  # safety valve
    iteration = 0

    while iteration < MAX_ITERATIONS:
        iteration += 1

        # Determine current step from the URL
        if "/custom/process" in current_url:
            print("  -> Reached /custom/process (all payors mapped)")
            break
        elif "/custom/validate" in current_url:
            print("  -> Reached /custom/validate (processing done)")
            break
        elif "/custom/preview" in current_url:
            page_html = r.text
            flashes = extract_flash_messages(page_html)
            for f in flashes:
                print(f"    Flash: {f}")

            # Determine if this is a preview form page or just a redirect landing.
            # The preview form page has an action=clean submit button / form.
            # If the page has a form with remove_top input, it is showing a real preview.
            soup_check = BeautifulSoup(page_html, "html.parser")
            has_preview_form = bool(soup_check.find("input", {"name": "remove_top"}))

            if not has_preview_form:
                # Not a real preview page (possibly blank or error); try GETting the URL directly
                print(f"    No preview form found at {current_url}, re-GETting...")
                r = sess.get(current_url, allow_redirects=True, timeout=30)
                check_ok(r, f"GET {current_url}")
                current_url = r.url
                # Re-check: did we redirect away from preview?
                if "/custom/preview" not in current_url:
                    continue
                # Re-check for form
                soup_check2 = BeautifulSoup(r.text, "html.parser")
                has_preview_form = bool(soup_check2.find("input", {"name": "remove_top"}))
                if not has_preview_form:
                    print(f"    Still no preview form; breaking to avoid loop")
                    break

            # It's showing a preview page - POST to accept defaults (action=clean)
            # Parse payor_idx and struct_idx from URL
            m = re.search(r'/custom/preview/(\d+)(?:/(\d+))?', current_url)
            if not m:
                print(f"  ERROR: Cannot parse preview URL: {current_url}")
                break
            payor_idx = m.group(1)
            struct_idx = m.group(2) or "0"
            print(f"  Preview payor_idx={payor_idx}, struct_idx={struct_idx}")

            # POST to accept preview (send cleaning defaults)
            post_url = f"{BASE}/custom/preview/{payor_idx}/{struct_idx}"
            r = sess.post(post_url, data={
                "action": "clean",
                "remove_top": "0",
                "remove_bottom": "0",
                "sheet": "",
            }, allow_redirects=True, timeout=30)
            check_ok(r, f"POST preview/{payor_idx}/{struct_idx}")
            current_url = r.url
            # This should redirect to /custom/map/{payor_idx}/{struct_idx}

        elif "/custom/map" in current_url:
            page_html = r.text
            flashes = extract_flash_messages(page_html)
            for f in flashes:
                print(f"    Flash: {f}")

            # Parse payor_idx and struct_idx from URL
            m = re.search(r'/custom/map/(\d+)(?:/(\d+))?', current_url)
            if not m:
                print(f"  ERROR: Cannot parse map URL: {current_url}")
                break
            payor_idx = m.group(1)
            struct_idx = m.group(2) or "0"
            print(f"  Map payor_idx={payor_idx}, struct_idx={struct_idx}")

            # Parse the proposed mappings from the page HTML
            # We need: headers_json (hidden), and map_{i} for each header
            soup = BeautifulSoup(page_html, "html.parser")

            # Find the headers_json hidden input
            headers_input = soup.find("input", {"name": "headers_json"})
            if headers_input:
                headers_json = headers_input.get("value", "[]")
            else:
                # Try to find it as a textarea or other element
                headers_json = "[]"
                print("    WARNING: Could not find headers_json input")

            try:
                headers = json.loads(headers_json)
            except (json.JSONDecodeError, ValueError):
                headers = []

            print(f"    Headers ({len(headers)}): {headers[:5]}{'...' if len(headers)>5 else ''}")

            # Build mapping form: use proposed auto-mappings from select elements
            map_data = {"headers_json": headers_json}
            for i, h in enumerate(headers):
                # Find the select element for this mapping
                select = soup.find("select", {"name": f"map_{i}"})
                if select:
                    # Get the selected option (the one with 'selected' attr)
                    selected_opt = select.find("option", selected=True)
                    if selected_opt:
                        val = selected_opt.get("value", "")
                    else:
                        # No explicit selected - take the first non-empty option
                        # Actually, the proposed mapping might be set via a different mechanism
                        val = ""
                        for opt in select.find_all("option"):
                            if opt.get("value") and opt.get("selected") is not None:
                                val = opt["value"]
                                break
                    map_data[f"map_{i}"] = val
                else:
                    map_data[f"map_{i}"] = ""

            # Count how many fields we're mapping
            mapped = sum(1 for k, v in map_data.items() if k.startswith("map_") and v)
            print(f"    Accepting {mapped}/{len(headers)} auto-proposed mappings")

            # POST to accept mapping
            post_url = f"{BASE}/custom/map/{payor_idx}/{struct_idx}"
            r = sess.post(post_url, data=map_data, allow_redirects=True, timeout=30)
            check_ok(r, f"POST map/{payor_idx}/{struct_idx}")
            current_url = r.url
            # This should redirect to next preview or /custom/process

        else:
            print(f"  Unexpected URL after loop: {current_url}")
            break

    # ── 4. Process (GET /custom/process) ─────────────────────────────
    banner("Step 4: Process files")
    if "/custom/process" in current_url:
        # We're already here from a redirect, but the GET triggers processing
        # Actually the custom_process route does the processing on GET
        # and redirects to /custom/validate
        # The r.text should already have the result since we followed redirects
        # But let's check if we need to explicitly GET it
        if "/custom/validate" in r.url:
            print("  Process completed, already at validate")
            current_url = r.url
        else:
            r = sess.get(f"{BASE}/custom/process", allow_redirects=True, timeout=120)
            check_ok(r, "GET /custom/process -> validate")
            current_url = r.url
    elif "/custom/validate" in current_url:
        print("  Already at validate (Quick Ingest path)")
    else:
        # Try to GET process explicitly
        r = sess.get(f"{BASE}/custom/process", allow_redirects=True, timeout=120)
        check_ok(r, "GET /custom/process -> validate")
        current_url = r.url

    flashes = extract_flash_messages(r.text)
    for f in flashes:
        print(f"    Flash: {f}")

    # ── 5. Validate ──────────────────────────────────────────────────
    banner("Step 5: Validate (POST action=continue)")
    if "/custom/validate" not in current_url:
        r = sess.get(f"{BASE}/custom/validate", allow_redirects=True, timeout=30)
        check_ok(r, "GET /custom/validate")

    # Show validation summary if present
    page_html = r.text
    soup = BeautifulSoup(page_html, "html.parser")
    # Look for validation result counts
    badges = soup.select(".badge")
    for b in badges:
        txt = b.get_text(strip=True)
        if txt:
            print(f"    Validation badge: {txt}")

    # POST to continue past validation
    r = sess.post(f"{BASE}/custom/validate", data={"action": "continue"}, allow_redirects=True, timeout=30)
    check_ok(r, "POST /custom/validate (continue)")
    current_url = r.url

    # ── 6. Calc ──────────────────────────────────────────────────────
    banner("Step 6: Calc (POST to continue - accept auto-calc)")
    if "/custom/calc" not in current_url:
        r = sess.get(f"{BASE}/custom/calc", allow_redirects=True, timeout=30)
        check_ok(r, "GET /custom/calc")

    # POST to accept calc defaults (no custom formulas)
    r = sess.post(f"{BASE}/custom/calc", data={}, allow_redirects=True, timeout=30)
    check_ok(r, "POST /custom/calc (accept defaults)")
    current_url = r.url

    # ── 7. Enrich ────────────────────────────────────────────────────
    banner("Step 7: Start enrichment (POST action=enrich, use_musicbrainz=1)")
    if "/custom/enrich" not in current_url:
        r = sess.get(f"{BASE}/custom/enrich", allow_redirects=True, timeout=30)
        check_ok(r, "GET /custom/enrich")

    # POST to start enrichment with MusicBrainz only
    r = sess.post(f"{BASE}/custom/enrich", data={
        "action": "enrich",
        "use_musicbrainz": "1",
    }, allow_redirects=True, timeout=30)
    check_ok(r, "POST /custom/enrich (start enrichment)")

    # ── 8. Poll enrichment status ────────────────────────────────────
    banner("Step 8: Polling /api/enrichment-status")
    start_time = time.time()
    final_status = None

    while True:
        elapsed = time.time() - start_time
        if elapsed > TIMEOUT:
            print(f"  TIMEOUT after {elapsed:.0f}s")
            break

        time.sleep(POLL_INTERVAL)

        try:
            r = sess.get(f"{BASE}/api/enrichment-status", timeout=10)
            status = r.json()
        except Exception as e:
            print(f"  Poll error: {e}")
            continue

        phase     = status.get("phase", "?")
        current   = status.get("current", 0)
        total     = status.get("total", 0)
        message   = status.get("message", "")
        eta       = status.get("eta_seconds", 0)
        running   = status.get("running", False)
        done      = status.get("done", False)
        error     = status.get("error")
        need      = status.get("need_lookup", 0)

        pct = f"{current}/{total}" if total > 0 else f"{current}/?"
        eta_str = f"ETA {eta}s" if eta and eta > 0 else ""
        print(f"  [{elapsed:5.1f}s] phase={phase:<12s} progress={pct:<10s} need_lookup={need}  {eta_str}  | {message}")

        if error:
            print(f"  ERROR: {error}")
            final_status = status
            break

        if done:
            print(f"  DONE in {elapsed:.1f}s")
            final_status = status
            break

        if not running and not done:
            # Not running and not done - might be an issue
            print(f"  WARNING: not running and not done (phase={phase})")
            final_status = status
            break

    # ── 9. Fetch final enrichment page ───────────────────────────────
    banner("Step 9: GET /custom/enrich (final results)")
    r = sess.get(f"{BASE}/custom/enrich", allow_redirects=True, timeout=30)
    check_ok(r, "GET /custom/enrich (results)")

    page_html = r.text
    flashes = extract_flash_messages(page_html)
    for f in flashes:
        print(f"    Flash: {f}")

    # Parse enrichment stats from the page
    soup = BeautifulSoup(page_html, "html.parser")

    # Try to find enrichment stats in the page
    # Look for common stat elements
    print("\n  --- Enrichment Results (from page) ---")

    # Look for stat cards or summary elements
    stat_cards = soup.select(".card, .stat-card, .enrichment-stat")
    for card in stat_cards:
        text = card.get_text(" ", strip=True)
        # Only print cards that seem related to enrichment
        if any(kw in text.lower() for kw in ["enrich", "release", "found", "lookup", "cache", "track", "miss", "total", "musicbrainz", "mb", "source"]):
            # Compact the text
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            compact = " | ".join(lines[:3])
            print(f"    {compact}")

    # Also look for table rows with stats
    tables = soup.select("table")
    for tbl in tables:
        rows = tbl.select("tr")
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.select("td, th")]
            if cells:
                print(f"    {'  |  '.join(cells)}")

    # Also try to extract from the enrichment-status JSON one more time
    try:
        r2 = sess.get(f"{BASE}/api/enrichment-status", timeout=10)
        final_json = r2.json()
        print(f"\n  --- Final enrichment-status JSON ---")
        for k, v in final_json.items():
            print(f"    {k}: {v}")
    except Exception as e:
        print(f"  Could not fetch final status JSON: {e}")

    # ── Summary ──────────────────────────────────────────────────────
    banner("SUMMARY")
    if final_status:
        print(f"  Phase:    {final_status.get('phase', '?')}")
        print(f"  Done:     {final_status.get('done', False)}")
        print(f"  Error:    {final_status.get('error', None)}")
        print(f"  Current:  {final_status.get('current', 0)}")
        print(f"  Total:    {final_status.get('total', 0)}")
        print(f"  Message:  {final_status.get('message', '')}")
    else:
        print("  No final status captured (timeout or error)")

    print("\nDone.")


if __name__ == "__main__":
    main()
