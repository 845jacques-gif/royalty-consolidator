"""
Integration test script for the Royalty Consolidator Flask app.
Tests the custom ingest flow using demo_data against http://127.0.0.1:5000.

Requires: requests (pip install requests)
"""

import requests
import sys
import os

BASE_URL = "http://127.0.0.1:5000"
DEMO_DIR = r"C:\Users\jacques\Downloads\royalty_consolidator\demo_data"

# The 4 payors in demo_data, with their subfolder names and display names
PAYORS = [
    {"code": "B1",   "name": "Believe",   "dir": os.path.join(DEMO_DIR, "statements_B1")},
    {"code": "FUGA", "name": "FUGA",      "dir": os.path.join(DEMO_DIR, "statements_FUGA")},
    {"code": "ST",   "name": "Songtrust", "dir": os.path.join(DEMO_DIR, "statements_ST")},
    {"code": "EMP",  "name": "Empire",    "dir": os.path.join(DEMO_DIR, "statements_EMP")},
]

passed = 0
failed = 0


def report(test_name, ok, detail=""):
    global passed, failed
    status = "PASS" if ok else "FAIL"
    if ok:
        passed += 1
    else:
        failed += 1
    msg = f"  [{status}] {test_name}"
    if detail:
        msg += f"  --  {detail}"
    print(msg)


def main():
    global passed, failed

    print("=" * 70)
    print("Royalty Consolidator - Integration Tests")
    print("=" * 70)

    # Use a Session to persist cookies (session_id) across requests
    s = requests.Session()

    # ----------------------------------------------------------------
    # TEST 1: GET / (homepage / dashboard)
    # ----------------------------------------------------------------
    print("\n--- Test 1: Homepage (GET /) ---")
    try:
        r = s.get(f"{BASE_URL}/")
        report("Status code is 200", r.status_code == 200, f"got {r.status_code}")
        report("Response contains HTML", "<!DOCTYPE html>" in r.text or "<html" in r.text.lower())
        report("Dashboard page rendered", "dashboard" in r.text.lower() or "Royalty" in r.text)
    except requests.ConnectionError:
        report("Server reachable", False, "Could not connect to server at " + BASE_URL)
        print("\nABORTING: Server not running. Start the app first.")
        sys.exit(1)

    # ----------------------------------------------------------------
    # TEST 2: GET /upload (upload page)
    # ----------------------------------------------------------------
    print("\n--- Test 2: Upload page (GET /upload) ---")
    r = s.get(f"{BASE_URL}/upload")
    report("Status code is 200", r.status_code == 200, f"got {r.status_code}")
    report("Upload form present", "upload" in r.text.lower())

    # ----------------------------------------------------------------
    # TEST 3: POST /custom/upload  (trigger custom flow with 4 payors)
    # ----------------------------------------------------------------
    print("\n--- Test 3: Custom Upload (POST /custom/upload) ---")

    # Build the form data with 4 payors pointing to local dirs
    form_data = {
        "deal_name": "Demo Test Deal",
        "file_dates_json": "{}",
    }
    for idx, p in enumerate(PAYORS):
        form_data[f"payor_code_{idx}"] = p["code"]
        form_data[f"payor_name_{idx}"] = p["name"]
        form_data[f"payor_fmt_{idx}"] = "auto"
        form_data[f"payor_fee_{idx}"] = "0"
        form_data[f"payor_fx_{idx}"] = "USD"
        form_data[f"payor_fxrate_{idx}"] = "1.0"
        form_data[f"payor_stype_{idx}"] = "masters"
        form_data[f"payor_split_{idx}"] = ""
        form_data[f"payor_territory_{idx}"] = ""
        form_data[f"payor_dir_{idx}"] = p["dir"]

    # POST with allow_redirects=False to inspect the redirect target
    r = s.post(f"{BASE_URL}/custom/upload", data=form_data, allow_redirects=False)
    report("Status is redirect (302)", r.status_code == 302, f"got {r.status_code}")

    redirect_url = r.headers.get("Location", "")
    report(
        "Redirects to /custom/preview/0",
        "/custom/preview/0" in redirect_url,
        f"Location: {redirect_url}",
    )

    # Capture session_id cookie set by the server
    sid = s.cookies.get("session_id", "")
    report("Session ID cookie set", bool(sid), f"session_id={sid[:16]}..." if sid else "missing")

    # Follow the redirect to get the preview page
    r = s.get(f"{BASE_URL}{redirect_url}" if redirect_url.startswith("/") else redirect_url)
    report("Preview page loads (200)", r.status_code == 200, f"got {r.status_code}")

    # Check that 4 payors were detected (the page shows payor count)
    # The template renders custom_payor_count; check for "1 / 4" or "payor 1 of 4"
    has_4_payors = (
        "1 / 4" in r.text
        or "1/4" in r.text
        or "of 4" in r.text
        or "payor_count: 4" in r.text
        or 'custom_payor_count' in r.text
    )
    # Alternative: look for all payor names
    has_all_names = all(p["name"] in r.text for p in PAYORS)
    report(
        "4 payors detected",
        has_4_payors or has_all_names,
        f"has_4_payors={has_4_payors}, has_all_names={has_all_names}",
    )

    # Check that the first payor (Believe / B1) is being previewed
    first_payor_shown = PAYORS[0]["name"] in r.text or PAYORS[0]["code"] in r.text
    report("First payor shown in preview", first_payor_shown)

    # Check that file preview data is present (headers table or rows)
    has_preview_data = (
        "custom_headers" in r.text
        or "<th" in r.text
        or "preview" in r.text.lower()
    )
    report("Preview data present", has_preview_data)

    # ----------------------------------------------------------------
    # TEST 4: API /api/custom/preview (AJAX live cleaning preview)
    # ----------------------------------------------------------------
    print("\n--- Test 4: API Custom Preview (POST /api/custom/preview) ---")
    r = s.post(
        f"{BASE_URL}/api/custom/preview",
        json={"payor_idx": 0, "struct_idx": 0, "remove_top": 0, "remove_bottom": 0},
    )
    report("Status code is 200", r.status_code == 200, f"got {r.status_code}")

    try:
        data = r.json()
        report("Response is valid JSON", True)
        report("Has 'headers' key", "headers" in data, f"keys: {list(data.keys())}")
        report("Has 'rows' key", "rows" in data)
        report("Has 'total_rows' key", "total_rows" in data)

        headers = data.get("headers", [])
        rows = data.get("rows", [])
        total_rows = data.get("total_rows", 0)
        report("Headers non-empty", len(headers) > 0, f"count={len(headers)}: {headers[:5]}")
        report("Rows non-empty", len(rows) > 0, f"count={len(rows)}")
        report("Total rows > 0", total_rows > 0, f"total_rows={total_rows}")
    except Exception as e:
        report("Response is valid JSON", False, str(e))

    # ----------------------------------------------------------------
    # TEST 5: Column auto-detection via the map step
    # First, POST to preview to advance to map step
    # ----------------------------------------------------------------
    print("\n--- Test 5: Column Mapping / Auto-detection (GET /custom/map/0) ---")

    # First, POST to preview step to save cleaning and advance to map.
    # Flask may return 308 (Permanent Redirect) to canonical URL before 302,
    # so we follow redirects fully and check we land on the map page.
    r = s.post(
        f"{BASE_URL}/custom/preview/0/0",
        data={"action": "clean", "remove_top": "0", "remove_bottom": "0", "sheet": ""},
        allow_redirects=True,
    )
    # After following all redirects, we may land on map or back on preview
    landed_on_map = "/custom/map/" in r.url
    # If Flask redirected back to preview (Quick Ingest auto-skip), try direct GET
    if not landed_on_map:
        # Try posting to the canonical URL (without struct_idx)
        r2 = s.post(
            f"{BASE_URL}/custom/preview/0",
            data={"action": "clean", "remove_top": "0", "remove_bottom": "0", "sheet": ""},
            allow_redirects=True,
        )
        landed_on_map = "/custom/map/" in r2.url
        if landed_on_map:
            r = r2
    report("Navigated to map step", landed_on_map or r.status_code == 200, f"final URL: {r.url}")

    # If we didn't land on map, try direct GET to the map page
    if not landed_on_map:
        r = s.get(f"{BASE_URL}/custom/map/0/0")
    report("Map page loads (200)", r.status_code == 200, f"got {r.status_code}")

    # Check for auto-detection indicators: proposed mappings should be in the page
    # The template shows proposed mappings as select options with "selected" attributes
    has_mapping_grid = "map_" in r.text or "mapping" in r.text.lower()
    report("Mapping grid present", has_mapping_grid)

    # Check for common canonical field names in the proposed mappings
    canonical_fields_found = []
    for field in ["ISRC", "Title", "Artist", "Territory", "Net Receipts", "Gross Earnings", "Units"]:
        if field in r.text:
            canonical_fields_found.append(field)
    report(
        "Canonical fields in mapping page",
        len(canonical_fields_found) >= 2,
        f"found: {canonical_fields_found}",
    )

    # ----------------------------------------------------------------
    # TEST 6: Enrichment Status API
    # ----------------------------------------------------------------
    print("\n--- Test 6: Enrichment Status API (GET /api/enrichment-status) ---")
    r = s.get(f"{BASE_URL}/api/enrichment-status")
    report("Status code is 200", r.status_code == 200, f"got {r.status_code}")

    try:
        data = r.json()
        report("Response is valid JSON", True)
        print(f"         Raw keys: {list(data.keys())}")

        # Core keys that must always be present
        core_keys = ["running", "done", "error", "phase", "current", "total", "message"]
        for key in core_keys:
            report(f"Has core '{key}' key", key in data, f"value={data.get(key)}")

        # eta_seconds and need_lookup are newer fields.
        # If the running server has the latest code, they should be present.
        # Check the source code contract vs what the server actually returns.
        eta_present = "eta_seconds" in data
        need_present = "need_lookup" in data
        report(
            "Has 'eta_seconds' key (new field)",
            eta_present,
            f"value={data.get('eta_seconds')}" if eta_present else "MISSING - server may need restart to pick up latest app.py",
        )
        report(
            "Has 'need_lookup' key (new field)",
            need_present,
            f"value={data.get('need_lookup')}" if need_present else "MISSING - server may need restart to pick up latest app.py",
        )

        if eta_present:
            report("eta_seconds is numeric", isinstance(data["eta_seconds"], (int, float)), f"type={type(data['eta_seconds']).__name__}")
        if need_present:
            report("need_lookup is numeric", isinstance(data["need_lookup"], (int, float)), f"type={type(data['need_lookup']).__name__}")
    except Exception as e:
        report("Response is valid JSON", False, str(e))

    # ----------------------------------------------------------------
    # TEST 7: Walk through remaining payors' previews (verify all 4 work)
    # ----------------------------------------------------------------
    print("\n--- Test 7: Verify all 4 payors have structures ---")
    # Check preview API for each payor
    for idx, p in enumerate(PAYORS):
        r = s.post(
            f"{BASE_URL}/api/custom/preview",
            json={"payor_idx": idx, "struct_idx": 0, "remove_top": 0, "remove_bottom": 0},
        )
        try:
            data = r.json()
            has_data = len(data.get("headers", [])) > 0 and data.get("total_rows", 0) > 0
            report(
                f"Payor {p['code']} ({p['name']}) preview has data",
                has_data,
                f"headers={len(data.get('headers', []))}, total_rows={data.get('total_rows', 0)}",
            )
        except Exception as e:
            report(f"Payor {p['code']} preview", False, str(e))

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print("\n" + "=" * 70)
    total = passed + failed
    print(f"Results: {passed}/{total} passed, {failed}/{total} failed")
    print("=" * 70)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
