"""End-to-end test of the Royalty Consolidator demo flow."""

import json
import re
import sys
import time
import os
import html as html_mod

import requests


BASE = "http://localhost:5000"
DEMO_DIR = 'C:\\Users\\jacques\\Downloads\\royalty_consolidator\\demo_data'


def extract_select_values(html_text, pattern="map_"):
    """Extract select name->selected value pairs."""
    results = {}
    pat = r'<select[^>]*name="(' + re.escape(pattern) + r'\d+)"[^>]*>(.*?)</select>'
    select_re = re.compile(pat, re.DOTALL)
    for m in select_re.finditer(html_text):
        name = m.group(1)
        options_html = m.group(2)
        sel_re = re.compile(r'<option\s+value="([^"]*)"[^>]*selected[^>]*>', re.DOTALL)
        sel_match = sel_re.search(options_html)
        if sel_match:
            results[name] = sel_match.group(1)
        else:
            results[name] = ""
    return results


def extract_hidden_field(html_text, name):
    """Extract a hidden input field value by name."""
    p1 = re.compile(
        r"<input\s+type=.hidden.\s+name=." + re.escape(name) + r".\s+value='([^']*)'",
        re.DOTALL,
    )
    m = p1.search(html_text)
    if m:
        return html_mod.unescape(m.group(1))
    p2 = re.compile(
        r'<input\s+type=.hidden.\s+name=.' + re.escape(name) + r'.\s+value="([^"]*)"',
        re.DOTALL,
    )
    m2 = p2.search(html_text)
    if m2:
        return html_mod.unescape(m2.group(1))
    return None


def step_ok(label, response, expected_status=200, expected_in=None):
    """Check a step succeeded."""
    ok = response.status_code == expected_status
    if expected_in and ok:
        ok = expected_in in response.text
    status = 'PASS' if ok else 'FAIL'
    extra = f' (status={response.status_code}, len={len(response.text)})'
    if not ok and expected_in:
        found = 'YES' if expected_in in response.text else 'NO'
        extra += f" [expected '{expected_in}' in body: {found}]"
    print(f'  [{status}] {label}{extra}')
    return ok


def main():
    s = requests.Session()
    results = {}

    print("=" * 70)
    print("ROYALTY CONSOLIDATOR - END-TO-END DEMO FLOW TEST")
    print("=" * 70)
    print()

    # STEP 1
    print("STEP 1: Initialize demo mode (GET /upload?demo=1)")
    try:
        r = s.get(f'{BASE}/upload?demo=1', allow_redirects=True, timeout=30)
    except requests.ConnectionError:
        print("  [FATAL] Cannot connect to server at", BASE)
        sys.exit(1)
    results["1_upload"] = step_ok("Upload page loaded", r, expected_in="Deal")
    print()

    # STEP 2
    print("STEP 2: POST upload form with 4 demo payors")
    payors = [
        {"code": "B1", "name": "Believe Digital", "stype": "masters",
         "dir": os.path.join(DEMO_DIR, "statements_B1"), "period_start": "202501", "period_end": "202503"},
        {"code": "FUGA", "name": "FUGA", "stype": "masters",
         "dir": os.path.join(DEMO_DIR, "statements_FUGA"), "period_start": "202501", "period_end": "202502"},
        {"code": "ST", "name": "Songtrust", "stype": "publishing",
         "dir": os.path.join(DEMO_DIR, "statements_ST"), "period_start": "202501", "period_end": "202502"},
        {"code": "EMP", "name": "Empire", "stype": "masters",
         "dir": os.path.join(DEMO_DIR, "statements_EMP"), "period_start": "202501", "period_end": "202501"},
    ]

    form_data = {"deal_name": "Demo Catalog", "file_dates_json": "{}"}
    for i, p in enumerate(payors):
        form_data[f"payor_code_{i}"] = p["code"]
        form_data[f"payor_name_{i}"] = p["name"]
        form_data[f"payor_fmt_{i}"] = "auto"
        form_data[f"payor_fee_{i}"] = ""
        form_data[f"payor_fx_{i}"] = "USD"
        form_data[f"payor_fxrate_{i}"] = ""
        form_data[f"payor_stype_{i}"] = p["stype"]
        form_data[f"payor_split_{i}"] = ""
        form_data[f"payor_territory_{i}"] = ""
        form_data[f"payor_dir_{i}"] = p["dir"]
        form_data[f"payor_period_start_{i}"] = p["period_start"]
        form_data[f"payor_period_end_{i}"] = p["period_end"]

    r = s.post(f'{BASE}/custom/upload', data=form_data, allow_redirects=True, timeout=60)
    results["2_upload_post"] = step_ok(f"Upload POST -> redirected (url={r.url})", r)
    print(f"  Landed on: {r.url}")
    print()

    # STEP 3: Preview & Map loop
    print("STEP 3: Preview & Map for all payors")
    current_url = r.url
    current_html = r.text
    max_iterations = 30
    iteration = 0

    while iteration < max_iterations:
        iteration += 1

        if "/custom/preview/" in current_url:
            m_url = re.search(r"/custom/preview/(\d+)(?:/(\d+))?", current_url)
            pidx = m_url.group(1) if m_url else "0"
            sidx = m_url.group(2) if (m_url and m_url.group(2)) else "0"
            print(f"  Preview step: payor_idx={pidx}, struct_idx={sidx}")

            preview_data = {"action": "clean", "remove_top": "0", "remove_bottom": "0", "sheet": ""}
            r = s.post(f'{BASE}/custom/preview/{pidx}/{sidx}', data=preview_data, allow_redirects=True, timeout=30)
            print(f"    -> Redirected to: {r.url} (status={r.status_code})")
            current_url = r.url
            current_html = r.text

        elif "/custom/map/" in current_url:
            m_url = re.search(r"/custom/map/(\d+)(?:/(\d+))?", current_url)
            pidx = m_url.group(1) if m_url else "0"
            sidx = m_url.group(2) if (m_url and m_url.group(2)) else "0"

            headers_json_val = extract_hidden_field(current_html, "headers_json")
            headers_list = []
            if headers_json_val:
                try:
                    headers_list = json.loads(headers_json_val)
                except json.JSONDecodeError:
                    pass

            mapping_selects = extract_select_values(current_html, "map_")

            keep_fields = {}
            keep_re = re.compile(r'name="(keep_\d+)"\s+value="1"', re.DOTALL)
            for km in keep_re.finditer(current_html):
                keep_fields[km.group(1)] = "1"

            print(f"  Map step: payor_idx={pidx}, struct_idx={sidx}")
            print(f"    Headers: {len(headers_list)} columns")
            mapped_count = sum(1 for v in mapping_selects.values() if v)
            print(f"    Auto-proposed mappings: {mapped_count}/{len(mapping_selects)}")
            for k, v in sorted(mapping_selects.items(), key=lambda x: int(x[0].replace('map_', ''))):
                if v:
                    idx_num = int(k.replace('map_', ''))
                    col_name = headers_list[idx_num] if idx_num < len(headers_list) else "?"
                    print(f"      {col_name} -> {v}")

            map_form_data = {"headers_json": json.dumps(headers_list)}
            map_form_data.update(mapping_selects)
            map_form_data.update(keep_fields)

            r = s.post(f'{BASE}/custom/map/{pidx}/{sidx}', data=map_form_data, allow_redirects=True, timeout=30)
            print(f"    -> Redirected to: {r.url} (status={r.status_code})")
            current_url = r.url
            current_html = r.text

        elif "/custom/process" in current_url:
            print("  Process step: parsing all files with mappings...")
            if "/custom/validate" not in r.url:
                r = s.get(f'{BASE}/custom/process', allow_redirects=True, timeout=120)
                current_url = r.url
                current_html = r.text
            print(f"    -> At: {r.url} (status={r.status_code})")
            break

        elif "/custom/validate" in current_url or "/custom/calc" in current_url:
            break
        else:
            print(f"  [WARN] Unexpected URL: {current_url}")
            break

    at_target = "/custom/validate" in current_url or "/custom/calc" in current_url or "/custom/process" in current_url
    results["3_preview_map"] = at_target
    tag = 'PASS' if at_target else 'FAIL'
    print(f"  [{tag}] Preview & Map completed -> {current_url}")
    print()

    # Step 3b: Process
    if "/custom/process" in current_url:
        print("STEP 3b: Process files")
        r = s.get(f'{BASE}/custom/process', allow_redirects=True, timeout=120)
        current_url = r.url
        current_html = r.text
        print(f"  -> Redirected to: {r.url}")
        print()

    # Step 3c: Validate
    if "/custom/validate" in current_url:
        print("STEP 3c: Validate step (POST continue)")
        issues_match = re.search(r"Issues Found.*?<.*?>(\d+)<", current_html, re.DOTALL)
        if issues_match:
            print(f"  Issues found: {issues_match.group(1)}")
        rows_match = re.search(r"Total Rows.*?<.*?>([\d,]+)<", current_html, re.DOTALL)
        if rows_match:
            print(f"  Total rows: {rows_match.group(1)}")

        r = s.post(f'{BASE}/custom/validate', data={"action": "continue"}, allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text
        results["3c_validate"] = step_ok("Validate -> Calc", r, expected_in="Waterfall")
        print()

    # Step 3d: Calc
    if "/custom/calc" in current_url:
        print("STEP 3d: Calc step (POST continue)")
        present_count = len(re.findall('>Present<', current_html))
        auto_count = len(re.findall('>Auto-calculated<', current_html))
        needs_count = len(re.findall('>Needs Formula<', current_html))
        print(f"  Present fields: {present_count}")
        print(f"  Auto-calculated fields: {auto_count}")
        print(f"  Needs formula fields: {needs_count}")

        r = s.post(f'{BASE}/custom/calc', data={}, allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text
        results["3d_calc"] = step_ok("Calc -> Enrich", r, expected_in="Enrichment")
        print()

    # Step 4: Enrichment
    print("STEP 4: Enrichment")
    if "/custom/enrich" not in current_url:
        r = s.get(f'{BASE}/custom/enrich', allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text

    results["4_enrich_page"] = step_ok("Enrich page loaded", r, expected_in="Enrichment")

    print("  Starting enrichment (MusicBrainz only)...")
    r = s.post(f'{BASE}/custom/enrich', data={"action": "enrich", "use_musicbrainz": "1"}, allow_redirects=True, timeout=30)
    print(f"  POST /custom/enrich -> status={r.status_code}, url={r.url}")
    print()

    # Step 5: Poll enrichment
    print("STEP 5: Poll enrichment status")
    max_poll = 300
    poll_interval = 3
    elapsed = 0
    enrich_done = False

    while elapsed < max_poll:
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            r_status = s.get(f'{BASE}/api/enrichment-status', timeout=10)
            sd = r_status.json()
        except Exception as e:
            print(f"  [WARN] Poll error at {elapsed}s: {e}")
            continue

        phase = sd.get("phase", "")
        cur = sd.get("current", 0)
        tot = sd.get("total", 0)
        msg = sd.get("message", "")
        eta = sd.get("eta_seconds", 0)
        done = sd.get("done", False)
        error = sd.get("error")
        need = sd.get("need_lookup", 0)

        print(f"  [{elapsed:3d}s] phase={phase}, {cur}/{tot}, need={need}, eta={eta}s, msg={msg[:80]}")

        if error:
            print(f"  [ERROR] Enrichment failed: {error}")
            break
        if done:
            enrich_done = True
            print("  Enrichment complete!")
            break

    results["5_enrichment"] = enrich_done
    tag = 'PASS' if enrich_done else 'FAIL'
    word = 'completed' if enrich_done else 'did not complete'
    print(f"  [{tag}] Enrichment {word}")
    print()

    # Step 5b: Review + continue
    if enrich_done:
        print("STEP 5b: Review enrichment results and continue")
        r = s.get(f"{BASE}/custom/enrich", allow_redirects=True, timeout=30)
        sm = re.search(r"Total Tracks.*?<.*?>(\d+)<", r.text, re.DOTALL)
        if sm: print(f"  Total tracks: {sm.group(1)}")
        fm = re.search(r"Dates Found.*?<.*?>(\d+)<", r.text, re.DOTALL)
        if fm: print(f"  Dates found: {fm.group(1)}")
        nm = re.search(r"Not Found.*?<.*?>(\d+)<", r.text, re.DOTALL)
        if nm: print(f"  Not found: {nm.group(1)}")

        r = s.post(f"{BASE}/custom/enrich", data={"action": "continue"}, allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text
        results["5b_enrich_continue"] = step_ok("Enrich -> Export", r, expected_in="Export")
        print()

    print("STEP 6: Export options")
    if "/custom/export" not in current_url:
        r = s.get(f"{BASE}/custom/export", allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text

    results["6_export_page"] = step_ok("Export page loaded", r, expected_in="Export")

    r = s.post(f"{BASE}/custom/export", data={"combined_csv": "1", "per_payor_csv": "1", "combined_excel": "1"}, allow_redirects=True, timeout=30)
    current_url = r.url
    current_html = r.text
    print(f"  POST /custom/export -> status={r.status_code}, url={r.url}")
    print()

    print("STEP 7: Finalize")
    if "/custom/finalize" in current_url:
        print(f"  Already on finalize page: {current_url}")
        results["7_finalize"] = step_ok("Finalize page rendered", r)
    else:
        r = s.get(f"{BASE}/custom/finalize", allow_redirects=True, timeout=30)
        current_url = r.url
        current_html = r.text
        results["7_finalize"] = step_ok("Finalize page rendered", r)

    has_proc = "Finalizing" in current_html or "pollStatus" in current_html or "api/status" in current_html
    print(f"  Has processing indicator: {has_proc}")
    print()

    print("STEP 8: Poll processing status")
    max_poll = 120
    poll_interval = 2
    elapsed = 0
    processing_done = False

    while elapsed < max_poll:
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            r_status = s.get(f"{BASE}/api/status", timeout=10)
            sd = r_status.json()
        except Exception as e:
            print(f"  [WARN] Poll error at {elapsed}s: {e}")
            continue

        running = sd.get("running", False)
        done = sd.get("done", False)
        progress = sd.get("progress", "")
        error = sd.get("error")

        print(f"  [{elapsed:3d}s] running={running}, done={done}, progress={progress}")

        if error:
            print(f"  [ERROR] Processing failed: {error}")
            break
        if done:
            processing_done = True
            print("  Processing complete!")
            break

    results["8_processing"] = processing_done
    tag = "PASS" if processing_done else "FAIL"
    word = "completed" if processing_done else "did not complete"
    print(f"  [{tag}] Processing {word}")
    print()

    print("STEP 9: Dashboard check")
    r = s.get(f"{BASE}/?loaded=1", allow_redirects=True, timeout=30)
    has_download = "/download/" in r.text
    has_payor_names = any(name in r.text for name in ["Believe", "FUGA", "Songtrust", "Empire"])
    results["9_dashboard"] = step_ok("Dashboard loaded", r)
    print(f"  Has download links: {has_download}")
    print(f"  Has payor names: {has_payor_names}")
    print()

    print("STEP 10: Download files")
    r_xlsx = s.get(f"{BASE}/download/consolidated", allow_redirects=True, timeout=30)
    xlsx_ok = r_xlsx.status_code == 200 and len(r_xlsx.content) > 100
    results["10a_download_xlsx"] = xlsx_ok
    tag = "PASS" if xlsx_ok else "FAIL"
    print(f"  [{tag}] /download/consolidated -> status={r_xlsx.status_code}, size={len(r_xlsx.content)} bytes")
    if xlsx_ok:
        cd = r_xlsx.headers.get("Content-Disposition", "")
        print(f"    Content-Disposition: {cd}")

    r_csv = s.get(f"{BASE}/download/csv", allow_redirects=True, timeout=30)
    csv_ok = r_csv.status_code == 200 and len(r_csv.content) > 100
    results["10b_download_csv"] = csv_ok
    tag = "PASS" if csv_ok else "FAIL"
    print(f"  [{tag}] /download/csv -> status={r_csv.status_code}, size={len(r_csv.content)} bytes")
    if csv_ok:
        cd = r_csv.headers.get("Content-Disposition", "")
        print(f"    Content-Disposition: {cd}")
        csv_text = r_csv.content.decode("utf-8", errors="replace")
        csv_lines = csv_text.split(chr(10))
        print(f"    CSV columns: {csv_lines[0][:200]}...")
        print(f"    CSV rows (excl header): {max(0, len(csv_lines) - 2)}")
    print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed
    for key, val in results.items():
        status = "PASS" if val else "FAIL"
        print(f"  [{status}] {key}")
    print()
    print(f"  Total: {total}, Passed: {passed}, Failed: {failed}")
    if failed == 0:
        print("  ALL TESTS PASSED!")
    else:
        print(f"  {failed} TEST(S) FAILED")
    print("=" * 70)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())