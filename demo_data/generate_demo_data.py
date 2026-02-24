"""
Generate realistic demo data for the Royalty Consolidator.
Creates 4 payors with different column structures, 10 shared tracks,
and realistic streaming royalty amounts.

Run:  python generate_demo_data.py
"""

import csv
import os
import random

# Seed for reproducibility
random.seed(42)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Shared catalog: 10 tracks
# ---------------------------------------------------------------------------
CATALOG = [
    {"isrc": "USRC12500001", "title": "Golden Hour",     "artist": "Maya Santos",           "album": "Sunlit",           "upc": "196589432101", "release": "2024-03-15", "iswc": "T-345.678.901-1", "writer": "Maya Santos",         "publisher": "Sunlit Publishing"},
    {"isrc": "USRC12500002", "title": "Midnight Drive",  "artist": "The Velvet Keys",       "album": "After Dark",       "upc": "196589432102", "release": "2024-06-01", "iswc": "T-345.678.902-2", "writer": "James Reed",          "publisher": "Velvet Songs"},
    {"isrc": "USRC12500003", "title": "Neon Lights",     "artist": "DJ Prism",              "album": "Spectrum",         "upc": "196589432103", "release": "2024-09-10", "iswc": "T-345.678.903-3", "writer": "Prism / Lee",         "publisher": "Prism Music"},
    {"isrc": "USRC12500004", "title": "Wildflower",      "artist": "Luna Park",             "album": "Botanical",        "upc": "196589432104", "release": "2024-01-20", "iswc": "T-345.678.904-4", "writer": "Luna Park",           "publisher": "Botanical Sounds"},
    {"isrc": "USRC12500005", "title": "City Rain",       "artist": "Marcus Cole",           "album": "Urban Stories",    "upc": "196589432105", "release": "2024-11-05", "iswc": "T-345.678.905-5", "writer": "Marcus Cole",         "publisher": "Urban Music Co"},
    {"isrc": "USRC12500006", "title": "Ocean Waves",     "artist": "The Drifters",          "album": "Tides",            "upc": "196589432106", "release": "2023-07-22", "iswc": "T-345.678.906-6", "writer": "Drift / Waves",       "publisher": "Tidal Publishing"},
    {"isrc": "USRC12500007", "title": "Starlight",       "artist": "Aria Moon",             "album": "Celestial",        "upc": "196589432107", "release": "2024-04-30", "iswc": "T-345.678.907-7", "writer": "Aria Moon",           "publisher": "Celestial Songs"},
    {"isrc": "USRC12500008", "title": "Thunder Road",    "artist": "Black Canyon",          "album": "Desert Storms",    "upc": "196589432108", "release": "2023-12-01", "iswc": "T-345.678.908-8", "writer": "Black Canyon",        "publisher": "Canyon Records"},
    {"isrc": "USRC12500009", "title": "Paper Planes",    "artist": "Indie Folk Co",         "album": "Origami",          "upc": "196589432109", "release": "2024-08-15", "iswc": "T-345.678.909-9", "writer": "Indie Folk Co",       "publisher": "Fold Music"},
    {"isrc": "USRC12500010", "title": "Electric Dreams", "artist": "Synthwave Collective",  "album": "Retro Future",     "upc": "196589432110", "release": "2025-01-01", "iswc": "T-345.678.910-0", "writer": "Synth Collective",    "publisher": "Retro Sounds"},
]

PLATFORMS = {
    "Spotify":       {"rate_min": 0.003, "rate_max": 0.005, "weight": 40},
    "Apple Music":   {"rate_min": 0.007, "rate_max": 0.010, "weight": 25},
    "Amazon Music":  {"rate_min": 0.004, "rate_max": 0.006, "weight": 12},
    "YouTube Music": {"rate_min": 0.001, "rate_max": 0.003, "weight": 10},
    "Deezer":        {"rate_min": 0.003, "rate_max": 0.005, "weight": 7},
    "Tidal":         {"rate_min": 0.008, "rate_max": 0.013, "weight": 6},
}

TERRITORIES = [
    ("US", 35), ("GB", 15), ("DE", 12), ("FR", 8), ("JP", 8),
    ("BR", 7), ("AU", 8), ("CA", 7),
]

SALE_TYPES = ["Stream", "Download", "Stream"]  # weighted towards streams


def _pick_platform():
    pop = list(PLATFORMS.keys())
    weights = [PLATFORMS[p]["weight"] for p in pop]
    return random.choices(pop, weights=weights, k=1)[0]


def _pick_territory():
    codes, weights = zip(*TERRITORIES)
    return random.choices(codes, weights=weights, k=1)[0]


def _stream_count(platform):
    """Generate a realistic stream count."""
    base = random.randint(500, 80000)
    if platform == "Spotify":
        base = int(base * 1.5)
    elif platform in ("Deezer", "Tidal"):
        base = int(base * 0.4)
    return base


def _gross(streams, platform):
    r = PLATFORMS[platform]
    rate = random.uniform(r["rate_min"], r["rate_max"])
    return round(streams * rate, 2)


# ---------------------------------------------------------------------------
# Believe Digital (B1) — 3 CSV files, Jan-Mar 2025
# Headers: ISRC, Track Name, Artist, Album, Platform, Country, Streams, Gross Revenue USD, Net Revenue USD, Reporting Period
# ---------------------------------------------------------------------------
def generate_believe():
    months = [("2025-01", "Believe_202501.csv"),
              ("2025-02", "Believe_202502.csv"),
              ("2025-03", "Believe_202503.csv")]

    folder = os.path.join(BASE_DIR, "statements_B1")
    os.makedirs(folder, exist_ok=True)

    for period, filename in months:
        rows = []
        # Pick 7-9 tracks per month (not always all 10)
        tracks = random.sample(CATALOG, random.randint(7, 10))
        for track in tracks:
            # 3-6 platform/territory combos per track
            combos = random.randint(3, 6)
            for _ in range(combos):
                plat = _pick_platform()
                terr = _pick_territory()
                streams = _stream_count(plat)
                gross = _gross(streams, plat)
                net = round(gross * random.uniform(0.70, 0.85), 2)
                rows.append({
                    "ISRC": track["isrc"],
                    "Track Name": track["title"],
                    "Artist": track["artist"],
                    "Album": track["album"],
                    "Platform": plat,
                    "Country": terr,
                    "Streams": streams,
                    "Gross Revenue USD": gross,
                    "Net Revenue USD": net,
                    "Reporting Period": period,
                })

        random.shuffle(rows)
        path = os.path.join(folder, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"  [B1] {filename}: {len(rows)} rows")


# ---------------------------------------------------------------------------
# FUGA — 2 CSV files, Jan-Feb 2025
# Headers: Track ID, Song Title, Performer, Release, Store, Territory, Units Sold, Total Revenue, Royalty Amount, Statement Month, UPC
# ---------------------------------------------------------------------------
def generate_fuga():
    months = [("January 2025", "FUGA_January_2025.csv"),
              ("February 2025", "FUGA_February_2025.csv")]

    folder = os.path.join(BASE_DIR, "statements_FUGA")
    os.makedirs(folder, exist_ok=True)

    for period, filename in months:
        rows = []
        tracks = random.sample(CATALOG, random.randint(7, 10))
        for track in tracks:
            combos = random.randint(3, 6)
            for _ in range(combos):
                plat = _pick_platform()
                terr = _pick_territory()
                units = _stream_count(plat)
                total_rev = _gross(units, plat)
                royalty = round(total_rev * random.uniform(0.65, 0.80), 2)
                rows.append({
                    "Track ID": track["isrc"],
                    "Song Title": track["title"],
                    "Performer": track["artist"],
                    "Release": track["album"],
                    "Store": plat,
                    "Territory": terr,
                    "Units Sold": units,
                    "Total Revenue": total_rev,
                    "Royalty Amount": royalty,
                    "Statement Month": period,
                    "UPC": track["upc"],
                })

        random.shuffle(rows)
        path = os.path.join(folder, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"  [FUGA] {filename}: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Songtrust (ST) — 2 CSV files, Jan-Feb 2025 (publishing)
# Headers: ISWC, Song Name, Writer, Publisher, Source, Region, Gross Earnings, Fees, Net Earnings, Period, ISRC
# ---------------------------------------------------------------------------
def generate_songtrust():
    months = [("Jan 2025", "Songtrust_Jan_2025.csv"),
              ("Feb 2025", "Songtrust_Feb_2025.csv")]

    folder = os.path.join(BASE_DIR, "statements_ST")
    os.makedirs(folder, exist_ok=True)

    sources = ["Mechanical", "Performance", "Sync", "Mechanical", "Performance"]

    for period, filename in months:
        rows = []
        tracks = random.sample(CATALOG, random.randint(7, 10))
        for track in tracks:
            combos = random.randint(2, 5)
            for _ in range(combos):
                source = random.choice(sources)
                region = _pick_territory()
                gross = round(random.uniform(5.0, 500.0), 2)
                fees = round(gross * random.uniform(0.10, 0.20), 2)
                net = round(gross - fees, 2)
                rows.append({
                    "ISWC": track["iswc"],
                    "Song Name": track["title"],
                    "Writer": track["writer"],
                    "Publisher": track["publisher"],
                    "Source": source,
                    "Region": region,
                    "Gross Earnings": gross,
                    "Fees": fees,
                    "Net Earnings": net,
                    "Period": period,
                    "ISRC": track["isrc"],
                })

        random.shuffle(rows)
        path = os.path.join(folder, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"  [ST] {filename}: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Empire (EMP) — 1 Excel file, Jan 2025
# Headers: ISRC Code, Track, Artist Name, DSP, Country Code, Quantity, Gross Amount, Net Amount, Sale Type, Date
# ---------------------------------------------------------------------------
def generate_empire():
    folder = os.path.join(BASE_DIR, "statements_EMP")
    os.makedirs(folder, exist_ok=True)

    rows = []
    tracks = random.sample(CATALOG, random.randint(8, 10))
    for track in tracks:
        combos = random.randint(4, 7)
        for _ in range(combos):
            plat = _pick_platform()
            terr = _pick_territory()
            qty = _stream_count(plat)
            gross = _gross(qty, plat)
            net = round(gross * random.uniform(0.72, 0.82), 2)
            sale_type = random.choice(SALE_TYPES)
            day = random.randint(1, 28)
            rows.append({
                "ISRC Code": track["isrc"],
                "Track": track["title"],
                "Artist Name": track["artist"],
                "DSP": plat,
                "Country Code": terr,
                "Quantity": qty,
                "Gross Amount": gross,
                "Net Amount": net,
                "Sale Type": sale_type,
                "Date": f"2025-01-{day:02d}",
                "Reporting Period": "2025-01",
            })

    random.shuffle(rows)

    try:
        import openpyxl
        path = os.path.join(folder, "Empire_Q1_2025.xlsx")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Royalties"
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row[h] for h in headers])
        wb.save(path)
        print(f"  [EMP] Empire_Q1_2025.xlsx: {len(rows)} rows")
    except ImportError:
        # Fallback to CSV if openpyxl not available
        path = os.path.join(folder, "Empire_Q1_2025.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"  [EMP] Empire_Q1_2025.csv (xlsx fallback): {len(rows)} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Generating demo data for Royalty Consolidator...\n")
    generate_believe()
    generate_fuga()
    generate_songtrust()
    generate_empire()

    # Summary
    total_files = 0
    for root, dirs, files in os.walk(BASE_DIR):
        for f in files:
            if f.endswith(('.csv', '.xlsx')) and f != 'generate_demo_data.py':
                total_files += 1

    print(f"\nDone! {total_files} files created in {BASE_DIR}")
    print("\nTo demo:")
    print("  1. Run the consolidator: python app.py")
    print("  2. Upload page -> point Work Directory to this demo_data folder")
    print("  3. All 4 payors will be detected with different column structures")
