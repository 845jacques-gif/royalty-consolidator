# Royalty Consolidator

A web app that ingests royalty statements from multiple music distributors, normalizes them into a unified schema, and generates analytics dashboards and consolidated exports.

## What it does

- Parses PDF, CSV, and Excel royalty statements from any distributor (DistroKid, Believe, RecordJet, etc.)
- Auto-detects columns, periods, and currencies from unstructured data
- Consolidates across payors into a 24-column normalized schema
- Interactive column mapping with AI-assisted detection (optional Gemini)
- Release date enrichment via MusicBrainz, Genius, and Gemini
- Formula engine for waterfall calculations (gross → fees → net → splits)
- Analytics dashboard with trends, top songs, YoY growth, payor comparisons
- Deal management — save, load, and compare named deal projects
- Export to Excel and CSV with per-payor breakdowns

## Quick start

### Local (Python)

```bash
pip install -r requirements.txt
python app.py
# Open http://localhost:5000
```

### Docker

```bash
cp .env.example .env
# Edit .env with your API keys
docker compose up -d
# Open http://localhost (HTTPS) or http://localhost:5000 (direct)
```

### Deploy to a server

```bash
# On your server with Docker installed:
mkdir royalty-consolidator && cd royalty-consolidator

# Create docker-compose.yml, Caddyfile, and .env (see below)
# Or clone this repo:
git clone https://github.com/845jacques-gif/royalty-consolidator.git
cd royalty-consolidator
cp .env.example .env
nano .env  # set DOMAIN=yourdomain.com + API keys

docker compose up -d
# Caddy auto-provisions HTTPS via Let's Encrypt
```

The pre-built image is on Docker Hub: `845jacques/royalty-consolidator:latest`

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `FLASK_SECRET_KEY` | Yes | Random string for session security |
| `DOMAIN` | For production | Your domain (default: `localhost`) |
| `GEMINI_API_KEY` | No | Enables AI-assisted column mapping |
| `GENIUS_TOKEN` | No | Enables Genius release date lookups |

## Tech stack

- **Backend:** Python 3.11, Flask, Pandas, gunicorn
- **Parsing:** pdfplumber (PDF), openpyxl (Excel), pandas (CSV)
- **Enrichment:** MusicBrainz (free), Genius API, Gemini API
- **Frontend:** Jinja2 templates, Chart.js
- **Deployment:** Docker, Caddy (auto-HTTPS)

## Project structure

```
app.py              Flask app + inline HTML template (~4800 lines)
consolidator.py     Core consolidation engine
mapper.py           Column mapping & schema definition
enrichment.py       Release date lookup pipeline
formula_engine.py   Waterfall calculations
validator.py        Data quality checks
Dockerfile          Container image definition
docker-compose.yml  App + Caddy reverse proxy
Caddyfile           HTTPS reverse proxy config
.env.example        Environment variable template
```

## Custom flow

Upload → Preview → Map columns → Validate → Calculate formulas → Enrich release dates → Export → Dashboard

Each step is interactive with the ability to review and correct auto-detected mappings before proceeding.
