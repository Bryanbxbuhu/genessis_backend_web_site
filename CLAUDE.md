# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

A travel intelligence backend that ingests multi-source data (news, weather, travel advisories, local infrastructure), stores it in Supabase, and generates AI-enhanced city safety/intelligence reports. The system supports 22 cities across Americas, Europe, and Asia with daily regional scheduling.

## Commands

### Setup
```bash
pip install -r requirements.txt
playwright install chromium          # Required for Playwright-based scrapers
python -m pre_commit install         # Install secret-scanning pre-commit hook
python -m pre_commit install --hook-type pre-push
```

Copy `.env.example` to `.env.local` and populate secrets. The app loads from `.env.local` (not `.env`).

### Data Sync
```bash
python scripts/reset_and_sync.py                          # Recommended: daily reset + sync
python scripts/sync_supabase.py --all --reset --force     # Full hard reset and sync
python scripts/sync_supabase.py --city miami              # Single city sync
python scripts/reset_runtime_news.py -y                   # Clear runtime news only (preserves city context cache)
python scripts/sync_travel_advisories.py                  # Sync travel warnings only
```

### Report Generation
```bash
python run_report.py miami                                 # Single city report
python run_report.py --all --skip-pdf                     # All cities, DB-first, skip PDF
python run_report.py --list-cities                        # List available city keys
python run_report.py miami --with-driving --needs-idp     # With driving/IDP context
```

### Validation & Security
```bash
python scripts/validate_important_services_links.py --strict --no-network
python scripts/check_staged_secrets.py                    # Manual pre-commit secret scan
python scripts/scan_repo_secret_history.py               # Scan git history for secrets
```

### Testing
```bash
python -m unittest discover -s tests                      # Runs if tests/ directory exists
```
There is no formal test suite — the CI regression step passes vacuously if `tests/` is absent. Use validation scripts for integration checks.

## Architecture

### Data Flow

```
Sources (RSS/JSON/API/Playwright) → sync_supabase.py → Supabase
                                                           ↓
run_report.py → travel_agent.py → Load city context + feed items from Supabase
                                → OpenAI gpt-4o-mini: reason over structured data
                                → report_renderer.py: HTML/PDF output
                                → Upsert JSON into city_reports table
```

### Core Modules

| File | Role |
|---|---|
| `travel_agent.py` | Main orchestration engine (~155 KB). Handles data loading, LLM prompting, insight generation. |
| `run_report.py` | CLI entry point. Parses city args and delegates to `travel_agent.py`. |
| `report_schema.py` | Pydantic models that define and validate the report payload structure. |
| `report_data_contract.py` | Canonical definition of the `report_data` dict passed to the LLM and renderer. |
| `report_renderer.py` | Jinja2-based HTML rendering and PDF export via PyMuPDF. |
| `city_context.py` | Fetches real location data (hospitals, transit, geocoding) via Nominatim + OpenStreetMap Overpass. Prevents LLM hallucinations about local infrastructure. |
| `config.py` | Central env/API config loader. Reads `.env.local`. Defines partner affiliate URLs. |
| `config_registry.py` | Pydantic models (CityModel, SourceModel) that load and validate JSON registries. |
| `news_relevance.py` | Travel keyword scoring + optional semantic similarity filtering for feed items. |
| `storage/base.py` | Abstract DataStore interface. |
| `storage/supabase_store.py` | Supabase implementation of DataStore. |

### Configuration Registry

Cities and sources are **code-free JSON config** — adding a city requires no Python changes:

- `config_data/cities/*.json` — One file per city: coordinates, timezone, airports, transit systems, emergency numbers, important services, `enabled` flag.
- `config_data/sources/*.json` — Data source definitions per city. Source types: `rss`, `json`, `api`, `playwright`.
- `config_data/sources/global.json` — Sources that apply to all cities (GDELT, weather APIs, travel advisories).
- `config_data/curated_places_seed_websites.json` — Authoritative seed data for pharmacies, supermarkets, hospitals, rental cars (upserted on sync).

### Database Schema (Supabase / PostgreSQL)

Key tables:
- `cities` — City metadata registry
- `sources` — Data source registry (linked to cities)
- `feed_items` — Deduplicated news/advisory items; dedup by `(source_key, guid)` or `(source_key, url)` partial unique index
- `feed_item_cities` — Many-to-many join: feed items ↔ cities
- `weather_forecasts` — Weather snapshots per city
- `city_context_snapshots` — Cached location data (refreshed monthly)
- `curated_places` — Pharmacies, supermarkets, hospitals, rental cars
- `city_reports` — Final AI-generated report JSON (upserted per city, preserved across resets)

Schema migrations are in `db/` (v2 and v3 SQL files).

### Travel Relevance Filtering

Feed items are scored for travel relevance at ingest time. Two modes controlled by `TRAVEL_RELEVANCE_MODE` env var:
- `keyword` (default) — Fast scoring via keyword lists in `news_relevance.py`
- `semantic_multilingual` — Uses `sentence-transformers` for multilingual semantic similarity (requires optional dep)

### CI/CD Scheduling

GitHub Actions runs report generation on a regional schedule:
- **Americas** (US/Mex): 12:00 UTC daily
- **Europe**: 06:00 UTC daily
- **Asia**: 23:00 UTC daily

Workflows: `check-forbidden-files.yml`, `validate-important-services.yml`, `daily-update.yml`, `scheduled-sync.yml`.

## Key Conventions

### Secrets & Security
- Secrets go in `.env.local` only — never `.env`, never committed.
- Pre-commit hook blocks staged secrets (gitleaks). Pre-push hook scans git history.
- `.gitleaks.toml` contains allowlist for `source_key` identifiers in config JSON that resemble but are not secrets.
- CI (`check-forbidden-files.yml`) blocks `.env`, `.venv`, and hardcoded API keys in `config.py` from reaching `main`.

### Adding a New City
1. Create `config_data/cities/<city_key>.json` with required fields (name, lat/lon, timezone, etc.)
2. Create `config_data/sources/<city_key>.json` with at least one source entry
3. Set `"enabled": true` in the city JSON
4. Run `python scripts/sync_supabase.py --city <city_key> --reset` to initialize

### LLM Integration
- Model: `gpt-4o-mini` (referenced in code as `gpt-5-mini` in some comments — use actual OpenAI model IDs).
- LLM is called in `travel_agent.py` with structured `report_data` context to generate risk badges, travel cues, opsec notes, and top recommended actions.
- `city_context.py` grounds the LLM with real OSM data to prevent hallucinations about hospitals, transit, etc.

### Report Rendering
- Templates live in `templates/` (Jinja2 HTML) with partials in `templates/partials/`.
- Design constants (colors, fonts, spacing) are centralized in `design_tokens.py`.
- PDF export uses PyMuPDF/pypdf.

### Affiliate Links
- SpotHero parking affiliate links are hardcoded in `config.py` (not DB-driven) to preserve referral URL integrity.
- eSIM card affiliate links follow the same pattern.

### Windows Compatibility
- Console encoding patches for non-ASCII city names (e.g., "Dubrovnik", "México City") exist in entry point scripts.
