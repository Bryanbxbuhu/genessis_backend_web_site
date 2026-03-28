# OSINT-Project

Traveler intelligence one-pager generator with **verified city context** to eliminate hallucinations.

## Overview
This tool generates focused travel intelligence briefs by analyzing **direct traveler-impact incidents** and providing **real, verified data** about local infrastructure.

**Multi-City Support:** Supports all enabled city configs under `config_data/cities/*.json` with no report code changes required.

### Incident Coverage (News-Based)
- **Crimes** (robberies, assaults, violent crimes)
- **Severe Weather** (hurricanes, floods, storms, extreme heat)
- **Terrorism & Security** (attacks, threats, security alerts)
- **Transit Disruptions** (airport closures, flight cancellations, metro strikes)
- **Health Emergencies** (outbreaks, disease alerts, quarantines)
- **Civil Unrest** (riots, protests, curfews)
- **Border/Entry Issues** (closures, travel bans, visa changes)

### Verified City Context (Prevents Hallucinations)
- **Local Hospitals** - Real hospital names and locations from OpenStreetMap
- **Public Transportation** - Actual transit modes and agencies from OpenStreetMap/Transitland
- **Geographic Data** - Precise coordinates and boundaries via Nominatim geocoding

### Data Sources
- **Local News Feeds** - Local 10 News (Miami), NBC New York (New York), SFist (San Francisco) for hyperlocal coverage
- **GDELT Project** - Global news database with geolocation-based event tracking
- **National Weather Service (NWS)** - Official US government weather alerts and warnings
- **Canada Travel Advisories** - Government of Canada travel advisories for the US
- **OpenStreetMap** (Overpass API) - Hospitals, transit infrastructure
- **Nominatim** - Geocoding (respects rate limits, caches results)
- **OpenAI GPT** - Analysis and synthesis (constrained to provided data only)

**Data Retention:** All feed items (news, alerts, advisories) are automatically purged after 30 days to keep the database current and relevant.

### OPSEC & Personal Safety Module (New!)
- **Contextual Security Guidance** - Dynamic safety tips based on recent incident patterns
- **Evidence-Based Recommendations** - Tips selected from actual incidents, not generic assumptions
- **Two-Layer Approach**:
  - **Baseline Tips**: Always-on best practices (hotel, transit, digital, emergency response)
  - **Contextual Tips**: Dynamic recommendations based on last 24h (e.g., active shooter â†’ venue awareness)
- **Professional Presentation**: Non-alarmist tone with evidence citations
- **Configurable**: Enable/disable, adjust tip counts, filter by confidence level

### Supported Cities (Enabled)
- barcelona
- berlin
- delhi
- dubai
- dubrovnik
- lisbon
- london
- los-angeles
- madrid
- mexico-city
- miami
- minneapolis
- moscow
- new-york
- paris
- prague
- rome
- san-francisco
- sevilla
- tokyo

## Setup

### Basic Setup (File-Based Storage - Default)

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure API keys and secrets via environment variables:
   - `OPENAI_API_KEY`
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `RAPIDAPI_KEY`

   Keep local secrets in `.env.local` only. Do not create or commit `.env` or other `.env.*` files.
   Start from `.env.example` and copy only the values you need into `.env.local`.

3. Set variables:
   ```bash
   # Linux/Mac
   export OPENAI_API_KEY="your-key"
   export OPENAI_MODEL="gpt-5-mini"
   
   # Windows PowerShell
   $env:OPENAI_API_KEY = "your-key"
   $env:OPENAI_MODEL = "gpt-5-mini"
   ```

   `OPENAI_MODEL` is optional; if not set, reports default to `gpt-5-mini`.

### Advanced Setup (Supabase Backend - Recommended for Production)

For scalable multi-instance deployments with centralized data storage:

1. Complete basic setup above

2. Create a Supabase project at https://supabase.com

3. Run the schema migration (see `SUPABASE_SETUP.md` for details):
   - Open Supabase SQL Editor
   - Copy and run `schema.sql` (Schema v2)

4. Set environment variables:
   ```bash
   # Linux/Mac
   export STORAGE_BACKEND="supabase"
   export SUPABASE_URL="https://your-project.supabase.co"
   export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
   
   # Windows PowerShell
   $env:STORAGE_BACKEND = "supabase"
   $env:SUPABASE_URL = "https://your-project.supabase.co"
   $env:SUPABASE_SERVICE_ROLE_KEY = "your-service-role-key"
   ```

5. Populate the database (schema v2 with --reset):
   ```bash
   # Option 1: Sync specific city (Miami)
   python scripts/sync_supabase.py --city miami --reset --force
   
   # Option 2: Sync all enabled cities
   python scripts/sync_supabase.py --all --reset --force
   ```
   This will:
   - Truncate all tables via the `reset_osint_data()` RPC
   - Seed cities and sources from `config_data/cities` and `config_data/sources`
   - Fetch and populate all data sources (news, weather, city context, Canada advisories)

**Full documentation:** See `SUPABASE_SETUP.md` for complete setup instructions, scheduling, and troubleshooting.

### Important Services Verification Model
`important_services` entries support these fields (backward compatible with existing configs):
- `category`
- `label`
- `url`
- `ios_url`
- `android_url`
- `verification_status`
- `verification_source`
- `last_verified_at`
- `fallback_generated`

Store-link precedence in normalization:
1. Explicit links from city config (`ios_url` / `android_url`)
2. Known app mappings in `APP_STORE_LINKS_BY_LABEL`
3. Generated App Store / Play Store search URLs (last resort)

When generated store-search links are used, `fallback_generated` is set to `true` and verification metadata is marked as generated fallback.

### CI Workflow Behavior
- Regional report jobs now run a fresh scoped sync in the same workflow path before report generation (`scripts/sync_supabase.py --city <city> --skip-context --force`).
- Feed preflight is intentionally not bypassed in CI batch runs.
- If preflight fails in CI, report generation fails loudly with a non-zero exit code.

---

## Usage

### Wikimedia Enrichment (Optional)
Wikimedia enrichment can fill missing websites from Wikidata's official website field.
It never overwrites existing websites and keeps seeded curated places authoritative.

Enable globally (all cities):
- Set `WIKIMEDIA_ENRICHMENT_ENABLED=true`
- Optional allowlist: `WIKIMEDIA_ENRICHMENT_CITY_ALLOWLIST=barcelona,minneapolis`
- Ensure `WIKIMEDIA_CLIENT_ID` and `WIKIMEDIA_CLIENT_SECRET` are set

Legacy per-city flags (used only when the global flag is off):
- `MINNEAPOLIS_WIKIMEDIA_ENRICHMENT_ENABLED=true`
- `BARCELONA_WIKIMEDIA_ENRICHMENT_ENABLED=true`

### Database Sync Modalities

The project uses `scripts/sync_supabase.py` to fetch and store data. Different flags optimize for different workflows:

#### ðŸ”„ **Initial Setup / Full Reset**
```bash
# Single city reset
python scripts/sync_supabase.py --city miami --reset --force

# All cities reset
python scripts/sync_supabase.py --all --reset --force
```
**Purpose:** First-time setup, schema migration, or complete database reset  
**Actions:** 
1. Calls `reset_osint_data()` RPC to truncate all tables
2. Seeds cities + sources from `config_data/cities` and `config_data/sources`
3. Reloads PostgREST schema cache automatically
4. Fetches Canada travel advisories (--all only)
5. Runs full data sync for each city (news, GDELT, NWS alerts, weather forecasts, city context)

**When:** Initial setup, schema v2 migration, or troubleshooting  
**Note:** Weather forecasts are now stored in dedicated `weather_forecasts` table (not feed_items)  

#### ðŸ”„ **Fast Daily Sync** (Recommended)
```bash
# Single city
python scripts/sync_supabase.py --city miami --skip-context --force

# All enabled cities
python scripts/sync_supabase.py --all --skip-context --force
```
**Syncs:** News feeds + GDELT + NWS alerts + weather forecasts + Canada advisories (--all only)  
**Skips:** Hospital/pharmacy/store infrastructure (rarely changes)  
**Speed:** ~10x faster than full sync  
**When:** Daily or multiple times per day  
**Note:** This is the recommended sync frequency for production deployments

#### ðŸ—‘ï¸ **Reset Runtime News Only**
```bash
# Clear news/weather data while preserving city context and config
python scripts/reset_runtime_news.py

# Reset and immediately re-sync fresh data
python scripts/reset_runtime_news.py -y && python scripts/sync_supabase.py --all --skip-context --force
```
**Clears:** News feeds + GDELT + NWS alerts + weather forecasts  
**Preserves:** Cities + sources + curated places + city reports + city context + transit snapshots  
**When:** When you want to refresh news data without re-syncing hospital/pharmacy infrastructure  
**Note:** This is much faster than a full reset and doesn't require re-syncing city context

#### ðŸ”„ **Reset and Sync Combined** (Recommended for Automation)
```bash
# One command to reset runtime data and sync fresh news (skip city context)
python scripts/reset_and_sync.py

# Scoped by region (keeps default full behavior when omitted)
python scripts/reset_and_sync.py --region americas
python scripts/reset_and_sync.py --region europe
python scripts/reset_and_sync.py --region asia

# With city context refresh (slower, use weekly)
python scripts/reset_and_sync.py --with-context
```
**Does:** Resets runtime data + syncs all cities in one operation (or only selected region with `--region`)  
**Use for:** GitHub Actions, scheduled tasks, daily automation  
**Speed:** ~2-5 minutes (without context), ~10-15 minutes (with context)  
**Note:** This is the command used in the GitHub Actions daily workflow

#### ðŸ¥ **Monthly Infrastructure Update**
```bash
python scripts/sync_supabase.py --city miami --context-only --force
```
**Syncs:** Hospital locations + transit data  
**Skips:** News feeds  
**When:** Once per month

#### ðŸŒ **Full Sync with Cleanup**
```bash
# Single city
python scripts/sync_supabase.py --city miami --force

# All enabled cities
python scripts/sync_supabase.py --all --force
```
**Syncs:** Everything (news + GDELT + NWS + weather forecasts + city context + Canada advisories for --all)  
**Includes:** Hospital/pharmacy/store data refresh  
**Bonus:** Auto-cleanup of old data (feeds 30d, weather 7d, orphaned items)  
**When:** Weekly or for complete refresh  
**Note:** Use this for initial population or when you suspect data inconsistencies

#### Available Flags
| Flag | Purpose |
|------|---------|
| `--city <city_key>` | Target specific city (e.g., `--city miami`) |
| `--all` | Sync all enabled cities (includes Canada advisories) |
| `--force` | Bypass time throttling (recommended for scheduled syncs) |
| `--reset` | Truncate & reseed database via RPC (MUST use with `--force`) |
| `--skip-context` | Skip hospital/pharmacy/store sync (10x faster, use daily) |
| `--context-only` | Only update hospital/pharmacy/store data (use monthly) |
| `--feeds-only` | Skip context, sync only news/weather/alerts (deprecated - use --skip-context) |

**Common Combinations:**
- `--all --skip-context --force` â†’ Daily sync for all cities (fastest)
- `--all --reset --force` â†’ Complete database reset and reseed
- `--city miami --context-only --force` â†’ Monthly infrastructure update for Miami

**Quick Commands for Daily Updates:**
```bash
# Method 1: Combined reset and sync (RECOMMENDED for automation)
python scripts/reset_and_sync.py

# Method 2: Reset then sync separately
python scripts/reset_runtime_news.py -y
python scripts/sync_supabase.py --all --skip-context --force
```
### Report Generation

#### Quick Start - List Available Cities
```bash
python run_report.py --list-cities

python run_report.py --all --skip-pdf
```
Displays all enabled cities with population and configuration details.

#### Ensure Latest News (Important!)
**Before generating reports**, ensure all cities have the latest news by running:

**Check Data Freshness First:**
```bash
python scripts/check_data_freshness.py
```
This shows when each city's data was last synced and whether you need to run a sync.

**Windows (Recommended):**
```cmd
sync_all_cities.bat
```

**Cross-platform:**
```bash
python scripts/reset_and_sync.py
python run_report.py --all --skip-pdf
```

This resets runtime data and syncs the latest news, GDELT data, weather alerts, and forecasts for all enabled cities. Without this step, reports may reflect outdated news.

**Automated Workflow (Sync + Generate):**
```cmd
REM Windows batch script - syncs all cities then generates all reports
sync_and_generate_all.bat
```

**Recommended Schedule:**
- **Daily**: Run `sync_all_cities.bat` or `python scripts/reset_and_sync.py`
- **Weekly**: Run full sync with `python scripts/reset_and_sync.py --with-context` (includes infrastructure refresh)
- **Monthly**: Infrastructure-only update with `python scripts/sync_supabase.py --city <key> --context-only --force`

#### Generate Intelligence Brief
```bash
# Miami report (default settings: 1-week lookback)
python run_report.py miami

# New York report
python run_report.py new-york

# San Francisco report
python run_report.py san-francisco

# Any configured city with driving pack
python run_report.py miami --with-driving --needs-idp
```
**Prerequisite:** Feed items must be populated first. If the report fails preflight, run:
```bash
python scripts/sync_supabase.py --city <city_key>
```

#### Advanced Options (via travel_agent.py directly)
```bash
# Custom parameters - hours, limit, output directory
python travel_agent.py miami --hours 24 --limit 50 --output-dir custom_reports/

# Extended news lookback and relaxed location filter
python travel_agent.py miami --news-lookback-hours 336 --news-ignore-location-filter

# With driving pack and rental details
python travel_agent.py new-york --will-drive --needs-idp --rental-provider "Hertz"
```

#### Available Parameters
- City selection: Any enabled key from `config_data/cities/*.json` (e.g., `miami`, `new-york`)
- `--with-driving` / `--will-drive` - Include Driving Pack section (car rental guidance)
- `--needs-idp` - Flag that International Driving Permit is required
- `--rental-provider` - Specify car rental company name
- `--hours` - Lookback window in hours (default: 168 / 1 week)
- `--news-lookback-hours` - Lookback window in hours for news items (default: 168)
- `--limit` - Number of news events to keep (default: 10)
- `--news-min-score` - Minimum travel relevance score to keep a news item (default: 0.5)
- `--news-min-keywords` - Minimum regular keyword matches to keep a news item (default: 2)
- `--news-min-strong` - Minimum strong keyword matches to keep a news item (default: 1)
- `--news-relax-keywords` - Relax keyword filter (allow 1 keyword or 0.3+ score)
- `--news-ignore-location-filter` - Ignore location matching and consider all events in window
- `--news-include-global-critical` - Keep strong-keyword events even if the city is not mentioned
- `--skip-news-preflight` - Skip feed_items preflight check (not recommended)
- `--output-dir` - Custom folder for PDF output (default: reports/)
- `--list-cities` - Show all available cities and exit

#### News Keyword Overrides
Add city- or language-specific terms without code changes:
- Set `EXTRA_TRAVEL_KEYWORDS` and `EXTRA_STRONG_TRAVEL_KEYWORDS` in `config.py`
- Or use environment variables (`EXTRA_TRAVEL_KEYWORDS`, `EXTRA_STRONG_TRAVEL_KEYWORDS`) as a JSON list or comma-separated string

Example:
```bash
# Windows PowerShell
$env:EXTRA_TRAVEL_KEYWORDS = "mass shooting,couvre-feu"
$env:EXTRA_STRONG_TRAVEL_KEYWORDS = "armed"
```



#### Local Verification Checklist
```bash
# 1) Preflight failure (no recent news)
python run_report.py <city_key_with_no_news>

# 2) Preflight success + logging (after syncing)
python scripts/sync_supabase.py --city miami
python run_report.py miami --limit 200 --with-driving

# 3) Verify output sections (JSON + PDF)
ls reports
```

## What Gets Included

The tool aggregates data from multiple sources with **30-day retention**:

### Signal Sources
1. **Local News** (Local 10 Miami, NBC New York, SFist) - Hyperlocal coverage for each city
2. **GDELT Project** - Tourism-relevant crime/safety news within 50km  
   - Pre-filtered queries for crimes affecting travelers (robbery, assault, pickpocket, etc.)
   - Post-processing removes political/sports/entertainment news
3. **NWS Weather Alerts** - Official US government weather warnings (US cities only)
4. **Open-Meteo Forecasts** - 7-day weather forecast (current conditions + hourly/daily outlook)
5. **Canada Travel Advisories** - International travel guidance

All sources use **strict filtering** to ensure only traveler-impacting signals:
- Must contain at least 1 strong travel-impact keyword (e.g., "shooting", "hurricane", "airport closed")
- OR must contain 3+ general travel keywords (e.g., "crime", "weather", "transit", "border")

This ensures you only get actionable intelligence about:
- Safety threats (crimes, terrorism, violence)
- Weather hazards (severe storms, floods, heat waves, NWS warnings)
- Transportation issues (flight cancellations, metro strikes, road closures)
- Health risks (disease outbreaks, contamination)
- Entry/border changes (travel bans, visa issues)

### Data Retention & Cleanup
- **Feed items** (news, alerts, advisories) are automatically purged after **30 days**
- **City context** (hospitals, infrastructure) refreshes every **30 days**
- **GDELT articles** are auto-cleaned when running full sync with `--force`
- Keeps database current and focused on recent, relevant events

## Anti-Hallucination Architecture

The tool uses a **strict data-driven approach** with **Supabase storage** to prevent AI hallucinations:

### How It Works
1. **Scheduled ingestion** â†’ `scripts/sync_supabase.py` fetches all sources (news, GDELT, NWS, city context)
2. **Geocode location** â†’ Get precise coordinates via Nominatim
3. **Fetch real data** â†’ Query OpenStreetMap for hospitals, GDELT for news, NWS for alerts
4. **Store in Supabase** â†’ Centralized PostgreSQL database with 30-day retention
5. **Pass to LLM** â†’ Provide structured JSON with strict rules: "Use ONLY this data, do not invent"

### What This Prevents
- âŒ Invented hospital names
- âŒ Non-existent transit agencies
- âŒ Fabricated infrastructure details
- âŒ Made-up addresses or locations
- âŒ Stale or outdated information (30-day purge)

### What You Get Instead
- âœ… Real hospital names from OpenStreetMap database
- âœ… Actual GDELT news events with geolocation
- âœ… Official NWS weather alerts with severity levels
- âœ… Verified geographic boundaries
- âœ… Only recent facts (within 30 days)
- âœ… All facts traceable to source APIs

The LLM's job is **formatting and synthesis only** - all factual content comes from verified APIs.

## Adding New Cities

No report code changes are required. City and source definitions are loaded from `config_data/`.

### Step 1: Add City Configuration

Create `config_data/cities/<city-key>.json` using an existing city file as a template.

Required fields:
- `name`
- `country_code`
- `timezone`
- `latitude`
- `longitude`
- `enabled`

Optional fields:
- `aliases`
- `population`
- `transit_systems`
- `important_services`
- `airports`

### Step 2: Add Source Configuration

Create or update `config_data/sources/<city-key>.json` with one or more source objects.
Each source must use a unique `source_key`.

### Step 3: Sync and Generate

```bash
python scripts/sync_supabase.py --city <city-key> --skip-context --force
python run_report.py <city-key>
```

For multiple cities:
```bash
python scripts/sync_supabase.py --all --skip-context --force
python run_report.py --all --skip-pdf
```

## Output
Reports are saved as PDFs in the `reports/` directory with the naming format:
`{city-name}-intel-brief.pdf`

**Database Storage:** When using Supabase backend, reports are automatically saved to the `city_reports` table with:
- Complete report data (JSON structure with all sections)
- Trend analysis data (30-day statistical trends)
- Metadata (model used, time window, generation timestamp)
- PDF file path reference

This allows you to:
- Track report history and changes over time
- Query reports programmatically via API
- Build dashboards or analytics on top of generated intelligence
- Compare reports across different cities or time periods

**Learn More:** See [REPORT_DATABASE_STORAGE.md](REPORT_DATABASE_STORAGE.md) for detailed documentation on database storage, querying, and use cases.

**Export city_reports safely (JSON object preserving):**
```bash
python scripts/export_city_reports.py --output reports/city_reports_export.json
```
The script round-trips the export and validates that nested `report_data` and `trend_data` remain JSON objects (not JSON-encoded strings).

**View Saved Reports:**
```bash
# List all saved reports
python scripts/view_saved_reports.py --list

# View specific city report
python scripts/view_saved_reports.py --city miami
```

**Generate Reports for All Cities:**
```bash
# Generate fresh reports for all enabled cities (auto-saves to database)
python scripts/generate_all_reports.py

# Custom lookback window (48 hours)
python scripts/generate_all_reports.py --hours 48
```

**Note:** The database uses an **upsert strategy** - each new report automatically replaces the previous entry for that city (based on `city_key` primary key). This keeps the database lean while maintaining the latest intelligence for each location.

## Data Sync and Retention

### Supabase Backend (Recommended)

All data sources are fetched and stored via the sync script:

```bash
# Daily sync: All cities, skip infrastructure (RECOMMENDED)
python scripts/sync_supabase.py --all --skip-context --force

# Weekly full sync: All cities with infrastructure refresh
python scripts/sync_supabase.py --all --force

# Monthly infrastructure update: Specific city
python scripts/sync_supabase.py --city miami --context-only --force

# Initial setup: Complete reset and reseed
python scripts/sync_supabase.py --all --reset --force
```

**What Gets Synced:**
- **News Feeds** â†’ Local 10 News (Miami), NBC New York (New York), SFist (San Francisco)
- **GDELT News** â†’ Global crime/safety news within 50km radius
- **NWS Alerts** â†’ Official weather warnings (US cities only)
- **Weather Forecasts** â†’ Open-Meteo 7-day forecasts (stored in `weather_forecasts` table)
- **Canada Advisories** â†’ Travel advisories for US (synced with `--all` flag)
- **City Context** â†’ Hospitals, pharmacies, supermarkets, stores (unless `--skip-context`)

**Automatic 30-Day Retention:**
- Feed items (news, GDELT, NWS alerts) older than 30 days are automatically purged
- Weather forecasts older than 7 days are automatically purged  
- City context data refreshes every 30 days
- Transit data is cached for 30 days (populated during report generation if missing)
- Keeps database lean and focused on current events

**Recommended Schedule:**
- **Daily**: Use `python scripts/reset_and_sync.py` (news + weather, ~2-5 min) - **Run this to ensure reports reflect current news!**
- **Weekly**: Use `python scripts/reset_and_sync.py --with-context` (full sync with infrastructure, ~10-15 min)
- **Monthly**: `--city <key> --context-only --force` (infrastructure only)

**Quick Sync Commands:**
```bash
# Windows: Use provided batch scripts
sync_all_cities.bat                    # Daily reset + sync (recommended)
sync_and_generate_all.bat              # Complete workflow: reset + sync + generate reports

# Cross-platform: Direct Python commands
python scripts/reset_and_sync.py                # Daily reset + sync
python scripts/generate_all_reports.py          # Generate all reports
```

**âš ï¸ Important**: Reports will only reflect news that has been synced to the database. If you notice stale news in reports, run the daily sync command first!

**Setup Automation:** Configure daily syncs via cron (Linux/Mac) or Task Scheduler (Windows) - see `SUPABASE_SETUP.md`
