-- OSINT Project Supabase Schema v3 (Modular & Report-Ready)
--
-- Goals:
-- 1) Multi-city ready: store each feed item once; link to many cities
-- 2) Weather is NOT a feed item: store weather snapshots separately
-- 3) Modular refresh: reports refresh often; context refresh less often
-- 4) Space-efficient: store latest AI report per city (upsert/replace)
-- 5) Easy reset via RPC helpers
-- 6) Reload PostgREST schema cache via RPC (no UI button needed)

BEGIN;

-- ---------------------------------------------------------------------------
-- PURGE: drop old objects (DELETES existing data)
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS public.reset_osint_runtime_data();
DROP FUNCTION IF EXISTS public.reset_osint_all();
DROP FUNCTION IF EXISTS public.reload_api_schema_cache();
DROP FUNCTION IF EXISTS public.set_updated_at();

DROP TABLE IF EXISTS public.feed_item_cities CASCADE;
DROP TABLE IF EXISTS public.weather_forecasts CASCADE;
DROP TABLE IF EXISTS public.feed_items CASCADE;
DROP TABLE IF EXISTS public.sources CASCADE;
DROP TABLE IF EXISTS public.city_context_snapshots CASCADE;
DROP TABLE IF EXISTS public.transit_snapshots CASCADE;
DROP TABLE IF EXISTS public.curated_places CASCADE;
DROP TABLE IF EXISTS public.city_reports CASCADE;
DROP TABLE IF EXISTS public.cities CASCADE;

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- Shared helper: updated_at trigger function
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- Cities
-- ---------------------------------------------------------------------------

CREATE TABLE public.cities (
  city_key TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  country_code TEXT,

  aliases JSONB NOT NULL DEFAULT '[]'::JSONB,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,

  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  timezone TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.cities IS 'Supported cities for OSINT reports';
COMMENT ON COLUMN public.cities.city_key IS 'Unique identifier matching config (e.g., "miami")';
COMMENT ON COLUMN public.cities.aliases IS 'Array of alternative names/spellings';

CREATE TRIGGER trg_cities_updated_at
BEFORE UPDATE ON public.cities
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- Sources (city-specific or global)
-- ---------------------------------------------------------------------------

CREATE TABLE public.sources (
  source_key TEXT PRIMARY KEY,
  name TEXT,
  type TEXT NOT NULL,        -- rss/json/api
  url TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,

  -- NULL = global source
  city_key TEXT REFERENCES public.cities(city_key) ON DELETE SET NULL,

  tags JSONB NOT NULL DEFAULT '[]'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT sources_type_check CHECK (type IN ('rss','json','api'))
);

COMMENT ON TABLE public.sources IS 'Configured data sources (RSS feeds, APIs, etc.)';
COMMENT ON COLUMN public.sources.city_key IS 'City-specific source or NULL for global sources';

CREATE INDEX sources_city_idx ON public.sources (city_key);

CREATE TRIGGER trg_sources_updated_at
BEFORE UPDATE ON public.sources
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- Feed Items (store ONCE per source; city association via join table)
-- ---------------------------------------------------------------------------

CREATE TABLE public.feed_items (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_key TEXT NOT NULL REFERENCES public.sources(source_key) ON DELETE CASCADE,

  guid TEXT,                 -- RSS GUID or source-provided unique id
  url TEXT,
  title TEXT,
  summary TEXT,
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB NOT NULL,
  travel_relevance_score DOUBLE PRECISION,
  travel_keywords_matched JSONB,
  travel_relevance_reason TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.feed_items IS 'Ingested items (stored once, city-agnostic)';

-- Dedupe with partial unique indexes (allow multiple NULLs)
CREATE UNIQUE INDEX feed_items_source_guid_uniq
  ON public.feed_items (source_key, guid)
  WHERE guid IS NOT NULL AND guid <> '';

CREATE UNIQUE INDEX feed_items_source_url_uniq
  ON public.feed_items (source_key, url)
  WHERE url IS NOT NULL AND url <> '';

CREATE INDEX feed_items_source_published_idx
  ON public.feed_items (source_key, published_at DESC NULLS LAST);

CREATE INDEX feed_items_fetched_idx
  ON public.feed_items (fetched_at DESC);

CREATE INDEX feed_items_published_idx
  ON public.feed_items (published_at DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- Feed Item <-> City mapping (many-to-many)
-- ---------------------------------------------------------------------------

CREATE TABLE public.feed_item_cities (
  feed_item_id UUID NOT NULL REFERENCES public.feed_items(id) ON DELETE CASCADE,
  city_key TEXT NOT NULL REFERENCES public.cities(city_key) ON DELETE CASCADE,

  match_meta JSONB NOT NULL DEFAULT '{}'::JSONB,   -- {"method":"geo","score":0.92,...}
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (feed_item_id, city_key)
);

CREATE INDEX feed_item_cities_city_idx ON public.feed_item_cities (city_key);
CREATE INDEX feed_item_cities_item_idx ON public.feed_item_cities (feed_item_id);

-- ---------------------------------------------------------------------------
-- Weather Forecast Snapshots (time-series)
-- ---------------------------------------------------------------------------

CREATE TABLE public.weather_forecasts (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  city_key TEXT NOT NULL REFERENCES public.cities(city_key) ON DELETE CASCADE,

  provider TEXT NOT NULL DEFAULT 'open_meteo',
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB NOT NULL,

  -- Optional extracted fields for fast filtering
  current_temp_c NUMERIC,
  current_wind_kph NUMERIC,
  current_precip_mm NUMERIC,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX weather_forecasts_city_provider_fetched_idx
  ON public.weather_forecasts (city_key, provider, fetched_at DESC);

-- ---------------------------------------------------------------------------
-- City Context Snapshots (latest only)
-- ---------------------------------------------------------------------------

CREATE TABLE public.city_context_snapshots (
  city_key TEXT PRIMARY KEY REFERENCES public.cities(city_key) ON DELETE CASCADE,
  context JSONB NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_city_context_snapshots_updated_at
BEFORE UPDATE ON public.city_context_snapshots
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- Transit Snapshots (latest only)
-- ---------------------------------------------------------------------------

CREATE TABLE public.transit_snapshots (
  city_key TEXT PRIMARY KEY REFERENCES public.cities(city_key) ON DELETE CASCADE,
  transit JSONB NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_transit_snapshots_updated_at
BEFORE UPDATE ON public.transit_snapshots
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- Curated Places
-- ---------------------------------------------------------------------------

CREATE TABLE public.curated_places (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  city_key TEXT NOT NULL REFERENCES public.cities(city_key) ON DELETE CASCADE,
  category TEXT NOT NULL,    -- hospital/urgent_care/etc
  name TEXT NOT NULL,
  website TEXT,
  website_canonical TEXT,
  website_source TEXT,
  website_status TEXT,
  website_verified_at TIMESTAMPTZ,
  website_verification_reason TEXT,
  notes TEXT,
  must_include BOOLEAN NOT NULL DEFAULT TRUE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX curated_places_city_cat_idx
  ON public.curated_places (city_key, category);

CREATE TRIGGER trg_curated_places_updated_at
BEFORE UPDATE ON public.curated_places
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- City Reports (latest AI-generated report per city; REPLACE IN PLACE)
-- ---------------------------------------------------------------------------

CREATE TABLE public.city_reports (
  city_key TEXT PRIMARY KEY REFERENCES public.cities(city_key) ON DELETE CASCADE,

  -- Keep this space-efficient:
  -- report_data should store narrative sections + arrays of feed_item_id references,
  -- NOT large duplicated copies of article bodies.
  report_data JSONB NOT NULL,

  -- Optional: store computed 30-day trend buckets (or just store ids and compute on read)
  trend_data JSONB,

  -- Metadata
  model TEXT NOT NULL,
  window_hours INTEGER NOT NULL,        -- e.g. 48
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  pdf_file_url TEXT,
  updated_by TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.city_reports IS 'Most recent AI-generated report per city (replaces previous report on update)';
COMMENT ON COLUMN public.city_reports.report_data IS
$$Structured JSON containing narrative sections (priority actions, what's happening now, weather conditions, etc.)$$;
COMMENT ON COLUMN public.city_reports.trend_data IS
'JSON summarising 30-day trend calculations (category counts per day)';

CREATE INDEX city_reports_generated_at_idx ON public.city_reports (generated_at DESC);

CREATE TRIGGER trg_city_reports_updated_at
BEFORE UPDATE ON public.city_reports
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ---------------------------------------------------------------------------
-- Reset helpers
-- ---------------------------------------------------------------------------

-- Runtime reset: wipes changing data but keeps config (cities, sources, curated_places) and reports
CREATE OR REPLACE FUNCTION public.reset_osint_runtime_data()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  TRUNCATE TABLE
    public.feed_item_cities,
    public.feed_items,
    public.weather_forecasts,
    public.city_context_snapshots,
    public.transit_snapshots
  CASCADE;
END;
$$;

COMMENT ON FUNCTION public.reset_osint_runtime_data() IS
'Truncate runtime OSINT data (keeps cities/sources/curated_places/city_reports)';

-- Full reset: wipes most data but keeps reports (and their city references)
CREATE OR REPLACE FUNCTION public.reset_osint_all()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  TRUNCATE TABLE
    public.feed_item_cities,
    public.feed_items,
    public.weather_forecasts,
    public.city_context_snapshots,
    public.transit_snapshots,
    public.curated_places,
    public.sources
  CASCADE;
END;
$$;

COMMENT ON FUNCTION public.reset_osint_all() IS
'Truncate OSINT tables while preserving city_reports and cities';

-- ---------------------------------------------------------------------------
-- Schema cache reload helper (PostgREST / Data API)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.reload_api_schema_cache()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  PERFORM pg_notify('pgrst', 'reload schema');
END;
$$;

COMMENT ON FUNCTION public.reload_api_schema_cache() IS
'Reload PostgREST schema cache via pg_notify';

COMMIT;

-- Optional but useful after running migrations:
SELECT public.reload_api_schema_cache();
