-- OSINT Project Supabase Schema v2 (FULL REBUILD)
-- Goals:
-- 1) Multi-city ready: store each feed item once; link to many cities
-- 2) Weather is NOT a feed item: store weather snapshots separately
-- 3) Easy reset: truncate via RPC
-- 4) Reload Data API schema cache via RPC (no UI button needed)

BEGIN;

-- ---------------------------------------------------------------------------
-- PURGE: drop old objects (deletes existing data)
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS public.reset_osint_data();
DROP FUNCTION IF EXISTS public.reload_api_schema_cache();

DROP TABLE IF EXISTS public.feed_item_cities CASCADE;
DROP TABLE IF EXISTS public.weather_forecasts CASCADE;
DROP TABLE IF EXISTS public.feed_items CASCADE;
DROP TABLE IF EXISTS public.sources CASCADE;
DROP TABLE IF EXISTS public.city_context_snapshots CASCADE;
DROP TABLE IF EXISTS public.transit_snapshots CASCADE;
DROP TABLE IF EXISTS public.curated_places CASCADE;
DROP TABLE IF EXISTS public.cities CASCADE;

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- Cities
-- ---------------------------------------------------------------------------

CREATE TABLE public.cities (
  city_key TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  country_code TEXT,

  aliases JSONB NOT NULL DEFAULT '[]'::JSONB,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,

  -- For scaling (weather + geo queries)
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  timezone TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.cities IS 'Supported cities for OSINT reports';
COMMENT ON COLUMN public.cities.city_key IS 'Unique identifier matching config (e.g., "miami")';
COMMENT ON COLUMN public.cities.aliases IS 'Array of alternative names/spellings';

-- ---------------------------------------------------------------------------
-- Sources (city-specific or global)
-- ---------------------------------------------------------------------------

CREATE TABLE public.sources (
  source_key TEXT PRIMARY KEY,
  name TEXT,
  type TEXT NOT NULL,        -- rss/json/api
  url TEXT NOT NULL,         -- base URL for APIs, full URL for RSS/JSON feeds
  enabled BOOLEAN NOT NULL DEFAULT TRUE,

  -- NULL = global source
  city_key TEXT REFERENCES public.cities(city_key) ON DELETE SET NULL,

  tags JSONB NOT NULL DEFAULT '[]'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT sources_type_check CHECK (type IN ('rss','json','api'))
);

COMMENT ON TABLE public.sources IS 'Configured data sources (RSS feeds, APIs, etc.)';
COMMENT ON COLUMN public.sources.city_key IS 'City-specific source or NULL for global sources';

CREATE INDEX sources_city_idx ON public.sources (city_key);

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
  travel_relevance_reason TEXT
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

-- Optional: helpful for "latest city feed" queries once joined
CREATE INDEX feed_items_published_idx
  ON public.feed_items (published_at DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- Feed Item <-> City mapping (many-to-many)
-- ---------------------------------------------------------------------------

CREATE TABLE public.feed_item_cities (
  feed_item_id UUID NOT NULL REFERENCES public.feed_items(id) ON DELETE CASCADE,
  city_key TEXT NOT NULL REFERENCES public.cities(city_key) ON DELETE CASCADE,

  match_meta JSONB NOT NULL DEFAULT '{}'::JSONB,   -- {"query":"...", "score":0.82, ...}
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
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Transit Snapshots (latest only)
-- ---------------------------------------------------------------------------

CREATE TABLE public.transit_snapshots (
  city_key TEXT PRIMARY KEY REFERENCES public.cities(city_key) ON DELETE CASCADE,
  transit JSONB NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX curated_places_city_cat_idx
  ON public.curated_places (city_key, category);

-- ---------------------------------------------------------------------------
-- Reset helper (truncate data; keep tables)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.reset_osint_data()
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
    public.sources,
    public.cities
  CASCADE;
END;
$$;

COMMENT ON FUNCTION public.reset_osint_data() IS 'Truncate all OSINT data tables (keeps schema)';

-- ---------------------------------------------------------------------------
-- Schema cache reload helper (PostgREST / Data API)
-- Useful when UI has no "Reload schema" button
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

COMMENT ON FUNCTION public.reload_api_schema_cache() IS 'Reload PostgREST schema cache via pg_notify';

COMMIT;
