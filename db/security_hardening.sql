-- Security hardening for an existing schema_v3 deployment.
-- Apply once in the Supabase SQL editor (or through the project's migration
-- runner) using an owner/admin role. Backend jobs use service_role and bypass
-- RLS; browser clients receive read-only access to public report data.

BEGIN;

ALTER TABLE public.cities ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.feed_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.feed_item_cities ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.weather_forecasts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.city_context_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.transit_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.curated_places ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.city_reports ENABLE ROW LEVEL SECURITY;

REVOKE ALL PRIVILEGES ON TABLE
  public.cities,
  public.sources,
  public.feed_items,
  public.feed_item_cities,
  public.weather_forecasts,
  public.city_context_snapshots,
  public.transit_snapshots,
  public.curated_places,
  public.city_reports
FROM PUBLIC, anon, authenticated;

-- The public website only needs enabled city metadata and its latest reports.
GRANT SELECT ON TABLE public.cities, public.city_reports TO anon, authenticated;

DROP POLICY IF EXISTS public_read_enabled_cities ON public.cities;
CREATE POLICY public_read_enabled_cities
ON public.cities
FOR SELECT
TO anon, authenticated
USING (enabled = TRUE);

DROP POLICY IF EXISTS public_read_enabled_city_reports ON public.city_reports;
CREATE POLICY public_read_enabled_city_reports
ON public.city_reports
FOR SELECT
TO anon, authenticated
USING (
  EXISTS (
    SELECT 1
    FROM public.cities
    WHERE cities.city_key = city_reports.city_key
      AND cities.enabled = TRUE
  )
);

-- SECURITY DEFINER functions are executable by PUBLIC unless explicitly
-- revoked. These functions truncate data or affect the API schema cache.
REVOKE EXECUTE ON FUNCTION public.reset_osint_runtime_data() FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.reset_osint_all() FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION public.reload_api_schema_cache() FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.reset_osint_runtime_data() TO service_role;
GRANT EXECUTE ON FUNCTION public.reset_osint_all() TO service_role;
GRANT EXECUTE ON FUNCTION public.reload_api_schema_cache() TO service_role;

COMMIT;
