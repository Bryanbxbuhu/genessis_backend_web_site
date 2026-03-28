-- Migration: Add partner_links table for affiliate URLs (SpotHero parking, etc.)
-- This table stores configurable partner affiliate URLs that can be updated in Supabase
-- without redeploying code.

CREATE TABLE IF NOT EXISTS public.partner_links (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_key TEXT NOT NULL,
  country_code TEXT,
  url TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique constraint: one partner per country (or NULL for global)
CREATE UNIQUE INDEX IF NOT EXISTS partner_links_partner_country_uq
ON public.partner_links (partner_key, country_code);

-- Enable RLS
ALTER TABLE public.partner_links ENABLE ROW LEVEL SECURITY;

-- Policy: Anyone can read enabled links
CREATE POLICY anon_select_partner_links
ON public.partner_links
FOR SELECT
TO anon
USING (enabled = true);

-- Seed SpotHero parking affiliate URL for US
INSERT INTO public.partner_links (partner_key, country_code, url, enabled)
VALUES (
  'spothero_parking',
  'US',
  'https://spothero.com/search?_branch_match_id=1540819861105523902&utm_source=Partnerships&utm_campaign=Tune_Platform&utm_medium=paid+advertising&_branch_referrer=H4sIAAAAAAAAA8soKSkottLXLy7IL8lILcrXSywo0MvJzMvWLyk2LDDyKwxONbSvK0pNSy0qysxLj08qyi8vTi2ydc4oys9NBQBq6R3%2BPgAAAA%3D%3D&view=dl',
  true
)
ON CONFLICT (partner_key, country_code)
DO UPDATE SET
  url = excluded.url,
  enabled = excluded.enabled,
  updated_at = now();

COMMENT ON TABLE public.partner_links IS 'Configurable affiliate partner URLs (SpotHero parking, etc.) that can be updated without code redeployment';
COMMENT ON COLUMN public.partner_links.partner_key IS 'Unique partner identifier (e.g., spothero_parking)';
COMMENT ON COLUMN public.partner_links.country_code IS 'ISO country code (e.g., US) or NULL for global';
COMMENT ON COLUMN public.partner_links.url IS 'Affiliate URL to link to in reports';
COMMENT ON COLUMN public.partner_links.enabled IS 'When false, the link will not be fetched or rendered';
