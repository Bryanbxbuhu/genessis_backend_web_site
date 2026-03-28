-- Backfill curated_places.website from website_canonical when website is missing.
UPDATE curated_places
SET website = website_canonical
WHERE (website IS NULL OR btrim(website) = '')
  AND (website_canonical IS NOT NULL AND btrim(website_canonical) <> '');
