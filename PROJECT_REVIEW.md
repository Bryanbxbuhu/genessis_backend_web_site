# Project review — November 11, 2026

Reviewed the Python entry points, configuration registry, report generation and
merging, Supabase storage, HTML/PDF rendering, sync orchestration, SQL hardening,
and GitHub Actions workflows. The review started from a clean checkout updated
to the current remote `main`.

## Changes

- Set the shared report and website-disambiguation model to `gpt-6-luna`.
  Preserve explicit `OPENAI_MODEL` overrides and treat blank values as unset.
- Reject incomplete, refused, empty, malformed, and non-object JSON completions
  before report merging or storage.
- Repair unreachable facility website search processing and rate-limit retries.
- Fix the undefined country-metadata variable in database-only city reporting
  and the missing `Optional` annotation import in the sync script.
- Respect cities disabled in Supabase and reuse the report agent's datastore
  for city configuration lookup.
- Replace the unsupported partner-link query with explicit Supabase filters.
- Allow configuration imports without credentials; require credentials when
  constructing the corresponding service client.
- Repair corrupted PDF TOC placeholders, distinguish headings from TOC labels,
  and find multiple sections on one page. Use the supported PyMuPDF import and
  avoid a Windows console encoding error in the success message.
- Supply the empty legacy sources list needed to render an empty report.
- Run offline regression checks on pushes and pull requests. Retry browser
  installation three times, preserving failure if retries are exhausted.

## Validation

- 30 regression tests passed, including all 22 configured cities and region
  coverage. The suite also passed with socket connections blocked.
- Live OpenAI checks returned `gpt-6-luna` and valid JSON. The actual report
  generation method passed a synthetic no-incidents case with mocked database
  reads and no production writes.
- Configuration validation loaded 22 cities and 220 sources. Strict important
  services validation passed without network access.
- Every city passed empty report contract validation and HTML rendering.
  A synthetic PDF rendered successfully; pages, bookmarks, and links were
  inspected, including a visual review.
- Dependency consistency, Python compilation, undefined-name checks, and Git
  whitespace checks passed. Staged-content and full-history secret scans passed.

## Production observation and limits

A read-only check found reports and weather for all 22 cities. Thirteen cities
in the Europe schedule group were older than 24 hours at review time. The
[failed scheduled run](https://github.com/Bryanbxbuhu/genessis_backend_web_site/actions/runs/43036181251)
failed during Chromium OS dependency installation because the Ubuntu package
mirror could not be resolved. No reset or sync step ran in that failed job.
Bounded installation retries address transient failures; sustained upstream DNS
outages can still fail a job. The next successful scheduled update must confirm
production freshness.

No production resets, report writes, schema migrations, or full live ingestion
were performed for validation. Optional semantic relevance dependencies and
every external source's current availability were not exercised. Automated
regression coverage and representative live checks do not guarantee all future
upstream services or model outputs.

GPT-6 Luna supports the existing no-tool Chat Completions workload and structured
outputs according to the [official model documentation](https://developers.openai.com/api/docs/models/gpt-6-luna).
