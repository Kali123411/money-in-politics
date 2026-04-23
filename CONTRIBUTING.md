# Contributing

Thanks for your interest. This is a small open-source project — there are
no corporate processes. Open a PR, or open an issue to discuss first.

## Easiest ways to contribute

### 1. Improve industry categorization

The biggest unclassified clients are visible with:

```bash
docker exec mip-postgres psql -U lobby -d lobbying -c "
  SELECT c.name, ROUND(SUM(f.amount)::numeric/1e6, 2) AS spend_M
  FROM clients c
  JOIN filings f ON f.client_id = c.id
  WHERE c.industry_code IS NULL
    AND f.filing_year = (SELECT latest_filing_year())
  GROUP BY c.name
  ORDER BY spend_M DESC
  LIMIT 50;
"
```

Pick some that look like they belong in a category, add regex patterns to
`ingest/categorize_clients.py`, and send a PR with your before/after
numbers.

### 2. Add a new dashboard panel

The dashboard JSON is at
`grafana/dashboards/lobbying_overview.json`. Grafana's built-in panel
editor is the easiest way to build new panels — edit in the UI, then
export and commit the JSON. Good candidates:

- Geographic map of client HQs (would need a geocoding pass)
- Bill drill-down showing which clients are lobbying on a specific bill
- Network graph of firm ↔ client relationships
- Foreign entity involvement panel (data is already in `filings.foreign_entity_issues`)
- Revolving-door deep dive focused on a specific former official's
  contacts

### 3. Handle the LDA.gov migration

The Senate is retiring `lda.senate.gov/api/` in favor of `LDA.gov` in
2026. Once the new endpoint is documented, the `API_BASE` constant in
`ingest/ingest_lda.py` needs updating and the JSON schema may have
changed. Heads-up welcome.

### 4. Add state-level data

Some states have decent APIs (CA, NY, TX). A new ingester targeting one
state as a proof-of-concept would be valuable — most of the schema
(registrants, clients, filings) maps over.

## Running tests

There aren't any yet. PRs adding pytest coverage for the ingester parsing
logic (`extract_bills`, `normalize_name`, etc.) are welcome.

## Style

- Keep dependencies minimal. The entire thing runs on `psycopg2-binary`
  and `requests` — let's keep it that way.
- Match the existing code's style: type hints where they help, no unused
  abstractions, comments that explain *why* not *what*.
- SQL queries that will run on a large database must use indexes — check
  with `EXPLAIN` before opening a PR.

## Ground rules

- No partisan framing. The dashboard shows disclosed money, not
  good/bad actors. Keep feature PRs neutral.
- Respect rate limits. Don't ship changes that would hammer the LDA or
  FEC APIs.
- Don't commit API keys. The `.env.example` shows what belongs there
  and `.gitignore` covers `.env`.
