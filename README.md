# Money in Politics — Federal Lobbying Dashboard

A self-hosted Grafana dashboard that visualizes US federal lobbying activity
using the Senate Lobbying Disclosure Act (LDA) API. Runs entirely on your
laptop or server with Docker.

![Dashboard screenshot](docs/dashboard.png)

## What you get

A dashboard showing:

- Total lobbying spend per year with year-over-year comparison
- Top 20 clients, top firms (by fees and by client count)
- Quarterly spend broken down by industry sector
- Industry share donut
- Most-lobbied bills
- Revolving-door tracker (lobbyists with disclosed prior government roles)
- A client lookup with partial-name search

All data comes from primary government sources — nothing is synthetic.

## Quick start

Requires **Docker** and **Python 3.10+**. No other dependencies.

```bash
git clone https://github.com/kali123411/money-in-politics.git
cd money-in-politics

# 1. Spin up Postgres and Grafana
docker compose up -d

# 2. (Optional but recommended) get a free LDA API key
#    https://lda.senate.gov/api/register/
export LDA_API_KEY="your_token_here"

# 3. Install Python deps and load some data
python3 -m venv .venv
source .venv/bin/activate
pip install -r ingest/requirements.txt

export DATABASE_URL="postgres://lobby:lobby@localhost:5432/lobbying"

# Smoke test — pulls ~100 filings, takes a minute
python ingest/ingest_lda.py --year 2024 --quarter Q3 --limit 100

# 4. Open Grafana
#    http://localhost:3000   (anonymous viewer mode enabled by default)
```

The dashboard should populate immediately. Pick a year from the dropdown,
type a name into the client search box, and explore.

## Full historical ingest

The smoke test above loads a tiny sample. For a useful dashboard, load at
least a full year:

```bash
# Tip: run inside tmux/screen since this takes 30-60 minutes per year
python ingest/ingest_lda.py --year 2024
python ingest/ingest_lda.py --year 2025
python ingest/ingest_lda.py --year 2026 --quarter Q1  # most recent

# Categorize clients by industry (rule-based keyword matching)
python ingest/categorize_clients.py --reset
```

Data available by year varies. As of this writing the LDA API covers
approximately 2008–present. Pre-2008 data exists on the Senate website but
not via the JSON API.

## Keeping it fresh

Lobbying filings are disclosed quarterly, with a 20-day grace period after
each quarter ends. New data flows in roughly on these schedules:

| Period           | Filing deadline |
|------------------|-----------------|
| Q1 (Jan–Mar)     | April 20        |
| Q2 (Apr–Jun)     | July 20         |
| Q3 (Jul–Sep)     | October 20      |
| Q4 (Oct–Dec)     | January 20      |

A nightly cron keeps things current:

```cron
0 3 * * * cd /path/to/money-in-politics && .venv/bin/python ingest/ingest_lda.py --since $(date -d '4 days ago' +\%Y-\%m-\%d) >> /var/log/lda-ingest.log 2>&1
```

The ingester is idempotent — overlapping runs never duplicate filings.

## Project layout

```
.
├── docker-compose.yml              # Postgres + Grafana in 2 containers
├── schema/
│   ├── 01_schema.sql               # Tables, indexes, industry lookup
│   └── 02_views.sql                # Views + materialized views for dashboard
├── ingest/
│   ├── ingest_lda.py               # Senate LDA — lobbying filings
│   ├── ingest_fec.py               # FEC — candidates/committees/contributions
│   ├── categorize_clients.py       # Rule-based industry classifier
│   └── requirements.txt            # Python deps
├── grafana/
│   ├── dashboards/
│   │   └── lobbying_overview.json  # 15-panel dashboard
│   └── provisioning/               # Auto-wires datasource + dashboard
└── docs/
    └── dashboard.png
```

## Data sources

**[Senate LDA API](https://lda.senate.gov/api/)** — free, public, no key
required but a free key raises rate limits substantially. Provides
Registrations (LD-1), Quarterly Activity Reports (LD-2), and Contributions
Reports (LD-203).

> ⚠️ The Senate is retiring `lda.senate.gov/api/` in favor of `LDA.gov`
> sometime in 2026. The `API_BASE` constant in `ingest/ingest_lda.py` will
> need to be updated once the new endpoint is documented.

**[FEC API](https://api.open.fec.gov/)** — free, requires a key from
[api.data.gov](https://api.data.gov/signup/). Optional; only needed if you
want campaign finance alongside lobbying.

## Industry categorization

Clients are categorized using a rule-based classifier
(`ingest/categorize_clients.py`). It matches regex patterns against client
names and descriptions across ~30 industry categories.

Typical coverage: **~40% of clients, ~55% of total spend.** The rest lands
in "Unclassified" (smaller trade associations, one-off LLCs, regional
advocacy groups). To improve coverage, run:

```bash
# See what's still unclassified, sorted by spend
docker exec mip-postgres psql -U lobby -d lobbying -c "
  SELECT c.name, ROUND(SUM(f.amount)::numeric/1e6, 2) AS spend_M
  FROM clients c
  JOIN filings f ON f.client_id = c.id
  WHERE c.industry_code IS NULL AND f.filing_year = 2025
  GROUP BY c.name
  ORDER BY spend_M DESC
  LIMIT 50;
"
```

Add matching patterns to `RULES` in `categorize_clients.py`, then re-run
with `--reset`. Pull requests adding categories welcome.

## What this does NOT include

It's important to be upfront with anyone looking at the dashboard: federal
LDA data is only one slice of political influence money. This dashboard
does **not** include:

- **State-level lobbying.** Every state has its own rules and formats;
  [FollowTheMoney.org](https://www.followthemoney.org/) aggregates these.
- **Dark-money 501(c)(4) activity.** Organizations that don't register as
  lobbyists but spend heavily on issue advocacy don't appear here.
- **FARA filings.** Foreign agents file with the DOJ, not the Senate.
- **Campaign contributions.** Separate schema via `ingest_fec.py` if you
  want to layer it on.
- **Grassroots / in-kind influence.** Ad buys, coalitions, etc. are
  invisible to LDA.

"Lobbying spend disclosed under the LDA" has a precise legal definition
and it's narrower than "money in politics" in general. Set expectations
accordingly.

## Known limitations

1. **Name normalization is imperfect.** "Meta Platforms, Inc." and "Meta
   Platforms" may appear as separate clients. The schema has `pg_trgm`
   indexes ready for fuzzy matching; building an alias table is an obvious
   next step.
2. **~45-day reporting lag.** Lobbying that happens today won't appear
   until the next quarterly deadline passes. This is structural to the
   LDA, not a bug.
3. **The YoY stat on partial years is misleading.** Comparing Q1 2026
   ($1.6B) to full 2025 ($6.7B) will show -76% even if 2026 is on track
   to grow.
4. **Industry categorization covers ~55% of spend.** See above.

## Troubleshooting

<details>
<summary>Grafana shows "Failed to upgrade legacy queries" / all panels are empty</summary>

The datasource UID in the dashboard must match the provisioned datasource.
Both are set to `lobbying` in this repo. If you ever see the mismatch
error, verify:

```bash
curl -s http://admin:admin@localhost:3000/api/datasources | python3 -m json.tool
```

Look for `"uid": "lobbying"` on the Lobbying datasource. If Grafana
generated a random UID instead (P402AA9DA... or similar), delete the
auto-generated one and restart:

```bash
curl -X DELETE http://admin:admin@localhost:3000/api/datasources/uid/SOME_UID
docker compose restart grafana
```
</details>

<details>
<summary>Ingester crashes with "named cursor isn't valid anymore"</summary>

You're on an older version. Pull the latest `categorize_clients.py` — it
uses separate read/write connections.
</details>

<details>
<summary>"No data" on every panel</summary>

Check the Year dropdown at the top of the dashboard — if it's empty, the
materialized views haven't been populated. Run:

```bash
docker exec mip-postgres psql -U lobby -d lobbying -c "SELECT refresh_dashboard_views();"
```

Then hard-refresh the browser (Ctrl-Shift-R).
</details>

<details>
<summary>LDA ingest hits rate limits</summary>

Get a free API key at
[lda.senate.gov/api/register/](https://lda.senate.gov/api/register/) and
set `LDA_API_KEY` before running the ingester. The free key raises your
limit substantially. Without it, full-year ingests may take hours with
frequent back-offs.
</details>

## License

MIT. The data itself is in the public domain (US federal disclosure
records).

## Credits

Built in a single session with [Claude](https://claude.ai) guiding the
architecture. Data sourced from the US Senate Office of Public Records
and the Federal Election Commission.
