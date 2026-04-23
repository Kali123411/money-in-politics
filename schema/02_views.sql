-- =============================================================================
-- Views, functions, and materialized views for the dashboard
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_quarterly_base: canonicalized quarterly filings
-- -----------------------------------------------------------------------------
-- LDA filing types have many variants (Q1Y, 1A, 1AY, 1T, etc.). Amendments
-- REPLACE original filings, so we keep only the latest version per
-- (registrant, client, year, quarter) to avoid double-counting.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quarterly_base AS
WITH ranked AS (
    SELECT
        f.*,
        CASE
            WHEN filing_type IN ('Q1','Q1Y','1A','1T','1TY','1AY') THEN 'Q1'
            WHEN filing_type IN ('Q2','Q2Y','2A','2T','2TY','2AY') THEN 'Q2'
            WHEN filing_type IN ('Q3','Q3Y','3A','3T','3TY','3AY') THEN 'Q3'
            WHEN filing_type IN ('Q4','Q4Y','4A','4T','4TY','4AY') THEN 'Q4'
        END AS canonical_quarter,
        ROW_NUMBER() OVER (
            PARTITION BY registrant_id, client_id, filing_year,
                         CASE
                            WHEN filing_type IN ('Q1','Q1Y','1A','1T','1TY','1AY') THEN 'Q1'
                            WHEN filing_type IN ('Q2','Q2Y','2A','2T','2TY','2AY') THEN 'Q2'
                            WHEN filing_type IN ('Q3','Q3Y','3A','3T','3TY','3AY') THEN 'Q3'
                            WHEN filing_type IN ('Q4','Q4Y','4A','4T','4TY','4AY') THEN 'Q4'
                         END
            ORDER BY
                CASE filing_type
                    WHEN 'Q1Y' THEN 1 WHEN 'Q2Y' THEN 1 WHEN 'Q3Y' THEN 1 WHEN 'Q4Y' THEN 1
                    WHEN 'Q1'  THEN 2 WHEN 'Q2'  THEN 2 WHEN 'Q3'  THEN 2 WHEN 'Q4'  THEN 2
                    WHEN '1AY' THEN 3 WHEN '2AY' THEN 3 WHEN '3AY' THEN 3 WHEN '4AY' THEN 3
                    WHEN '1A'  THEN 4 WHEN '2A'  THEN 4 WHEN '3A'  THEN 4 WHEN '4A'  THEN 4
                    ELSE 5
                END,
                posted_at DESC NULLS LAST,
                id DESC
        ) AS version_rank
    FROM filings f
    WHERE filing_type IN (
        'Q1','Q2','Q3','Q4',
        'Q1Y','Q2Y','Q3Y','Q4Y',
        '1A','2A','3A','4A',
        '1AY','2AY','3AY','4AY',
        '1T','2T','3T','4T',
        '1TY','2TY','3TY','4TY'
    )
)
SELECT
    id AS filing_id,
    filing_uuid,
    filing_year,
    filing_type,
    canonical_quarter,
    period_start,
    period_end,
    registrant_id,
    client_id,
    amount,
    posted_at
FROM ranked
WHERE version_rank = 1
  AND canonical_quarter IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Year helpers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION latest_complete_year() RETURNS INT
LANGUAGE sql STABLE AS $$
    SELECT MAX(filing_year)
    FROM (
        SELECT filing_year
        FROM v_quarterly_base
        GROUP BY filing_year
        HAVING COUNT(DISTINCT canonical_quarter) = 4
           AND COUNT(*) > 10000
    ) t;
$$;

CREATE OR REPLACE FUNCTION latest_year_any() RETURNS INT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(MAX(filing_year), EXTRACT(YEAR FROM CURRENT_DATE)::INT)
    FROM v_quarterly_base;
$$;

CREATE OR REPLACE FUNCTION latest_filing_year() RETURNS INT
LANGUAGE sql STABLE AS $$
    SELECT GREATEST(
        COALESCE(latest_complete_year(), 0),
        COALESCE(latest_year_any(),      0)
    );
$$;

-- -----------------------------------------------------------------------------
-- v_filings_enriched: joined registrants + clients + industry
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_filings_enriched AS
SELECT
    b.filing_id,
    b.filing_year,
    b.filing_type,
    b.canonical_quarter,
    b.period_start,
    b.period_end,
    b.amount,
    r.id   AS registrant_id,
    r.name AS registrant_name,
    c.id   AS client_id,
    c.name AS client_name,
    c.industry_code,
    c.industry_label,
    i.sector AS industry_sector
FROM v_quarterly_base b
JOIN registrants r ON r.id = b.registrant_id
JOIN clients     c ON c.id = b.client_id
LEFT JOIN industries i ON i.code = c.industry_code;

-- -----------------------------------------------------------------------------
-- Materialized views (dashboard hits these for instant load)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_yearly_totals AS
SELECT
    filing_year,
    COUNT(*)                      AS filings,
    COUNT(DISTINCT client_id)     AS unique_clients,
    COUNT(DISTINCT registrant_id) AS unique_registrants,
    SUM(amount)                   AS total_spend
FROM v_filings_enriched
WHERE amount IS NOT NULL
GROUP BY filing_year
WITH NO DATA;
CREATE UNIQUE INDEX ON mv_yearly_totals (filing_year);

CREATE MATERIALIZED VIEW mv_quarterly_sector AS
SELECT
    filing_year,
    canonical_quarter,
    make_date(filing_year,
              CASE canonical_quarter
                WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 4
                WHEN 'Q3' THEN 7 WHEN 'Q4' THEN 10
              END, 1)                         AS quarter_start,
    COALESCE(industry_sector, 'Unclassified') AS sector,
    COUNT(*)                                  AS filings,
    SUM(amount)                               AS spend
FROM v_filings_enriched
WHERE amount IS NOT NULL
GROUP BY filing_year, canonical_quarter, industry_sector
WITH NO DATA;
CREATE INDEX ON mv_quarterly_sector (filing_year);
CREATE INDEX ON mv_quarterly_sector (quarter_start);

CREATE MATERIALIZED VIEW mv_client_year AS
SELECT
    filing_year,
    client_id,
    client_name,
    COALESCE(industry_sector, 'Unclassified') AS sector,
    SUM(amount)                               AS total_spend,
    COUNT(*)                                  AS filings
FROM v_filings_enriched
WHERE amount IS NOT NULL
GROUP BY filing_year, client_id, client_name, industry_sector
WITH NO DATA;
CREATE INDEX ON mv_client_year (filing_year, total_spend DESC);
CREATE INDEX ON mv_client_year (client_name);
CREATE INDEX ON mv_client_year USING gin (client_name gin_trgm_ops);

CREATE MATERIALIZED VIEW mv_firm_year AS
SELECT
    filing_year,
    registrant_id,
    registrant_name,
    COUNT(DISTINCT client_id) AS client_count,
    COUNT(*)                  AS filings,
    SUM(amount)               AS total_fees
FROM v_filings_enriched
WHERE amount IS NOT NULL
GROUP BY filing_year, registrant_id, registrant_name
WITH NO DATA;
CREATE INDEX ON mv_firm_year (filing_year, client_count DESC);
CREATE INDEX ON mv_firm_year (filing_year, total_fees DESC);

-- -----------------------------------------------------------------------------
-- Refresh function (run after ingest)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_dashboard_views() RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_yearly_totals;
    REFRESH MATERIALIZED VIEW mv_quarterly_sector;
    REFRESH MATERIALIZED VIEW mv_client_year;
    REFRESH MATERIALIZED VIEW mv_firm_year;
END;
$$;
