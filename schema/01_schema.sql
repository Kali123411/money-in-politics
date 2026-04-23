-- =============================================================================
-- Money in Politics — consolidated PostgreSQL schema
-- =============================================================================
-- Loads automatically on first Postgres container start (docker-compose
-- mounts ./schema to /docker-entrypoint-initdb.d). Idempotent-ish: safe to
-- re-run against an empty database.
--
-- Covers:
--   - Senate LDA: registrants, clients, filings, activities, bills, lobbyists
--   - FEC: candidates, committees, contributions (optional)
--   - Industry categorization lookup
--   - Materialized views for fast dashboard queries
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS citext;

-- -----------------------------------------------------------------------------
-- REGISTRANTS (lobbying firms + self-filing orgs)
-- -----------------------------------------------------------------------------
CREATE TABLE registrants (
    id              BIGSERIAL PRIMARY KEY,
    senate_id       INTEGER UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    normalized_name CITEXT,
    description     TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    country         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_registrants_name_trgm  ON registrants USING gin (name gin_trgm_ops);
CREATE INDEX idx_registrants_normalized ON registrants (normalized_name);

-- -----------------------------------------------------------------------------
-- CLIENTS
-- -----------------------------------------------------------------------------
CREATE TABLE clients (
    id                  BIGSERIAL PRIMARY KEY,
    senate_id           INTEGER UNIQUE NOT NULL,
    name                TEXT NOT NULL,
    normalized_name     CITEXT,
    general_description TEXT,
    state_of_business   TEXT,
    country_of_business TEXT,
    industry_code       TEXT,
    industry_label      TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_clients_name_trgm  ON clients USING gin (name gin_trgm_ops);
CREATE INDEX idx_clients_normalized ON clients (normalized_name);
CREATE INDEX idx_clients_industry   ON clients (industry_code);

-- -----------------------------------------------------------------------------
-- FILINGS
-- -----------------------------------------------------------------------------
CREATE TABLE filings (
    id            BIGSERIAL PRIMARY KEY,
    filing_uuid   UUID UNIQUE NOT NULL,
    filing_type   TEXT NOT NULL,
    filing_year   INTEGER NOT NULL,
    period_start  DATE,
    period_end    DATE,
    registrant_id BIGINT NOT NULL REFERENCES registrants(id),
    client_id     BIGINT NOT NULL REFERENCES clients(id),
    income        NUMERIC(12,2),
    expenses      NUMERIC(12,2),
    amount        NUMERIC(12,2) GENERATED ALWAYS AS (COALESCE(income, expenses)) STORED,
    posted_at     TIMESTAMPTZ,
    dt_posted     DATE,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_filings_year       ON filings (filing_year);
CREATE INDEX idx_filings_period     ON filings (period_start, period_end);
CREATE INDEX idx_filings_client     ON filings (client_id);
CREATE INDEX idx_filings_registrant ON filings (registrant_id);
CREATE INDEX idx_filings_amount     ON filings (amount DESC NULLS LAST);
CREATE INDEX idx_filings_year_type  ON filings (filing_year, filing_type);

-- -----------------------------------------------------------------------------
-- LOBBYING ACTIVITIES (issue areas disclosed within a filing)
-- -----------------------------------------------------------------------------
CREATE TABLE lobbying_activities (
    id                    BIGSERIAL PRIMARY KEY,
    filing_id             BIGINT NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    general_issue_code    TEXT,
    general_issue_label   TEXT,
    description           TEXT,
    foreign_entity_issues TEXT
);
CREATE INDEX idx_activities_filing ON lobbying_activities (filing_id);
CREATE INDEX idx_activities_issue  ON lobbying_activities (general_issue_code);

-- -----------------------------------------------------------------------------
-- BILLS mentioned within activities (parsed from free-text descriptions)
-- -----------------------------------------------------------------------------
CREATE TABLE activity_bills (
    id           BIGSERIAL PRIMARY KEY,
    activity_id  BIGINT NOT NULL REFERENCES lobbying_activities(id) ON DELETE CASCADE,
    bill_number  TEXT NOT NULL,
    congress_num INTEGER,
    chamber      TEXT
);
CREATE INDEX idx_activity_bills_activity ON activity_bills (activity_id);
CREATE INDEX idx_activity_bills_number   ON activity_bills (bill_number);

-- -----------------------------------------------------------------------------
-- LOBBYISTS
-- -----------------------------------------------------------------------------
CREATE TABLE lobbyists (
    id              BIGSERIAL PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    normalized_name CITEXT UNIQUE
);

CREATE TABLE activity_lobbyists (
    activity_id      BIGINT NOT NULL REFERENCES lobbying_activities(id) ON DELETE CASCADE,
    lobbyist_id      BIGINT NOT NULL REFERENCES lobbyists(id),
    covered_position TEXT,
    new_lobbyist     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (activity_id, lobbyist_id)
);
CREATE INDEX idx_activity_lobbyists_lobbyist ON activity_lobbyists (lobbyist_id);
CREATE INDEX idx_activity_lobbyists_covered  ON activity_lobbyists (covered_position)
    WHERE covered_position IS NOT NULL;

-- -----------------------------------------------------------------------------
-- GOVERNMENT ENTITIES LOBBIED
-- -----------------------------------------------------------------------------
CREATE TABLE government_entities (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE activity_gov_entities (
    activity_id BIGINT NOT NULL REFERENCES lobbying_activities(id) ON DELETE CASCADE,
    entity_id   BIGINT NOT NULL REFERENCES government_entities(id),
    PRIMARY KEY (activity_id, entity_id)
);

-- -----------------------------------------------------------------------------
-- FEC: CANDIDATES / COMMITTEES / CONTRIBUTIONS
-- -----------------------------------------------------------------------------
CREATE TABLE candidates (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    party               TEXT,
    office              TEXT,
    state               TEXT,
    district            TEXT,
    incumbent_challenge TEXT,
    first_file_date     DATE,
    last_file_date      DATE
);
CREATE INDEX idx_candidates_state_office ON candidates (state, office);

CREATE TABLE committees (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    committee_type     TEXT,
    party              TEXT,
    connected_org_name TEXT,
    designation        TEXT,
    filing_frequency   TEXT
);
CREATE INDEX idx_committees_type          ON committees (committee_type);
CREATE INDEX idx_committees_connected_org ON committees (connected_org_name);

CREATE TABLE contributions (
    id                     BIGSERIAL PRIMARY KEY,
    committee_id           TEXT REFERENCES committees(id),
    contributor_name       TEXT,
    contributor_employer   TEXT,
    contributor_occupation TEXT,
    contributor_state      TEXT,
    contributor_zip        TEXT,
    amount                 NUMERIC(12,2),
    contribution_date      DATE,
    recipient_candidate_id TEXT REFERENCES candidates(id),
    recipient_committee_id TEXT REFERENCES committees(id),
    contribution_type      TEXT,
    transaction_id         TEXT,
    created_at             TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_contributions_date      ON contributions (contribution_date);
CREATE INDEX idx_contributions_candidate ON contributions (recipient_candidate_id);
CREATE INDEX idx_contributions_committee ON contributions (recipient_committee_id);
CREATE INDEX idx_contributions_amount    ON contributions (amount DESC);
CREATE INDEX idx_contributions_employer  ON contributions USING gin (contributor_employer gin_trgm_ops);

-- -----------------------------------------------------------------------------
-- INDUSTRIES lookup
-- -----------------------------------------------------------------------------
CREATE TABLE industries (
    code   TEXT PRIMARY KEY,
    label  TEXT NOT NULL,
    sector TEXT
);

INSERT INTO industries (code, label, sector) VALUES
    ('PHARMA',       'Pharmaceuticals & Health Products',     'Health'),
    ('INSUR',        'Insurance',                             'Finance'),
    ('OILGAS',       'Oil & Gas',                             'Energy'),
    ('TECH',         'Computers & Internet',                  'Technology'),
    ('TELECOM',      'Telecom Services',                      'Communications'),
    ('DEFENSE',      'Defense & Aerospace',                   'Defense'),
    ('BANKS',        'Commercial Banks & Finance',            'Finance'),
    ('REALEST',      'Real Estate',                           'Finance'),
    ('HOSP',         'Hospitals & Health Services',           'Health'),
    ('ELEC',         'Electric Utilities',                    'Energy'),
    ('AIRL',         'Air Transport',                         'Transportation'),
    ('EDUC',         'Education',                             'Ideological'),
    ('GOVT',         'State & Local Governments',             'Other'),
    ('AUTO',         'Automotive & Manufacturing',            'Manufacturing'),
    ('AGRI',         'Agriculture & Food',                    'Agriculture'),
    ('LABOR',        'Labor Unions',                          'Labor'),
    ('TOBALC',       'Tobacco & Alcohol',                     'Consumer Goods'),
    ('BIZORG',       'Business Associations (Cross-industry)','Cross-industry'),
    ('IDEO',         'Ideological / Single-issue Advocacy',   'Ideological'),
    ('HEALTH_OTHER', 'Health (other)',                        'Health'),
    ('ENERGY_OTHER', 'Energy (other)',                        'Energy'),
    ('CHEM',         'Chemicals & Materials',                 'Manufacturing'),
    ('CRYPTO',       'Crypto & Blockchain',                   'Finance'),
    ('MEDIA',        'Media & Entertainment',                 'Media'),
    ('RETAIL',       'Retail',                                'Consumer'),
    ('LEGAL',        'Legal & Trial Lawyers',                 'Legal'),
    ('NONPROF',      'Nonprofits & Charitable Advocacy',      'Nonprofit'),
    ('TRANSP',       'Transportation (surface)',              'Transportation'),
    ('FIREARMS',     'Firearms Industry',                     'Ideological'),
    ('MISC',         'Miscellaneous',                         'Other');
