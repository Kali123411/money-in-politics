#!/usr/bin/env python3
"""
ingest_lda.py — pull Senate LDA filings into Postgres.

The Senate LDA API (https://lda.senate.gov/api/) is free but rate-limited.
A free API key (https://lda.senate.gov/api/register/) raises the limit
substantially. Set it via LDA_API_KEY env var.

Usage:
    python ingest_lda.py --year 2024
    python ingest_lda.py --year 2024 --quarter Q3
    python ingest_lda.py --since 2024-01-01
    python ingest_lda.py --year 2024 --limit 100    # smoke test

Environment:
    DATABASE_URL   postgres://user:pass@host:5432/dbname  (required)
    LDA_API_KEY    token from lda.senate.gov/api/         (optional)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterator

import psycopg2
import requests

API_BASE = "https://lda.senate.gov/api/v1/filings/"
PAGE_SIZE = 25
REQUEST_TIMEOUT = 30
RETRY_BACKOFF = [5, 15, 45, 120, 300, 600]  # up to 10 min

log = logging.getLogger("lda")


@dataclass
class LdaClient:
    api_key: str | None = None

    def _headers(self) -> dict[str, str]:
        h = {"Accept": "application/json", "User-Agent": "money-in-politics/1.0"}
        if self.api_key:
            h["Authorization"] = f"Token {self.api_key}"
        return h

    def fetch(self, params: dict[str, Any]) -> Iterator[dict]:
        url: str | None = API_BASE
        first_params: dict | None = {**params, "page_size": PAGE_SIZE}

        while url:
            resp = None
            for attempt, backoff in enumerate(RETRY_BACKOFF, start=1):
                try:
                    resp = requests.get(
                        url, params=first_params,
                        headers=self._headers(), timeout=REQUEST_TIMEOUT,
                    )
                    if resp.status_code == 429:
                        log.warning("rate-limited (attempt %d), sleeping %ss",
                                    attempt, backoff)
                        time.sleep(backoff)
                        continue
                    resp.raise_for_status()
                    break
                except requests.RequestException as e:
                    if attempt == len(RETRY_BACKOFF):
                        raise
                    log.warning("request failed (%s), retrying in %ss", e, backoff)
                    time.sleep(backoff)
            else:
                raise RuntimeError("exhausted retries on LDA API")

            payload = resp.json()
            yield from payload.get("results", [])
            url = payload.get("next")
            first_params = None


_WS = re.compile(r"\s+")
_SUFFIXES = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c\.|corp|corporation|co|company|ltd|limited|"
    r"plc|lp|l\.p\.|llp|pllc|the)\b\.?",
    flags=re.IGNORECASE,
)

def normalize_name(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.lower()
    s = _SUFFIXES.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = _WS.sub(" ", s).strip()
    return s or None


def parse_date(val: str | None) -> date | None:
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None


_BILL_RE = re.compile(r"\b(H\.?R\.?|S\.?|H\.?J\.?\s?Res\.?|S\.?J\.?\s?Res\.?)\s?(\d{1,5})\b",
                      flags=re.IGNORECASE)

def extract_bills(desc: str | None) -> list[dict]:
    if not desc:
        return []
    found, seen = [], set()
    for m in _BILL_RE.finditer(desc):
        prefix = re.sub(r"[^A-Z]", "", m.group(1).upper())
        number = f"{prefix} {m.group(2)}"
        if number in seen:
            continue
        seen.add(number)
        found.append({"number": number, "chamber": "H" if prefix.startswith("H") else "S"})
    return found


def quarter_bounds(year: int | None, q: str | None) -> tuple[date | None, date | None]:
    if not year or not q:
        return None, None
    ranges = {
        "Q1": ((1, 1), (3, 31)),   "Q2": ((4, 1), (6, 30)),
        "Q3": ((7, 1), (9, 30)),   "Q4": ((10, 1), (12, 31)),
    }
    r = ranges.get(str(q).upper()[:2])
    if not r:
        return None, None
    return date(year, *r[0]), date(year, *r[1])


class Ingester:
    def __init__(self, conn):
        self.conn = conn

    def upsert_registrant(self, r: dict) -> int:
        sql = """
        INSERT INTO registrants
            (senate_id, name, normalized_name, description,
             address, city, state, zip, country, updated_at)
        VALUES
            (%(senate_id)s, %(name)s, %(normalized_name)s, %(description)s,
             %(address)s, %(city)s, %(state)s, %(zip)s, %(country)s, NOW())
        ON CONFLICT (senate_id) DO UPDATE SET
            name = EXCLUDED.name,
            normalized_name = EXCLUDED.normalized_name,
            description = EXCLUDED.description,
            updated_at = NOW()
        RETURNING id;
        """
        params = {
            "senate_id": r["id"], "name": r.get("name"),
            "normalized_name": normalize_name(r.get("name")),
            "description": r.get("description"),
            "address": " ".join(filter(None, [r.get("address_1"), r.get("address_2")])) or None,
            "city": r.get("city"), "state": r.get("state"),
            "zip": r.get("zip"), "country": r.get("country"),
        }
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

    def upsert_client(self, c: dict) -> int:
        sql = """
        INSERT INTO clients
            (senate_id, name, normalized_name, general_description,
             state_of_business, country_of_business, updated_at)
        VALUES
            (%(senate_id)s, %(name)s, %(normalized_name)s, %(general_description)s,
             %(state_of_business)s, %(country_of_business)s, NOW())
        ON CONFLICT (senate_id) DO UPDATE SET
            name = EXCLUDED.name,
            normalized_name = EXCLUDED.normalized_name,
            general_description = EXCLUDED.general_description,
            updated_at = NOW()
        RETURNING id;
        """
        params = {
            "senate_id": c["id"], "name": c.get("name"),
            "normalized_name": normalize_name(c.get("name")),
            "general_description": c.get("general_description"),
            "state_of_business": c.get("state"),
            "country_of_business": c.get("country"),
        }
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

    def upsert_filing(self, f: dict, registrant_id: int, client_id: int) -> int:
        sql = """
        INSERT INTO filings
            (filing_uuid, filing_type, filing_year, period_start, period_end,
             registrant_id, client_id, income, expenses, posted_at, dt_posted)
        VALUES
            (%(filing_uuid)s, %(filing_type)s, %(filing_year)s, %(period_start)s,
             %(period_end)s, %(registrant_id)s, %(client_id)s, %(income)s,
             %(expenses)s, %(posted_at)s, %(dt_posted)s)
        ON CONFLICT (filing_uuid) DO UPDATE SET
            income = EXCLUDED.income, expenses = EXCLUDED.expenses
        RETURNING id;
        """
        period_start, period_end = quarter_bounds(f.get("filing_year"), f.get("filing_type"))
        params = {
            "filing_uuid": f["filing_uuid"],
            "filing_type": f.get("filing_type"),
            "filing_year": f.get("filing_year"),
            "period_start": period_start, "period_end": period_end,
            "registrant_id": registrant_id, "client_id": client_id,
            "income": f.get("income"), "expenses": f.get("expenses"),
            "posted_at": f.get("dt_posted"), "dt_posted": parse_date(f.get("dt_posted")),
        }
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

    def replace_activities(self, filing_id: int, activities: list[dict]) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM lobbying_activities WHERE filing_id = %s", (filing_id,))
            for a in activities:
                cur.execute(
                    """
                    INSERT INTO lobbying_activities
                        (filing_id, general_issue_code, general_issue_label,
                         description, foreign_entity_issues)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (filing_id, a.get("general_issue_code"),
                     a.get("general_issue_code_display"), a.get("description"),
                     a.get("foreign_entity_issues")),
                )
                activity_id = cur.fetchone()[0]

                for bill in extract_bills(a.get("description")):
                    cur.execute(
                        "INSERT INTO activity_bills (activity_id, bill_number, chamber) "
                        "VALUES (%s, %s, %s);",
                        (activity_id, bill["number"], bill["chamber"]),
                    )

                for l in a.get("lobbyists", []) or []:
                    person = l.get("lobbyist") or {}
                    full = f"{person.get('first_name','')} {person.get('last_name','')}".strip()
                    if not full:
                        continue
                    cur.execute(
                        """
                        INSERT INTO lobbyists (first_name, last_name, full_name, normalized_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (normalized_name) DO UPDATE SET full_name = EXCLUDED.full_name
                        RETURNING id;
                        """,
                        (person.get("first_name"), person.get("last_name"),
                         full, normalize_name(full)),
                    )
                    lobbyist_id = cur.fetchone()[0]
                    cur.execute(
                        """
                        INSERT INTO activity_lobbyists
                            (activity_id, lobbyist_id, covered_position, new_lobbyist)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (activity_id, lobbyist_id) DO UPDATE SET
                            covered_position = EXCLUDED.covered_position;
                        """,
                        (activity_id, lobbyist_id,
                         l.get("covered_position"), bool(l.get("new"))),
                    )


def run(args: argparse.Namespace) -> None:
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False
    client = LdaClient(api_key=os.environ.get("LDA_API_KEY"))
    ing = Ingester(conn)

    params: dict[str, Any] = {}
    if args.year: params["filing_year"] = args.year
    if args.quarter: params["filing_type"] = args.quarter
    if args.since: params["filing_dt_posted_after"] = args.since

    processed = 0
    for filing in client.fetch(params):
        try:
            reg_id = ing.upsert_registrant(filing["registrant"])
            cli_id = ing.upsert_client(filing["client"])
            fid = ing.upsert_filing(filing, reg_id, cli_id)
            ing.replace_activities(fid, filing.get("lobbying_activities", []) or [])
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error("failed filing %s: %s", filing.get("filing_uuid"), e)
            continue

        processed += 1
        if processed % 50 == 0:
            log.info("processed %d filings", processed)
        if args.limit and processed >= args.limit:
            break

    log.info("done — %d filings ingested", processed)

    if not args.skip_refresh:
        log.info("refreshing materialized views...")
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_dashboard_views();")
            conn.commit()
        log.info("views refreshed")

    conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--year", type=int)
    p.add_argument("--quarter", choices=["Q1","Q2","Q3","Q4"])
    p.add_argument("--since", help="ISO date — ingest filings posted on/after")
    p.add_argument("--limit", type=int, help="cap filings (smoke test)")
    p.add_argument("--skip-refresh", action="store_true",
                   help="skip materialized view refresh at the end")
    args = p.parse_args()

    if not any([args.year, args.since]):
        p.error("specify at least --year or --since")
    if "DATABASE_URL" not in os.environ:
        print("error: DATABASE_URL env var is required", file=sys.stderr)
        sys.exit(2)
    if not os.environ.get("LDA_API_KEY"):
        log.warning("LDA_API_KEY not set — ingestion will be slow and may hit rate limits")

    run(args)


if __name__ == "__main__":
    main()
