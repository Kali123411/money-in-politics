#!/usr/bin/env python3
"""
ingest_fec.py — pull candidate, committee, and contribution data from the FEC.

The FEC API (https://api.open.fec.gov/) is free; get a key at
https://api.data.gov/signup/. Rate limit is 1000 req/hour.

For full-cycle contribution loads, FEC bulk downloads
(https://www.fec.gov/data/browse-data/) are MUCH faster than the API.

Usage:
    python ingest_fec.py candidates --cycle 2024
    python ingest_fec.py committees --cycle 2024
    python ingest_fec.py contributions --cycle 2024 --min-amount 1000

Environment:
    DATABASE_URL   postgres://...  (required)
    FEC_API_KEY    your key         (required)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Iterator

import psycopg2
import requests

API_BASE = "https://api.open.fec.gov/v1"
PER_PAGE = 100
REQUEST_TIMEOUT = 30

log = logging.getLogger("fec")


class FecClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def paginate(self, endpoint: str, params: dict[str, Any]) -> Iterator[dict]:
        params = {**params, "api_key": self.api_key, "per_page": PER_PAGE, "page": 1}
        while True:
            for attempt in range(4):
                resp = requests.get(
                    f"{API_BASE}{endpoint}", params=params, timeout=REQUEST_TIMEOUT
                )
                if resp.status_code == 429:
                    wait = 60 * (attempt + 1)
                    log.warning("rate-limited, sleeping %ss", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            else:
                raise RuntimeError("exhausted retries")

            body = resp.json()
            yield from body.get("results", [])

            pagination = body.get("pagination", {})
            if params["page"] >= pagination.get("pages", 0):
                return
            params["page"] += 1


def _parse_date(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None


def load_candidates(conn, fec: FecClient, cycle: int) -> None:
    sql = """
    INSERT INTO candidates
        (id, name, party, office, state, district, incumbent_challenge,
         first_file_date, last_file_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name, party = EXCLUDED.party,
        last_file_date = EXCLUDED.last_file_date,
        incumbent_challenge = EXCLUDED.incumbent_challenge;
    """
    n = 0
    with conn.cursor() as cur:
        for row in fec.paginate("/candidates/", {"cycle": cycle}):
            cur.execute(sql, (
                row["candidate_id"], row.get("name"), row.get("party"),
                row.get("office"), row.get("state"), row.get("district"),
                row.get("incumbent_challenge_full"),
                _parse_date(row.get("first_file_date")),
                _parse_date(row.get("last_file_date")),
            ))
            n += 1
            if n % 500 == 0:
                conn.commit()
                log.info("%d candidates", n)
    conn.commit()
    log.info("done — %d candidates", n)


def load_committees(conn, fec: FecClient, cycle: int) -> None:
    sql = """
    INSERT INTO committees
        (id, name, committee_type, party, connected_org_name,
         designation, filing_frequency)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        committee_type = EXCLUDED.committee_type,
        connected_org_name = EXCLUDED.connected_org_name;
    """
    n = 0
    with conn.cursor() as cur:
        for row in fec.paginate("/committees/", {"cycle": cycle}):
            cur.execute(sql, (
                row["committee_id"], row.get("name"),
                row.get("committee_type"), row.get("party"),
                row.get("organization_type_full") or row.get("connected_organization_name"),
                row.get("designation"), row.get("filing_frequency"),
            ))
            n += 1
            if n % 500 == 0:
                conn.commit()
                log.info("%d committees", n)
    conn.commit()
    log.info("done — %d committees", n)


def load_contributions(conn, fec: FecClient, cycle: int, min_amount: float) -> None:
    sql = """
    INSERT INTO contributions
        (committee_id, contributor_name, contributor_employer, contributor_occupation,
         contributor_state, contributor_zip, amount, contribution_date,
         recipient_committee_id, contribution_type, transaction_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    params = {
        "two_year_transaction_period": cycle,
        "min_amount": min_amount,
        "is_individual": "true",
        "sort": "-contribution_receipt_date",
    }
    n = 0
    with conn.cursor() as cur:
        for row in fec.paginate("/schedules/schedule_a/", params):
            cur.execute(sql, (
                row.get("committee_id"), row.get("contributor_name"),
                row.get("contributor_employer"), row.get("contributor_occupation"),
                row.get("contributor_state"), row.get("contributor_zip"),
                row.get("contribution_receipt_amount"),
                _parse_date(row.get("contribution_receipt_date")),
                row.get("committee_id"), row.get("receipt_type_full"),
                row.get("transaction_id"),
            ))
            n += 1
            if n % 1000 == 0:
                conn.commit()
                log.info("%d contributions", n)
    conn.commit()
    log.info("done — %d contributions", n)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("target", choices=["candidates", "committees", "contributions"])
    p.add_argument("--cycle", type=int, required=True)
    p.add_argument("--min-amount", type=float, default=1000)
    args = p.parse_args()

    if "DATABASE_URL" not in os.environ or "FEC_API_KEY" not in os.environ:
        print("error: DATABASE_URL and FEC_API_KEY env vars required", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    fec = FecClient(os.environ["FEC_API_KEY"])

    if args.target == "candidates":
        load_candidates(conn, fec, args.cycle)
    elif args.target == "committees":
        load_committees(conn, fec, args.cycle)
    elif args.target == "contributions":
        load_contributions(conn, fec, args.cycle, args.min_amount)

    conn.close()


if __name__ == "__main__":
    main()
