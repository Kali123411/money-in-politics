#!/usr/bin/env python3
"""
categorize_clients.py — v2: expanded rules based on observed top misses.

New categories over v1:
  CHEM, MEDIA, CRYPTO, LEGAL, RETAIL, NONPROF, TRANSP, FIREARMS
Heavy additions to DEFENSE, PHARMA, TECH, REALEST, BANKS, INSUR, OILGAS.

Usage:
    python categorize_clients.py              # classify unclassified only
    python categorize_clients.py --reset      # wipe + re-classify everything
    python categorize_clients.py --dry-run    # preview only
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import psycopg2

# Rules evaluated top to bottom — first match wins. Order matters.
RULES: list[tuple[str, str, list[str]]] = [

    # ===== PHARMA & BIOTECH =====
    ("PHARMA", "Pharmaceuticals & Health Products", [
        r"\bpharma", r"\bbiotech", r"biosciences?\b", r"\btherapeutics\b",
        r"\bmerck\b", r"\bpfizer\b", r"\bnovartis\b", r"\babbvie\b",
        r"\bgilead\b", r"\bmoderna\b", r"\bastrazeneca\b", r"\bamgen\b",
        r"\bgsk\b", r"glaxosmithkline", r"bristol[- ]myers", r"eli lilly",
        r"johnson ?& ?johnson", r"\bj&j\b", r"\bphrma\b", r"\bbio\b",
        r"pharmaceutical", r"drug manufacturers?", r"\bmedicines?\b",
        r"\bbayer\b", r"\bgenentech\b", r"\bnovo nordisk\b", r"\broche\b",
        r"\btakeda\b", r"\bsanofi\b", r"\bbiogen\b", r"\bregeneron\b",
        r"\balkermes\b", r"\billumina\b", r"\bvertex\b.*\bpharma",
    ]),

    # ===== CHEMICALS =====
    ("CHEM", "Chemicals & Materials", [
        r"\bamerican chemistry\b", r"\bchemistry council\b",
        r"\bdow chemical\b", r"\bdow inc\b", r"\bdupont\b",
        r"\bbasf\b", r"\b3m company\b", r"\blyondellbasell\b",
        r"\beastman chemical\b", r"\bchemours\b", r"\bcorteva\b",
        r"\bhuntsman\b.*\bcorp", r"\binternational flavors\b",
        r"\bchemical\b.*\bassociation\b", r"\bplastics industry\b",
        r"\bpetrochemical\b", r"\bspecialty chemicals?\b",
        r"\bfertilizer\b.*\binstitute\b",
    ]),

    # ===== CRYPTO =====
    ("CRYPTO", "Crypto & Blockchain", [
        r"\bcoinbase\b", r"\bbinance\b", r"\bkraken\b.*\bexchange",
        r"\bblockchain\b", r"\bcryptocurrency\b", r"\bripple labs\b",
        r"\bcircle\b.*\bfinancial\b", r"\bgemini\b.*\btrust\b",
        r"\bdigital asset\b", r"\bweb3\b", r"\bstablecoin\b",
        r"\bcrypto\b.*\bcouncil\b",
    ]),

    # ===== HOSPITALS =====
    ("HOSP", "Hospitals & Health Services", [
        r"\bhospital", r"\bhealth system", r"\bhealthcare\b", r"\bmedical center\b",
        r"nursing home", r"\bclinic\b", r"\bmayo clinic\b", r"kaiser permanente",
        r"cleveland clinic", r"\bhealth care\b", r"american hospital",
        r"\bchildren'?s\b.*\bhospital\b", r"\bhca healthcare\b",
        r"\buniversal health\b", r"\bhealth plan\b",
    ]),

    # ===== INSURANCE =====
    ("INSUR", "Insurance", [
        r"\binsurance\b", r"\binsurer\b", r"blue cross", r"blue shield",
        r"\bcigna\b", r"\baetna\b", r"\bhumana\b", r"unitedhealth",
        r"\banthem\b", r"\belevance\b", r"\bmetlife\b", r"\ballstate\b",
        r"state farm", r"\bgeico\b", r"progressive corp", r"\bprudential\b",
        r"american council of life insurers", r"\bmutual\b.*\binsurance",
        r"\baflac\b", r"\bchubb\b.*\blimited\b", r"\btravelers\b.*\bcompanies\b",
        r"\bhartford financial\b", r"\bmanulife\b", r"\bnationwide mutual\b",
        r"\bnew york life\b", r"\bguardian life\b", r"\bmassmutual\b",
    ]),

    # ===== OIL & GAS (includes Koch, major refiners) =====
    ("OILGAS", "Oil & Gas", [
        r"\bexxon", r"\bchevron\b", r"\bshell\b", r"conocophillips",
        r"\bbp\b", r"\bhess\b", r"\bpetroleum\b", r"\bpipeline\b",
        r"\bnatural gas\b", r"\boil\b", r"\bgas\b.*\bassociation\b",
        r"\brefining\b", r"\bupstream\b", r"american petroleum",
        r"\bliquefied natural\b", r"\blng\b", r"\bfracking\b",
        r"\bdrilling\b", r"\bshale\b", r"\bphillips 66\b", r"\bvalero\b",
        r"\boccidental petroleum\b", r"\bkoch industries\b",
        r"\bkoch government\b", r"\bkoch companies\b", r"\bkoch supply\b",
        r"\bpioneer natural\b", r"\bwilliams companies\b",
        r"\bkinder morgan\b", r"\benterprise products\b",
        r"\benergy transfer\b", r"\bcheniere\b", r"\bhalliburton\b",
        r"\bschlumberger\b", r"\bbaker hughes\b", r"\bmarathon petroleum\b",
    ]),

    # ===== ELECTRIC UTILITIES =====
    ("ELEC", "Electric Utilities", [
        r"\belectric\b.*\butility\b", r"\belectric\b.*\bcompany\b",
        r"\benergy\b.*\butility\b", r"\bpower\b.*\bcompany\b",
        r"\bduke energy\b", r"\bsouthern company\b", r"\bexelon\b",
        r"\bnextera\b", r"\bdominion energy\b", r"\bconed\b",
        r"edison electric", r"nuclear energy", r"\butilities?\b",
        r"\butility\b.*\bassociation\b", r"\brenewable\b",
        r"\bsolar\b.*\bindustr", r"\bwind\b.*\bassociation\b",
        r"\bamerican electric power\b", r"\baep\b", r"\bfirstenergy\b",
        r"\bxcel energy\b", r"\bnisource\b", r"\bentergy\b", r"\bppl corp\b",
    ]),

    # ===== DEFENSE & AEROSPACE =====
    ("DEFENSE", "Defense & Aerospace", [
        r"\blockheed\b", r"\braytheon\b", r"\bnorthrop\b", r"\bboeing\b",
        r"general dynamics", r"\bl3harris\b", r"\bbae systems\b",
        r"\bdefense\b", r"\bweapons\b", r"\bmunitions\b", r"\barmaments\b",
        r"aerospace industries", r"\bmilitary\b", r"\barmament",
        r"\brtx corporation\b", r"\brtx\b.*\baffiliates\b",
        r"\bhoneywell\b", r"\bgeneral atomics\b",
        r"\bhuntington ingalls\b", r"\btextron\b", r"\bleidos\b",
        r"\bkbr\b.*\binc\b", r"\bbooz allen\b", r"\bcaci\b.*\binternational",
        r"\bmaxar\b", r"\bpratt\b.*\bwhitney\b", r"\bcollins aerospace\b",
        r"\banduril\b", r"\bsaic\b", r"\bscience applications\b",
    ]),

    # ===== TECH =====
    ("TECH", "Computers & Internet", [
        r"\bmicrosoft\b", r"\bgoogle\b", r"\balphabet\b", r"\bamazon\b",
        r"\bmeta\b", r"\bfacebook\b", r"\bapple inc\b", r"\boracle\b",
        r"\bsalesforce\b", r"\bintel corp\b", r"\bnvidia\b", r"\bibm\b",
        r"\badobe\b", r"\bcisco\b", r"\btiktok\b", r"\bbytedance\b",
        r"\bsnap inc\b", r"\bsnapchat\b", r"\bnetflix\b", r"\bspotify\b",
        r"\bairbnb\b", r"\buber\b", r"\blyft\b", r"\bopenai\b",
        r"\banthropic\b", r"\bpalantir\b", r"\belectronics? association\b",
        r"\bsoftware\b.*\balliance\b", r"\binformation technology\b",
        r"\binternet association\b", r"\bqualcomm\b", r"\bsamsung\b",
        r"\bhp inc\b", r"\bdell technologies\b", r"\bhewlett packard\b",
        r"\bbroadcom\b", r"\bmicron\b.*\btechnology", r"\btexas instruments\b",
        r"\bamd\b.*\binc\b", r"\bapplied materials\b", r"\bwestern digital\b",
        r"\bsemiconductor\b", r"\bcloudflare\b", r"\bzoom\b.*\bvideo",
        r"\bdoordash\b", r"\binstacart\b", r"\bpaypal\b", r"\bstripe\b.*\binc",
        r"\bdatadog\b", r"\bshopify\b",
    ]),

    # ===== BANKS & FINANCE =====
    ("BANKS", "Commercial Banks & Finance", [
        r"\bbank of america\b", r"\bjpmorgan\b", r"\bjp morgan\b",
        r"\bwells fargo\b", r"\bcitigroup\b", r"\bcitibank\b",
        r"goldman sachs", r"\bmorgan stanley\b", r"\bblackrock\b",
        r"\bvanguard\b", r"fidelity investments", r"\bcharles schwab\b",
        r"american bankers", r"independent community bankers",
        r"credit union national", r"\bsifma\b", r"\bfidelity\b",
        r"securities industry", r"financial services roundtable",
        r"\bbanking\b.*\bassociation\b", r"\bfintech\b",
        r"\binvestment company institute\b", r"\bmastercard\b",
        r"\bvisa\b.*\binc\b", r"\bamerican express\b", r"\bdiscover\b.*\bfinancial\b",
        r"\belectronic payments\b", r"\baicpa\b", r"\baccountants\b",
        r"\bberkshire hathaway\b", r"\bcredit union\b", r"\bus bancorp\b",
        r"\bpnc financial\b", r"\btruist\b", r"\bcapital one\b",
        r"\bally financial\b", r"\bcboe\b", r"\bnasdaq\b",
        r"\bhedge fund\b", r"\bprivate equity\b", r"\bkkr\b.*\binc",
        r"\bblackstone\b", r"\bapollo global\b", r"\bcarlyle group\b",
    ]),

    # ===== REAL ESTATE =====
    ("REALEST", "Real Estate", [
        r"\bnational association of realtors\b", r"\brealtors?\b",
        r"\breal estate\b", r"mortgage bankers", r"\bhome builders\b",
        r"\bapartment\b.*\bassociation\b", r"\bfannie mae\b",
        r"\bfreddie mac\b", r"\brealty\b", r"\bmultifamily housing\b",
        r"\bcommercial real estate\b", r"\bmanufactured housing\b",
    ]),

    # ===== AIRLINES =====
    ("AIRL", "Air Transport", [
        r"\bairlines\b", r"\bairways\b", r"\baviation\b",
        r"\bdelta air\b", r"\bunited airlines\b", r"american airlines",
        r"\bsouthwest airlines\b", r"\bjetblue\b", r"\bfedex\b",
        r"\bups\b.*\binc\b", r"united parcel", r"\bair transport\b",
        r"airline pilots", r"airports? council", r"\balaska airlines\b",
        r"\bspirit airlines\b", r"\bfrontier airlines\b",
    ]),

    # ===== TRANSPORTATION (rail, auto dealers, etc.) =====
    ("TRANSP", "Transportation (surface)", [
        r"\bassociation of american railroads\b",
        r"\bamerican association of railroads\b",
        r"\brailroad\b", r"\bunion pacific\b", r"\bcsx\b.*\btransportation",
        r"\bnorfolk southern\b", r"\bbnsf\b", r"\btrucking\b.*\bassociation\b",
        r"\bamerican trucking\b", r"\bshipbuilders\b", r"\bport authority\b",
        r"\bautomobile dealers\b", r"\bauto dealers association\b",
        r"\bnational automobile dealers\b", r"\benterprise holdings\b",
        r"\benterprise mobility\b", r"\bhertz\b.*\bcorp",
        r"\bmaritime\b", r"\bintermodal\b",
    ]),

    # ===== AUTOS / MANUFACTURING =====
    ("AUTO", "Automotive & Manufacturing", [
        r"\bgeneral motors\b", r"\bford motor\b", r"\bstellantis\b",
        r"\btoyota\b", r"\bhonda motor\b", r"\btesla\b", r"\brivian\b",
        r"\bnissan\b", r"\bvolkswagen\b", r"\balliance for automotive\b",
        r"\bauto\b.*\bmanufacturers\b", r"\bautomotive\b",
        r"\bnational association of manufacturers\b",
        r"\bcaterpillar\b.*\binc", r"\bdeere\b.*\bcompany",
        r"\bcummins\b.*\binc", r"\beaton\b.*\bcorp",
    ]),

    # ===== TELECOM =====
    ("TELECOM", "Telecom", [
        r"\bat&t\b", r"\bverizon\b", r"\bt-mobile\b", r"\btmobile\b",
        r"\bcomcast\b", r"\bcharter communications\b", r"\bcox\b.*\bcommunications\b",
        r"\btelecom\b", r"\bwireless\b.*\bassociation\b", r"\bctia\b",
        r"\bbroadband\b", r"\bcable\b.*\bassociation\b", r"\bncta\b",
        r"\bdish network\b", r"\bdirectv\b", r"\bsatellite\b.*\bindustry",
    ]),

    # ===== MEDIA =====
    ("MEDIA", "Media & Entertainment", [
        r"\bnational association of broadcasters\b", r"\bbroadcasters\b",
        r"\brecording industry association\b", r"\briaa\b",
        r"\bmotion picture\b.*\bassociation\b", r"\bmpaa\b",
        r"\bwalt disney\b", r"\bwarner bros\b", r"\bparamount\b.*\bglobal",
        r"\bnbcuniversal\b", r"\bnbc universal\b",
        r"\bfox corporation\b", r"\bfox news\b", r"\bnewspaper\b.*\bassociation\b",
        r"\bsony music\b", r"\bsony pictures\b",
        r"\buniversal music\b", r"\bpublishers?\b.*\bassociation\b",
    ]),

    # ===== AGRICULTURE & FOOD =====
    ("AGRI", "Agriculture & Food", [
        r"\bagriculture\b", r"\bfarm bureau\b", r"\bfarmers\b",
        r"\bcrop\b.*\binsurance\b", r"\btyson foods\b", r"\bcargill\b",
        r"\barcher daniels\b", r"\badm\b", r"\bconagra\b", r"\bpepsico\b",
        r"coca[- ]cola", r"\bnestle\b", r"\bkraft\b", r"\bgeneral mills\b",
        r"\bfood\b.*\bassociation\b", r"\bgrocery\b.*\bassociation\b",
        r"national restaurant", r"\bcorn growers\b", r"\bdairy\b.*\bassociation\b",
        r"\bsoybean\b", r"\bcattlemen\b", r"\bpork\b.*\bproducers\b",
        r"\bsugar\b.*\bassociation\b", r"\bpoultry\b", r"\bfisheries\b",
        r"\bseafood\b", r"\bmondelez\b", r"\bkellogg\b.*\bcompany",
    ]),

    # ===== RETAIL =====
    ("RETAIL", "Retail", [
        r"\bwalmart\b", r"\bcostco\b.*\bwholesale", r"\btarget corp",
        r"\bhome depot\b", r"\blowe'?s\b.*\bcompanies\b",
        r"\bbest buy\b", r"\bkroger\b", r"\bcvs health\b", r"\bwalgreens\b",
        r"\bretail federation\b", r"\bretail leaders\b",
        r"\bretail industry\b", r"\bautozone\b",
        r"\bnordstrom\b", r"\bdollar general\b", r"\bdollar tree\b",
    ]),

    # ===== LEGAL =====
    ("LEGAL", "Legal & Trial Lawyers", [
        r"\bamerican bar association\b",
        r"\bamerican association for justice\b",
        r"\btrial lawyers\b", r"\bassociation of trial lawyers\b",
        r"\blegal services corporation\b",
    ]),

    # ===== LABOR =====
    ("LABOR", "Labor Unions", [
        r"\bafl[- ]cio\b", r"\bseiu\b", r"\bteamsters\b",
        r"\bunited steelworkers\b", r"\bnea\b.*\beducation\b",
        r"\bamerican federation of\b", r"\bunion\b.*\baffiliated\b",
        r"\bunited auto workers\b", r"\buaw\b", r"\bunited food\b.*\bcommercial\b",
        r"\bafscme\b", r"\bboilermakers\b", r"\biron workers\b",
    ]),

    # ===== TOBACCO & ALCOHOL =====
    ("TOBALC", "Tobacco & Alcohol", [
        r"\baltria\b", r"\breynolds\b.*\btobacco\b", r"\bphilip morris\b",
        r"\btobacco\b", r"\bdistilled spirits\b", r"\bbeer institute\b",
        r"\bwine\b.*\binstitute\b", r"anheuser[- ]busch", r"\bmolson coors\b",
        r"\bcigar\b", r"\bvaping\b", r"\brai services\b",
        r"\bpmi us\b", r"\bpmi global\b", r"\bjuul\b",
        r"\bconstellation brands\b", r"\bdiageo\b",
    ]),

    # ===== FIREARMS =====
    ("FIREARMS", "Firearms Industry", [
        r"\bnational shooting sports\b", r"\bshooting sports foundation\b",
        r"\bnational rifle association\b", r"\bfirearms industry\b",
        r"\bsturm ruger\b", r"\bsmith\b.*\bwesson\b",
        r"\bammunition\b.*\bassociation\b",
    ]),

    # ===== EDUCATION =====
    ("EDUC", "Education", [
        r"\buniversity\b", r"\bcollege\b", r"\bschool\b.*\bdistrict\b",
        r"\bschools?\b.*\bassociation\b", r"\beducation\b.*\bassociation\b",
        r"\bstudent\b.*\baid\b", r"\bfor[- ]profit\b.*\beducation\b",
        r"\bhigher education\b", r"\bacademy\b", r"\binstitute of technology\b",
    ]),

    # ===== GOVT =====
    ("GOVT", "State & Local Governments", [
        r"\bcounty of\b", r"\bcity of\b", r"\btown of\b",
        r"\bstate of\b", r"\bcommonwealth of\b",
        r"national governors", r"national conference of state",
        r"\bmunicipal\b.*\bassociation\b", r"national association of counties",
        r"\btransit\b.*\bauthority\b",
    ]),

    # ===== NONPROFITS =====
    ("NONPROF", "Nonprofits & Charitable Advocacy", [
        r"\bamerican cancer society\b", r"\bamerican heart\b.*\bassociation\b",
        r"\bamerican diabetes\b.*\bassociation\b",
        r"\bamerican lung\b.*\bassociation\b",
        r"\bmarch of dimes\b", r"\bred cross\b", r"\bunited way\b",
        r"\bfeeding america\b", r"\bworld wildlife\b",
    ]),

    # ===== BIZ ORGS =====
    ("BIZORG", "Business Associations (Cross-industry)", [
        r"chamber of commerce", r"\bbusiness roundtable\b",
        r"\bnational federation of independent business\b", r"\bnfib\b",
        r"\bsmall business\b.*\bassociation\b", r"\bsmall business council\b",
    ]),

    # ===== IDEOLOGICAL =====
    ("IDEO", "Ideological / Single-issue Advocacy", [
        r"\baarp\b", r"\baclu\b", r"\bsierra club\b", r"\bnatural resources defense\b",
        r"\bnrdc\b", r"\benvironmental defense fund\b", r"\bhuman rights\b",
        r"\bplanned parenthood\b", r"\bheritage foundation\b",
        r"\bcato institute\b", r"\bbrookings\b",
        r"\bopen society\b", r"\bfriends committee\b",
        r"\bfreedomworks\b", r"\bfederalist society\b",
        r"\bamericans for tax reform\b", r"\bamericans for prosperity\b",
        r"\bnaacp\b", r"\bcommon cause\b", r"\bpublic citizen\b",
    ]),

    # ===== CATCH-ALL BUCKETS =====
    ("HEALTH_OTHER", "Health (other)", [
        r"\bhealth\b", r"\bmedical\b", r"\bdental\b", r"\bnursing\b",
        r"\bphysician\b", r"\bamerican medical association\b",
        r"\bsurgeons?\b", r"\bhospice\b", r"\btelemedicine\b",
    ]),
    ("ENERGY_OTHER", "Energy (other)", [
        r"\benergy\b", r"\bnuclear\b", r"\bcoal\b",
    ]),
]

EXTRA_INDUSTRIES = [
    ("AUTO",         "Automotive & Manufacturing",             "Manufacturing"),
    ("AGRI",         "Agriculture & Food",                     "Agriculture"),
    ("LABOR",        "Labor Unions",                           "Labor"),
    ("TOBALC",       "Tobacco & Alcohol",                      "Consumer Goods"),
    ("BIZORG",       "Business Associations (Cross-industry)", "Cross-industry"),
    ("IDEO",         "Ideological / Single-issue Advocacy",    "Ideological"),
    ("HEALTH_OTHER", "Health (other)",                         "Health"),
    ("ENERGY_OTHER", "Energy (other)",                         "Energy"),
    ("CHEM",         "Chemicals & Materials",                  "Manufacturing"),
    ("CRYPTO",       "Crypto & Blockchain",                    "Finance"),
    ("MEDIA",        "Media & Entertainment",                  "Media"),
    ("RETAIL",       "Retail",                                 "Consumer"),
    ("LEGAL",        "Legal & Trial Lawyers",                  "Legal"),
    ("NONPROF",      "Nonprofits & Charitable Advocacy",       "Nonprofit"),
    ("TRANSP",       "Transportation (surface)",               "Transportation"),
    ("FIREARMS",     "Firearms Industry",                      "Ideological"),
]


def compile_rules():
    return [(code, label, re.compile("|".join(pats), flags=re.IGNORECASE))
            for code, label, pats in RULES]


def classify(name, description, rules):
    if not name:
        return None
    blob = f"{name} {description or ''}".lower()
    for code, label, regex in rules:
        if regex.search(blob):
            return code, label
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if "DATABASE_URL" not in os.environ:
        print("error: DATABASE_URL not set", file=sys.stderr)
        sys.exit(2)

    read_conn  = psycopg2.connect(os.environ["DATABASE_URL"])
    write_conn = psycopg2.connect(os.environ["DATABASE_URL"])

    with write_conn.cursor() as cur:
        for code, label, sector in EXTRA_INDUSTRIES:
            cur.execute(
                "INSERT INTO industries (code, label, sector) VALUES (%s, %s, %s) "
                "ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, sector = EXCLUDED.sector;",
                (code, label, sector),
            )
        if args.reset and not args.dry_run:
            cur.execute("UPDATE clients SET industry_code = NULL, industry_label = NULL;")
        write_conn.commit()

    rules = compile_rules()
    print("Loading clients...", flush=True)
    with read_conn.cursor() as rcur:
        if args.reset:
            rcur.execute("SELECT id, name, general_description FROM clients;")
        else:
            rcur.execute(
                "SELECT id, name, general_description FROM clients "
                "WHERE industry_code IS NULL;"
            )
        rows = rcur.fetchall()
    print(f"  loaded {len(rows):,} clients to classify", flush=True)

    n_total = n_classified = 0
    by_code: dict[str, int] = {}
    batch: list[tuple[str, str, int]] = []

    with write_conn.cursor() as wcur:
        for client_id, name, description in rows:
            n_total += 1
            result = classify(name, description, rules)
            if result:
                code, label = result
                n_classified += 1
                by_code[code] = by_code.get(code, 0) + 1
                batch.append((code, label, client_id))

            if len(batch) >= 1000:
                if not args.dry_run:
                    wcur.executemany(
                        "UPDATE clients SET industry_code = %s, industry_label = %s WHERE id = %s;",
                        batch,
                    )
                    write_conn.commit()
                print(f"  processed {n_total:,} / {len(rows):,}  classified={n_classified:,}", flush=True)
                batch.clear()

        if batch and not args.dry_run:
            wcur.executemany(
                "UPDATE clients SET industry_code = %s, industry_label = %s WHERE id = %s;",
                batch,
            )
            write_conn.commit()

    print(f"\n=== Classification summary ===")
    print(f"Total processed: {n_total:,}")
    if n_total > 0:
        pct = n_classified / n_total * 100
        print(f"Classified:      {n_classified:,}  ({pct:.1f}%)")
        print(f"Still unclassif: {n_total - n_classified:,}")
    print("\nBy industry (this run only):")
    for code, count in sorted(by_code.items(), key=lambda x: -x[1]):
        print(f"  {code:16s}  {count:>6,}")

    if not args.dry_run:
        print("\nRefreshing materialized views...")
        with write_conn.cursor() as cur:
            cur.execute("SELECT refresh_dashboard_views();")
            write_conn.commit()
        print("Done.")

    read_conn.close()
    write_conn.close()


if __name__ == "__main__":
    main()
