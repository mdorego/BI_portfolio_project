"""
SEMrush API Bulk Export Tool

Purpose:
    Pulls SEMrush data (organic keywords, domain ranks, backlinks, paid keywords)
    for a target domain (merch.google) and 3 competitors via the SEMrush REST API.

    **Important**: Run this BEFORE cancelling your SEMrush trial. The script is
    designed to safely extract maximum data within the 50,000 unit monthly limit.

Usage:
    pip install requests
    python ingestion/semrush_api_pull.py --key YOUR_API_KEY_HERE

    Optional flags:
      --skip-backlinks   Skip backlinks extraction to save API units

Input:
    Command-line arguments:
      --key (required)              Your SEMrush API key
      --skip-backlinks (optional)   Skip backlinks to save units

Output:
    CSV files in ./output/:
      - organic_keywords_{domain}.csv (500 rows per domain)
      - domain_ranks_{domain}.csv     (1 row per domain — aggregate metrics)
      - backlinks_{domain}.csv        (500 rows per domain, if not skipped)
      - paid_keywords_{domain}.csv    (500 rows per domain)
      - keyword_difficulty_top_keywords.csv (bulk difficulty lookup)

API Unit Budget:
    domain_organic  500 rows x 4 domains x 10 units = 20,000 units
    domain_ranks    1 row    x 4 domains x 10 units =    400 units  (negligible)
    backlinks       500 rows x 4 domains x  1 unit  =  2,000 units
    paid keywords   500 rows x 4 domains x 10 units = 20,000 units
    ───────────────────────────────────────────────────────────────
    Total estimate: ~42,000 units  (safely within 50,000 monthly limit)

Domains Covered:
    - merch.google    (target store)
    - cafepress.com   (competitor)
    - redbubble.com   (competitor)
    - zazzle.com      (competitor)

Database: US (aligns with GA4 demo dataset geography)
"""

import argparse
import os
import time
import requests

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_URL = "https://api.semrush.com/"

DOMAINS = [
    "merch.google",
    "cafepress.com",
    "redbubble.com",
    "zazzle.com",
]

DATABASE = "us"  # US database — aligns with GA4 demo dataset


def fetch(params: dict, label: str) -> str | None:
    """
    Make a single SEMrush API request and save response to CSV.

    Args:
        params (dict): Query parameters for the API request (type, key, domain, etc.)
        label (str):   Filename prefix (e.g., "organic_keywords_merch_google")

    Returns:
        str | None: Full filepath to saved CSV on success; None on error

    Raises:
        None (prints errors instead of raising; returns None on failure)

    Note:
        SEMrush returns errors as plain text (not HTTP 4xx/5xx codes), so we check
        both HTTP status and response body for error strings like "ERROR 10" (invalid key).
    """
    print(f"  → Fetching: {label}")
    r = requests.get(BASE_URL, params=params, timeout=30)

    # Check HTTP status code
    if r.status_code != 200:
        print(f"    ERROR {r.status_code}: {r.text[:200]}")
        return None

    # SEMrush returns an error string (not HTTP error) for bad requests
    # Examples: "ERROR 10" (invalid key), "ERROR 50" (invalid domain)
    if r.text.startswith("ERROR"):
        print(f"    SEMrush error: {r.text[:200]}")
        return None

    filepath = os.path.join(OUTPUT_DIR, f"{label}.csv")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(r.text)

    lines = r.text.strip().count("\n")
    print(f"    ✓ {lines} rows saved → {filepath}")
    return filepath


def check_balance(api_key: str):
    """
    Query and print remaining SEMrush API units.

    Args:
        api_key (str): Your SEMrush API key

    Raises:
        None (prints results to stdout)

    Note:
        This call is free and costs 0 units. Run this before and after the full
        export to verify you have enough balance for all requested data.
    """
    r = requests.get(BASE_URL, params={"type": "api_units", "key": api_key}, timeout=10)
    print(f"\nAPI units remaining: {r.text.strip()}\n")


def pull_organic_keywords(api_key: str, domain: str, limit: int = 500):
    """
    Extract organic search keywords ranking for a domain.

    Args:
        api_key (str):      Your SEMrush API key
        domain (str):       Target domain (e.g., "merch.google")
        limit (int):        Max rows to return (default: 500, max: 10000 but slow)

    Returns:
        None (calls fetch() which saves to CSV)

    Columns Extracted:
        Ph (Keyword), Po (Position), Pp (Position prev month), Pd (Position diff),
        Nq (Search Volume), Cp (CPC), Ur (URL), Tr (Traffic %), Tc (Traffic cost),
        Co (Competition), Nr (Results), Td (Trend)

    Cost: 10 units per request
    """
    fetch(
        {
            "type": "domain_organic",
            "key": api_key,
            "domain": domain,
            "database": DATABASE,
            "display_limit": limit,
            "display_sort": "tr_desc",  # sort by traffic descending
            "export_columns": "Ph,Po,Pp,Pd,Nq,Cp,Ur,Tr,Tc,Co,Nr,Td",
            # Ph=keyword, Po=position, Pp=position prev month, Pd=position diff,
            # Nq=search volume, Cp=CPC, Ur=URL, Tr=traffic %, Tc=traffic cost,
            # Co=competition, Nr=results, Td=trend
        },
        label=f"organic_keywords_{domain.replace('.', '_')}",
    )


def pull_domain_ranks(api_key: str, domain: str):
    """
    Extract domain-level authority metrics (rank, keywords, traffic estimates).

    Args:
        api_key (str): Your SEMrush API key
        domain (str):  Target domain

    Returns:
        None (calls fetch() which saves to CSV)

    Columns Extracted:
        Dn (Domain), Rk (Rank), Or (Organic Keywords), Ot (Organic Traffic),
        Oc (Organic Cost), Ad (Paid Keywords), At (Paid Traffic),
        Ac (Paid Cost), Sh (Social), Sv (Display Ads)

    Cost: 10 units per request
    Typical Output: 1 row (one summary row per domain)
    """
    fetch(
        {
            "type": "domain_ranks",
            "key": api_key,
            "domain": domain,
            "database": DATABASE,
            "export_columns": "Dn,Rk,Or,Ot,Oc,Ad,At,Ac,Sh,Sv",
            # Dn=domain, Rk=rank, Or=organic keywords, Ot=organic traffic,
            # Oc=organic cost, Ad=paid keywords, At=paid traffic,
            # Ac=paid cost, Sh=social, Sv=display ads
        },
        label=f"domain_ranks_{domain.replace('.', '_')}",
    )


def pull_backlinks(api_key: str, domain: str, limit: int = 500):
    """
    Extract backlink profile (inbound links) for a domain.

    Args:
        api_key (str): Your SEMrush API key
        domain (str):  Target domain
        limit (int):   Max rows to return (default: 500)

    Returns:
        None (calls fetch() which saves to CSV)

    Columns Extracted:
        source_url, source_title, target_url, anchor, domain_score,
        page_score, first_seen, last_seen

    Cost: 1 unit per request (cheapest endpoint)
    Use Case: Competitive benchmarking; understanding link sources
    """
    fetch(
        {
            "type": "backlinks",
            "key": api_key,
            "target": domain,
            "target_type": "root_domain",
            "display_limit": limit,
            "export_columns": "source_url,source_title,target_url,anchor,domain_score,page_score,first_seen,last_seen",
        },
        label=f"backlinks_{domain.replace('.', '_')}",
    )


def pull_paid_keywords(api_key: str, domain: str, limit: int = 500):
    """
    Extract paid search (Google Ads) keywords and ad copy for a domain.

    Args:
        api_key (str): Your SEMrush API key
        domain (str):  Target domain
        limit (int):   Max rows to return (default: 500)

    Returns:
        None (calls fetch() which saves to CSV)

    Columns Extracted:
        Ph (Keyword), Po (Ad Position), Nq (Search Volume), Cp (CPC),
        Tr (Traffic %), Tc (Traffic Cost), Co (Competition),
        Ur (Landing URL), Tt (Ad Title), Ds (Description)

    Cost: 10 units per request
    Use Case: Competitive SEM analysis; identifying high-value paid terms
    """
    fetch(
        {
            "type": "domain_adwords",
            "key": api_key,
            "domain": domain,
            "database": DATABASE,
            "display_limit": limit,
            "display_sort": "tr_desc",
            "export_columns": "Ph,Po,Nq,Cp,Tr,Tc,Co,Ur,Tt,Ds",
            # Ph=keyword, Po=position, Nq=volume, Cp=CPC, Tr=traffic %,
            # Tc=traffic cost, Co=competition, Ur=URL, Tt=title, Ds=description
        },
        label=f"paid_keywords_{domain.replace('.', '_')}",
    )


def pull_keyword_difficulty(api_key: str, keywords: list[str]):
    """
    Bulk lookup keyword difficulty and volume for a list of known keywords.

    Args:
        api_key (str):      Your SEMrush API key
        keywords (list):    List of keyword strings to look up (semicolon-joined in API call)

    Returns:
        None (calls fetch() which saves to CSV)

    Columns Extracted:
        Ph (Keyword), Nq (Search Volume), Cp (CPC), Co (Competition),
        Nr (Results), Td (Trend), Kd (Keyword Difficulty 0-100)

    Cost: 10 units per request
    Use Case: Enriching known keywords with difficulty metrics for prioritization
    """
    kw_string = ";".join(keywords)
    fetch(
        {
            "type": "phrase_these",
            "key": api_key,
            "phrase": kw_string,
            "database": DATABASE,
            "export_columns": "Ph,Nq,Cp,Co,Nr,Td,Kd",
            # Kd = keyword difficulty (0-100)
        },
        label="keyword_difficulty_top_keywords",
    )


def main():
    """
    Main entry point: orchestrate data extraction from SEMrush API.

    Workflow:
        1. Parse command-line arguments (--key, optional --skip-backlinks)
        2. Check API unit balance before and after extraction
        3. Extract organic keywords for all domains (20,000 units)
        4. Extract domain authority summaries (400 units)
        5. Extract backlinks if not skipped (2,000 units)
        6. Extract paid keywords (20,000 units)
        7. Extract keyword difficulty for known terms (negligible units)

    Error Handling:
        - If any API call fails, the script logs the error and continues with the next
        - Use --skip-backlinks to conserve units if approaching the monthly limit

    Output:
        All CSV files written to ./output/ directory (created automatically)
    """
    parser = argparse.ArgumentParser(description="SEMrush bulk export")
    parser.add_argument("--key", required=True, help="Your SEMrush API key")
    parser.add_argument(
        "--skip-backlinks",
        action="store_true",
        help="Skip backlinks to save units",
    )
    args = parser.parse_args()

    api_key = args.key

    check_balance(api_key)

    # ── Keywords from your SEMrush CSV exports (to enrich with difficulty) ──
    # These are pre-identified high-value keywords for merch.google to track
    known_keywords = [
        "plush google",
        "google merchandise store",
        "google merch store",
        "google merch",
        "google store",
        "google plush",
        "google swag",
        "merch google",
    ]

    print("=" * 60)
    print("PULLING ORGANIC KEYWORDS")
    print("=" * 60)
    for domain in DOMAINS:
        pull_organic_keywords(api_key, domain)
        time.sleep(0.5)  # stay well under 10 req/sec rate limit

    print("\n" + "=" * 60)
    print("PULLING DOMAIN RANKS (summary metrics)")
    print("=" * 60)
    for domain in DOMAINS:
        pull_domain_ranks(api_key, domain)
        time.sleep(0.5)

    if not args.skip_backlinks:
        print("\n" + "=" * 60)
        print("PULLING BACKLINKS")
        print("=" * 60)
        for domain in DOMAINS:
            pull_backlinks(api_key, domain)
            time.sleep(0.5)

    print("\n" + "=" * 60)
    print("PULLING PAID KEYWORDS")
    print("=" * 60)
    for domain in DOMAINS:
        pull_paid_keywords(api_key, domain)
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("PULLING KEYWORD DIFFICULTY for known keywords")
    print("=" * 60)
    pull_keyword_difficulty(api_key, known_keywords)

    check_balance(api_key)

    print("\n✓ All done. Files saved to ./output/")
    print("You can now cancel your SEMrush trial.")


if __name__ == "__main__":
    main()