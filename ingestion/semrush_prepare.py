"""
semrush_prepare.py
------------------
Cleans raw SEMrush CSVs and produces prepared files ready for BigQuery ingestion.

Input  : data/raw/organic_keywords_<domain>.csv
         data/raw/paid_keywords_<domain>.csv
Output : data/prepared/semrush_keywords_clean.csv
         data/prepared/semrush_clean_<domain>.csv  (one per domain)

Run from project root:
    python ingestion/semrush_prepare.py

Logic
-----
Two columns are added to every row:

1. page_path
   The URL path component of the SEMrush `Url` column, with scheme and host
   stripped, lowercased, and trailing slash removed.
   This is the join key used in the BigQuery star schema to link SEMrush
   keyword data to GA4 page-level session data.

   SEMrush tracks merch.google while GA4 logs googlemerchandisestore.com —
   different hostnames for the same store. Path-level joining resolves this.

   Expression used in BigQuery staging (for reference):
       RTRIM(REGEXP_EXTRACT(LOWER(TRIM(url)), r'https?://[^/]+(/[^?#]*)'), '/')

2. join_category
   A normalised category string derived from page_path using regex rules.
   This bridges the structural URL difference between SEMrush and GA4:
   individual product pages (/product/... and legacy /*.html) have no
   GA4 counterpart in the public dataset and are flagged as None.
   Category-level paths (apparel, lifestyle, brands, etc.) resolve cleanly.

   join_category = None means the row is kept for competitive keyword
   analysis but will not join to GA4 session data.

3. joinable_to_ga4
   Boolean. True only for merch.google rows where join_category is not None.
   Competitors are never joinable to GA4 — they are included for benchmarking.

4. snapshot_date
   ISO date string of when the script is run. SEMrush API responses carry
   no native date column; this timestamp is essential for tracking changes
   over time in BigQuery.
"""

import os
import re
import datetime
import logging
import pandas as pd
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR      = os.path.join(PROJECT_ROOT, "data", "raw")
PREPARED_DIR = os.path.join(PROJECT_ROOT, "data", "prepared")

os.makedirs(PREPARED_DIR, exist_ok=True)

# ── Domain config ─────────────────────────────────────────────────────────────
DOMAINS = {
    "merch_google":  "merch.google",
    "cafepress_com": "cafepress.com",
    "redbubble_com": "redbubble.com",
    "zazzle_com":    "zazzle.com",
}

# ── Category rules ────────────────────────────────────────────────────────────
# Applied in order — first match wins.
# Derived from exploratory analysis in notebooks/03_url_alias_mapping.ipynb.
# Each tuple is (regex_pattern, join_category).
#
# Rules cover only merch.google paths — competitors are never joinable to GA4.
# Pattern matches are case-insensitive and tested against the page_path string.

CATEGORY_RULES: list[tuple[str, str]] = [
    # ── Apparel ───────────────────────────────────────────────────────────────
    (r"hoodie|sweatshirt|sweats|^/apparel/apparel/hoodies|^/shop/apparel/hoodies",  "apparel/hoodies"),
    (r"jacket|gilet|^/apparel/apparel/jackets",                                      "apparel/jackets"),
    (r"^/apparel/mens|^/shop/apparel/mens",                                          "apparel/mens"),
    (r"^/apparel/hats|^/shop/apparel/head|hat|cap(?!e)",                            "apparel/headgear"),
    (r"^/apparel/accessories/bags/backpack|backpack",                                "apparel/bags/backpacks"),
    (r"^/apparel/accessories/bags|^/shop/lifestyle/bags",                            "apparel/bags"),
    (r"^/apparel/misc/socks|^/shop/apparel/socks|socks",                            "apparel/socks"),
    (r"^/shop/apparel/accessories",                                                   "apparel/accessories"),
    (r"^/apparel(?:/|$)|^/shop/apparel(?:/|$)",                                      "apparel"),

    # ── Lifestyle ─────────────────────────────────────────────────────────────
    (r"drinkware|bottle|mug|tumbler",                                                "lifestyle/drinkware"),
    (r"blanket|towel|^/lifestyle/home",                                              "lifestyle/home"),
    (r"^/lifestyle/accessories/lanyards|lanyard",                                    "lifestyle/accessories"),
    (r"^/shop/lifestyle|^/lifestyle",                                                "lifestyle"),

    # ── Brands ────────────────────────────────────────────────────────────────
    (r"^/shop/shop-by-brand/youtube|^/brands/youtube|youtube",                      "brands/youtube"),
    (r"^/shop/shop-by-brand/android|^/brands/android|android",                     "brands/android"),
    (r"^/brands/google/google-cloud|google.cloud",                                  "brands/google-cloud"),
    (r"^/brands/gemini|gemini",                                                      "brands/gemini"),
    (r"^/brands",                                                                    "brands"),

    # ── Collections ───────────────────────────────────────────────────────────
    (r"^/shop/collections/chrome-dino|chrome.dino",                                 "collections/chrome-dino"),
    (r"^/shop/collections/emoji|emoji",                                              "collections/emoji"),
    (r"^/shop/collections/google-bike|google.bike|model.bike",                      "collections/google-bike"),
    (r"^/shop/collections/campus|campus",                                            "collections/campus"),
    (r"^/collections/super-g|super.g",                                               "collections/super-g"),
    (r"^/shop/new(?:/|$)",                                                            "new-arrivals"),

    # ── Stationery ────────────────────────────────────────────────────────────
    (r"sticker|magnet|pin|patch|pen|notebook|keychain|keyring|^/shop/stationery",   "stationery"),

    # ── Checkout / Search ─────────────────────────────────────────────────────
    (r"^/checkout",                                                                   "checkout"),
    (r"^/catalogsearch",                                                              "search"),

    # ── Homepage ──────────────────────────────────────────────────────────────
    (r"^/$",                                                                          "homepage"),
]


# ── Helper functions ──────────────────────────────────────────────────────────

def load_csv(filepath: str) -> pd.DataFrame:
    """
    Load a SEMrush CSV file with semicolon separation.

    Args:
        filepath (str): Full path to CSV file

    Returns:
        pd.DataFrame: Parsed CSV as DataFrame

    Raises:
        FileNotFoundError: If filepath does not exist
        pd.errors.ParserError: If CSV is malformed

    Note:
        SEMrush exports use semicolon (;) as delimiter and UTF-8 encoding.
        on_bad_lines='skip' tolerates occasional malformed rows from SEMrush API.
    """
    return pd.read_csv(filepath, sep=";", encoding="utf-8", on_bad_lines="skip")


def extract_path(url: str) -> str:
    """
    Extract the path component from a URL, normalized for comparison.

    Args:
        url (str): Full URL (e.g., "https://merch.google/shop/apparel?utm=...")

    Returns:
        str: Extracted path, lowercased, trailing slash removed
             Root path "/" is returned as "/" (not empty string)
             Malformed URLs return "/"

    Examples:
        https://merch.google/shop/apparel/hoodies       → /shop/apparel/hoodies
        https://merch.google/shop/apparel/hoodies/      → /shop/apparel/hoodies
        https://merch.google/                           → /
        https://merch.google                            → /
        malformed_url                                   → /

    Note:
        This normalisation matches the path extraction logic in
        stg_ga4_sessions.sql to ensure GA4 and SEMrush paths are comparable.
    """
    try:
        path = urlparse(str(url).lower().strip()).path.rstrip("/")
        return path if path else "/"
    except Exception:
        return "/"


def assign_category(path: str) -> str | None:
    """
    Map a page_path to a product category using regex rules.

    Args:
        path (str): Extracted and normalized page path (e.g., "/shop/apparel/hoodies")

    Returns:
        str | None: Category label if matched (e.g., "apparel/hoodies"), or None if unmatched

    Behavior:
        - Rules are applied in order; first match wins (see CATEGORY_RULES)
        - Product pages (/product/*) return None (intentionally, as they don't exist in GA4)
        - Legacy Magento pages (*.html with long slug) return None (also GA4-absent)
        - Unmatched paths return None (kept in output for keyword analysis, but not joinable to GA4)

    Return Value Semantics:
        None means: "This page exists in SEMrush but won't join to GA4 session data."
        This could be a product page, legacy page, or unrecognised category.
        These rows are kept for keyword analysis but flagged as non-joinable.

    Cross-Reference:
        The same category rules are implemented in SQL as REGEXP_CONTAINS conditions
        in stg_ga4_sessions.sql (first_page_categorised CTE). Keep them in sync.
    """
    if not path:
        return None

    # Individual product pages are never present in the GA4 public dataset.
    # Examples: /product/123456, /product/hoodie-blue
    # These are excluded to prevent orphaned product-level data.
    if re.match(r"^/product/", path):
        return None

    # Legacy Magento single-product URLs (from the old merchandisestore.com site).
    # Identified by: directory path + long alphanumeric slug + .html extension.
    # Examples: /apparel/google-zip-hoodie.html, /lifestyle/blue-water-bottle.html
    # Pattern: must have at least 4 alphanumeric characters before .html
    if re.search(r"-[a-z0-9]{4,}\.html$", path):
        return None

    # Apply category rules in order; first match wins.
    # All rules are case-insensitive (ignoring leading slashes and caps in category names).
    for pattern, category in CATEGORY_RULES:
        if re.search(pattern, path, re.IGNORECASE):
            return category

    return None


# ── Main pipeline ─────────────────────────────────────────────────────────────

def load_all_domains() -> pd.DataFrame:
    """
    Load all SEMrush CSV files (organic + paid for all domains) and concatenate.

    Returns:
        pd.DataFrame: Combined DataFrame with all domain data, columns:
            - domain (str): e.g., "merch.google", "cafepress.com"
            - source (str): "organic" or "paid"
            - All SEMrush CSV columns (Keyword, Position, Search Volume, CPC, etc.)

    Raises:
        FileNotFoundError: If no SEMrush CSV files are found in RAW_DIR

    Process:
        1. For each domain in DOMAINS:
           - Look for organic_keywords_{domain_key}.csv
           - Look for paid_keywords_{domain_key}.csv
        2. Add domain and source columns to each DataFrame
        3. Log row counts per file
        4. Concatenate all DataFrames

    Note:
        Files that don't exist are skipped with a warning (e.g., if backlinks were
        skipped in semrush_api_pull.py). If NO files exist, raises FileNotFoundError.
    """
    frames = []

    for domain_key, domain_label in DOMAINS.items():
        for source in ("organic", "paid"):
            filename = f"{source}_keywords_{domain_key}.csv"
            filepath = os.path.join(RAW_DIR, filename)

            if not os.path.exists(filepath):
                log.warning("File not found, skipping: %s", filepath)
                continue

            df = load_csv(filepath)
            df["domain"] = domain_label
            df["source"] = source
            frames.append(df)
            log.info("Loaded %-45s  %d rows", filename, len(df))

    if not frames:
        raise FileNotFoundError(
            f"No SEMrush CSV files found in {RAW_DIR}. "
            "Run semrush_api.py first."
        )

    return pd.concat(frames, ignore_index=True)


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning and enrichment transformations to raw SEMrush data.

    Args:
        df (pd.DataFrame): Raw concatenated SEMrush CSV data

    Returns:
        pd.DataFrame: Enriched DataFrame with four new columns added, reordered for clarity

    New Columns Added:
        1. page_path (str)
           - Extracted from the Url column using extract_path()
           - Normalised URL path matching stg_ga4_sessions logic
           - Used as the join key to GA4 sessions

        2. join_category (str | None)
           - Category assigned via assign_category(page_path)
           - Bridges structural differences between GA4 and SEMrush
           - None = product page or unmatched (not joinable to GA4)

        3. joinable_to_ga4 (bool)
           - True only for merch.google rows where join_category is not None
           - Competitors are never joinable (included only for benchmarking)
           - Used for filtering in downstream analysis

        4. snapshot_date (str)
           - ISO date string of when the script runs (YYYY-MM-DD)
           - Essential for tracking keyword data changes over time
           - All rows from a single run share the same snapshot_date

    Column Reordering:
        Priority columns (domain, source, metrics) listed first for readability.
        Extra columns from SEMrush API appended at the end.
    """
    snapshot_date = datetime.date.today().isoformat()

    log.info("Extracting page_path from Url column...")
    df["page_path"] = df["Url"].apply(extract_path)

    log.info("Assigning join_category...")
    df["join_category"] = df["page_path"].apply(assign_category)

    log.info("Computing joinable_to_ga4 flag...")
    # Only merch.google rows with a valid category are joinable.
    # Competitors are never joinable, even if a category matches.
    df["joinable_to_ga4"] = (
        (df["domain"] == "merch.google") &
        (df["join_category"].notna())
    )

    log.info("Adding snapshot_date: %s", snapshot_date)
    df["snapshot_date"] = snapshot_date

    # Reorder columns — metadata first, then SEMrush fields, then derived cols
    priority_cols = [
        "domain", "source", "snapshot_date",
        "Keyword", "Position", "Search Volume", "CPC", "Traffic (%)",
        "Competition", "Url", "page_path", "join_category", "joinable_to_ga4",
    ]
    extra_cols = [c for c in df.columns if c not in priority_cols]
    df = df[[c for c in priority_cols if c in df.columns] + extra_cols]

    return df


def save_outputs(df: pd.DataFrame) -> None:
    """
    Write prepared data to CSV files (combined + per-domain splits).

    Args:
        df (pd.DataFrame): Prepared SEMrush data with enrichment columns

    Outputs:
        1. data/prepared/semrush_keywords_clean.csv
           - Combined file with all domains and sources
           - Used as input to semrush_to_bq.py

        2. data/prepared/semrush_clean_{domain_key}.csv (one per domain)
           - Split per domain for flexibility
           - Useful for per-domain analysis or testing
           - Includes joinable_to_ga4 percentage in log output

    Side Effects:
        - Creates files in PREPARED_DIR with UTF-8 encoding
        - Logs row counts and joinability percentages
    """

    # Combined file (input to semrush_to_bq.py)
    combined_path = os.path.join(PREPARED_DIR, "semrush_keywords_clean.csv")
    df.to_csv(combined_path, index=False, encoding="utf-8")
    log.info("Saved combined file → %s  (%d rows)", combined_path, len(df))

    # Per-domain files for granular analysis
    for domain_label in df["domain"].unique():
        domain_key   = domain_label.replace(".", "_").replace(" ", "_")
        domain_df    = df[df["domain"] == domain_label]
        domain_path  = os.path.join(PREPARED_DIR, f"semrush_clean_{domain_key}.csv")
        joinable_pct = domain_df["joinable_to_ga4"].mean()

        domain_df.to_csv(domain_path, index=False, encoding="utf-8")
        log.info(
            "  %-25s  %4d rows  |  %.0f%% joinable to GA4  →  %s",
            domain_label, len(domain_df), joinable_pct * 100, domain_path,
        )


def log_summary(df: pd.DataFrame) -> None:
    """
    Print a human-readable summary of the prepared dataset to the log.

    Args:
        df (pd.DataFrame): Prepared data with joinable_to_ga4 flag

    Output:
        Summary section with:
        - Total rows processed
        - Joinable to GA4 (count + %)
        - Product pages excluded (count + %)
        - Other non-joinable rows (count + %)
        - Per-category breakdown for merch.google (the target domain)

    Note:
        Provides quick validation that data preparation succeeded as expected.
    """
    total      = len(df)
    joinable   = df["joinable_to_ga4"].sum()
    product    = df["page_path"].str.startswith("/product/", na=False).sum()
    no_match   = df["join_category"].isna().sum() - product

    log.info("─" * 60)
    log.info("SUMMARY")
    log.info("  Total rows          : %d", total)
    log.info("  Joinable to GA4     : %d  (%.1f%%)", joinable, joinable / total * 100)
    log.info("  Product pages (skip): %d  (%.1f%%)", product, product / total * 100)
    log.info("  Other non-joinable  : %d  (%.1f%%)", no_match, no_match / total * 100)
    log.info("─" * 60)

    log.info("Category breakdown (merch.google):")
    merch = df[df["domain"] == "merch.google"]
    counts = (
        merch["join_category"]
        .fillna("None (not joinable)")
        .value_counts()
    )
    for category, count in counts.items():
        log.info("  %-40s  %d", category, count)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Main entry point: orchestrate SEMrush data preparation pipeline.

    Workflow:
        1. Load all raw SEMrush CSVs (organic + paid for all domains)
        2. Extract and normalise page paths from URLs
        3. Assign categories using regex rules (with intent flags for GA4 joinability)
        4. Save combined + per-domain CSVs to data/prepared/
        5. Print summary statistics (total rows, joinability %, category breakdown)

    Prerequisites:
        - data/raw/ directory must contain organic_keywords_*.csv and paid_keywords_*.csv
          files exported by semrush_api_pull.py

    Output:
        - data/prepared/semrush_keywords_clean.csv (input to semrush_to_bq.py)
        - data/prepared/semrush_clean_{domain}.csv (per-domain analysis files)
    """
    log.info("Starting SEMrush data preparation...")
    log.info("Raw dir     : %s", RAW_DIR)
    log.info("Prepared dir: %s", PREPARED_DIR)

    raw     = load_all_domains()
    clean   = prepare(raw)
    save_outputs(clean)
    log_summary(clean)

    log.info("Done. Run semrush_to_bq.py to load into BigQuery.")


if __name__ == "__main__":
    main()