"""
run_staging.py
--------------
Builds the staging layer from raw sources.

  Step 1 — staging.ga4_sessions
    Executes transform/sql/staging/stg_ga4_sessions.sql against the GA4 public
    dataset and writes the result to staging.ga4_sessions (WRITE_TRUNCATE).
    Prints row count, date range, and sessions-with-purchases after loading.

  Step 2 — staging.domain_ranks
    Reads all data/raw/domain_ranks_*.csv files and loads them into
    staging.domain_ranks (WRITE_TRUNCATE). This table is required by
    marts.dim_competitor. The raw CSV columns are renamed to snake_case
    during the pandas load step.

Both steps are idempotent — re-running replaces the target tables cleanly.

Prerequisites:
    ingestion/semrush_to_bq.py must have run (staging.semrush_keywords needed
    by mart transforms, not by this script directly).

Run from project root:
    python transform/run/run_staging.py
"""

import os
import pickle
import logging
import datetime
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.api_core.exceptions import NotFound
from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
TRANSFORM_DIR = os.path.dirname(SCRIPT_DIR)
PROJECT_ROOT  = os.path.dirname(TRANSFORM_DIR)
SQL_DIR       = os.path.join(TRANSFORM_DIR, "sql", "staging")
RAW_DIR       = os.path.join(PROJECT_ROOT, "data", "raw")

CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials", "credentials.json")
TOKEN_PATH       = os.path.join(PROJECT_ROOT, "credentials", "token.pickle")

# ── BigQuery config ───────────────────────────────────────────────────────────
GCP_PROJECT     = "bi-portfolio-project"
STAGING_DATASET = "staging"
SCOPES          = ["https://www.googleapis.com/auth/bigquery"]

# ── Domain ranks column mapping ──────────────────────────────────────────────
# Maps raw SEMrush domain_ranks CSV headers to clean snake_case BQ column names.
DOMAIN_RANKS_COLUMNS = {
    "Domain":           "domain",
    "Rank":             "semrush_rank",
    "Organic Keywords": "organic_keywords",
    "Organic Traffic":  "organic_traffic",
    "Organic Cost":     "organic_cost",
    "Adwords Keywords": "paid_keywords",
    "Adwords Traffic":  "paid_traffic",
    "Adwords Cost":     "paid_cost",
    "PLA keywords":     "pla_keywords",
    "PLA uniques":      "pla_uniques",
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_credentials():
    """Load cached OAuth2 credentials, refreshing or re-running the flow as needed."""
    creds = None

    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "rb") as fh:
            creds = pickle.load(fh)
        log.info("Loaded cached credentials from %s", TOKEN_PATH)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            log.info("No valid credentials found — starting OAuth2 flow...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "wb") as fh:
            pickle.dump(creds, fh)
        log.info("Credentials saved to %s", TOKEN_PATH)

    return creds


def get_bq_client() -> bigquery.Client:
    """Return an authenticated BigQuery client for GCP_PROJECT."""
    creds = get_credentials()
    client = bigquery.Client(project=GCP_PROJECT, credentials=creds)
    log.info("BigQuery client ready  →  project: %s", GCP_PROJECT)
    return client


# ── Dataset setup ─────────────────────────────────────────────────────────────

def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Create the dataset in US if it does not already exist."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset exists        →  %s.%s", GCP_PROJECT, dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Dataset created       →  %s.%s  (location: US)", GCP_PROJECT, dataset_id)


# ── Step 1: GA4 sessions ──────────────────────────────────────────────────────

def build_ga4_sessions(client: bigquery.Client) -> None:
    """
    Execute stg_ga4_sessions.sql and write the result to staging.ga4_sessions.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Process:
        1. Read stg_ga4_sessions.sql from disk
        2. Wrap it in CREATE OR REPLACE TABLE DDL
        3. Execute the query to materialize the sessions table
        4. Log row count and schema info

    Output:
        Table: bi-portfolio-project.staging.ga4_sessions
        - 1 row per GA4 session (user_pseudo_id + ga_session_id)
        - Includes path normalisation to match SEMrush keywords
        - Includes session metrics (page_views, purchases, revenue, duration)
    """
    sql_path = os.path.join(SQL_DIR, "stg_ga4_sessions.sql")
    with open(sql_path, "r", encoding="utf-8") as fh:
        sql = fh.read()

    table_ref = f"`{GCP_PROJECT}.{STAGING_DATASET}.ga4_sessions`"
    ddl = f"CREATE OR REPLACE TABLE {table_ref}\nAS\n{sql}"

    log.info("Executing  %s  →  staging.ga4_sessions ...", os.path.basename(sql_path))
    client.query(ddl).result()

    table = client.get_table(f"{GCP_PROJECT}.{STAGING_DATASET}.ga4_sessions")
    log.info(
        "Load complete         →  %d rows, %d columns in %s.%s.ga4_sessions",
        table.num_rows, len(table.schema), GCP_PROJECT, STAGING_DATASET,
    )


def validate_ga4_sessions(client: bigquery.Client) -> None:
    """
    Validate ga4_sessions table: row count, date range, conversion stats.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Queries:
        - Total row count (sessions)
        - Earliest and latest session_date (date range of data)
        - Count of sessions with at least one purchase (conversion metric)

    Note:
        These quick sanity checks confirm that the GA4 transformation succeeded
        and data is present for the expected date range (2020-11-01 to 2021-01-31).
    """
    query = f"""
        SELECT
            COUNT(*)               AS row_count,
            MIN(session_date)      AS earliest_date,
            MAX(session_date)      AS latest_date,
            COUNTIF(purchases > 0) AS sessions_with_purchases
        FROM `{GCP_PROJECT}.{STAGING_DATASET}.ga4_sessions`
    """
    log.info("Running ga4_sessions validation...")
    rows = list(client.query(query).result())
    row  = rows[0]

    log.info("─" * 60)
    log.info("VALIDATION — staging.ga4_sessions")
    log.info("  Row count              : %d", row.row_count)
    log.info("  Date range             : %s → %s", row.earliest_date, row.latest_date)
    log.info("  Sessions with purchases: %d", row.sessions_with_purchases)
    log.info("─" * 60)


# ── Step 2: Domain ranks ──────────────────────────────────────────────────────

def load_domain_ranks(client: bigquery.Client) -> None:
    """
    Load domain-level SEMrush metrics from raw CSV files to BigQuery.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Process:
        1. Find all domain_ranks_*.csv files in data/raw/
        2. Load each CSV (semicolon-separated, UTF-8)
        3. Rename columns to snake_case (maps SEMrush headers to BQ format)
        4. Cast numeric columns to Int64 (nullable integers)
        5. Add snapshot_date (today's date)
        6. Upload to staging.domain_ranks using WRITE_TRUNCATE

    Output:
        Table: bi-portfolio-project.staging.domain_ranks
        - 1 row per domain (typically 4 rows: merch.google + 3 competitors)
        - Columns: domain, semrush_rank, organic_keywords, organic_traffic, etc.
        - Used by dim_competitor.sql to build the competitor dimension

    Note:
        If no domain_ranks_*.csv files exist, logs a warning and returns
        (this is acceptable if backlinks were skipped in semrush_api_pull.py).
    """
    frames = []

    for filename in sorted(os.listdir(RAW_DIR)):
        if not (filename.startswith("domain_ranks_") and filename.endswith(".csv")):
            continue
        filepath = os.path.join(RAW_DIR, filename)
        df = pd.read_csv(filepath, sep=";", encoding="utf-8")
        frames.append(df)
        log.info("Read %-45s  %d rows", filename, len(df))

    if not frames:
        log.warning("No domain_ranks_*.csv files found in %s — skipping step 2", RAW_DIR)
        return

    df = pd.concat(frames, ignore_index=True)

    # Rename to snake_case to match dim_competitor.sql column references.
    df = df.rename(columns=DOMAIN_RANKS_COLUMNS)

    # Cast all numeric columns; domain stays STRING.
    for col in df.columns:
        if col != "domain":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["snapshot_date"] = datetime.date.today()

    table_ref = f"{GCP_PROJECT}.{STAGING_DATASET}.domain_ranks"
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE)

    log.info("Loading %d domain rows  →  %s ...", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    log.info(
        "Load complete         →  %d rows, %d columns in %s",
        table.num_rows, len(table.schema), table_ref,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Main entry point: build the staging layer (GA4 sessions + domain ranks).

    Workflow:
        1. Authenticate with GCP using OAuth2 credentials
        2. Create staging dataset if needed (in US location)
        3. Step 1 — Build staging.ga4_sessions:
           - Execute stg_ga4_sessions.sql against GA4 public dataset
           - Normalise URLs to match SEMrush paths
           - Deduplicate sessions (GA4 can fire multiple session_start events)
           - Extract landing page, session metrics
        4. Validate ga4_sessions (row count, date range, purchases)
        5. Step 2 — Load staging.domain_ranks:
           - Concatenate all domain_ranks_*.csv files
           - Rename to snake_case
           - Add snapshot_date

    Prerequisites:
        - ingestion/semrush_to_bq.py must have run (semrush_keywords table)
        - GA4 public dataset is accessible (bigquery-public-data.*)
        - data/raw/ contains domain_ranks_*.csv files (from semrush_api_pull.py)

    Outputs:
        - Table: bi-portfolio-project.staging.ga4_sessions (millions of rows)
        - Table: bi-portfolio-project.staging.domain_ranks (4 rows)

    Next Steps:
        Run transform/run/run_marts.py to build the dimensional model
    """
    log.info("Starting staging layer build...")
    log.info("Project root : %s", PROJECT_ROOT)
    log.info("Target       : %s.%s.*", GCP_PROJECT, STAGING_DATASET)

    client = get_bq_client()
    ensure_dataset(client, STAGING_DATASET)

    log.info("=" * 60)
    log.info("STEP 1 — staging.ga4_sessions")
    log.info("=" * 60)
    build_ga4_sessions(client)
    validate_ga4_sessions(client)

    log.info("=" * 60)
    log.info("STEP 2 — staging.domain_ranks")
    log.info("=" * 60)
    load_domain_ranks(client)

    log.info("Done. Run transform/run/run_marts.py to build the mart layer.")


if __name__ == "__main__":
    main()
