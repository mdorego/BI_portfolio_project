"""
semrush_to_bq.py
----------------
Loads the prepared SEMrush CSV into a BigQuery staging table.

Input  : data/prepared/semrush_keywords_clean.csv
         credentials/credentials.json  (OAuth2 desktop-app credentials)
         credentials/token.pickle      (cached OAuth2 token, written on first run)

Output : BigQuery table  bi-portfolio-project.staging.semrush_keywords
         - Replaced on every run (WRITE_TRUNCATE) — re-runs are idempotent.
         - The staging dataset is created automatically if it does not exist.

Run from project root:
    python ingestion/semrush_to_bq.py

Schema
------
Most columns are inferred from the CSV via pandas / pyarrow type detection.
The following columns use explicit type overrides applied before the load:

    snapshot_date   → DATE
    Position        → INTEGER
    Search Volume   → INTEGER
    CPC             → FLOAT
    Traffic (%)     → FLOAT
    Competition     → FLOAT
    joinable_to_ga4 → BOOLEAN
"""

import os
import re
import pickle
import logging
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
PROJECT_ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREPARED_DIR     = os.path.join(PROJECT_ROOT, "data", "prepared")
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials", "credentials.json")
TOKEN_PATH       = os.path.join(PROJECT_ROOT, "credentials", "token.pickle")

# ── BigQuery config ───────────────────────────────────────────────────────────
GCP_PROJECT  = "bi-portfolio-project"
DATASET_ID   = "staging"
TABLE_ID     = "semrush_keywords"
CSV_FILENAME = "semrush_keywords_clean.csv"

SCOPES = ["https://www.googleapis.com/auth/bigquery"]

# ── Schema overrides ──────────────────────────────────────────────────────────
# Columns where pandas inference must be replaced with an explicit type.
# Keys match CSV column headers exactly (case-sensitive).
SCHEMA_OVERRIDES = {
    "snapshot_date":   "date",
    "Position":        "int",
    "Search Volume":   "int",
    "CPC":             "float",
    "Traffic (%)":     "float",
    "Competition":     "float",
    "joinable_to_ga4": "bool",
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

def ensure_dataset(client: bigquery.Client) -> None:
    """Create the staging dataset in US if it does not already exist."""
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset exists        →  %s.%s", GCP_PROJECT, DATASET_ID)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Dataset created       →  %s.%s  (location: US)", GCP_PROJECT, DATASET_ID)


# ── Data preparation ──────────────────────────────────────────────────────────

def load_csv() -> pd.DataFrame:
    """
    Load the prepared SEMrush CSV file with all values initially as strings.

    Returns:
        pd.DataFrame: CSV data with all columns as dtype=str (no type inference yet)

    Raises:
        FileNotFoundError: If prepared CSV does not exist at PREPARED_DIR

    Note:
        dtype=str and keep_default_na=False ensure that:
        - All values are loaded as strings (type casting happens later in apply_schema_overrides)
        - Empty strings are preserved, not converted to NaN
        This prevents pandas from incorrectly inferring types from the raw CSV.
    """
    csv_path = os.path.join(PREPARED_DIR, CSV_FILENAME)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Prepared CSV not found: {csv_path}\n"
            "Run semrush_prepare.py first."
        )
    df = pd.read_csv(csv_path, encoding="utf-8", dtype=str, keep_default_na=False)
    log.info("Read CSV              →  %d rows, %d columns", len(df), len(df.columns))
    return df


def apply_schema_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast CSV columns to their explicit BigQuery target types.

    Args:
        df (pd.DataFrame): DataFrame with all columns as strings (from load_csv)

    Returns:
        pd.DataFrame: DataFrame with columns cast to target types per SCHEMA_OVERRIDES

    Type Conversions:
        - date       : pd.to_datetime + .dt.date → python date objects → BQ DATE
        - int        : pd.to_numeric with errors='coerce' → Int64 (nullable integer)
        - float      : pd.to_numeric → float64 (preserves NaN)
        - bool       : string 'true'/'false' → python bool → BQ BOOLEAN

    Error Handling:
        - Non-numeric values in int/float columns become NaN (coerced)
        - Missing override columns are skipped with a warning
        - Logs all renamed columns and type conversions

    Note:
        Int64 (capital I) is pandas' nullable integer type — allows NaN values
        without downcasting to float. Standard int64 cannot represent NaN.
    """
    for col, dtype in SCHEMA_OVERRIDES.items():
        if col not in df.columns:
            log.warning("Override column not found in CSV, skipping: %s", col)
            continue

        if dtype == "date":
            # datetime.date objects → pyarrow date32 → BigQuery DATE
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif dtype == "int":
            # nullable integer — preserves NaN rows without downcasting to float
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        elif dtype == "bool":
            df[col] = df[col].str.strip().str.lower().map({"true": True, "false": False})

    log.info(
        "Schema overrides applied  →  %s",
        ", ".join(SCHEMA_OVERRIDES.keys()),
    )
    return df


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to BigQuery-legal identifiers (letters, digits, underscores only).

    Args:
        df (pd.DataFrame): DataFrame with potentially problematic column names

    Returns:
        pd.DataFrame: DataFrame with all columns renamed to BigQuery-safe names

    Transformation Rules:
        1. Replace "(%)" with "pct" (SEMrush uses this in "Traffic (%)" columns)
        2. Replace all non-alphanumeric/underscore chars with underscores
        3. Collapse consecutive underscores into single underscores
        4. Strip leading/trailing underscores

    Examples:
        "Search Volume" → "Search_Volume"
        "Traffic (%)" → "Traffic_pct"
        "Organic-Cost" → "Organic_Cost"
        "___messy___" → "messy"

    Validation:
        Raises ValueError if sanitization produces duplicate column names (collision).

    Note:
        BigQuery column names must match: [a-zA-Z_][a-zA-Z0-9_]*
        This function enforces that requirement before uploading.
    """
    def _clean(name: str) -> str:
        name = name.replace("(%)", "pct")
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name)
        return name.strip("_")

    renamed = {col: _clean(col) for col in df.columns}
    collisions = len(renamed.values()) != len(set(renamed.values()))
    if collisions:
        raise ValueError(f"Column sanitization produced duplicate names: {renamed}")

    df = df.rename(columns=renamed)
    changed = {k: v for k, v in renamed.items() if k != v}
    if changed:
        log.info("Columns renamed for BigQuery compatibility:")
        for old, new in changed.items():
            log.info("  %-30s →  %s", old, new)
    return df


# ── BigQuery load ─────────────────────────────────────────────────────────────

def load_to_bigquery(client: bigquery.Client, df: pd.DataFrame) -> None:
    """
    Upload DataFrame to BigQuery, replacing the target table on every run.

    Args:
        client (bigquery.Client): Authenticated BigQuery client
        df (pd.DataFrame): Prepared data ready for upload

    Behavior:
        - Uses WRITE_TRUNCATE: deletes all rows in target table before inserting
        - Makes the operation idempotent: safe to re-run without duplicates
        - Uploads the full DataFrame including all columns and rows

    Post-Upload:
        Retrieves the loaded table and logs final row count and column count
        for verification.

    Note:
        The target table (staging.semrush_keywords) is created automatically
        by BigQuery if it doesn't exist. Schema is inferred from the DataFrame.
    """
    table_ref = f"{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}"
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE)

    log.info("Loading %d rows → %s ...", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    log.info(
        "Load complete         →  %d rows, %d columns in %s",
        table.num_rows, len(table.schema), table_ref,
    )


# ── Validation ────────────────────────────────────────────────────────────────

def validate_load(client: bigquery.Client) -> None:
    """
    Query and validate the loaded data: row counts grouped by domain and source.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Behavior:
        Executes a validation query that groups loaded rows by domain and source,
        printing a table showing the breakdown. Useful for quickly verifying that
        all expected domains and sources were loaded correctly.

    Note:
        This is a lightweight sanity check; data quality validation happens
        downstream in the staging layer (run_staging.py).
    """
    query = f"""
        SELECT
            domain,
            source,
            COUNT(*) AS row_count
        FROM `{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}`
        GROUP BY domain, source
        ORDER BY domain, source
    """
    log.info("Running validation query...")
    rows = list(client.query(query).result())

    log.info("─" * 60)
    log.info("VALIDATION — row counts by domain and source")
    log.info("  %-30s  %-10s  %s", "domain", "source", "rows")
    log.info("  " + "─" * 48)
    for row in rows:
        log.info("  %-30s  %-10s  %d", row.domain, row.source, row.row_count)
    log.info("─" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Main entry point: orchestrate SEMrush data load to BigQuery.

    Workflow:
        1. Authenticate with GCP using OAuth2 (cached token or browser flow)
        2. Ensure staging dataset exists (created in US location if needed)
        3. Read prepared CSV from data/prepared/semrush_keywords_clean.csv
        4. Apply BigQuery schema type overrides (date, int, float, bool)
        5. Sanitise column names to BigQuery legal identifiers
        6. Upload to staging.semrush_keywords using WRITE_TRUNCATE (idempotent)
        7. Run validation query to confirm load success

    Prerequisites:
        - ingestion/semrush_prepare.py must have run first
        - credentials/credentials.json must exist (from GCP OAuth2 setup)
        - User must have BigQuery permissions on bi-portfolio-project

    Output:
        - Table: bi-portfolio-project.staging.semrush_keywords
        - Contains: All SEMrush keywords (organic + paid) for all 4 domains

    Next Steps:
        Run transform/run/run_staging.py to build the complete staging layer
    """
    log.info("Starting SEMrush → BigQuery ingestion...")
    log.info("Source  : %s", os.path.join(PREPARED_DIR, CSV_FILENAME))
    log.info("Target  : %s.%s.%s", GCP_PROJECT, DATASET_ID, TABLE_ID)

    client = get_bq_client()
    ensure_dataset(client)

    df     = load_csv()
    df     = apply_schema_overrides(df)
    df     = sanitize_column_names(df)
    load_to_bigquery(client, df)
    validate_load(client)

    log.info("Done.")


if __name__ == "__main__":
    main()
