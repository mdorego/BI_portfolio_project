"""
run_marts.py
------------
Builds the mart layer by executing all mart SQL files in dependency order.

Execution order (dims before fact to avoid forward references):
  1. marts.dim_date        — no upstream mart dependencies
  2. marts.dim_channel     — no upstream mart dependencies
  3. marts.dim_page        — no upstream mart dependencies
  4. marts.dim_competitor  — no upstream mart dependencies
  5. marts.fact_sessions   — joins to all four dims above

Each SQL file is read from disk and executed as a CREATE OR REPLACE TABLE
targeting the appropriate marts.* table. All runs are idempotent (WRITE_TRUNCATE).

After building all tables, the script also:
  - Creates marts.ai_insights (CREATE TABLE IF NOT EXISTS, partitioned by date)
  - Creates or replaces marts.v_channel_summary (reporting view over fact_sessions)

Then prints a row-count summary for table validation.

Prerequisites:
    staging.ga4_sessions    — built by transform/run/run_staging.py
    staging.semrush_keywords — built by ingestion/semrush_to_bq.py
    staging.domain_ranks    — built by transform/run/run_staging.py

Run from project root:
    python transform/run/run_marts.py
"""

import os
import pickle
import logging
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.api_core.exceptions import NotFound
from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import bigquery

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
SQL_DIR          = os.path.join(TRANSFORM_DIR, "sql", "marts")

CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials", "credentials.json")
TOKEN_PATH       = os.path.join(PROJECT_ROOT, "credentials", "token.pickle")

# ── BigQuery config ───────────────────────────────────────────────────────────
GCP_PROJECT   = "bi-portfolio-project"
MARTS_DATASET = "marts"
SCOPES        = ["https://www.googleapis.com/auth/bigquery"]

# ── Execution plan ────────────────────────────────────────────────────────────
# Ordered by dependency — no table is built before the tables it joins to.
MART_TABLES = [
    ("dim_date.sql",       "dim_date"),
    ("dim_channel.sql",    "dim_channel"),
    ("dim_page.sql",       "dim_page"),
    ("dim_competitor.sql", "dim_competitor"),
    ("fact_sessions.sql",  "fact_sessions"),
]

# ── Additional objects (executed as raw SQL, not wrapped in CREATE OR REPLACE TABLE) ──
# Each entry: (sql_file, label_for_logging, search_dir)
# ai_insights lives in sql/marts/ (it's a physical table setup).
# v_channel_summary lives in sql/reporting/ (it's a view over the mart).
RAW_SQL_OBJECTS = [
    ("ai_insights_table.sql",  "ai_insights",       SQL_DIR),
    ("v_channel_summary.sql",  "v_channel_summary",  SQL_DIR),
]


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
    """Create the marts dataset in US if it does not already exist."""
    dataset_ref = client.dataset(MARTS_DATASET)
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset exists        →  %s.%s", GCP_PROJECT, MARTS_DATASET)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Dataset created       →  %s.%s  (location: US)", GCP_PROJECT, MARTS_DATASET)


# ── Table builder ─────────────────────────────────────────────────────────────

def build_mart_table(client: bigquery.Client, sql_file: str, table_name: str) -> None:
    """
    Read a mart SQL file and execute it as-is, then log row/column counts.

    Args:
        client (bigquery.Client): Authenticated BigQuery client
        sql_file (str):           Filename of SQL file (e.g., "dim_date.sql")
        table_name (str):         Target table name in marts dataset (used for post-run metadata fetch)

    Note:
        SQL files are expected to contain their own CREATE OR REPLACE TABLE DDL.
        This function executes them verbatim and fetches table metadata afterwards
        to log row and column counts for validation.
    """
    sql_path = os.path.join(SQL_DIR, sql_file)
    with open(sql_path, "r", encoding="utf-8") as fh:
        sql = fh.read()

    log.info("Executing  %-25s  →  %s.%s ...", sql_file, MARTS_DATASET, table_name)
    client.query(sql).result()

    table = client.get_table(f"{GCP_PROJECT}.{MARTS_DATASET}.{table_name}")
    log.info(
        "  Done  →  %d rows, %d columns",
        table.num_rows, len(table.schema),
    )


# ── Raw SQL executor (tables / views that manage their own DDL) ───────────────

def execute_raw_sql(client: bigquery.Client, sql_file: str, label: str, sql_dir: str) -> None:
    """
    Execute a SQL file exactly as written — no CREATE OR REPLACE TABLE wrapper.

    Use this for objects that own their own DDL:
      - ai_insights_table.sql  → CREATE TABLE IF NOT EXISTS (partitioned, idempotent)
      - v_channel_summary.sql  → CREATE OR REPLACE VIEW (reporting layer)

    Args:
        client (bigquery.Client): Authenticated BigQuery client
        sql_file (str):           Filename of the SQL file to execute
        label (str):              Human-readable name used in log output
        sql_dir (str):            Directory containing the SQL file
    """
    sql_path = os.path.join(sql_dir, sql_file)
    with open(sql_path, "r", encoding="utf-8") as fh:
        sql = fh.read()

    log.info("Executing  %-30s  →  marts.%s ...", sql_file, label)
    client.query(sql).result()
    log.info("  Done  →  marts.%s", label)


# ── Validation ────────────────────────────────────────────────────────────────

def validate_marts(client: bigquery.Client) -> None:
    """
    Validate all mart tables: print row counts in a single query.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Queries:
        Executes a UNION ALL of COUNT(*) for each mart table in MART_TABLES,
        printing a summary table with row counts. Confirms all tables were
        built successfully and contain expected data volumes.
    """
    union_query = "\nUNION ALL\n".join(
        f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
        f"FROM `{GCP_PROJECT}.{MARTS_DATASET}.{table_name}`"
        for _, table_name in MART_TABLES
    )

    log.info("Running mart validation query...")
    rows = list(client.query(union_query).result())

    log.info("─" * 60)
    log.info("VALIDATION — mart row counts")
    log.info("  %-25s  %s", "table", "rows")
    log.info("  " + "─" * 38)
    for row in rows:
        log.info("  %-25s  %d", row.table_name, row.row_count)
    log.info("─" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Main entry point: build the dimensional mart layer (dims + fact table),
    then set up the ai_insights table and v_channel_summary reporting view.

    Workflow:
        1. Authenticate with GCP using OAuth2 credentials
        2. Create marts dataset if needed (in US location)
        3. Execute all mart SQL files in dependency order:
           - dim_date        (no dependencies)
           - dim_channel     (reads staging.ga4_sessions)
           - dim_page        (reads staging.ga4_sessions + staging.semrush_keywords)
           - dim_competitor  (reads staging.domain_ranks)
           - fact_sessions   (reads staging.ga4_sessions + all 4 dims)
        4. Execute additional DDL objects (raw SQL, no wrapper):
           - ai_insights      (CREATE TABLE IF NOT EXISTS, partitioned by date)
           - v_channel_summary (CREATE OR REPLACE VIEW over fact_sessions)
        5. Print row count validation for physical mart tables

    Execution Order:
        All dimension tables are built before the fact table to avoid
        forward references. dim_date has no external dependencies and
        is always built first. Additional objects run after fact_sessions
        since v_channel_summary depends on it.

    Prerequisites:
        - transform/run/run_staging.py must have run
        - All staging.* tables must exist and be populated

    Outputs:
        - Table: bi-portfolio-project.marts.dim_date
        - Table: bi-portfolio-project.marts.dim_channel
        - Table: bi-portfolio-project.marts.dim_page
        - Table: bi-portfolio-project.marts.dim_competitor
        - Table: bi-portfolio-project.marts.fact_sessions
        - Table: bi-portfolio-project.marts.ai_insights       (partitioned, IF NOT EXISTS)
        - View:  bi-portfolio-project.marts.v_channel_summary  (reporting view)

    Next Steps:
        Connect Looker Studio to marts.fact_sessions or marts.v_channel_summary
        for channel analysis. Write AI insights to marts.ai_insights via the
        Claude API pipeline.
    """
    log.info("Starting mart layer build...")
    log.info("SQL dir  : %s", SQL_DIR)
    log.info("Target   : %s.%s.*", GCP_PROJECT, MARTS_DATASET)

    client = get_bq_client()
    ensure_dataset(client)

    for sql_file, table_name in MART_TABLES:
        log.info("=" * 60)
        log.info("Building  %s.%s", MARTS_DATASET, table_name)
        log.info("=" * 60)
        build_mart_table(client, sql_file, table_name)

    log.info("=" * 60)
    log.info("Building additional objects (raw DDL)")
    log.info("=" * 60)
    for sql_file, label, sql_dir in RAW_SQL_OBJECTS:
        execute_raw_sql(client, sql_file, label, sql_dir)

    validate_marts(client)
    log.info("Done. Connect Looker Studio to %s.%s.*", GCP_PROJECT, MARTS_DATASET)


if __name__ == "__main__":
    main()