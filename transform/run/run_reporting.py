"""
run_reporting.py
----------------
Builds the reporting (semantic) layer by creating BigQuery views from
SQL files in transform/sql/reporting/.

Execution order (dependency-aware):
  1. rpt_sessions_enriched      — base enriched view; all other views depend on it
  2. rpt_channel_performance    — aggregated by channel_group × device_group
  3. rpt_seo_performance        — aggregated by seo_rank_bucket × page_category
  4. rpt_landing_page_performance — aggregated by landing_page_path

Each SQL file contains a SELECT statement. This script wraps it in
CREATE OR REPLACE VIEW targeting bi-portfolio-project.reporting.<name>.

All views are idempotent — safe to re-run after any upstream mart update.

Prerequisites:
    marts.fact_sessions  — built by transform/run/run_marts.py
    marts.dim_date       — built by transform/run/run_marts.py
    marts.dim_page       — built by transform/run/run_marts.py

Run from project root:
    python transform/run/run_reporting.py
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
SQL_DIR       = os.path.join(TRANSFORM_DIR, "sql", "reporting")

CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials", "credentials.json")
TOKEN_PATH       = os.path.join(PROJECT_ROOT, "credentials", "token.pickle")

# ── BigQuery config ───────────────────────────────────────────────────────────
GCP_PROJECT        = "bi-portfolio-project"
REPORTING_DATASET  = "reporting"
SCOPES             = ["https://www.googleapis.com/auth/bigquery"]

# ── Execution plan ────────────────────────────────────────────────────────────
# rpt_sessions_enriched must be created before the three aggregated views
# that SELECT from it.
REPORTING_VIEWS = [
    ("rpt_sessions_enriched.sql",         "rpt_sessions_enriched"),
    ("rpt_channel_performance.sql",        "rpt_channel_performance"),
    ("rpt_seo_performance.sql",            "rpt_seo_performance"),
    ("rpt_landing_page_performance.sql",   "rpt_landing_page_performance"),
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
    """Create the reporting dataset in US if it does not already exist."""
    dataset_ref = client.dataset(REPORTING_DATASET)
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset exists        →  %s.%s", GCP_PROJECT, REPORTING_DATASET)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Dataset created       →  %s.%s  (location: US)", GCP_PROJECT, REPORTING_DATASET)


# ── View builder ──────────────────────────────────────────────────────────────

def build_reporting_view(client: bigquery.Client, sql_file: str, view_name: str) -> None:
    """
    Read a reporting SQL file and execute it as CREATE OR REPLACE VIEW.

    Args:
        client (bigquery.Client): Authenticated BigQuery client
        sql_file (str):           Filename of SQL template (e.g., "rpt_sessions_enriched.sql")
        view_name (str):          Target view name in reporting dataset

    Process:
        1. Read SQL file from transform/sql/reporting/ directory
        2. Wrap in CREATE OR REPLACE VIEW DDL
        3. Execute the query to create the view
        4. Log success

    Note:
        Views are lightweight logical queries (no data materialised).
        Views are idempotent — safe to re-run without affecting downstream analysis.
        Use WRITE_TRUNCATE is not applicable to views; CREATE OR REPLACE is the standard.
    """
    sql_path = os.path.join(SQL_DIR, sql_file)
    with open(sql_path, "r", encoding="utf-8") as fh:
        sql = fh.read()

    view_ref = f"`{GCP_PROJECT}.{REPORTING_DATASET}.{view_name}`"
    ddl = f"CREATE OR REPLACE VIEW {view_ref}\nAS\n{sql}"

    log.info("Creating view  %-35s  →  %s.%s ...", sql_file, REPORTING_DATASET, view_name)
    client.query(ddl).result()
    log.info("  Done  →  %s.%s", REPORTING_DATASET, view_name)


# ── Validation ────────────────────────────────────────────────────────────────

def validate_reporting(client: bigquery.Client) -> None:
    """
    Validate reporting views: print row counts via a UNION ALL query.

    Args:
        client (bigquery.Client): Authenticated BigQuery client

    Queries:
        Executes a UNION ALL of COUNT(*) for each reporting view,
        printing row counts. Confirms all views were created successfully.

    Note:
        Row counts for views are computed at query time and can be expensive
        for large result sets. Use this for validation only, not in production
        reporting pipelines.
    """
    union_query = "\nUNION ALL\n".join(
        f"SELECT '{view_name}' AS view_name, COUNT(*) AS row_count "
        f"FROM `{GCP_PROJECT}.{REPORTING_DATASET}.{view_name}`"
        for _, view_name in REPORTING_VIEWS
    )

    log.info("Running reporting validation query...")
    rows = list(client.query(union_query).result())

    log.info("─" * 60)
    log.info("VALIDATION — reporting view row counts")
    log.info("  %-35s  %s", "view", "rows")
    log.info("  " + "─" * 45)
    for row in rows:
        log.info("  %-35s  %d", row.view_name, row.row_count)
    log.info("─" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    """
    Main entry point: build the reporting (semantic) layer of views.

    Workflow:
        1. Authenticate with GCP using OAuth2 credentials
        2. Create reporting dataset if needed (in US location)
        3. Execute all reporting view SQL files in dependency order:
           - rpt_sessions_enriched       (base enriched view; all others depend on it)
           - rpt_channel_performance     (aggregated by channel_group × device_group)
           - rpt_seo_performance         (aggregated by seo_rank_bucket × page_category)
           - rpt_landing_page_performance (aggregated by landing_page_path)
        4. Print row count validation for all views

    Execution Order:
        rpt_sessions_enriched must be created before the three aggregated views
        that SELECT from it. The three aggregated views are independent of
        each other and can be created in any order.

    Prerequisites:
        - transform/run/run_marts.py must have run
        - All marts.* tables must exist and be populated

    Outputs:
        - View: bi-portfolio-project.reporting.rpt_sessions_enriched
        - View: bi-portfolio-project.reporting.rpt_channel_performance
        - View: bi-portfolio-project.reporting.rpt_seo_performance
        - View: bi-portfolio-project.reporting.rpt_landing_page_performance

    Next Steps:
        Connect Looker Studio to reporting views for dashboard creation.
        All views are driven by marts.fact_sessions + dimension tables.
    """
    log.info("Starting reporting layer build...")
    log.info("SQL dir  : %s", SQL_DIR)
    log.info("Target   : %s.%s.*", GCP_PROJECT, REPORTING_DATASET)

    client = get_bq_client()
    ensure_dataset(client)

    for sql_file, view_name in REPORTING_VIEWS:
        log.info("=" * 60)
        log.info("Building  %s.%s", REPORTING_DATASET, view_name)
        log.info("=" * 60)
        build_reporting_view(client, sql_file, view_name)

    validate_reporting(client)
    log.info("Done. Connect Looker Studio to %s.%s.*", GCP_PROJECT, REPORTING_DATASET)


if __name__ == "__main__":
    main()
