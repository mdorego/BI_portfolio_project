# BI Portfolio: Complete Project Summary

**Last Updated**: 2026-05-04  
**Project**: SEMrush + GA4 Competitive Intelligence Data Warehouse  
**Tech Stack**: Python 3.8+, BigQuery, pandas, Google Cloud (OAuth2)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Folder Structure](#folder-structure)
3. [Data Architecture](#data-architecture)
4. [Ingestion Pipeline](#ingestion-pipeline)
5. [Transform Layers](#transform-layers)
6. [Data Model (Star Schema)](#data-model-star-schema)
7. [BigQuery Datasets & Tables](#bigquery-datasets--tables)
8. [External Data Sources](#external-data-sources)
9. [Setup & Execution](#setup--execution)
10. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project builds a **business intelligence data warehouse** combining:
- **SEMrush API data**: Organic keywords, paid keywords, backlinks, domain authority for merch.google + 3 competitors
- **GA4 public dataset**: Session-level web analytics for the Google Merchandise Store
- **Purpose**: Understand SEO/SEM performance, traffic sources, conversion rates, and competitive benchmarking

**Key Use Cases**:
- SEO ranking → conversion correlation analysis
- Channel/device performance breakdown
- Landing page effectiveness measurement
- Competitive domain authority tracking

---

## Folder Structure

```
bi-portfolio/
├── .env.example                          # Template for environment variables
├── .gitignore                            # Git ignore rules (Python + GCP)
├── requirements.txt                      # Python package dependencies
│
├── credentials/
│   ├── credentials.json                  # ⚠️ OAuth2 credentials (DO NOT COMMIT)
│   └── token.pickle                      # ⚠️ Cached OAuth2 token (auto-generated)
│
├── data/
│   ├── raw/                              # Input CSVs from SEMrush API
│   │   ├── organic_keywords_*.csv        # SEMrush domain_organic exports
│   │   ├── paid_keywords_*.csv           # SEMrush domain_adwords exports
│   │   └── domain_ranks_*.csv            # SEMrush domain_ranks exports
│   └── prepared/                         # Cleaned CSVs ready for BigQuery ingestion
│       ├── semrush_keywords_clean.csv    # Combined + enriched keywords
│       └── semrush_clean_*.csv           # Per-domain splits
│
├── ingestion/                            # Data ingestion layer
│   ├── semrush_api_pull.py               # Pulls raw data from SEMrush API
│   ├── semrush_prepare.py                # Cleans + enriches SEMrush data
│   └── semrush_to_bq.py                  # Loads prepared CSV to BigQuery
│
├── transform/                            # Data transformation layer
│   ├── run/
│   │   ├── run_staging.py                # Builds staging.* tables
│   │   ├── run_marts.py                  # Builds marts.dim_* + fact_sessions
│   │   └── run_reporting.py              # Builds reporting.rpt_* views
│   └── sql/
│       ├── staging/
│       │   └── stg_ga4_sessions.sql      # GA4 → normalized sessions
│       ├── marts/
│       │   ├── dim_date.sql              # Calendar date dimension
│       │   ├── dim_channel.sql           # Traffic channel dimension
│       │   ├── dim_page.sql              # Page/URL dimension with SEO data
│       │   ├── dim_competitor.sql        # Competitor benchmark dimension
│       │   └── fact_sessions.sql         # Central fact table (session grain)
│       └── reporting/
│           ├── rpt_sessions_enriched.sql # Base enriched session view
│           ├── rpt_channel_performance.sql
│           ├── rpt_seo_performance.sql
│           └── rpt_landing_page_performance.sql
│
├── docs/                                 # Documentation
│   └── project_summary.md                # This file
│
└── venv/                                 # Python virtual environment (excluded from git)
```

---

## Data Architecture

### High-Level Flow

```
┌─────────────────────┐
│  SEMrush API        │  (4 domains)
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│  semrush_api_pull   │  → ./output/*.csv
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│ semrush_prepare.py  │  → data/prepared/*.csv
└──────────┬──────────┘
           │
           v
┌─────────────────────────────────────┐
│    semrush_to_bq.py                 │
│    ↓                                 │
│    bi-portfolio-project              │
│    └── staging.semrush_keywords      │
└──────────────┬──────────────────────┘
               │
        ┌──────┴──────┐
        │             │
        v             v
   ┌────────────────┐ ┌─────────────────────────┐
   │ GA4 Public     │ │ run_staging.py          │
   │ Dataset        │ │ ├── stg_ga4_sessions    │
   │ (events_*)     │ │ └── domain_ranks        │
   └────────────────┘ └──────────┬──────────────┘
        │                        │
        └────────────┬───────────┘
                     v
        ┌────────────────────────┐
        │  run_marts.py          │
        │  ├── dim_date          │
        │  ├── dim_channel       │
        │  ├── dim_page          │
        │  ├── dim_competitor    │
        │  └── fact_sessions     │
        └────────────┬───────────┘
                     v
        ┌────────────────────────┐
        │ run_reporting.py       │
        │ ├── rpt_sessions_enriched
        │ ├── rpt_channel_perf   │
        │ ├── rpt_seo_perf       │
        │ └── rpt_landing_page   │
        └────────────────────────┘
                     │
                     v
              ┌──────────────┐
              │ Looker Studio│
              │   Dashboards │
              └──────────────┘
```

### Processing Stages

**Stage 1: Ingestion** (Python)
- Pull from SEMrush API → save CSVs
- Parse + enrich SEMrush data → add path normalization, category mapping
- Load to BigQuery staging layer

**Stage 2: Staging** (SQL)
- Transform raw GA4 events → normalized sessions
- Normalize URLs (GA4 vs SEMrush structure differences)
- Load domain authority metrics from CSV

**Stage 3: Marts** (SQL)
- Build conformed dimensions (date, channel, page, competitor)
- Build central fact table (sessions with all foreign keys)
- All tables use CREATE OR REPLACE (idempotent)

**Stage 4: Reporting** (SQL Views)
- Create semantic layer with computed columns
- Aggregate views for specific analysis dimensions
- Views are lightweight (no materialized data)

---

## Ingestion Pipeline

### 1. semrush_api_pull.py

**Purpose**: Extract SEMrush data via REST API

**Inputs**:
- Command-line arguments: `--key YOUR_API_KEY [--skip-backlinks]`

**Outputs** (to `./output/`):
- `organic_keywords_{domain}.csv` (500 rows per domain, 10 units each)
- `paid_keywords_{domain}.csv` (500 rows per domain, 10 units each)
- `domain_ranks_{domain}.csv` (1 row per domain, 10 units each)
- `backlinks_{domain}.csv` (500 rows per domain, 1 unit each) *[optional]*
- `keyword_difficulty_top_keywords.csv` (bulk lookup, 10 units)

**API Budget**: ~42,000 units (safely within 50,000 monthly limit)

**Domains**: merch.google, cafepress.com, redbubble.com, zazzle.com

---

### 2. semrush_prepare.py

**Purpose**: Clean + enrich SEMrush CSVs with metadata for GA4 joining

**Inputs**:
- `data/raw/organic_keywords_*.csv`
- `data/raw/paid_keywords_*.csv`

**Outputs**:
- `data/prepared/semrush_keywords_clean.csv` (combined)
- `data/prepared/semrush_clean_{domain}.csv` (per-domain)

**Transformations**:
1. **Extract page_path** from Url column
   - Strip scheme + host
   - Lowercase
   - Remove trailing slash
   - Used as join key to GA4 sessions

2. **Assign join_category** (product category)
   - Uses regex rules (apparel/hoodies, lifestyle/drinkware, etc.)
   - Returns None for product pages (`/product/*`) and legacy URLs (`*.html`)
   - Same rules as SQL staging layer (kept in sync manually)

3. **Set joinable_to_ga4** flag
   - True only for merch.google rows where category is not None
   - Competitors never joinable (included only for benchmarking)

4. **Add snapshot_date** (ISO date string)
   - Essential for tracking keyword changes over time

**Example Row**:
```
domain          | source  | snapshot_date | Keyword              | Position | page_path            | join_category        | joinable_to_ga4
merch.google    | organic | 2026-05-04    | google merchandise   | 2        | /shop/apparel        | apparel              | True
cafepress.com   | organic | 2026-05-04    | merchandise printing | 5        | /shop/apparel        | NULL                 | False
```

---

### 3. semrush_to_bq.py

**Purpose**: Load prepared CSV to BigQuery staging table

**Inputs**:
- `data/prepared/semrush_keywords_clean.csv`
- `credentials/credentials.json` (OAuth2)

**Outputs**:
- Table: `bi-portfolio-project.staging.semrush_keywords`

**Type Overrides**:
- `snapshot_date` → DATE
- `Position`, `Search Volume` → INT64
- `CPC`, `Traffic (%)`, `Competition` → FLOAT64
- `joinable_to_ga4` → BOOLEAN

**Column Name Sanitization**:
- `Traffic (%)` → `traffic_pct`
- Non-alphanumeric chars → underscores
- Collapse consecutive underscores

---

## Transform Layers

### Stage 1: Staging Layer (`staging.*`)

**Purpose**: Normalize raw data + basic transformations

#### stg_ga4_sessions.sql (320 lines of CTEs)

**Grain**: 1 row per session (user_pseudo_id + ga_session_id)

**Source**: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` (2020-11-01 to 2021-01-31)

**Key Transforms**:
1. Extract session attributes from event_params array
2. Normalize page paths:
   - Step 1: Extract path from full URL (e.g., `https://googlemerchandisestore.com/google+redesign/apparel`)
   - Step 2: Strip `/google+redesign` prefix (old site structure)
   - Result: `/apparel` (matches SEMrush path)
3. Assign join_category using same regex rules as semrush_prepare.py
4. Lookup canonical SEMrush page_path via exact match or category fallback
5. Deduplicate sessions (GA4 fires multiple session_start events in milliseconds)
6. Aggregate page_view, purchase, revenue metrics across all events in session

**Output Columns**:
- session_id (PK)
- user_pseudo_id, ga_session_id
- session_date, channel, source, device_category, country
- page_path (join key to SEMrush)
- landing_page_path
- engaged_session (boolean)
- session_duration_sec
- page_views, purchases, revenue

**Row Count**: ~760K sessions (Nov 2020 - Jan 2021)

---

#### staging.domain_ranks (loaded from CSV)

**Grain**: 1 row per domain

**Source**: `data/raw/domain_ranks_*.csv` (from semrush_api_pull.py)

**Columns**:
- domain (STRING): "merch.google", "cafepress.com", etc.
- semrush_rank (INT64): Overall authority ranking
- organic_keywords (INT64): Keywords ranking in search
- organic_traffic (INT64): Estimated monthly organic visitors
- organic_cost (INT64): Estimated SEM budget replacement value
- paid_keywords (INT64): Keywords in Google Ads
- snapshot_date (DATE): Data collection date

---

### Stage 2: Marts Layer (`marts.*`)

**Purpose**: Conformed dimensions + central fact table (star schema)

#### Dimensions

**dim_date** (93 rows)
- **Grain**: 1 row per date (2020-11-01 to 2021-01-31)
- **PK**: date_id
- **Columns**: year, month, month_name, week_of_year, day_of_week, day_name, is_weekend
- **Purpose**: Time attributes for all dates in the fact table

**dim_channel** (5-10 rows, varies)
- **Grain**: 1 row per (channel, source) pair
- **PK**: channel, source
- **Columns**: channel_group (organic search, paid search, direct, social, etc.)
- **Purpose**: Standardized channel grouping for reporting
- **Source**: Distinct (channel, source) pairs from staging.ga4_sessions

**dim_page** (~1,200 rows)
- **Grain**: 1 row per page_path
- **PK**: page_path
- **Columns**:
  - page_category (join_category from SEMrush)
  - top_keyword (best-ranking SEMrush keyword for this page)
  - organic_position (rank #)
  - search_volume (monthly)
  - has_seo_data (boolean)
- **Purpose**: SEO attributes for pages; links to SEMrush data
- **Source**: All GA4 page_paths UNION with category-collapsed paths from session entry points

**dim_competitor** (4 rows)
- **Grain**: 1 row per domain
- **PK**: domain
- **Columns**: semrush_rank, organic_keywords, organic_traffic, organic_cost, paid_keywords, is_target (boolean)
- **Purpose**: Competitive benchmarking metrics
- **Source**: staging.domain_ranks

#### Fact Table

**fact_sessions** (~760K rows)
- **Grain**: 1 row per session
- **PK**: session_id
- **Foreign Keys**:
  - session_date → dim_date.date_id
  - channel, source → dim_channel (channel, source)
  - page_path → dim_page.page_path
- **Metrics**:
  - page_views, purchases, revenue, session_duration_sec
  - engaged_session (boolean)
- **Dimensions**: device_category, country (denormalized, low cardinality)
- **Enrichments from dim_page**: page_category, organic_position, has_seo_data

---

### Stage 3: Reporting Layer (`reporting.*` Views)

**Purpose**: Semantic layer with computed columns for BI tools

#### rpt_sessions_enriched

**Grain**: 1 row per session (same as fact_sessions)

**Added Columns**:
- device_group: "Mobile" (mobile + tablet) or "Desktop"
- seo_rank_bucket: "1 — Top 3", "2 — Top 10", ..., "6 — No SEO Data"
- is_engaged: Alias for engaged_session
- is_converted: (purchases > 0)
- Date attributes from dim_date: year, month, month_name, week_of_year, day_name, is_weekend

**Use Case**: Detail-level analysis; export to Looker Studio for row-level filtering

---

#### rpt_channel_performance

**Grain**: 1 row per (channel_group, device_group)

**Metrics**:
- sessions (count)
- engaged_sessions (count)
- conversions (count with purchases > 0)
- revenue (sum)
- engagement_rate = engaged_sessions / sessions
- conversion_rate = conversions / sessions
- revenue_per_session = revenue / sessions
- avg_session_duration_sec
- avg_page_views

**Use Case**: Marketing channel ROI analysis; device comparison

---

#### rpt_seo_performance

**Grain**: 1 row per (seo_rank_bucket, page_category)

**Metrics**:
- sessions, engaged_sessions, conversions, revenue (same as channel_performance)
- best_organic_position (MIN)
- avg_search_volume
- engagement_rate, conversion_rate, revenue_per_session

**Use Case**: SEO ranking impact on conversions; category analysis

---

#### rpt_landing_page_performance

**Grain**: 1 row per landing_page_path

**Metrics**:
- sessions, engaged_sessions, conversions, revenue
- engagement_rate, conversion_rate, revenue_per_session
- avg_page_views

**SEO Attributes** (from dim_page joined via landing_page_path):
- page_category, organic_position, search_volume, top_keyword, seo_rank_bucket

**Use Case**: Landing page optimization; identify high-performing entry pages

---

## Data Model (Star Schema)

```
                    ┌──────────────────┐
                    │   dim_date       │
                    ├──────────────────┤
                    │ PK: date_id      │
                    │ year, month      │
                    │ day_of_week      │
                    │ is_weekend       │
                    └────────┬─────────┘
                             │
                    ┌────────v────────┐
                    │  fact_sessions   │
                    ├──────────────────┤
                    │ PK: session_id   │
                    │ FK: date_id      │
                    │ FK: channel      │
                    │ FK: source       │
                    │ FK: page_path    │
                    │ Metrics:         │
                    │  - page_views    │
                    │  - purchases     │
                    │  - revenue       │
                    │  - duration_sec  │
                    └────┬──────┬──────┘
                         │      │
          ┌──────────────┘      └───────────────┐
          │                                      │
    ┌─────v────────┐                    ┌──────v───────────┐
    │ dim_channel  │                    │   dim_page       │
    ├──────────────┤                    ├──────────────────┤
    │ PK: (channel,source)              │ PK: page_path    │
    │ channel_group                     │ page_category    │
    │                                   │ top_keyword      │
    │                                   │ organic_position │
    │                                   │ search_volume    │
    │                                   │ has_seo_data     │
    └──────────────┘                    └────┬─────────────┘
                                             │
                                        ┌────v────────┐
                                        │dim_competitor│
                                        ├──────────────┤
                                        │ PK: domain   │
                                        │ semrush_rank │
                                        │ org_keywords │
                                        │ org_traffic  │
                                        │ is_target    │
                                        └──────────────┘
```

**Relationships**:
- fact_sessions.session_date → dim_date.date_id
- fact_sessions.channel + source → dim_channel.channel + source
- fact_sessions.page_path → dim_page.page_path
- dim_page implicitly references staging.semrush_keywords

---

## BigQuery Datasets & Tables

### Project: `bi-portfolio-project`

#### Dataset: `staging`
- **semrush_keywords**: Keywords from all 4 domains (organic + paid), ~10K rows
- **ga4_sessions**: Normalized GA4 sessions, ~760K rows
- **domain_ranks**: Domain authority metrics, 4 rows

#### Dataset: `marts`
- **dim_date**: Calendar dates, 93 rows
- **dim_channel**: Traffic channel/source pairs, ~10 rows
- **dim_page**: Page paths with SEO data, ~1,200 rows
- **dim_competitor**: Domains, 4 rows
- **fact_sessions**: Central fact table, ~760K rows

#### Dataset: `reporting`
- **rpt_sessions_enriched**: Session-level view (VIEW)
- **rpt_channel_performance**: Channel aggregation (VIEW)
- **rpt_seo_performance**: SEO ranking aggregation (VIEW)
- **rpt_landing_page_performance**: Landing page aggregation (VIEW)

**Location**: US (all datasets)

---

## External Data Sources

### GA4 Public Dataset
- **Project**: `bigquery-public-data`
- **Dataset**: `ga4_obfuscated_sample_ecommerce`
- **Table**: `events_*` (wildcard; one table per date)
- **Date Range**: 2020-11-01 to 2021-01-31
- **Domain**: Google Merchandise Store (googlemerchandisestore.com)
- **Events**: session_start, page_view, purchase, etc. (event-level granularity)

### SEMrush API
- **Base URL**: `https://api.semrush.com/`
- **Database**: US (aligns with GA4)
- **Endpoints**:
  - domain_organic: Organic keywords ranking
  - domain_adwords: Paid keywords
  - domain_ranks: Authority metrics
  - backlinks: Inbound links
  - phrase_these: Keyword difficulty lookup
- **Authentication**: API key (command-line argument)
- **Rate Limit**: 10 requests/second (respectful spacing in code)

---

## Setup & Execution

### Prerequisites

1. **Python 3.8+** with pip
2. **GCP Account** with:
   - BigQuery API enabled
   - Service account or OAuth2 credentials
3. **SEMrush Account** with API access (if running ingestion layer)

### Installation

```bash
# Clone / navigate to project directory
cd bi-portfolio

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### GCP Setup

1. **Create OAuth2 credentials**:
   - Go to Google Cloud Console → APIs & Services → Credentials
   - Create "OAuth 2.0 Client ID" (Desktop application)
   - Download credentials JSON → save to `credentials/credentials.json`
   - First run will open browser for authorization

2. **Grant BigQuery permissions**:
   - Ensure your GCP user has "BigQuery Admin" or "Editor" role

### Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your GCP project ID and SEMrush API key (if using ingestion layer)

### Execution Order

**Option A: Full Pipeline (from scratch)**

```bash
# Step 1: Extract from SEMrush (requires API key + trial account)
python ingestion/semrush_api_pull.py --key YOUR_API_KEY

# Step 2: Prepare SEMrush data
python ingestion/semrush_prepare.py

# Step 3: Load to BigQuery
python ingestion/semrush_to_bq.py

# Step 4: Build staging layer
python transform/run/run_staging.py

# Step 5: Build marts
python transform/run/run_marts.py

# Step 6: Build reporting views
python transform/run/run_reporting.py
```

**Option B: Staging → Reporting (assuming semrush_keywords already in BigQuery)**

```bash
# Create staging tables from GA4 public dataset
python transform/run/run_staging.py

# Build marts
python transform/run/run_marts.py

# Build reporting views
python transform/run/run_reporting.py
```

### BigQuery Queries

Once built, explore the data:

```sql
-- Row counts
SELECT 'dim_date' as table_name, COUNT(*) as rows FROM bi-portfolio-project.marts.dim_date
UNION ALL
SELECT 'fact_sessions', COUNT(*) FROM bi-portfolio-project.marts.fact_sessions
...

-- Channel performance
SELECT * FROM bi-portfolio-project.reporting.rpt_channel_performance
ORDER BY sessions DESC;

-- Top pages by revenue
SELECT * FROM bi-portfolio-project.reporting.rpt_landing_page_performance
WHERE has_seo_data = TRUE
ORDER BY revenue DESC
LIMIT 20;

-- SEO impact (ranking → conversions)
SELECT seo_rank_bucket, page_category, sessions, conversion_rate, avg_search_volume
FROM bi-portfolio-project.reporting.rpt_seo_performance
ORDER BY seo_rank_bucket, conversion_rate DESC;
```

---

## Troubleshooting

### OAuth2 Authentication Failed

**Error**: `FileNotFoundError: credentials/credentials.json`

**Solution**:
1. Download OAuth2 credentials from GCP Console
2. Save to `credentials/credentials.json`
3. Delete `credentials/token.pickle` if stale
4. Re-run script; browser will open for authorization

### SEMrush API Errors

**Error**: `ERROR 10` (invalid API key)

**Solution**: Verify your API key is correct and trial account is active

**Error**: `ERROR 50` (invalid domain)

**Solution**: Check domain spelling (use "merch.google", not "merch.google.com")

### BigQuery Permission Denied

**Error**: `google.api_core.exceptions.Forbidden: 403 Access Denied`

**Solution**:
1. Verify GCP user has BigQuery role (Editor or BigQuery Admin)
2. Ensure project ID in script matches your GCP project
3. Check if datasets exist; if not, script should create them automatically

### CSV Parse Errors

**Error**: `ParserError: Error tokenizing data`

**Solution**: Ensure data/raw/ contains correct CSV format from semrush_api_pull.py

### Path Normalization Mismatches

**Symptom**: GA4 pages don't join to SEMrush keywords

**Investigation**:
1. Check `page_path` column in `staging.ga4_sessions` vs `staging.semrush_keywords`
2. Verify regex rules in `stg_ga4_sessions.sql` match `semrush_prepare.py` CATEGORY_RULES
3. Run query to find unmatched paths:
   ```sql
   SELECT DISTINCT page_path FROM bi-portfolio-project.marts.fact_sessions
   WHERE has_seo_data = FALSE
   ORDER BY 1
   LIMIT 20
   ```

---

## Key Design Decisions

1. **WRITE_TRUNCATE for idempotency**: All tables/marts use CREATE OR REPLACE or WRITE_TRUNCATE so scripts are safe to re-run
2. **Category mapping**: Same regex rules in Python and SQL (manual sync required if changing)
3. **Path normalization**: Two-step process (extract URL path, strip old site prefix) handles GA4 ↔ SEMrush structure differences
4. **Snapshot dates**: SEMrush data includes extraction date for time-series analysis
5. **Nullable foreign keys**: Views use LEFT JOIN to handle pages/channels with no SEO data
6. **Semantic layer**: Reporting views add computed flags (device_group, seo_rank_bucket) for BI tool usability

---

## Future Enhancements

- [ ] Automate SEMrush API pulls via Cloud Scheduler
- [ ] Add incremental load (append vs. truncate)
- [ ] Implement data quality tests (Great Expectations)
- [ ] Build Looker Studio dashboards (template)
- [ ] Add competitor trend analysis (snapshots over time)
- [ ] Expand GA4 date range (if available beyond Jan 2021)
- [ ] Add paid search data enrichment (Google Ads API)

---

**Document Version**: 1.0  
**Author**: Data Engineering  
**Last Reviewed**: 2026-05-04
