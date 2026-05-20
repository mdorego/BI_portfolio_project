# BI Portfolio: Analytics Foundation for Google Merchandise Store

> For project context, business framing, and dashboard walkthrough, see [`docs/BI_Portfolio.pdf`](docs/BI_Portfolio.pdf). This README covers architecture, data model, and setup instructions.

## Project Overview

This project implements a complete analytics architecture from raw GA4 event data through to executive-level insights and visualizations. It demonstrates:

- **Star schema design** centered on `fact_sessions` with dimensional modeling
- **Semantic layer** for stakeholder-friendly reporting
- **Data quality** through staging, marts, and reporting layers
- **AI-powered insights** using Claude API for automated analysis
- **Scalable architecture** suitable for enterprise environments

- **Technology Stack:** Google BigQuery, Python, Claude API, Looker Studio

### Key Features

- Real GA4 data from Google Merchandise Store (via BigQuery public dataset)
- Automated dimensionality enrichment (date, channel, page, keyword)
- Channel performance reporting with trend analysis
- SEO correlation analysis
- Landing page effectiveness metrics
- AI-generated channel recommendations for marketing leaders

---

## Architecture

### Data Flow

```
GA4 Events (BigQuery Public Dataset)
    ↓
[Transformation Layer — SQL]
    ↓
Staging: stg_ga4_sessions (raw, cleaned, normalized)
    ↓
[Dimensional Layer — SQL]
    ↓
Marts: fact_sessions + dimensions
    ↓
[Reporting & Analytics — SQL Views]
    ↓
Reporting: rpt_channel_performance, rpt_seo_performance, ...
    ↓
[Visualization & Insights]
    ↓
Looker Studio + AI Insights (Claude API)
```

### Dataset Structure

```
GCP Project: bi-portfolio-project

├── staging
│   ├── ga4_sessions              (raw, cleaned GA4 events)
│   ├── semrush_keywords          (SEO keyword data)
│   └── domain_ranks              (competitor domain metrics)
│
├── marts
│   ├── fact_sessions            (star schema fact table)
│   ├── dim_date                 (date dimension)
│   ├── dim_channel              (channel/source dimension)
│   ├── dim_page                 (page/URL dimension)
│   ├── dim_competitor           (competitor dimension)
│   ├── v_channel_summary        (channel aggregates with trends)
│   └── ai_insights              (AI-generated insights)
│
└── reporting
    ├── rpt_sessions_enriched    (semantic layer)
    ├── rpt_channel_performance  (KPIs by channel/device)
    ├── rpt_seo_performance      (rank/category analysis)
    └── rpt_landing_page_performance (landing page KPIs)
```

### Star Schema

**Fact Table:** `fact_sessions`  
One row per GA4 session with behavioral metrics.

**Dimensions:**
- `dim_date` — date attributes (year, month, day, week, weekend flag)
- `dim_channel` — traffic channel grouping (organic, paid search, social, direct, email, other)
- `dim_page` — page/URL attributes (SEMrush-enriched: ranking position, search volume, category)
- `dim_competitor` — competitor domain-level metrics (rank, organic traffic, paid metrics)

**Metrics:**
- Sessions, page views, engaged sessions
- Conversions (purchases > 0), revenue
- Session duration, device category
- Organic ranking position (for SEO analysis)

### URL Consolidation (SEMrush ↔ GA4)

#### The Problem

This project joins SEMrush data (current merch.google domain) with GA4 session data (historical: Nov 2020 – Jan 2021, from the old googlemerchandisestore.com site). These datasets present three major URL normalization challenges:

1. **Hostname mismatch**: GA4 events recorded `googlemerchandisestore.com` while SEMrush tracks `merch.google` — different domains, same store.
2. **Path prefix divergence**: GA4 URLs from the old site prefix all product/category paths with `/Google+Redesign/`, which the new merch.google site does not use. Example:
   - GA4: `https://googlemerchandisestore.com/Google+Redesign/apparel/hoodies`
   - SEMrush: `https://merch.google/apparel/hoodies`
3. **File extension differences**: Legacy GA4 paths use `.html` suffixes for category pages (e.g., `/apparel/hoodies.html`), while modern SEMrush URLs do not.

#### The Solution: Four-Step Consolidation

**Step 1: Python Path Normalization** (`ingestion/semrush_prepare.py`)
- Extract path component from full URL
- Lowercase, remove query parameters, strip trailing slashes
- Example: `https://merch.google/shop/apparel/hoodies?utm_source=sem` → `/shop/apparel/hoodies`

**Step 2: Category Classification** (`ingestion/semrush_prepare.py`)
- Apply 30+ regex rules (defined in `CATEGORY_RULES`) to map normalized paths to business categories (e.g., `apparel/hoodies`, `lifestyle/drinkware`, `brands/youtube`)
- **Special case handling:**
  - Individual product pages (`/product/*`) → excluded (no GA4 equivalent in public dataset)
  - Legacy Magento URLs (`/apparel/*-*.html` with 4+ char slug) → excluded (GA4-absent)
  - Unmatched paths → marked as non-joinable (kept for keyword analysis, not linked to sessions)
- Add `joinable_to_ga4` boolean: True only for merch.google rows with a valid category

**Step 3: SQL Path Normalization** (`transform/sql/staging/stg_ga4_sessions.sql`)
- Apply identical path extraction logic to GA4 landing page URLs
- Strip `/google+redesign/` prefix from GA4 paths:
  - `/google+redesign/apparel/hoodies` → `/apparel/hoodies`
- Apply identical category rules (via `REGEXP_CONTAINS` conditions) to GA4 paths

**Step 4: Three-Way Fallback Resolution** (`transform/sql/staging/stg_ga4_sessions.sql`)
For each GA4 session, resolve to canonical SEMrush page path with priority:
1. **Exact path match** (from `semrush_path_lookup` CTE): `/apparel/hoodies` matches SEMrush's `/apparel/hoodies.html`
2. **Category fallback** (from `semrush_category_lookup` CTE): If no exact match, use highest-volume SEMrush URL in the matched category
3. **Raw path fallback**: If no SEMrush match, keep the normalized GA4 path as-is

#### Key Assumptions & Limitations

- **Historical data only**: GA4 data spans 2020-11-01 to 2021-01-31. Real-time SEMrush data would not match sessions from 2+ years ago in a production scenario.
- **Product pages are intentionally excluded**: Individual product URLs (`/product/*`) exist in SEMrush but not in the GA4 public dataset, so they cannot join to sessions.
- **Category rules are hardcoded**: Rules are synchronized between Python (`semrush_prepare.py`) and SQL (`stg_ga4_sessions.sql`). Changes must be made in both places.
- **Partial coverage**: Not all SEMrush URLs will join to GA4 (joinable rate varies by domain, typically 60–75% for merch.google). Non-joinable rows are retained for competitive benchmarking.

#### Cross-Reference

- **Python implementation:** `ingestion/semrush_prepare.py` (functions `extract_path()`, `assign_category()`)
- **SQL implementation:** `transform/sql/staging/stg_ga4_sessions.sql` (CTEs `semrush_path_lookup`, `semrush_category_lookup`, `first_page_categorised`)
- **Join key:** Both datasets use normalized `page_path` field

---

## AI Insights Layer

### What It Does

The AI Insights Layer automates executive-level analysis of channel performance data. Using Claude API, it generates actionable insights and recommendations that a marketing director can immediately act on — removing the manual effort of data interpretation and enabling faster decision-making.

**Current Capabilities:**
- Analyzes last 30 days of channel performance vs. previous 30 days
- Compares conversion rates, revenue, and trends across channels
- Generates 2-3 bullet points of executive-level recommendations
- Identifies high-performing channels and optimization opportunities
- Stores insights in BigQuery for audit trail and Looker Studio integration

### Architecture

```
BigQuery
├── v_channel_summary view          (aggregated metrics + trends)
│
↓ Python script
├── Query v_channel_summary
├── Format metrics as readable text
│
↓ Claude API (claude-sonnet-4-6)
├── Send channel data + marketing context
├── Receive executive recommendation
│
↓ BigQuery
└── ai_insights table (insight_type='channel_recommendation')
    → Looker Studio (future: surfaced in dashboards)
```

### Dataset & Date Windows

This project uses a fixed historical dataset spanning **2020-11-01 to 2021-01-31**. The `v_channel_summary.sql` view has hardcoded date windows: current period is January 2021 (2021-01-01 to 2021-01-31) and previous period is December 2020 (2020-12-01 to 2020-12-31). In a production environment, these windows would be dynamic—using `CURRENT_DATE()` with a rolling 30-day comparison to enable daily insight generation with fresh data.

### How to Run

#### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 2. Set Up API Keys

Add your Anthropic API key to `.env`:

```bash
cp .env.example .env
# Edit .env and set ANTHROPIC_API_KEY=sk-...
```

Verify BigQuery credentials are configured:
```bash
echo $GOOGLE_APPLICATION_CREDENTIALS
```

#### 3. Create BigQuery Staging Layer

The staging layer transforms raw GA4 events and ingests SEMrush data. Run this first:

```bash
python transform/run/run_staging.py
```

This script:
1. **Builds staging.ga4_sessions**: Queries GA4 public dataset, normalizes paths, extracts metrics
2. **Loads staging.domain_ranks**: Reads domain_ranks_*.csv files from data/raw/, prepares for dim_competitor

#### 4. Create BigQuery Marts Layer

The marts layer contains dimensions, facts, and views that feed reporting and analytics. Tables must be created in **dependency order**: dimensions first, then facts, then views.

**Automated Setup (Recommended):**
```bash
python transform/run/run_marts.py
```

This script runs all SQL files in the correct dependency order:
1. **Dimensions** (no dependencies): `dim_date`, `dim_channel`, `dim_page`, `dim_competitor`
2. **Facts** (depend on dimensions): `fact_sessions`
3. **Views & Analytics tables** (depend on facts): `v_channel_summary`, `ai_insights`

**Manual Setup (if needed):**
```bash
bq query --use_legacy_sql=false < transform/sql/marts/dim_date.sql
bq query --use_legacy_sql=false < transform/sql/marts/dim_channel.sql
bq query --use_legacy_sql=false < transform/sql/marts/dim_page.sql
bq query --use_legacy_sql=false < transform/sql/marts/dim_competitor.sql
bq query --use_legacy_sql=false < transform/sql/marts/fact_sessions.sql
bq query --use_legacy_sql=false < transform/sql/marts/v_channel_summary.sql
bq query --use_legacy_sql=false < transform/sql/marts/ai_insights_table.sql
```

#### 5. Generate Insights

```bash
python ai_insights/generate_insights.py
```

**Output:**
```
Initializing BigQuery client...
Querying channel summary view...
Formatting data for Claude...
Generating insights with Claude API...
Writing insights to BigQuery...

✓ Insights generated and stored successfully!

Generated insight:
TOP CHANNEL: Organic Search leads with 3.2% conversion rate and $45,320 revenue, up 12% vs December
MOMENTUM: Paid Search growing 8% in conversion rate — consider reallocating budget
ACTION: Maintain organic visibility through SEO while capturing short-term demand via Paid Search
```

### Production Deployment

In production, this script would run on a daily schedule via **Cloud Scheduler**:

```yaml
Job: "generate-channel-insights"
Schedule: "0 8 * * *"  # 8 AM daily
Cloud Function: ai_insights/generate_insights.py
Trigger: Pub/Sub topic → Cloud Run
Service Account: analytics-reader (BigQuery + Anthropic access)
Retry Policy: 2 retries on transient failure
Error Notification: Slack webhook on failure
```

**Monitoring & Logging:**
- Cloud Logging: All executions logged with timestamps and error traces
- Cloud Monitoring: Alert on script failure or API latency > 30s
- Looker Studio: ai_insights table surfaced in daily email digest to stakeholders

### Views: v_channel_summary vs rpt_channel_performance

These serve different analytical purposes and should not be confused:

| Aspect | `v_channel_summary` (marts) | `rpt_channel_performance` (reporting) |
|--------|---------------------------|--------------------------------------|
| **Purpose** | AI/ML insights feed | Operational reporting |
| **Grain** | One row per channel_group | One row per (date, channel_group, device_group) |
| **Time Scope** | Aggregated across 30-day periods | Daily detail |
| **Dimensions** | Channel only | Channel + Device + Date |
| **Key Features** | Month-over-month trends for trend analysis | Daily KPIs with engagement metrics |
| **Use Case** | Claude API analysis, trend insights | Looker Studio dashboards, operational monitoring |
| **Metrics** | Revenue, conversion rate, sessions (+ previous period) | Engagement rate, conversion rate, depth metrics |

**When to use:**
- **v_channel_summary**: Running AI analysis (generate_insights.py) or comparing channel performance across periods
- **rpt_channel_performance**: Building dashboards, daily reporting, device-level analysis

---

## Project Structure

```
bi-portfolio/
├── README.md                          (this file)
├── requirements.txt                   (Python dependencies)
├── .env.example                       (environment variables template)
├── .env                               (local configuration — git-ignored)
│
├── transform/
│   ├── run/
│   │   ├── run_staging.py             (build staging layer: ga4_sessions + domain_ranks)
│   │   ├── run_marts.py               (build marts layer: dimensions + fact + views)
│   │   └── run_reporting.py           (build reporting layer: reporting views)
│   └── sql/
│       ├── staging/
│       │   └── stg_ga4_sessions.sql   (raw GA4 events, cleaned + path normalized)
│       ├── marts/
│       │   ├── dim_date.sql
│       │   ├── dim_channel.sql
│       │   ├── dim_page.sql
│       │   ├── dim_competitor.sql
│       │   ├── fact_sessions.sql      (star schema fact table)
│       │   ├── v_channel_summary.sql  (channel aggregates + trends)
│       │   └── ai_insights_table.sql  (insights table schema)
│       └── reporting/
│           ├── rpt_sessions_enriched.sql
│           ├── rpt_channel_performance.sql
│           ├── rpt_seo_performance.sql
│           └── rpt_landing_page_performance.sql
│
├── ai_insights/
│   └── generate_insights.py           (Claude API integration script)
│
├── ingestion/
│   ├── semrush_api_pull.py            (SEMrush API data export — optional)
│   ├── semrush_prepare.py             (SEMrush data cleaning + preparation)
│   └── semrush_to_bq.py               (Load prepared SEMrush data to BigQuery)
│
├── docs/
│   ├── project_summary.md             (project overview)
│   ├── star_schema.dbml               (DBML diagram of star schema)
│   ├── star_schema.png                (visual diagram)
│   └── BI_Portfolio.pdf               (full documentation)
│
├── data/
│   ├── raw/
│   │   ├── organic_keywords_*.csv     (raw SEMrush organic keyword data)
│   │   ├── paid_keywords_*.csv        (raw SEMrush paid keyword data)
│   │   └── domain_ranks_*.csv         (raw SEMrush domain metrics)
│   └── prepared/
│       └── semrush_*.csv              (cleaned/prepared SEMrush data)
│
├── credentials/
│   ├── credentials.json               (GCP service account key — git-ignored)
│   └── token.pickle                   (OAuth2 token cache — git-ignored)
│
└── venv/                              (Python virtual environment)
```

---

## Getting Started

### Prerequisites

- Python 3.9+
- Google Cloud Project with BigQuery enabled
- GCP Service Account with BigQuery and Cloud Logging access
- Anthropic API key (for AI Insights)

### Setup Steps

1. **Clone the repository:**
   ```bash
   git clone <repo-url>
   cd bi-portfolio
   ```

2. **Create a Python virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure GCP credentials:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=./credentials/credentials.json
   ```

5. **Set environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and GCP project ID
   ```

6. **Create BigQuery datasets:**
   ```bash
   # Using bq command-line tool
   bq mk --dataset --location=US $GCP_PROJECT:staging
   bq mk --dataset --location=US $GCP_PROJECT:marts
   bq mk --dataset --location=US $GCP_PROJECT:reporting
   ```

7. **Run the staging layer:**
   ```bash
   python transform/run/run_staging.py
   ```

8. **Run the marts layer:**
   ```bash
   python transform/run/run_marts.py
   ```

9. **Test the AI Insights script:**
   ```bash
   python ai_insights/generate_insights.py
   ```

---

## Execution Order & Dependencies

To successfully build the entire pipeline, run scripts in this order:

1. **Staging Layer** (queries GA4 public dataset, loads CSVs)
   ```bash
   python transform/run/run_staging.py
   ```
   Creates: `staging.ga4_sessions`, `staging.domain_ranks`

2. **Marts Layer** (builds dimensions → facts → views)
   ```bash
   python transform/run/run_marts.py
   ```
   Creates: `dim_date`, `dim_channel`, `dim_page`, `dim_competitor`, `fact_sessions`, `v_channel_summary`, `ai_insights`

3. **Reporting Layer** (optional, creates reporting views)
   ```bash
   python transform/run/run_reporting.py
   ```
   Creates: `rpt_sessions_enriched`, `rpt_channel_performance`, `rpt_seo_performance`, `rpt_landing_page_performance`

4. **AI Insights** (generates Claude-powered insights)
   ```bash
   python ai_insights/generate_insights.py
   ```
   Writes to: `ai_insights` table

---

## Data Sources

### GA4 (Google Analytics 4)
- **Source**: BigQuery public dataset (`bigquery-public-data.ga4_obfuscated_sample_ecommerce`)
- **Time Range**: Nov 2020 – Jan 2021
- **Method**: Direct SQL query in `stg_ga4_sessions.sql` (no Python ingestion script)
- **Processing**: Path normalization, session deduplication, metric aggregation

### SEMrush (Optional)
- **Source**: SEMrush API (via `ingestion/semrush_api_pull.py`)
- **Data Types**: Organic keywords, paid keywords, domain ranks
- **Processing Pipeline**:
  1. `semrush_api_pull.py` → exports CSV files to `data/raw/`
  2. `semrush_prepare.py` → cleans + normalizes paths → `data/prepared/`
  3. `semrush_to_bq.py` → loads prepared data to `staging.semrush_keywords`
  4. Marts SQL files join SEMrush data to GA4 sessions

---

## Key Metrics & KPIs

All aggregated in the reporting layer views:

| Metric | Definition | Business Use |
|--------|-----------|--------------|
| Sessions | Total GA4 session count | Traffic volume |
| Engaged Sessions | Sessions with active engagement (GA4 flag) | Quality of traffic |
| Conversions | Sessions with at least one purchase | Business outcome |
| Conversion Rate | Conversions / Sessions | Efficiency metric |
| Revenue | Total purchase revenue (USD) | Business impact |
| Revenue per Session | Revenue / Sessions | Value per visit |
| Avg Session Duration | Total time / Sessions | Content engagement |
| Avg Page Views | Total page views / Sessions | Content depth |

---

## Roles & Permissions

### Data Analyst / BI Developer
- Read/write access to all BigQuery datasets
- Execute transformation jobs
- Modify SQL views

### Marketing Stakeholder
- Read-only access to reporting views
- Access to Looker Studio dashboards
- Receive daily AI insights digest

### Analytics Engineer
- Full access to staging, marts, reporting layers
- Ability to modify transformations
- Manage API integrations (SEMrush, Anthropic)

---

## Troubleshooting

### BigQuery Connection Issues
```bash
# Test BigQuery access
python -c "from google.cloud import bigquery; print(bigquery.Client().list_datasets())"
```

### AI Insights Script Fails

1. **Verify API Key:**
   ```bash
   echo $ANTHROPIC_API_KEY  # Should not be empty
   ```

2. **Check v_channel_summary view exists:**
   ```bash
   bq query --use_legacy_sql=false "SELECT * FROM \`bi-portfolio-project.marts.v_channel_summary\` LIMIT 1"
   ```

3. **Check logs:**
   ```bash
   python ai_insights/generate_insights.py  # Run with full output
   ```

### Insufficient Data

If scripts return no data, ensure:
- Staging layer has been run (`python transform/run/run_staging.py`)
- BigQuery datasets exist and are accessible
- GA4 public dataset is available in your GCP project

### Staging Layer Fails

If `run_staging.py` fails:
1. Verify `credentials.json` is in `/credentials/` directory
2. Ensure GCP project ID in `.env` matches actual project
3. Check that GA4 public dataset is accessible (it is public, but your credentials need BigQuery read access)

---

## Documentation & Resources

- [Google GA4 API Documentation](https://developers.google.com/analytics/devguides/collection/ga4)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Anthropic Claude API](https://docs.anthropic.com)
- [Looker Studio Guide](https://support.google.com/looker-studio)

---

## Contact & Support

For questions or issues with this BI portfolio:
- Documentation: See `/docs` directory
- Issues: GitHub Issues (if applicable)

---

**Last Updated:** May 2026  
**Status:** In Development (AI Insights Layer v1.0)
