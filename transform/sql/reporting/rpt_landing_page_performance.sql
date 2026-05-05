/*
  rpt_landing_page_performance.sql
  --------------------------------
  Landing page performance: traffic volume vs conversion outcomes,
  enriched with SEO data where available.

  Grain   : landing_page_path
  Source  : reporting.rpt_sessions_enriched (session facts)
            marts.dim_page                  (page-level SEO attributes)

  The join uses landing_page_path → dim_page.page_path so that SEO
  attributes describe the page the user first saw, not the current
  browse page (page_path already joined in the enriched view).

  KPI definitions
  ---------------
  sessions            Sessions where this was the entry page.
  engaged_sessions    Sessions with active engagement.
  conversions         Sessions with at least one purchase.
  revenue             Total purchase revenue (USD).
  engagement_rate     engaged_sessions / sessions.
  conversion_rate     conversions / sessions.
  revenue_per_session revenue / sessions — page revenue efficiency.
  avg_page_views      Total page views / sessions — depth of visit.
*/

WITH

enriched AS (
    SELECT * FROM `bi-portfolio-project.reporting.rpt_sessions_enriched`
),

page_seo AS (
    SELECT
        page_path,
        page_category,
        top_keyword,
        organic_position,
        search_volume,
        has_seo_data
    FROM `bi-portfolio-project.marts.dim_page`
)

SELECT
    e.landing_page_path,

    -- SEO attributes for the landing page (NULL when not in dim_page)
    ps.page_category,
    ps.has_seo_data,
    ps.organic_position,
    ps.search_volume,
    ps.top_keyword,
    CASE
        WHEN ps.organic_position BETWEEN 1  AND 3  THEN '1 — Top 3'
        WHEN ps.organic_position BETWEEN 4  AND 10 THEN '2 — Top 10'
        WHEN ps.organic_position BETWEEN 11 AND 20 THEN '3 — Top 20'
        WHEN ps.organic_position BETWEEN 21 AND 50 THEN '4 — Top 50'
        WHEN ps.organic_position > 50              THEN '5 — 51+'
        ELSE                                            '6 — No SEO Data'
    END AS seo_rank_bucket,

    -- Volume
    COUNT(e.session_id)                                              AS sessions,
    COUNTIF(e.is_engaged)                                            AS engaged_sessions,
    COUNTIF(e.is_converted)                                          AS conversions,
    SUM(e.revenue)                                                   AS revenue,

    -- Rates
    SAFE_DIVIDE(COUNTIF(e.is_engaged),   COUNT(e.session_id))        AS engagement_rate,
    SAFE_DIVIDE(COUNTIF(e.is_converted), COUNT(e.session_id))        AS conversion_rate,
    SAFE_DIVIDE(SUM(e.revenue),          COUNT(e.session_id))        AS revenue_per_session,

    -- Depth
    SAFE_DIVIDE(SUM(e.page_views), COUNT(e.session_id))              AS avg_page_views

FROM enriched                   e
LEFT JOIN page_seo              ps ON e.landing_page_path = ps.page_path
GROUP BY
    e.landing_page_path,
    ps.page_category,
    ps.has_seo_data,
    ps.organic_position,
    ps.search_volume,
    ps.top_keyword
