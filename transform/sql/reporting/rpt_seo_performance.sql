/*
  rpt_seo_performance.sql
  -----------------------
  SEO-to-outcome view: how organic search ranking correlates with
  engagement and conversion at the category level.

  Grain   : seo_rank_bucket × page_category
  Source  : reporting.rpt_sessions_enriched

  KPI definitions
  ---------------
  sessions              Sessions that landed on a page in this rank/category bucket.
  engaged_sessions      Sessions with active engagement.
  conversions           Sessions with at least one purchase.
  revenue               Total purchase revenue (USD).
  best_organic_position Lowest (best) rank number within the bucket/category.
  avg_search_volume     Average monthly search volume across matching pages.
  engagement_rate       engaged_sessions / sessions.
  conversion_rate       conversions / sessions.
  revenue_per_session   revenue / sessions.

  Note: sessions with no SEO data appear in bucket '6 — No SEO Data'
  so the view covers all sessions and no data is silently excluded.
*/

WITH enriched AS (
    SELECT * FROM `bi-portfolio-project.reporting.rpt_sessions_enriched`
)

SELECT
    session_date,
    seo_rank_bucket,
    page_category,

    -- Representative SEO signals for this slice
    MIN(organic_position)                                          AS best_organic_position,
    AVG(CAST(search_volume AS FLOAT64))                            AS avg_search_volume,

    -- Volume
    COUNT(session_id)                                              AS sessions,
    COUNTIF(is_engaged)                                            AS engaged_sessions,
    COUNTIF(is_converted)                                          AS conversions,
    SUM(revenue)                                                   AS revenue,

    -- Rates
    SAFE_DIVIDE(COUNTIF(is_engaged),   COUNT(session_id))          AS engagement_rate,
    SAFE_DIVIDE(COUNTIF(is_converted), COUNT(session_id))          AS conversion_rate,
    SAFE_DIVIDE(SUM(revenue),          COUNT(session_id))          AS revenue_per_session

FROM enriched
GROUP BY
    session_date,
    seo_rank_bucket,
    page_category
