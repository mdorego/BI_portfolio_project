/*
  rpt_channel_performance.sql
  ---------------------------
  Channel-level marketing performance: one row per (channel_group, device_group).

  Grain   : session_date × channel_group × device_group
  Source  : reporting.rpt_sessions_enriched

  KPI definitions
  ---------------
  sessions              Total session count for this channel/device slice.
  engaged_sessions      Sessions where the user actively engaged (GA4 engaged_session flag).
  conversions           Sessions that resulted in at least one purchase.
  revenue               Total purchase revenue (USD) attributed to this slice.
  engagement_rate       engaged_sessions / sessions  — how often visitors engage.
  conversion_rate       conversions / sessions       — how often visitors buy.
  revenue_per_session   revenue / sessions           — average revenue value per visit.
  avg_session_duration  Total engaged time (sec) / sessions.
  avg_page_views        Total page views / sessions  — proxy for content depth.
*/

WITH enriched AS (
    SELECT * FROM `bi-portfolio-project.reporting.rpt_sessions_enriched`
)

SELECT
    session_date,
    channel_group,
    device_group,

    -- Volume
    COUNT(session_id)                                              AS sessions,
    COUNTIF(is_engaged)                                            AS engaged_sessions,
    COUNTIF(is_converted)                                          AS conversions,
    SUM(revenue)                                                   AS revenue,

    -- Rates  (SAFE_DIVIDE guards against zero-session slices)
    SAFE_DIVIDE(COUNTIF(is_engaged),  COUNT(session_id))           AS engagement_rate,
    SAFE_DIVIDE(COUNTIF(is_converted), COUNT(session_id))          AS conversion_rate,
    SAFE_DIVIDE(SUM(revenue),          COUNT(session_id))          AS revenue_per_session,

    -- Depth
    SAFE_DIVIDE(SUM(session_duration_sec), COUNT(session_id))      AS avg_session_duration_sec,
    SAFE_DIVIDE(SUM(page_views),           COUNT(session_id))      AS avg_page_views

FROM enriched
GROUP BY
    session_date,
    channel_group,
    device_group
