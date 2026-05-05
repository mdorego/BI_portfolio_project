/*
  rpt_sessions_enriched.sql
  -------------------------
  Row-level semantic view: one row per GA4 session, enriched with
  standardized flags and derived dimensions ready for BI tools.

  Grain      : one row per session
  Primary key: session_id
  Source     : marts.fact_sessions (base)
  Extra dims : marts.dim_date  — time attributes not stored in fact
               marts.dim_page  — top_keyword + search_volume not stored in fact

  New columns added on top of the fact layer:
    device_group     — Mobile (mobile + tablet) vs Desktop
    seo_rank_bucket  — categorical bucket from organic_position
    is_engaged       — BOOLEAN flag, alias for engaged_session
    is_converted     — BOOLEAN flag, TRUE when purchases > 0
*/

SELECT
    -- Session identity
    fs.session_id,
    fs.user_pseudo_id,
    fs.ga_session_id,

    -- Date dimension (raw key + time attributes from dim_date)
    fs.session_date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.week_of_year,
    dd.day_name,
    dd.is_weekend,

    -- Channel & acquisition
    fs.channel,
    fs.source,
    fs.channel_group,

    -- Device: raw category + standardised group
    fs.device_category,
    CASE
        WHEN LOWER(fs.device_category) IN ('mobile', 'tablet') THEN 'Mobile'
        ELSE 'Desktop'
    END AS device_group,

    -- Geography
    fs.country,

    -- Page & SEO attributes
    fs.page_path,
    fs.page_category,
    fs.landing_page_path,
    fs.has_seo_data,
    fs.organic_position,
    dp.top_keyword,
    dp.search_volume,

    -- Rank bucket: maps a raw position number to an ordered categorical label
    CASE
        WHEN fs.organic_position BETWEEN 1  AND 3  THEN '1 — Top 3'
        WHEN fs.organic_position BETWEEN 4  AND 10 THEN '2 — Top 10'
        WHEN fs.organic_position BETWEEN 11 AND 20 THEN '3 — Top 20'
        WHEN fs.organic_position BETWEEN 21 AND 50 THEN '4 — Top 50'
        WHEN fs.organic_position > 50              THEN '5 — 51+'
        ELSE                                            '6 — No SEO Data'
    END AS seo_rank_bucket,

    -- Behaviour metrics (raw)
    fs.session_duration_sec,
    fs.page_views,
    fs.purchases,
    fs.revenue,

    -- Standardised Boolean flags
    fs.engaged_session                   AS is_engaged,
    (fs.purchases > 0)                   AS is_converted

FROM `bi-portfolio-project.marts.fact_sessions`  fs
LEFT JOIN `bi-portfolio-project.marts.dim_date`  dd ON fs.date_id   = dd.date_id
LEFT JOIN `bi-portfolio-project.marts.dim_page`  dp ON fs.page_path = dp.page_path
