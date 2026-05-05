/*
  fact_sessions.sql
  -----------------
  One row per GA4 session, enriched with attributes from all dimension tables.

  Grain      : one row per session
  Primary key: session_id (CONCAT(user_pseudo_id, '_', ga_session_id))
  Source     : staging.ga4_sessions (fact base)
  Dimensions : marts.dim_date     → date attributes for session_date
               marts.dim_channel  → channel_group label
               marts.dim_page     → SEMrush page category and organic position
*/

WITH

-- Base session rows from the staging layer — all columns carried forward.
sessions AS (
    SELECT *
    FROM `bi-portfolio-project.staging.ga4_sessions`
)

-- Enrich each session row with dimension attributes for Looker Studio analysis.
SELECT
    -- Session identity
    s.session_id,
    s.user_pseudo_id,
    s.ga_session_id,

    -- Date dimension attributes
    s.session_date,
    dd.date_id,

    -- Channel dimension attributes
    s.channel,
    s.source,
    COALESCE(dc.channel_group, 'other')    AS channel_group,

    -- Device and geo (low cardinality — stored inline, not pulled from a dim)
    s.device_category,
    s.country,

    -- Page dimension attributes
    s.page_path,
    dp.page_category,
    dp.organic_position,
    dp.has_seo_data,

    -- Session behaviour metrics
    s.landing_page_path,
    s.engaged_session,
    s.session_duration_sec,
    s.page_views,
    s.purchases,
    s.revenue

FROM sessions                                    s
LEFT JOIN `bi-portfolio-project.marts.dim_date`    dd ON s.session_date = dd.date_id
LEFT JOIN `bi-portfolio-project.marts.dim_channel` dc ON s.channel      = dc.channel
                                                      AND s.source       = dc.source
LEFT JOIN `bi-portfolio-project.marts.dim_page`    dp ON s.page_path    = dp.page_path
