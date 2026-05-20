/*
  v_channel_summary.sql
  ---------------------
  Channel summary view aggregated by channel_group with trend comparison.
  Compares 2021-01-01 to 2021-01-31 (current) vs 2020-12-01 to 2020-12-31 (previous).

  Grain       : one row per channel_group
  Source      : marts.fact_sessions (base)
  Dimensions  : marts.dim_date   → date attributes
               marts.dim_channel → channel_group label

  Metrics (current period, 2021-01-01 to 2021-01-31):
  - sessions            Total session count
  - conversions         Sessions with at least one purchase
  - conversion_rate     conversions / sessions (%)
  - revenue             Total purchase revenue (USD)
  - avg_revenue_per_session

  Metrics (previous period, 2020-12-01 to 2020-12-31):
  - prev_sessions
  - prev_conversions
  - prev_conversion_rate
  - prev_revenue
*/

CREATE OR REPLACE VIEW `bi-portfolio-project.marts.v_channel_summary` AS

WITH
-- Base sessions enriched with channel grouping
enriched AS (
    SELECT
        fs.session_date,
        COALESCE(dc.channel_group, 'other')  AS channel_group,
        fs.session_id,
        fs.purchases,
        fs.revenue
    FROM `bi-portfolio-project.marts.fact_sessions` fs
    LEFT JOIN `bi-portfolio-project.marts.dim_channel` dc
        ON fs.channel = dc.channel
        AND fs.source = dc.source
),

-- Current period: 2021-01-01 to 2021-01-31 (last 30 days of available data)
current_period AS (
    SELECT
        e.channel_group,
        COUNT(DISTINCT e.session_id)                  AS sessions,
        COUNTIF(e.purchases > 0)                      AS conversions,
        ROUND(
            SAFE_DIVIDE(
                COUNTIF(e.purchases > 0),
                COUNT(DISTINCT e.session_id)
            ) * 100,
            2
        )                                             AS conversion_rate,
        ROUND(SUM(COALESCE(e.revenue, 0)), 2)        AS revenue,
        ROUND(
            SAFE_DIVIDE(
                SUM(COALESCE(e.revenue, 0)),
                COUNT(DISTINCT e.session_id)
            ),
            2
        )                                             AS avg_revenue_per_session
    FROM enriched e
    WHERE e.session_date >= '2021-01-01'
        AND e.session_date <= '2021-01-31'
    GROUP BY e.channel_group
),

-- Previous period: 2020-12-01 to 2020-12-31 (30 days before current)
previous_period AS (
    SELECT
        e.channel_group,
        COUNT(DISTINCT e.session_id)                  AS prev_sessions,
        COUNTIF(e.purchases > 0)                      AS prev_conversions,
        ROUND(
            SAFE_DIVIDE(
                COUNTIF(e.purchases > 0),
                COUNT(DISTINCT e.session_id)
            ) * 100,
            2
        )                                             AS prev_conversion_rate,
        ROUND(SUM(COALESCE(e.revenue, 0)), 2)        AS prev_revenue,
        ROUND(
            SAFE_DIVIDE(
                SUM(COALESCE(e.revenue, 0)),
                COUNT(DISTINCT e.session_id)
            ),
            2
        )                                             AS prev_avg_revenue_per_session
    FROM enriched e
    WHERE e.session_date >= '2020-12-01'
        AND e.session_date <= '2020-12-31'
    GROUP BY e.channel_group
)

-- Join current and previous periods
SELECT
    c.channel_group,
    c.sessions,
    c.conversions,
    c.conversion_rate,
    c.revenue,
    c.avg_revenue_per_session,
    COALESCE(p.prev_sessions, 0)                      AS prev_sessions,
    COALESCE(p.prev_conversions, 0)                   AS prev_conversions,
    COALESCE(p.prev_conversion_rate, 0)               AS prev_conversion_rate,
    COALESCE(p.prev_revenue, 0)                       AS prev_revenue,
    COALESCE(p.prev_avg_revenue_per_session, 0)       AS prev_avg_revenue_per_session
FROM current_period c
LEFT JOIN previous_period p ON c.channel_group = p.channel_group
ORDER BY c.revenue DESC
