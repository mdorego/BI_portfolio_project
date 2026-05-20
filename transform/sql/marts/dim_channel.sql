/*
  dim_channel.sql
  ---------------
  One row per distinct traffic channel + source combination seen in GA4 sessions,
  enriched with a clean channel_group label for Looker Studio reporting.

  Grain  : one row per (channel, source) pair
  Source : staging.ga4_sessions
*/

CREATE OR REPLACE TABLE `bi-portfolio-project.marts.dim_channel` AS

WITH

-- Collect every distinct (channel, source) pairing that appears in the session data.
-- channel = traffic_source.medium; source = traffic_source.source from GA4.
distinct_channels AS (
    SELECT DISTINCT
        channel,
        source
    FROM `bi-portfolio-project.staging.ga4_sessions`
)

-- Map raw GA4 medium values to the standard channel groups used in reporting.
SELECT
    channel,
    source,
    CASE
        WHEN LOWER(channel) IN ('organic')                                  THEN 'organic search'
        WHEN LOWER(channel) IN ('cpc', 'ppc', 'paid', 'paidsearch')        THEN 'paid search'
        WHEN LOWER(channel) IN ('email', 'e-mail', 'newsletter')           THEN 'email'
        WHEN LOWER(channel) IN ('social', 'social-network',
                                'social-media', 'sm')                      THEN 'social'
        WHEN LOWER(channel) IN ('(none)', 'direct')                        THEN 'direct'
        WHEN LOWER(channel) IN ('referral')                                THEN 'referral'
        ELSE                                                                    'other'
    END                                                                    AS channel_group
FROM distinct_channels
