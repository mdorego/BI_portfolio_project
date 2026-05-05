/*
  dim_page.sql
  ------------
  One row per distinct page_path seen in GA4 sessions,
  enriched with the best-ranked SEMrush organic keyword for that page.

  Grain   : one row per page_path
  Source  : bigquery-public-data GA4 events (universe of pages — all page_views)
            staging.ga4_sessions (UNION to guarantee zero orphans in fact_sessions)
            staging.semrush_keywords (organic enrichment, merch.google only)
  Join key: page_path (exact match — both sides normalised identically)

  Page universe: reads all page_view events from raw GA4, normalises paths with the
  same two-step logic used in stg_ga4_sessions (extract path, strip /google+redesign),
  then resolves to the canonical SEMrush page_path via an exact stripped-path lookup.
  staging.ga4_sessions is unioned in so that category-collapsed entry-page paths
  (e.g. /apparel/apparel/hoodies-sweats.html written by the session-level category
  lookup) are always present — preventing orphaned page_paths in fact_sessions.

  Where multiple SEMrush keywords rank for the same page_path,
  the keyword with the lowest (best) Position is selected as the top keyword.
  All GA4 page paths are retained; SEMrush columns are NULL where no match exists.
*/

WITH

-- All distinct page paths ever viewed across the full GA4 event stream.
-- Applies the same two-step normalisation as stg_ga4_sessions:
--   Step 1 — extract path from full URL and lowercase.
--   Step 2 — strip the /google+redesign prefix used by the old merchandisestore site.
raw_page_views AS (
    SELECT DISTINCT
        CASE
            WHEN raw_path = '/google+redesign'
                THEN '/'
            WHEN STARTS_WITH(raw_path, '/google+redesign/')
                THEN SUBSTR(raw_path, 17)
            ELSE raw_path
        END AS ga4_path
    FROM (
        SELECT
            COALESCE(
                NULLIF(
                    RTRIM(
                        REGEXP_EXTRACT(
                            LOWER(TRIM(COALESCE(
                                (SELECT value.string_value
                                 FROM UNNEST(event_params)
                                 WHERE key = 'page_location'),
                                ''
                            ))),
                            r'https?://[^/]+(/[^?#]*)'
                        ),
                        '/'
                    ),
                    ''
                ),
                '/'
            ) AS raw_path
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
          AND event_name = 'page_view'
    )
),

-- Exact-match lookup: resolves a stripped GA4 path to its canonical SEMrush page_path.
-- Strips .html from SEMrush URLs so /apparel/apparel/hoodies-sweats maps to
-- /apparel/apparel/hoodies-sweats.html.  One SEMrush page_path per stripped key.
semrush_path_lookup AS (
    SELECT
        REGEXP_REPLACE(page_path, r'\.html$', '') AS stripped_path,
        page_path                                  AS semrush_page_path
    FROM `bi-portfolio-project.staging.semrush_keywords`
    WHERE domain     = 'merch.google'
      AND source     = 'organic'
      AND page_path IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY REGEXP_REPLACE(page_path, r'\.html$', '')
        ORDER BY LENGTH(page_path) DESC, page_path
    ) = 1
),

-- Universe of pages:
--   1. All GA4 page_view paths resolved to their canonical SEMrush form (exact match only).
--   2. UNIONed with staging.ga4_sessions page_paths so that category-collapsed entry
--      pages (written by the session-level category lookup in stg_ga4_sessions) are
--      always present, guaranteeing fact_sessions has zero orphaned page_paths.
distinct_pages AS (
    SELECT DISTINCT COALESCE(spl.semrush_page_path, rpv.ga4_path) AS page_path
    FROM raw_page_views rpv
    LEFT JOIN semrush_path_lookup spl ON rpv.ga4_path = spl.stripped_path

    UNION DISTINCT

    SELECT DISTINCT page_path
    FROM `bi-portfolio-project.staging.ga4_sessions`
),

-- Rank SEMrush organic keywords per page by position ascending (1 = best rank).
-- Restricted to merch.google organic rows — the only rows joinable to GA4 paths.
semrush_organic_ranked AS (
    SELECT
        page_path,
        join_category,
        Keyword        AS keyword,
        Position       AS organic_position,
        Search_Volume  AS search_volume,
        ROW_NUMBER() OVER (
            PARTITION BY page_path
            ORDER BY Position ASC
        )              AS rn
    FROM `bi-portfolio-project.staging.semrush_keywords`
    WHERE domain  = 'merch.google'
      AND source  = 'organic'
      AND page_path IS NOT NULL
),

-- Keep only the top-ranked (lowest position number) keyword per page_path.
best_keyword AS (
    SELECT
        page_path,
        join_category,
        keyword,
        organic_position,
        search_volume
    FROM semrush_organic_ranked
    WHERE rn = 1
)

-- Left-join all GA4 pages to their best SEMrush keyword.
-- has_seo_data is TRUE only when a SEMrush row matched this page_path.
SELECT
    dp.page_path,
    bk.join_category                       AS page_category,
    bk.keyword                             AS top_keyword,
    bk.organic_position,
    bk.search_volume,
    bk.page_path IS NOT NULL               AS has_seo_data
FROM distinct_pages  dp
LEFT JOIN best_keyword bk ON dp.page_path = bk.page_path
