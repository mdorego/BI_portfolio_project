/*
  stg_ga4_sessions.sql
  --------------------
  Produces one row per GA4 session from the obfuscated e-commerce public dataset.

  Grain   : one row per session (user_pseudo_id + ga_session_id)
  Source  : bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*
  Range   : 2020-11-01 → 2021-01-31
  Join key: page_path — normalised URL path matching staging.semrush_keywords.page_path.

  Path normalisation (two-step):
    Step 1 — extract path from full URL:
        RTRIM(REGEXP_EXTRACT(LOWER(TRIM(page_location)), r'https?://[^/]+(/[^?#]*)'), '/')
    Step 2 — bridge the GA4 ↔ SEMrush URL structure gap:
        The GA4 dataset records the OLD googlemerchandisestore.com site which prefixed
        every category/product URL with /Google+Redesign/.  SEMrush tracked the NEW
        merch.google site where those same paths have no prefix, and old-style category
        pages carry a .html suffix.  semrush_path_lookup strips .html from SEMrush paths
        and strips /google+redesign from GA4 paths, then resolves each GA4 path to its
        canonical SEMrush page_path (including .html where present).

  Prerequisites:
    staging.semrush_keywords must be loaded before this script runs.

  Deduplication:
    The GA4 public dataset can fire multiple session_start events for the same
    (user_pseudo_id, ga_session_id) within milliseconds.  session_attrs keeps only
    the earliest event_timestamp to guarantee one row per session.
*/

WITH

-- Pull all raw events within the target date window.
events AS (
    SELECT *
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
),

-- Extract named scalar columns from the event_params array once.
-- All downstream CTEs work with clean named columns instead of repeated UNNEST subqueries.
events_extracted AS (
    SELECT
        user_pseudo_id,
        event_date,
        event_name,
        event_timestamp,
        traffic_source,
        device,
        geo,
        ecommerce,
        (SELECT value.int_value
         FROM UNNEST(event_params)
         WHERE key = 'ga_session_id')                          AS ga_session_id,
        (SELECT value.string_value
         FROM UNNEST(event_params)
         WHERE key = 'page_location')                          AS page_location,
        (SELECT value.string_value
         FROM UNNEST(event_params)
         WHERE key = 'session_engaged')                        AS session_engaged,
        (SELECT value.int_value
         FROM UNNEST(event_params)
         WHERE key = 'engagement_time_msec')                   AS engagement_time_msec
    FROM events
),

-- Build a lookup from stripped GA4-style paths to canonical SEMrush page_paths.
-- Strips .html so /apparel/accessories/bags.html is keyed as /apparel/accessories/bags,
-- matching what GA4 produces after the /google+redesign prefix is removed.
-- One SEMrush page_path per stripped key; prefers the .html version on tie.
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

-- Build a lookup from join_category to the highest-volume SEMrush page_path.
-- Used when a GA4 path (e.g. /apparel/google+zip+hoodie) matches a category rule
-- but not any individual SEMrush URL — falls back to the category landing page.
semrush_category_lookup AS (
    SELECT
        join_category,
        page_path AS semrush_page_path
    FROM `bi-portfolio-project.staging.semrush_keywords`
    WHERE domain        = 'merch.google'
      AND source        = 'organic'
      AND join_category IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY join_category
        ORDER BY COALESCE(Search_Volume, 0) DESC, page_path
    ) = 1
),

-- One row per session derived from session_start events.
-- ROW_NUMBER deduplicates sessions where GA4 fires multiple session_start events
-- (observed in checkout flows where several fire within milliseconds).
-- Keeps the earliest event_timestamp to capture the true session-start context.
session_attrs AS (
    SELECT * EXCEPT (rn)
    FROM (
        SELECT
            user_pseudo_id,
            ga_session_id,
            PARSE_DATE('%Y%m%d', event_date)                       AS session_date,
            COALESCE(traffic_source.medium,   '(none)')            AS channel,
            COALESCE(traffic_source.source,   '(direct)')          AS source,
            COALESCE(device.category,         'unknown')           AS device_category,
            COALESCE(geo.country,             'unknown')           AS country,
            -- Landing page from the session_start event's page_location param.
            -- NULLIF catches root-path edge case where RTRIM('/', '/') = ''.
            COALESCE(
                NULLIF(
                    RTRIM(
                        REGEXP_EXTRACT(
                            LOWER(TRIM(COALESCE(page_location, ''))),
                            r'https?://[^/]+(/[^?#]*)'
                        ),
                        '/'
                    ),
                    ''
                ),
                '/'
            )                                                      AS landing_page_path,
            -- session_engaged is a string param: '1' = engaged, '0' = not.
            COALESCE(session_engaged, '0') = '1'                   AS engaged_session,
            -- engagement_time_msec on session_start is cumulative for the session.
            CAST(COALESCE(engagement_time_msec, 0) AS INT64) / 1000 AS session_duration_sec,
            ROW_NUMBER() OVER (
                PARTITION BY user_pseudo_id, ga_session_id
                ORDER BY event_timestamp ASC
            )                                                      AS rn
        FROM events_extracted
        WHERE event_name = 'session_start'
    )
    WHERE rn = 1
),

-- Rank all page_view events within each session by event_timestamp ascending.
-- Rank 1 is the first page the user actually viewed — the SEMrush join key.
page_views_ranked AS (
    SELECT
        user_pseudo_id,
        ga_session_id,
        COALESCE(
            NULLIF(
                RTRIM(
                    REGEXP_EXTRACT(
                        LOWER(TRIM(COALESCE(page_location, ''))),
                        r'https?://[^/]+(/[^?#]*)'
                    ),
                    '/'
                ),
                ''
            ),
            '/'
        )                                                      AS raw_page_path,
        ROW_NUMBER() OVER (
            PARTITION BY user_pseudo_id, ga_session_id
            ORDER BY event_timestamp ASC
        )                                                      AS rn
    FROM events_extracted
    WHERE event_name = 'page_view'
),

-- Step 1: isolate first page_view per session and strip /google+redesign prefix.
-- /google+redesign/apparel/google+zip+hoodie+fc+md → /apparel/google+zip+hoodie+fc+md
first_page_prep AS (
    SELECT
        user_pseudo_id,
        ga_session_id,
        CASE
            WHEN raw_page_path = '/google+redesign'
                THEN '/'
            WHEN STARTS_WITH(raw_page_path, '/google+redesign/')
                THEN SUBSTR(raw_page_path, 17)   -- drop /google+redesign (16 chars), keep leading /
            ELSE raw_page_path
        END                                                    AS ga4_stripped
    FROM page_views_ranked
    WHERE rn = 1
),

-- Step 2: assign a join_category to each stripped GA4 path.
-- Rules mirror semrush_prepare.py CATEGORY_RULES in order; first match wins.
-- cap(?!e) from Python (no negative lookahead in RE2) is split into two conditions.
first_page_categorised AS (
    SELECT
        user_pseudo_id,
        ga_session_id,
        ga4_stripped,
        CASE
            WHEN REGEXP_CONTAINS(ga4_stripped, r'hoodie|sweatshirt|sweats|^/apparel/apparel/hoodies|^/shop/apparel/hoodies')
                THEN 'apparel/hoodies'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'jacket|gilet|^/apparel/apparel/jackets')
                THEN 'apparel/jackets'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/apparel/mens|^/shop/apparel/mens')
                THEN 'apparel/mens'
            WHEN (REGEXP_CONTAINS(ga4_stripped, r'^/apparel/hats|^/shop/apparel/head|hat')
                 OR (REGEXP_CONTAINS(ga4_stripped, r'cap') AND NOT REGEXP_CONTAINS(ga4_stripped, r'cape')))
                THEN 'apparel/headgear'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/apparel/accessories/bags/backpack|backpack')
                THEN 'apparel/bags/backpacks'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/apparel/accessories/bags|^/shop/lifestyle/bags')
                THEN 'apparel/bags'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/apparel/misc/socks|^/shop/apparel/socks|socks')
                THEN 'apparel/socks'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/apparel/accessories')
                THEN 'apparel/accessories'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/apparel(?:/|$)|^/shop/apparel(?:/|$)')
                THEN 'apparel'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'drinkware|bottle|mug|tumbler')
                THEN 'lifestyle/drinkware'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'blanket|towel|^/lifestyle/home')
                THEN 'lifestyle/home'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/lifestyle/accessories/lanyards|lanyard')
                THEN 'lifestyle/accessories'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/lifestyle|^/lifestyle')
                THEN 'lifestyle'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/shop-by-brand/youtube|^/brands/youtube|youtube')
                THEN 'brands/youtube'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/shop-by-brand/android|^/brands/android|android')
                THEN 'brands/android'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/brands/google/google-cloud|google.cloud')
                THEN 'brands/google-cloud'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/brands/gemini|gemini')
                THEN 'brands/gemini'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/brands')
                THEN 'brands'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/collections/chrome-dino|chrome.dino')
                THEN 'collections/chrome-dino'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/collections/emoji|emoji')
                THEN 'collections/emoji'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/collections/google-bike|google.bike|model.bike')
                THEN 'collections/google-bike'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/collections/campus|campus')
                THEN 'collections/campus'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/collections/super-g|super.g')
                THEN 'collections/super-g'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/shop/new(?:/|$)')
                THEN 'new-arrivals'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'sticker|magnet|pin|patch|pen|notebook|keychain|keyring|^/shop/stationery')
                THEN 'stationery'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/checkout')
                THEN 'checkout'
            WHEN REGEXP_CONTAINS(ga4_stripped, r'^/catalogsearch')
                THEN 'search'
            WHEN ga4_stripped = '/'
                THEN 'homepage'
            ELSE NULL
        END                                                    AS ga4_join_category
    FROM first_page_prep
),

-- Step 3: resolve to canonical SEMrush page_path.
-- Priority: (1) exact stripped-path match, (2) category match, (3) stripped GA4 path.
first_page AS (
    SELECT
        fpc.user_pseudo_id,
        fpc.ga_session_id,
        COALESCE(
            spl.semrush_page_path,    -- exact: /apparel/accessories/bags → /apparel/accessories/bags.html
            scl.semrush_page_path,    -- category: any hoodie path → /apparel/apparel/hoodies-sweats.html
            fpc.ga4_stripped          -- fallback: keep stripped path as-is
        )                                                      AS page_path
    FROM first_page_categorised fpc
    LEFT JOIN semrush_path_lookup     spl ON fpc.ga4_stripped      = spl.stripped_path
    LEFT JOIN semrush_category_lookup scl ON fpc.ga4_join_category = scl.join_category
),

-- Aggregate page view count, purchase count, revenue, and engagement per session across all events.
-- engaged_session is derived here (not from session_start) because session_engaged = '1' is only
-- set on events AFTER engagement criteria are met — the session_start event always carries '0'.
session_metrics AS (
    SELECT
        user_pseudo_id,
        ga_session_id,
        COUNTIF(event_name = 'page_view')                      AS page_views,
        COUNTIF(event_name = 'purchase')                       AS purchases,
        LOGICAL_OR(session_engaged = '1')                      AS engaged_session,
        SUM(
            IF(event_name = 'purchase',
               COALESCE(ecommerce.purchase_revenue, 0.0),
               0.0)
        )                                                      AS revenue
    FROM events_extracted
    WHERE ga_session_id IS NOT NULL
    GROUP BY user_pseudo_id, ga_session_id
)

-- Assemble one complete row per session: identity + attributes + entry page + metrics.
SELECT
    CONCAT(sa.user_pseudo_id, '_',
           CAST(sa.ga_session_id AS STRING))                   AS session_id,
    sa.user_pseudo_id,
    sa.ga_session_id,
    sa.session_date,
    sa.channel,
    sa.source,
    sa.device_category,
    sa.country,
    COALESCE(fp.page_path, '/')                                AS page_path,
    sa.landing_page_path,
    COALESCE(sm.engaged_session, FALSE)                        AS engaged_session,
    CAST(sa.session_duration_sec AS INT64)                     AS session_duration_sec,
    COALESCE(sm.page_views, 0)                                 AS page_views,
    COALESCE(sm.purchases,  0)                                 AS purchases,
    COALESCE(sm.revenue,    0.0)                               AS revenue
FROM session_attrs           sa
LEFT JOIN first_page         fp ON sa.user_pseudo_id = fp.user_pseudo_id
                                AND sa.ga_session_id  = fp.ga_session_id
LEFT JOIN session_metrics    sm ON sa.user_pseudo_id = sm.user_pseudo_id
                                AND sa.ga_session_id  = sm.ga_session_id
