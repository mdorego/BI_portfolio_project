/*
  dim_date.sql
  ------------
  One row per calendar date covering the full GA4 dataset window.

  Grain  : one row per date
  Range  : 2020-11-01 → 2021-01-31 (matches stg_ga4_sessions date range)
  Source : GENERATE_DATE_ARRAY — no upstream table dependency
*/

SELECT
    date_val                                           AS date_id,
    EXTRACT(YEAR        FROM date_val)                 AS year,
    EXTRACT(MONTH       FROM date_val)                 AS month,
    FORMAT_DATE('%B',   date_val)                      AS month_name,
    EXTRACT(WEEK        FROM date_val)                 AS week_of_year,
    EXTRACT(DAYOFWEEK   FROM date_val)                 AS day_of_week,  -- 1 = Sunday
    FORMAT_DATE('%A',   date_val)                      AS day_name,
    EXTRACT(DAYOFWEEK   FROM date_val) IN (1, 7)       AS is_weekend
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2020-11-01', DATE '2021-01-31', INTERVAL 1 DAY)
) AS date_val
