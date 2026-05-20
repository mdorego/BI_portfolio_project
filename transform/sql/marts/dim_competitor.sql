/*
  dim_competitor.sql
  ------------------
  One row per domain from the SEMrush domain-level authority snapshot.

  Grain   : one row per domain (4 rows total — merch.google + 3 competitors)
  Source  : staging.domain_ranks
            Loaded from data/raw/domain_ranks_*.csv by run_staging.py.
            Columns are renamed to snake_case during the pandas load step.
  Context : merch.google is the target store (is_target = TRUE).
            cafepress.com, redbubble.com, zazzle.com are competitor benchmarks.
*/

CREATE OR REPLACE TABLE `bi-portfolio-project.marts.dim_competitor` AS

SELECT
    domain,
    semrush_rank,
    organic_keywords,
    organic_traffic,
    organic_cost,
    paid_keywords,
    domain = 'merch.google'   AS is_target
FROM `bi-portfolio-project.staging.domain_ranks`
