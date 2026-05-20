/*
  ai_insights_table.sql
  ---------------------
  Table to store AI-generated insights from Claude API.

  Grain    : one row per insight
  Columns  :
    - generated_at    When the insight was generated
    - insight_type    Category of insight (e.g., 'channel_recommendation')
    - insight_text    The full insight text from Claude
    - created_at      Record insertion timestamp
*/

CREATE TABLE IF NOT EXISTS `bi-portfolio-project.marts.ai_insights` (
    generated_at TIMESTAMP NOT NULL,
    insight_type STRING NOT NULL,
    insight_text STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(generated_at)
OPTIONS(
    description='AI-generated insights from Claude API analysis of channel performance data'
)
