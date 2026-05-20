#!/usr/bin/env python3
"""
generate_insights.py
---------------------
Generate AI insights from channel performance data using Claude API.

Workflow:
1. Query mart.v_channel_summary view from BigQuery
2. Format channel metrics as readable text
3. Call Anthropic Claude API for executive-level recommendations
4. Write insights to mart.ai_insights table with insight_type='channel_recommendation'

Configuration:
- Requires ANTHROPIC_API_KEY in .env
- Uses GOOGLE_APPLICATION_CREDENTIALS for BigQuery authentication
"""

import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from anthropic import Anthropic

# Load environment variables from .env
load_dotenv()

# Configuration
GCP_PROJECT = os.getenv("GCP_PROJECT", "bi-portfolio-project")
BQ_DATASET = "marts"
CHANNEL_SUMMARY_VIEW = f"`{GCP_PROJECT}.{BQ_DATASET}.v_channel_summary`"
INSIGHTS_TABLE = f"`{GCP_PROJECT}.{BQ_DATASET}.ai_insights`"
ANTHROPIC_MODEL = "claude-sonnet-4-6"


def get_bigquery_client() -> bigquery.Client:
    """Initialize and return BigQuery client."""
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        client.get_dataset(BQ_DATASET)  # Verify access
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to initialize BigQuery client: {e}")


def query_channel_summary(client: bigquery.Client) -> pd.DataFrame:
    """Query the v_channel_summary view and return as DataFrame."""
    query = f"SELECT * FROM {CHANNEL_SUMMARY_VIEW}"

    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            raise ValueError("No data returned from v_channel_summary view")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to query channel summary: {e}")


def format_channel_data(df: pd.DataFrame) -> str:
    """Format channel summary DataFrame as readable text for Claude."""
    lines = ["Channel Performance Summary (2021-01-01 to 2021-01-31):\n"]

    for _, row in df.iterrows():
        channel = row["channel_group"]
        sessions = row["sessions"]
        conversions = row["conversions"]
        conv_rate = row["conversion_rate"]
        revenue = row["revenue"]
        prev_conv_rate = row["prev_conversion_rate"]
        prev_revenue = row["prev_revenue"]

        # Calculate trend indicators
        rate_change = conv_rate - prev_conv_rate
        revenue_change = revenue - prev_revenue

        lines.append(f"Channel: {channel}")
        lines.append(f"  Sessions: {sessions:,}")
        lines.append(f"  Conversions: {conversions:,}")
        lines.append(f"  Conversion Rate: {conv_rate}% (Previous: {prev_conv_rate}%, Change: {rate_change:+.2f}%)")
        lines.append(f"  Revenue: ${revenue:,.2f} (Previous: ${prev_revenue:,.2f}, Change: ${revenue_change:+,.2f})")
        lines.append("")

    return "\n".join(lines)


def generate_insights(channel_data: str) -> str:
    """
    Call Anthropic Claude API to generate channel insights.

    Returns insight text from Claude.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "ANTHROPIC_API_KEY not found in environment. "
            "Please set it in .env file."
        )

    client = Anthropic(api_key=api_key)

    prompt = f"""You are a marketing analytics expert providing insights for a marketing director at an e-commerce company.

Based on the channel performance data below, provide a brief insight block that fits in a small dashboard card.

Rules:
- 3 bullet points maximum, each 1 sentence
- No markdown: no asterisks, no hashtags, no bold, no hyphens for bullets
- Use a line return between each point
- Use ALL CAPS for emphasis instead of bold
- Start each point with a capitalized label like "TOP CHANNEL:" or "ACTION:"
- Reference specific numbers

{channel_data}"""

    try:
        message = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as e:
        raise RuntimeError(f"Failed to call Claude API: {e}")


def write_insights_to_bigquery(
    client: bigquery.Client, insight_text: str
) -> bool:
    """
    Write generated insight to ai_insights table.

    Returns True on success, False otherwise.
    """
    
    from datetime import timezone

    now = datetime.now(timezone.utc)

    insert_query = f"""
    INSERT INTO {INSIGHTS_TABLE} (generated_at, insight_type, insight_text, created_at)
    VALUES (@generated_at, @insight_type, @insight_text, @created_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("generated_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("insight_type", "STRING", "channel_recommendation"),
            bigquery.ScalarQueryParameter("insight_text", "STRING", insight_text),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
        ]
    )

    try:
        job = client.query(insert_query, job_config=job_config)
        job.result()
        return True
    except Exception as e:
        raise RuntimeError(f"Failed to insert insights into BigQuery: {e}")


def main():
    """Main workflow: query → format → generate → insert."""
    try:
        print("Initializing BigQuery client...")
        bq_client = get_bigquery_client()

        print("Querying channel summary view...")
        channel_df = query_channel_summary(bq_client)

        print("Formatting data for Claude...")
        formatted_data = format_channel_data(channel_df)

        print("Generating insights with Claude API...")
        insights = generate_insights(formatted_data)

        print("Writing insights to BigQuery...")
        write_insights_to_bigquery(bq_client, insights)

        print("\n✓ Insights generated and stored successfully!")
        print(f"\nGenerated insight:\n{insights}\n")

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
