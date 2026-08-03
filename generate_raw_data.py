# ~/code/saas_mrr_dbt/generate_raw_data.py
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Parse CLI arguments for database selection
parser = argparse.ArgumentParser(description="Generate synthetic SaaS MRR data.")
parser.add_argument(
    "--target",
    choices=["snowflake", "duckdb"],
    default=os.getenv("TARGET_DB", "snowflake"),
    help="Target database platform (default: snowflake)",
)
args = parser.parse_args()

# Initialize modern NumPy Generator
rng = np.random.default_rng(42)
n_customers = 100

start_dates = [
    datetime(2025, 1, 1) + timedelta(days=int(x))
    for x in rng.integers(0, 365, size=n_customers)
]
records = []

for i in range(n_customers):
  cust_id = f"CUST_{1000 + i}"
  c_start = start_dates[i]
  initial_mrr = rng.choice([100, 250, 500, 1000])

  records.append({
      "event_id": f"EVT_{len(records)+1}",
      "customer_id": cust_id,
      "event_type": "NEW",
      "event_date": c_start.strftime("%Y-%m-%d"),
      "mrr_change": initial_mrr,
      "current_mrr": initial_mrr,
  })

  if rng.random() > 0.3:
    change_date = c_start + timedelta(days=int(rng.integers(60, 180)))
    if change_date < datetime(2026, 8, 1):
      change_type = rng.choice(
          ["EXPANSION", "CONTRACTION", "CHURN"], p=[0.5, 0.2, 0.3]
      )
      if change_type == "EXPANSION":
        delta = rng.choice([50, 100, 250])
        new_mrr = initial_mrr + delta
      elif change_type == "CONTRACTION":
        delta = -min(initial_mrr / 2, 100)
        new_mrr = initial_mrr + delta
      else:  # CHURN
        delta = -initial_mrr
        new_mrr = 0

      records.append({
          "event_id": f"EVT_{len(records)+1}",
          "customer_id": cust_id,
          "event_type": change_type,
          "event_date": change_date.strftime("%Y-%m-%d"),
          "mrr_change": delta,
          "current_mrr": new_mrr,
      })

df = pd.DataFrame(records)

# Uppercase column names for database identifier normalization
df.columns = [c.upper() for c in df.columns]

if args.target == "duckdb":
  import duckdb

  os.makedirs("data", exist_ok=True)
  conn = duckdb.connect("data/saas_analytics.duckdb")
  conn.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
  conn.execute(
      "CREATE OR REPLACE TABLE RAW.SUBSCRIPTION_EVENTS AS SELECT * FROM df"
  )
  conn.close()
  print(
      "Successfully generated and loaded RAW.SUBSCRIPTION_EVENTS to DuckDB"
      " (data/saas_analytics.duckdb)."
  )

elif args.target == "snowflake":
  import snowflake.connector
  from snowflake.connector.pandas_tools import write_pandas

  conn = snowflake.connector.connect(
      user=os.getenv("SNOWFLAKE_USER"),
      password=os.getenv("SNOWFLAKE_PASSWORD"),
      account=os.getenv("SNOWFLAKE_ACCOUNT"),
      warehouse="COMPUTE_WH",
      database="SAAS_ANALYTICS",
      schema="RAW",
  )

  cursor = conn.cursor()
  cursor.execute("CREATE DATABASE IF NOT EXISTS SAAS_ANALYTICS;")
  cursor.execute("CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS.RAW;")
  cursor.execute("USE SCHEMA SAAS_ANALYTICS.RAW;")

  write_pandas(
      conn, df, "SUBSCRIPTION_EVENTS", auto_create_table=True, overwrite=True
  )
  print(
      "Successfully generated and loaded RAW.SUBSCRIPTION_EVENTS to Snowflake."
  )
