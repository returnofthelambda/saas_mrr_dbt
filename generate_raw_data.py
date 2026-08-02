# ~/code/saas_mrr_dbt/generate_raw_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize modern NumPy Generator
rng = np.random.default_rng(42)
n_customers = 100

# rng.integers replaces np.random.randint
start_dates = [datetime(2025, 1, 1) + timedelta(days=int(x)) for x in rng.integers(0, 365, size=n_customers)]
records = []

for i in range(n_customers):
    cust_id = f"CUST_{1000 + i}"
    c_start = start_dates[i]
    # Base tiers of $100, $250, $500, and $1,000
    initial_mrr = rng.choice([100, 250, 500, 1000])
      
    # Initial subscription
    records.append({
        "event_id": f"EVT_{len(records)+1}",
        "customer_id": cust_id,
        "event_type": "NEW",
        "event_date": c_start.strftime("%Y-%m-%d"),
        "mrr_change": initial_mrr,
        "current_mrr": initial_mrr
    })
      
    # Simulate potential expansion/contraction or churn
    if rng.random() > 0.3:
        change_date = c_start + timedelta(days=int(rng.integers(60, 180)))
        if change_date < datetime(2026, 8, 1):
            # 50% chance of Expansion, 20% chance of Contraction, and 30% chance of Churn
            change_type = rng.choice(["EXPANSION", "CONTRACTION", "CHURN"],
                                     p=[0.5, 0.2, 0.3]
                                    )
            if change_type == "EXPANSION":
                # Equal weighted chance for Subscription Expansion Revenue
                # For thsi example we're using fixed revenue addons, but you could also use percentages and multiply by the initial_mrr value
                delta = rng.choice([50, 100, 250])
                new_mrr = initial_mrr + delta
            elif change_type == "CONTRACTION":
                delta = -min(initial_mrr / 2, 100)
                new_mrr = initial_mrr + delta
            else: # CHURN
                delta = -initial_mrr
                new_mrr = 0
                  
            records.append({
                "event_id": f"EVT_{len(records)+1}",
                "customer_id": cust_id,
                "event_type": change_type,
                "event_date": change_date.strftime("%Y-%m-%d"),
                "mrr_change": delta,
                "current_mrr": new_mrr
            })

df = pd.DataFrame(records)

# Push to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse='COMPUTE_WH',
    database='SAAS_ANALYTICS',
    schema='RAW'
)

cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS SAAS_ANALYTICS;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS.RAW;")
cursor.execute("USE SCHEMA SAAS_ANALYTICS.RAW;")

# Uppercase column names for Snowflake schema normaliztion
df.columns = [c.upper() for c in df.columns]

write_pandas(conn, df, "SUBSCRIPTION_EVENTS", auto_create_table=True, overwrite=True)
print("Successfully generated and loaded RAW.SUBSCRIPTION_EVENTS to Snowflake.")

