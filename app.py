import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="SaaS MRR Dashboard", layout="wide")

@st.cache_data
def load_mrr_data():
    target = os.getenv("TARGET_DB", "snowflake").lower()

    if target == "duckdb":
        import duckdb
        conn = duckdb.connect("data/saas_analytics.duckdb")
        df = conn.query("SELECT * FROM dev.fct_mrr_monthly_waterfall ORDER BY mrr_month").df()
        conn.close()
    else:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        df = session.sql("SELECT * FROM SAAS_ANALYTICS.DEV.FCT_MRR_MONTHLY_WATERFALL ORDER BY MRR_MONTH").to_pandas()

    # Standardize column headers to uppercase across platforms
    df.columns = [c.upper() for c in df.columns]
    return df

df = load_mrr_data()

st.title("SaaS Executive MRR Dashboard")

if df.empty:
    st.warning("No MRR waterfall data found. Please run your dbt models first.")
    st.stop()

latest = df.iloc[-1]

# Executive KPI Cards
col1, col2, col3 = st.columns(3)
col1.metric("Ending MRR", f"${latest['ENDING_MRR']:,.0f}")
col2.metric("New MRR", f"${latest['NEW_MRR']:,.0f}")
col3.metric("Churn MRR", f"${latest['CHURN_MRR']:,.0f}")

st.markdown("---")

# Visual Trend Analysis
st.subheader("MRR Movement Over Time")
st.bar_chart(
        df, 
        x="MRR_MONTH", 
        y=["NEW_MRR", "EXPANSION_MRR", "CONTRACTION_MRR", "CHURN_MRR"]
        )

st.subheader("Ending MRR Trajectory")
st.line_chart(df, x="MRR_MONTH", y="ENDING_MRR")
