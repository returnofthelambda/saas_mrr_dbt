# Automated SaaS MRR Analytics Pipeline
> **A 3-Part Analytics Engineering Guide: Cloud-Native (Snowflake) or Locally Hosted (DuckDB)**

An end-to-end analytics engineering pipeline that generates synthetic SaaS subscription log data, transforms raw event streams into an automated Monthly Recurring Revenue (MRR) waterfall model with dbt data quality assertions, and renders executive reporting dashboards.

### Business Architecture & Financial Integrity
This pipeline bridges the gap between raw transactional data and board-level financial reporting. By using dbt to enforce schema-level assertions and cumulative window aggregations, it ensures balance sheet integrity across time periods. The resulting models provide automated and auditable MRR metrics. This provides finance and operations teams the confidence in their data to analyze expansion, contraction, and churn with full-cycle accounting precision.

### Choose Your Own Adventure
* **Option A (Cloud-Native):** `Python` -> `Snowflake` -> `dbt-snowflake` -> `Streamlit in Snowflake`
* **Option B (Zero-Cost Local):** `Python` -> `DuckDB` -> `dbt-duckdb` -> `Local Streamlit`

---

### Series Navigation
1. **[Part 1: Environment Setup & Synthetic Data Ingestion](./docs/PART_1_INGESTION.md)**  
   *Terminal isolation (`tmux`/`direnv`), `.env` credential security, modern NumPy `default_rng` lifecycle generation, and loading raw streams into Snowflake or DuckDB.*
2. **[Part 2: Analytics Engineering & Data Governance](./docs/PART_2_DBT.md)**  
   *`dbt` profile setup, staging data cleaning (`stg_`), cumulative window functions in the MRR waterfall mart (`fct_`), schema assertions, and lineage docs.*
3. **[Part 3: Executive Reporting & Visualization](./docs/PART_3_BI.md)**  
   *Building executive-facing financial dashboards using Streamlit in Snowflake (SiS) or locally with Streamlit.*

---

![Streamlit top](./images/streamlit_top.png)

![Streamlit bottom](./images/streamlit_bottom.png)
