# Mini Data Lakehouse (Bronze → Silver → Gold)

A self-contained, local-first mini lakehouse demonstrating:
- Medallion architecture
- Columnar storage (Parquet)
- DuckDB for analytics
- Python ETL with data quality checks
- CI/CD via GitHub Actions

## Repo Layout
- `data/bronze` — raw synthetic CSV (generated)
- `data/silver` — cleaned Parquet
- `data/gold` — star schema Parquet + `lakehouse.duckdb`
- `etl/` — generators, transforms, DQ checks
- `sql/` — analytics & views
- `notebooks/` — optional exploration
- `.github/workflows/ci.yml` — pipeline

## Quickstart

```bash
# 1) Setup
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

# 2) Generate data & build layers
python etl/generate_synthetic_data.py
python etl/bronze_to_silver.py
python etl/silver_to_gold.py
python etl/build_duckdb.py
python etl/dq_checks.py   # should print "All DQ checks passed."

# 3) Query DuckDB
duckdb data/gold/lakehouse.duckdb -c ".read sql/create_views.sql"
duckdb data/gold/lakehouse.duckdb -c ".read sql/gold_insights.sql"
