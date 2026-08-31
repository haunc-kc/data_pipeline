import sys
import os
import argparse

_parser = argparse.ArgumentParser()
_parser.add_argument("--repo-root")
_parser.add_argument("--pipeline-catalog")
_parser.add_argument("--pipeline-schema")
_args, _ = _parser.parse_known_args()
_repo_root = _args.repo_root or os.getcwd()
sys.path.insert(0, _repo_root)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from utils.hashing import make_row_hash
from datetime import datetime, timezone, timedelta
from utils.init_load import initial_load



# Hau test - 16:40
CATALOG       = _args.pipeline_catalog or os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA        = _args.pipeline_schema  or os.getenv("PIPELINE_SCHEMA",  "mention_dw")

CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.etl_fact_pipeline_config"


# Read all entries from the control table (single Spark job)
control_rows = spark.table(CONTROL_TABLE).collect()

# Build one UNION ALL query to get all max dates in a single Spark job
union_parts = []
for row in control_rows:
    tbl = row["table_name"]
    col = row["column_name"]
    union_parts.append(
        f"SELECT '{tbl}' AS table_name, '{col}' AS column_name, "
        f"CAST(MAX({col}) AS DATE) AS max_date "
        f"FROM {CATALOG}.{SCHEMA}.{tbl}"
    )

max_dates_rows = spark.sql(" UNION ALL ".join(union_parts)).collect()

# Compute prev_date in Python (no Spark round-trip needed for date math)
prev_days_map = {(r["table_name"], r["column_name"]): r["previous_days"] for r in control_rows}
updates = []

for r in max_dates_rows:
    tbl, col, max_date = r["table_name"], r["column_name"], r["max_date"]
    if max_date is not None:
        prev_date = str(max_date - timedelta(days=prev_days_map[(tbl, col)]))
        updates.append((tbl, col, prev_date))
        print(f"[OK] {tbl}.{col} → date_control = {prev_date}")
    else:
        print(f"[SKIP] {tbl}.{col}: no data found, skipping update.")

# Single MERGE to update all rows at once (one Delta transaction)
if updates:
    updates_df = spark.createDataFrame(updates, ["table_name", "column_name", "date_control"])
    updates_df.createOrReplaceTempView("_v_etl_updates")
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS t
        USING _v_etl_updates AS s
        ON t.table_name = s.table_name AND t.column_name = s.column_name
        WHEN MATCHED THEN UPDATE SET t.date_control = s.date_control
    """)
    spark.catalog.dropTempView("_v_etl_updates")
    print(f"[DONE] Updated {len(updates)} config entries in a single MERGE.")

