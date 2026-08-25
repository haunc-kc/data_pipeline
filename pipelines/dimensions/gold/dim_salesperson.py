from pyspark import pipelines as dp


# Step 1: Declare the target table (streaming table required for Auto CDC)
dp.create_streaming_table(
    name="dim_salesperson",
    comment="Salesperson dimension with SCD Type-2 history, auto-managed by DLT"
)

# Step 2: Apply SCD-2 from snapshot (DLT compares snapshots automatically)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_salesperson",
    source="stg_salesperson",
    keys=["nk_salesperson_id"],
    stored_as_scd_type=2,
    track_history_column_list=["last_name","first_name","department","branch_code"]
)
