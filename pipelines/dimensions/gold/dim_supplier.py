from pyspark import pipelines as dp


# Step 1: Declare the target table (streaming table required for Auto CDC)
dp.create_streaming_table(
    name="dim_supplier",
    comment="Supplier dimension with SCD Type-2 history, auto-managed by Auto CDC from snapshot"
)

# Step 2: Apply SCD-2 from snapshot (compares snapshots automatically)
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_supplier",
    source="stg_supplier",
    keys=["nk_supplier_id"],
    stored_as_scd_type=2,
    track_history_column_list=[
       "supplier_name","street","house_number","website"
    ]
)
