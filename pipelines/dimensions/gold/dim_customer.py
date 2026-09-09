from pyspark import pipelines as dp

dp.create_streaming_table(
    name="dim_customer",
    cluster_by=["nk_customer_id"],
    comment="Customer dimension with SCD Type-2 history, auto-managed by DLT",
    table_properties={"delta.feature.timestampNtz": "supported"}
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_customer",
    source="stg_customer",
    keys=["nk_customer_id"],
    stored_as_scd_type=2,
    track_history_column_list=["account_manager_id","customer_name","phone","email","street","house_number"]
)