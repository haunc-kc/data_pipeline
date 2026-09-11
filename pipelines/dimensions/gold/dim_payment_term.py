from pyspark import pipelines as dp

dp.create_streaming_table(
    name="dim_payment_term",
    comment="Payment terms dimension with SCD Type-2 history, auto-managed by DLT"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_payment_term",
    source="stg_payment_term",
    keys=["nk_payment_term_id"],
    stored_as_scd_type=2,
    track_history_column_list=[
        "payment_term_name",
        "cash_discount_pct",
        "cash_discount_days",
        "minimum_order_value",
    ]
)
