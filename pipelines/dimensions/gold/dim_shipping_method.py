from pyspark import pipelines as dp 

dp.create_streaming_table(name="dim_shipping_method")
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_shipping_method"
    ,source="stg_shipping_method"
    ,keys=["nk_shipping_method_id"]
    ,stored_as_scd_type=2
    ,track_history_column_list =["shipping_method_name","shipping_type_flag","shipping_rate"]
)

