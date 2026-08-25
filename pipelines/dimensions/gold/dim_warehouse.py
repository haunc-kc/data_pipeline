
from pyspark import pipelines as dp

dp.create_streaming_table(name="dim_warehouse")

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_warehouse",
    source="stg_warehouse",
    keys=["nk_warehouse_id"],
    stored_as_scd_type=2,
    track_history_column_list=["warehouse_name","purchase_price_avg","purchase_price_repair","purchase_price_repair","purchase_price_net","stock_available"]
)
