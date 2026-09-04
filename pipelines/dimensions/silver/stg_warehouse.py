from pyspark import pipelines as dp 
from pyspark.sql import functions as F
from pyspark.sql.functions import col


SOURE_TABLE = "`bigquery-udp_catalog`.`mention_data`.`aellager`"

@dp.materialized_view(name="stg_warehouse")
def build_stg_warehouse():
    df = spark.read.table(SOURE_TABLE).where(col("MLMANKEY") == 1).select(
                             F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,F.col("MLIDNR")
                                ,F.col("MLLAGER")
                                ,F.col("MLEKPREIS")
                                ,F.col("MLEKPREP")
                                ,F.col("mlekrein")
                                ,F.col("MLBESTAND")
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_warehouse_id"),
                            F.col("MLIDNR").alias("nk_warehouse_id")
                            ,F.col("MLLAGER").alias("warehouse_name"),
                            F.col("MLEKPREIS").alias("purchase_price_avg"),
                            F.col("MLEKPREP").alias("purchase_price_repair"),
                            F.col("mlekrein").alias("purchase_price_net"),
                            F.col("mlekinabw").alias("purchase_price_deviation"),
                            F.col("MLLETZTEK1").alias("last_purchase_price_1"),
                            F.col("MLLETZTEK2").alias("last_purchase_price_2"),
                            F.col("MLLETZTEK3").alias("last_purchase_price_3"),
                            F.col("MLLETZTEK4").alias("last_purchase_price_4"),
                            F.col("mlherabatt").alias("manufacturer_discount"),
                            F.col("MLBESTAND").alias("stock_available"),
                            F.col("MLBEST2").alias("stock_secondary"),
                            F.col("MLRESBEST").alias("stock_reserved"),
                            F.col("MLREPBEST").alias("stock_repair"),
                            F.col("MLFVORFAKT").alias("qty_for_pre_invoiced"),
                            F.col("MLVORFAKT").alias("qty_pre_invoiced"),
                            F.col("MLBESTWE").alias("qty_goods_receipt_stock"),
                            F.col("MLWEBEST").alias("qty_goods_receipt_open"),
                            F.col("MLINABWICK").alias("qty_in_processing"),
                            F.col("MLKUNDBEST").alias("qty_customer_orders"),
                            F.col("MLBESTELLT").alias("qty_ordered"),
                            F.col("mlonlbest").alias("qty_online_stock"),
                            F.col("mlonlres").alias("qty_online_reserved"),
                            F.col("mlonlvor").alias("qty_online_pre_order"),
                            F.col("mldirbest").alias("qty_direct_stock"),
                            F.col("MLDISPMEN").alias("qty_disposition"),
                            F.col("MLPROD").alias("qty_in_production"),
                            F.col("mlsperbest").alias("qty_blocked"),
                            F.col("mlrgabest").alias("qty_return_rga"),
                            F.col("mlfehl").alias("qty_shortage"),
                            F.col("MLZURUECK").alias("qty_returned"),
                            F.col("MLABBUCH").alias("qty_deduction"),
                            F.col("MLZUBUCH").alias("qty_addition"),
                            F.col("mllvorbest").alias("qty_pre_orders_supplier"),
                            F.col("MLWEDAT").cast("string").alias("last_goods_receipt_date"),
                            F.col("MLWEMENGE").alias("last_goods_receipt_qty"),
                            F.col("MLMINBEST").alias("reorder_level"),
                            F.col("mlmaxbest").alias("max_stock"),
                            F.col("mlsolbest").alias("target_stock"),
                            F.col("mlmindbest").alias("minimum_stock"),
                            F.col("MLSICHBEST").alias("safety_stock_qty"),
                            F.col("MLSICHPROZ").alias("safety_stock_pct"),
                            F.col("MLSICHTYP").alias("safety_stock_type"),
                            F.col("MLLIEFZEIT").alias("lead_time_days"),
                            F.col("MLMINAUTO").alias("reorder_level_auto"),
                            F.col("MLTVKTAGE").alias("sales_velocity_days"),
                            F.col("MLTAGESVK").alias("daily_sales_qty"),
                            F.col("mlanztagum").alias("days_with_sales"),
                            F.col("mlmvkmon").alias("monthly_sales_count"),
                            F.col("WSLOCK").alias("row_lock"),
                            F.col("mlinvsperr").alias("inventory_block_flag"),
                            F.col("mlfrei1").alias("free_field_1"),
                            F.col("mlfrei2").alias("free_field_2"),
                            F.col("mlfrei3").alias("free_field_3"),
                            F.col("mlfrei4").alias("free_field_4"),
                            F.col("mlfrei5").alias("free_field_5"),
                            F.col("mlfrei6").alias("free_field_6"),
                            F.col("mlfrei7").alias("free_field_7"),
                            F.col("mlfrei8").alias("free_field_8"),
                            F.col("mlfrei9").alias("free_field_9"),
                            F.col("mlfrei10").alias("free_field_10"),
                            F.col("updtime").cast("string").alias("updated_at"),
                            F.col("MLLETZTBEW").alias("movement_date"),
    )
    return df.dropDuplicates(['nk_warehouse_id'])

# @dp.materialized_view(name="_stg_warehouse")
# def _stg_warehouse():
#     return build_stg_warehouse()

# @dp.materialized_view(name="stg_warehouse")
# def stg_warehouse():
#     key_columns = get_key_columns(spark, "mention_dw._stg_warehouse")
#     print(key_columns)
#     return build_stg_warehouse().dropDuplicates(key_columns)
    
