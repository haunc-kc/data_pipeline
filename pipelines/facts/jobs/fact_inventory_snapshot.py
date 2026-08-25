import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.."))
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from utils.hashing import make_row_hash
from datetime import datetime, timezone
from utils.init_load import initial_load

CATALOG       = "workspace"
SCHEMA        = "mention_dw"

SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_inventory_snapshot"
LABEL = "Inventory Snapshot"

_HEADER_CLUSTER_COLS = ["sk_product_id","warehouse","updated_at","last_movement_date"]

_HEADER_HASH_COLS = ["stock_on_hand","warehouse","stock_secondary","stock_reserved","stock_ordered","stock_in_production","stock_repair","stock_shortage","stock_goods_receipt","stock_pre_invoiced","stock_for_pre_invoiced","stock_in_processing","stock_blocked","stock_online","stock_direct","reorder_level","max_stock","target_stock","minimum_stock","safety_stock_qty","safety_stock_pct","purchase_price_avg","purchase_price_net","purchase_price_repair","last_purchase_price_1","last_purchase_price_2","last_purchase_price_3","last_purchase_price_4","manufacturer_discount","inventory_value_eur","last_goods_receipt_date","disposition_date","daily_sales_qty","sales_velocity_days","monthly_sales_qty","days_with_sales","monthly_sales_count","coverage_days_1","coverage_days_2","days_of_supply","lead_time_days","days_since_last_movement","days_since_last_receipt","updated_at","dw_created_date"]

today = F.current_date()

try:
    decision = spark.sql(f"""Select decision 
                             From {CATALOG}.{SCHEMA}.etl_run_decision
                             Where label = '{LABEL}'
                             Limit 1
                        """).first()
    if decision and decision["decision"] == "SKIP":
        print("[SKIP] etl_run_decision = SKIP  exiting.")
        dbutils.notebook.exit("SKIP")
except Exception as e:
    print(f"[INFO] etl_run_decision not available ({e}) proceeding as RUN.")


dateControl = spark.sql(f"""SELECT date_control FROM {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                            WHERE table_name  = 'fact_inventory_snapshot'
                                    AND column_name = 'updated_at'
                            LIMIT 1
                    """).first()["date_control"]

aellager_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aellager`")
latest_df = (
    aellager_df
    .where(col("MLMANKEY") == 1)
    .where(trim(coalesce(col("MLLAGER"), lit(""))) != lit(""))
    .where(f"Cast(UPDTIME As Date) >= '{dateControl}'")
    .groupBy("MLIDNR")
    .agg(
        F.max("UPDTIME").alias("updtime"),
        F.max("MLLETZTBEW").alias("mlletztbew")
    )
)

df = (
    aellager_df.alias("t")
    .join(
        latest_df.alias("tmp"),
        (
            (col("t.MLIDNR") == col("tmp.MLIDNR")) &
            (col("t.UPDTIME") == col("tmp.updtime")) &
            (col("t.MLLETZTBEW") == col("tmp.mlletztbew"))
        ),
        "inner"
    )
).select(
            col("t.MLIDNR").alias("nk_product_id"),
            trim(col("MLLAGER")).alias("warehouse"),
            coalesce(col("MLBESTAND"),  lit(0)).alias("stock_on_hand"),
            coalesce(col("MLBEST2"),    lit(0)).alias("stock_secondary"),
            coalesce(col("MLRESBEST"),  lit(0)).alias("stock_reserved"),
            coalesce(col("MLBESTELLT"), lit(0)).alias("stock_ordered"),
            coalesce(col("MLPROD"),     lit(0)).alias("stock_in_production"),
            coalesce(col("MLREPBEST"),  lit(0)).alias("stock_repair"),
            coalesce(col("MLFEHL"),     lit(0)).alias("stock_shortage"),
            coalesce(col("MLBESTWE"),   lit(0)).alias("stock_goods_receipt"),
            coalesce(col("MLVORFAKT"),  lit(0)).alias("stock_pre_invoiced"),
            coalesce(col("MLFVORFAKT"), lit(0)).alias("stock_for_pre_invoiced"),
            coalesce(col("MLINABWICK"), lit(0)).alias("stock_in_processing"),
            coalesce(col("mlsperbest"), lit(0)).alias("stock_blocked"),
            coalesce(col("mlonlbest"),  lit(0)).alias("stock_online"),
            coalesce(col("mldirbest"),  lit(0)).alias("stock_direct"),
            coalesce(col("MLMINBEST"),  lit(0)).alias("reorder_level"),
            coalesce(col("mlmaxbest"),  lit(0)).alias("max_stock"),
            coalesce(col("mlsolbest"),  lit(0)).alias("target_stock"),
            coalesce(col("mlmindbest"), lit(0)).alias("minimum_stock"),
            coalesce(col("MLSICHBEST"), lit(0)).alias("safety_stock_qty"),
            coalesce(col("MLSICHPROZ"), lit(0)).alias("safety_stock_pct"),
            coalesce(col("MLEKPREIS"),  lit(0)).alias("purchase_price_avg"),
            coalesce(col("mlekrein"),   lit(0)).alias("purchase_price_net"),
            coalesce(col("MLEKPREP"),   lit(0)).alias("purchase_price_repair"),
            coalesce(col("MLLETZTEK1"), lit(0)).alias("last_purchase_price_1"),
            coalesce(col("MLLETZTEK2"), lit(0)).alias("last_purchase_price_2"),
            coalesce(col("MLLETZTEK3"), lit(0)).alias("last_purchase_price_3"),
            coalesce(col("MLLETZTEK4"), lit(0)).alias("last_purchase_price_4"),
            coalesce(col("mlherabatt"), lit(0)).alias("manufacturer_discount"),
            (coalesce(col("MLBESTAND"), lit(0)) * coalesce(col("MLEKPREIS"), lit(0)))
                .alias("inventory_value_eur"),
            col("t.MLLETZTBEW").cast("date").alias("last_movement_date"),
            col("MLWEDAT").cast("date").alias("last_goods_receipt_date"),
            col("MLDISPDAT").cast("date").alias("disposition_date"),
            coalesce(col("MLTAGESVK"),   lit(0)).alias("daily_sales_qty"),
            coalesce(col("MLTVKTAGE"),   lit(0)).alias("sales_velocity_days"),
            coalesce(col("mlmonatsvk"),  lit(0)).alias("monthly_sales_qty"),
            coalesce(col("mlanztagum"),  lit(0)).alias("days_with_sales"),
            coalesce(col("mlmvkmon"),    lit(0)).alias("monthly_sales_count"),
            coalesce(col("mlrwelag1"),   lit(0)).alias("coverage_days_1"),
            coalesce(col("mlrwelag2"),   lit(0)).alias("coverage_days_2"),
            when(
                coalesce(col("MLTAGESVK"), lit(0)) > lit(0),
                coalesce(col("MLBESTAND"), lit(0)) / col("MLTAGESVK")
            ).otherwise(lit(0)).alias("days_of_supply"),

            coalesce(col("MLLIEFZEIT"), lit(0)).alias("lead_time_days"),
            F.datediff(today, col("t.MLLETZTBEW").cast("date")).alias("days_since_last_movement"),
            F.datediff(today, col("MLWEDAT").cast("date")).alias("days_since_last_receipt"),
            col("t.UPDTIME").cast("timestamp").alias("updated_at"),
        )

dim_product = F.broadcast(
        spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product")
        .select(
            "sk_product_id",
            "nk_product_id",
            "is_active",
            "is_stock_item",
            "is_discontinued",
            "__START_AT",
            "__END_AT",
        )
    ).alias("prd")

df = df.alias("stg").join(dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.updated_at") >= col("prd.__START_AT")) &
            (col("stg.updated_at") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left").select(
            col("prd.sk_product_id"),
            col("prd.is_active"),
            col("prd.is_stock_item"),
            col("prd.is_discontinued"),
            col("stg.stock_on_hand"),
            col("stg.warehouse"),
            col("stg.updated_at"),
            col("stg.last_movement_date"),
            col("stg.stock_secondary"),
            col("stg.stock_reserved"),
            col("stg.stock_ordered"),
            col("stg.stock_in_production"),
            col("stg.stock_repair"),
            col("stg.stock_shortage"),
            col("stg.stock_goods_receipt"),
            col("stg.stock_pre_invoiced"),
            col("stg.stock_for_pre_invoiced"),
            col("stg.stock_in_processing"),
            col("stg.stock_blocked"),
            col("stg.stock_online"),
            col("stg.stock_direct"),
            col("stg.reorder_level"),
            col("stg.max_stock"),
            col("stg.target_stock"),
            col("stg.minimum_stock"),
            col("stg.safety_stock_qty"),
            col("stg.safety_stock_pct"),
            col("stg.purchase_price_avg"),
            col("stg.purchase_price_net"),
            col("stg.purchase_price_repair"),
            col("stg.last_purchase_price_1"),
            col("stg.last_purchase_price_2"),
            col("stg.last_purchase_price_3"),
            col("stg.last_purchase_price_4"),
            col("stg.manufacturer_discount"),
            col("stg.inventory_value_eur"),
            col("stg.last_goods_receipt_date"),
            col("stg.disposition_date"),
            col("stg.daily_sales_qty"),
            col("stg.sales_velocity_days"),
            col("stg.monthly_sales_qty"),
            col("stg.days_with_sales"),
            col("stg.monthly_sales_count"),
            col("stg.coverage_days_1"),
            col("stg.coverage_days_2"),
            col("stg.days_of_supply"),
            col("stg.lead_time_days"),
            col("stg.days_since_last_movement"),
            col("stg.days_since_last_receipt"),
            F.current_timestamp().alias("dw_created_date"),
    ).withColumn('row_hash',make_row_hash(_HEADER_HASH_COLS)).dropDuplicates(_HEADER_CLUSTER_COLS + ["stock_secondary"])
try:
    DeltaTable.forName(spark, SOURCE_HEADER_TABLE).alias("t").merge(df.alias("s"),"t.sk_product_id = s.sk_product_id"
                                                                    " And t.updated_at = s.updated_at"
                                                                    " And t.last_movement_date = s.last_movement_date").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _HEADER_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_inventory_snapshot MERGE completed.")
except Exception as e:
    initial_load(df,SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS) 
    print(e)
