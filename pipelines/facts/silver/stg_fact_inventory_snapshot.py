from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType, IntegerType, DoubleType,TimestampType)

_STG_INVENTORY_SNAPSHOT_SCHEMA = StructType([
    # Identity
    StructField("nk_product_id",            LongType(),          True),
    StructField("nk_warehouse_id",          StringType(),        True),
    # Stock quantities
    StructField("stock_on_hand",            DecimalType(38, 9),  True),
    StructField("stock_secondary",          DecimalType(38, 9),  True),
    StructField("stock_reserved",           DecimalType(38, 9),  True),
    StructField("stock_ordered",            DecimalType(38, 9),  True),
    StructField("stock_in_production",      DecimalType(38, 9),  True),
    StructField("stock_repair",             DecimalType(38, 9),  True),
    StructField("stock_shortage",           DecimalType(38, 9),  True),
    StructField("stock_goods_receipt",      DecimalType(38, 9),  True),
    StructField("stock_pre_invoiced",       DecimalType(38, 9),  True),
    StructField("stock_for_pre_invoiced",   DecimalType(38, 9),  True),
    StructField("stock_in_processing",      DecimalType(38, 9),  True),
    StructField("stock_blocked",            DecimalType(38, 9),  True),
    StructField("stock_online",             DecimalType(38, 9),  True),
    StructField("stock_direct",             DecimalType(38, 9),  True),
    # Stock levels / thresholds
    StructField("reorder_level",            DecimalType(38, 9),  True),
    StructField("max_stock",                DecimalType(38, 9),  True),
    StructField("target_stock",             DecimalType(38, 9),  True),
    StructField("minimum_stock",            DecimalType(38, 9),  True),
    StructField("safety_stock_qty",         DecimalType(38, 9),  True),
    StructField("safety_stock_pct",         DecimalType(38, 9),  True),
    # Valuation
    StructField("purchase_price_avg",       DecimalType(38, 9),  True),
    StructField("purchase_price_net",       DecimalType(38, 9),  True),
    StructField("purchase_price_repair",    DecimalType(38, 9),  True),
    StructField("last_purchase_price_1",    DecimalType(38, 9),  True),
    StructField("last_purchase_price_2",    DecimalType(38, 9),  True),
    StructField("last_purchase_price_3",    DecimalType(38, 9),  True),
    StructField("last_purchase_price_4",    DecimalType(38, 9),  True),
    StructField("manufacturer_discount",    DecimalType(38, 9),  True),
    # Inventory value (computed)
    StructField("inventory_value_eur",      DecimalType(38, 9),  True),
    # Movement dates
    StructField("last_movement_date",       DateType(),          True),
    StructField("last_goods_receipt_date",  DateType(),          True),
    StructField("disposition_date",         DateType(),          True),
    # Sales velocity
    StructField("daily_sales_qty",          DecimalType(38, 9),  True),
    StructField("sales_velocity_days",      DecimalType(38, 9),  True),
    StructField("monthly_sales_qty",        DecimalType(38, 9),  True),
    StructField("days_with_sales",          DecimalType(38, 9),  True),
    StructField("monthly_sales_count",      DecimalType(38, 9),  True),
    StructField("coverage_days_1",          DecimalType(38, 9),  True),
    StructField("coverage_days_2",          DecimalType(38, 9),  True),
    # Days of supply (computed)
    StructField("days_of_supply",           DoubleType(),        True),
    # Lead time
    StructField("lead_time_days",           DecimalType(38, 9),  True),
    # Aging signals
    StructField("days_since_last_movement", IntegerType(),       True),
    StructField("days_since_last_receipt",  IntegerType(),       True),
    # Audit
    StructField("updated_at",              TimestampType(),      True),
])

# @dp.materialized_view(name="stg_inventory_snapshot")
@dp.temporary_view(name="stg_inventory_snapshot")
def stg_inventory_snapshot():
    
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Inventory Snapshot' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_INVENTORY_SNAPSHOT_SCHEMA)
    except Exception:
        pass 

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
        WHERE table_name  = 'fact_inventory_snapshot'
          AND column_name = 'updated_at'
        LIMIT 1
    """).first()["date_control"]
    df = (
        spark.read
        .table("`bigquery-udp_catalog`.`mention_data`.`aellager`")
        .where(col("MLMANKEY") == 1)
        .where(trim(coalesce(col("MLLAGER"), lit(""))) != lit(""))
        # .where(col("UPDTIME") >= dateControl)  
        .where(f"Cast(UPDTIME As Date) >= '{dateControl}'")
    )

    today = F.current_date()

    return (
        df.select(
            # ── Foreign keys (resolved in Gold to SKs) ───────────
            col("MLIDNR").alias("nk_product_id"),          # FK → ael.aridnr → dim_product
            trim(col("MLLAGER")).alias("nk_warehouse_id"), # FK → dim_warehouse
           

            # ── Stock quantities ──────────────────────────────────
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

            # ── Stock levels / thresholds ─────────────────────────
            coalesce(col("MLMINBEST"),  lit(0)).alias("reorder_level"),
            coalesce(col("mlmaxbest"),  lit(0)).alias("max_stock"),
            coalesce(col("mlsolbest"),  lit(0)).alias("target_stock"),
            coalesce(col("mlmindbest"), lit(0)).alias("minimum_stock"),
            coalesce(col("MLSICHBEST"), lit(0)).alias("safety_stock_qty"),
            coalesce(col("MLSICHPROZ"), lit(0)).alias("safety_stock_pct"),

            # ── Valuation ─────────────────────────────────────────
            coalesce(col("MLEKPREIS"),  lit(0)).alias("purchase_price_avg"),
            coalesce(col("mlekrein"),   lit(0)).alias("purchase_price_net"),
            coalesce(col("MLEKPREP"),   lit(0)).alias("purchase_price_repair"),
            coalesce(col("MLLETZTEK1"), lit(0)).alias("last_purchase_price_1"),
            coalesce(col("MLLETZTEK2"), lit(0)).alias("last_purchase_price_2"),
            coalesce(col("MLLETZTEK3"), lit(0)).alias("last_purchase_price_3"),
            coalesce(col("MLLETZTEK4"), lit(0)).alias("last_purchase_price_4"),
            coalesce(col("mlherabatt"), lit(0)).alias("manufacturer_discount"),

            # ── Inventory value (computed) ────────────────────────
            (coalesce(col("MLBESTAND"), lit(0)) * coalesce(col("MLEKPREIS"), lit(0)))
                .alias("inventory_value_eur"),

            # ── Movement dates ────────────────────────────────────
            col("MLLETZTBEW").cast("date").alias("last_movement_date"),
            col("MLWEDAT").cast("date").alias("last_goods_receipt_date"),
            col("MLDISPDAT").cast("date").alias("disposition_date"),

            # ── Sales velocity ────────────────────────────────────
            coalesce(col("MLTAGESVK"),   lit(0)).alias("daily_sales_qty"),
            coalesce(col("MLTVKTAGE"),   lit(0)).alias("sales_velocity_days"),
            coalesce(col("mlmonatsvk"),  lit(0)).alias("monthly_sales_qty"),
            coalesce(col("mlanztagum"),  lit(0)).alias("days_with_sales"),
            coalesce(col("mlmvkmon"),    lit(0)).alias("monthly_sales_count"),
            coalesce(col("mlrwelag1"),   lit(0)).alias("coverage_days_1"),
            coalesce(col("mlrwelag2"),   lit(0)).alias("coverage_days_2"),

            # ── Days of supply (computed) ─────────────────────────
            when(
                coalesce(col("MLTAGESVK"), lit(0)) > lit(0),
                coalesce(col("MLBESTAND"), lit(0)) / col("MLTAGESVK")
            ).otherwise(lit(0)).alias("days_of_supply"),

            # ── Lead time ─────────────────────────────────────────
            coalesce(col("MLLIEFZEIT"), lit(0)).alias("lead_time_days"),

            # ── Raw aging signals (dates + stock) ─────────────────
            # is_aging_article is computed in Gold (fact_inventory_snapshot)
            # after joining dim_product to apply arnoaktiv/ararchiv/argruppe filters
            F.datediff(today, col("MLLETZTBEW").cast("date")).alias("days_since_last_movement"),
            F.datediff(today, col("MLWEDAT").cast("date")).alias("days_since_last_receipt"),

            # ── Audit ─────────────────────────────────────────────
            col("UPDTIME").cast("timestamp").alias("updated_at"),
        )
        .dropDuplicates(["nk_product_id", "nk_warehouse_id"])
    )
