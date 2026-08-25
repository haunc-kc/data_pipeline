from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, when


@dp.materialized_view(
    name = '_fact_inventory_snapshot',
    comment="Daily inventory snapshot fact — stock levels, valuation, aging flags per product × warehouse",
    cluster_by=["sk_product_id"]
)
def fact_inventory_snapshot():
    """
    Gold: Inventory Snapshot Fact
    Source  : stg_inventory_snapshot (silver)
    Grain   : one row per (product × warehouse × client) — refreshed daily
    Dims    : dim_product (temporal join on updated_at)
              dim_warehouse (SCD-1 join on nk_warehouse_id + nk_client_id)

    is_aging_article logic (GetDailyOffersQuery — Lagerhüter):
        Computed HERE in Gold using dim_product — NOT in Silver.
        Reason: arnoaktiv, ararchiv, is_stock_item come from ael → dim_product.
                Joining ael again in Silver would be a redundant BigQuery read.
        Conditions:
            stock_on_hand > 0
            dim_product.is_active      = TRUE  (arnoaktiv = 0)
            dim_product.is_stock_item  = TRUE  (arlagware = TRUE)
            days_since_last_movement  >= 90
            days_since_last_receipt   >= 90
    """

    stg = (
        spark.read.table("stg_inventory_snapshot")
        .withColumn("updated_at", col("updated_at").cast("date"))
        .alias("stg")
    )

    # Pull is_active + is_stock_item from dim_product for aging filter
    dim_product = F.broadcast(
        spark.read.table("dim_product")
        .select(
            "sk_product_id",
            "nk_product_id",
            "is_active",        # arnoaktiv = 0
            "is_stock_item",    # arlagware = TRUE (stock-keeping article)
            "is_discontinued",  # arauslauf > 0
            "__START_AT",
            "__END_AT",
        )
    ).alias("prd")

    # dim_warehouse = F.broadcast(
    #     spark.read.table("dim_warehouse")
    #     .select("sk_warehouse_id", "nk_warehouse_id", "nk_client_id"  "__START_AT",
    #         "__END_AT",)
    # ).alias("wh")

    # temporal join — product version valid at snapshot date
    df = stg.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.updated_at") >= col("prd.__START_AT")) &
            (col("stg.updated_at") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    # SCD-1 join — warehouse
    # df = df.join(
    #     dim_warehouse,
    #     on=(
    #         (col("stg.nk_warehouse_id") == col("wh.nk_warehouse_id")) &
    #         (col("stg.nk_client_id")    == col("wh.nk_client_id"))            
    #     ),
    #     how="left"
    # )

    # Compute is_aging_article in Gold using dim_product flags
    # Matches GetDailyOffersQuery exactly:
    #   arnoaktiv = 0  → dim_product.is_active = TRUE
    #   arlagware = 1  → dim_product.is_stock_item = TRUE
    #   stock > 0, last_movement <= today-90, last_receipt <= today-90

    # df = df.withColumn(
    #     "is_aging_article",
    #     when(
    #         (col("stg.stock_on_hand") > lit(0)) &
    #         (coalesce(col("prd.is_active"),     lit(False)) == lit(True)) &
    #         (coalesce(col("prd.is_stock_item"), lit(False)) == lit(True)) &
    #         (col("stg.days_since_last_movement") >= lit(90)) &
    #         (col("stg.days_since_last_receipt")  >= lit(90)),
    #         lit(True)
    #     ).otherwise(lit(False))
    # )

    return df.select(
        # ── Surrogate keys ────────────────────────────────────
        col("prd.sk_product_id"),
        # col("wh.sk_warehouse_id"),


        # ── Product status (from dim_product — no redundant ael join) ──
        col("prd.is_active"),
        col("prd.is_stock_item"),
        col("prd.is_discontinued"),

        # ── Stock quantities ──────────────────────────────────
        col("stg.stock_on_hand"),
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

        # ── Stock thresholds ──────────────────────────────────
        col("stg.reorder_level"),
        col("stg.max_stock"),
        col("stg.target_stock"),
        col("stg.minimum_stock"),
        col("stg.safety_stock_qty"),
        col("stg.safety_stock_pct"),

        # ── Valuation ─────────────────────────────────────────
        col("stg.purchase_price_avg"),
        col("stg.purchase_price_net"),
        col("stg.purchase_price_repair"),
        col("stg.last_purchase_price_1"),
        col("stg.last_purchase_price_2"),
        col("stg.last_purchase_price_3"),
        col("stg.last_purchase_price_4"),
        col("stg.manufacturer_discount"),
        col("stg.inventory_value_eur"),

        # ── Movement dates ────────────────────────────────────
        col("stg.last_movement_date"),
        col("stg.last_goods_receipt_date"),
        col("stg.disposition_date"),

        # ── Sales velocity ────────────────────────────────────
        col("stg.daily_sales_qty"),
        col("stg.sales_velocity_days"),
        col("stg.monthly_sales_qty"),
        col("stg.days_with_sales"),
        col("stg.monthly_sales_count"),
        col("stg.coverage_days_1"),
        col("stg.coverage_days_2"),
        col("stg.days_of_supply"),
        col("stg.lead_time_days"),

        # # ── Aging article ─────────────────────────────────────
        # col("is_aging_article"),
        col("stg.days_since_last_movement"),
        col("stg.days_since_last_receipt"),

        # ── Audit ─────────────────────────────────────────────
        col("stg.updated_at"),
        F.current_timestamp().alias("dw_created_date"),
    )
