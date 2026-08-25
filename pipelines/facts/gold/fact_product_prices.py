from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, when


@dp.materialized_view(
    name = '_fact_product_prices',)
def fact_product_prices():
    
    stg = spark.read.table("stg_fact_product_prices").alias("stg")

    dim_product = F.broadcast(
        spark.read.table("dim_product")
        .select(
            "sk_product_id",
            "nk_product_id",
            "__START_AT",
            "__END_AT",
        )
    ).alias("prd")

    df = stg.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.updated_at") >= col("prd.__START_AT")) &
            (col("stg.updated_at") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    return (df.select(
        col("prd.sk_product_id"),
        col("stg.currency"),
        col("stg.price_id"),

        # ── Price group metadata (veprpar) ────────────────────────
        col("stg.price_label"),
        col("stg.price_group_desc"),
        col("stg.is_gross_calc"),
        col("stg.min_gross_profit_pct"),
        col("stg.max_discount_pct"),

        # ── Sales prices (aelpprs) ────────────────────────────────
        col("stg.price_net"),
        col("stg.price_gross"),
        col("stg.calc_pct"),
        col("stg.allow_zero_price"),
        col("stg.price_min_max_gross"),
        col("stg.price_min"),
        col("stg.price_max"),

        # ── Tier prices ───────────────────────────────────────────
        col("stg.tier_price_net_1"),
        col("stg.tier_price_gross_1"),
        col("stg.tier_price_net_2"),
        col("stg.tier_price_gross_2"),
        col("stg.tier_price_net_3"),
        col("stg.tier_price_gross_3"),
        col("stg.tier_price_net_4"),
        col("stg.tier_price_gross_4"),
        col("stg.tier_price_net_5"),
        col("stg.tier_price_gross_5"),
        col("stg.tier_price_net_6"),
        col("stg.tier_price_gross_6"),
        col("stg.tier_price_net_7"),
        col("stg.tier_price_gross_7"),
        col("stg.tier_price_net_8"),
        col("stg.tier_price_gross_8"),
        col("stg.tier_price_net_9"),
        col("stg.tier_price_gross_9"),
        col("stg.tier_price_net_10"),
        col("stg.tier_price_gross_10"),

        # ── Cost-calc parameters (aelpwah) ────────────────────────
        col("stg.recommended_price"),
        col("stg.no_rounding"),
        col("stg.component_price_min"),
        col("stg.component_price_max"),
        col("stg.cost_procurement"),
        col("stg.cost_procurement_is_pct"),
        col("stg.cost_handling"),
        col("stg.cost_handling_is_pct"),
        col("stg.cost_testing"),
        col("stg.cost_testing_is_pct"),
        col("stg.cost_overhead"),
        col("stg.cost_overhead_is_pct"),
        col("stg.formula_type"),
        col("stg.formula_x"),
        col("stg.formula_y"),
        col("stg.shipping_cost_type"),
        col("stg.shipping_cost_amt"),
        col("stg.gema_type"),
        col("stg.gema_value"),
        col("stg.warranty_calc_item"),

        # ── Tier quantities ───────────────────────────────────────
        col("stg.tier_qty_1"),
        col("stg.tier_qty_2"),
        col("stg.tier_qty_3"),
        col("stg.tier_qty_4"),
        col("stg.tier_qty_5"),
        col("stg.tier_qty_6"),
        col("stg.tier_qty_7"),
        col("stg.tier_qty_8"),
        col("stg.tier_qty_9"),
        col("stg.tier_qty_10"),
        col("stg.updated_at"),
        F.current_timestamp().alias("dw_created_date")
    ))
