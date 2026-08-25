from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    BooleanType, DecimalType, TimestampType,
)
import pyspark.pipelines as dp

_STG_PRODUCT_PRICES_SCHEMA = StructType([
    StructField("nk_product_id",                LongType(),         True),
    StructField("currency",                     StringType(),       True),
    StructField("price_id",                     LongType(),         True),
    # Price group metadata (veprpar)
    StructField("price_label",                  StringType(),       True),
    StructField("price_group_desc",             StringType(),       True),
    StructField("is_gross_calc",                BooleanType(),      True),
    StructField("min_gross_profit_pct",         DecimalType(38, 9), True),
    StructField("max_discount_pct",             DecimalType(38, 9), True),
    # Sales prices (aelpprs)
    StructField("price_net",                    DecimalType(38, 9), True),
    StructField("price_gross",                  DecimalType(38, 9), True),
    StructField("calc_pct",                     DecimalType(38, 9), True),
    StructField("allow_zero_price",             BooleanType(),      True),
    StructField("price_min_max_gross",          BooleanType(),      True),
    StructField("price_min",                    DecimalType(38, 9), True),
    StructField("price_max",                    DecimalType(38, 9), True),
    # Tier prices
    StructField("tier_price_net_1",             DecimalType(38, 9), True),
    StructField("tier_price_gross_1",           DecimalType(38, 9), True),
    StructField("tier_price_net_2",             DecimalType(38, 9), True),
    StructField("tier_price_gross_2",           DecimalType(38, 9), True),
    StructField("tier_price_net_3",             DecimalType(38, 9), True),
    StructField("tier_price_gross_3",           DecimalType(38, 9), True),
    StructField("tier_price_net_4",             DecimalType(38, 9), True),
    StructField("tier_price_gross_4",           DecimalType(38, 9), True),
    StructField("tier_price_net_5",             DecimalType(38, 9), True),
    StructField("tier_price_gross_5",           DecimalType(38, 9), True),
    StructField("tier_price_net_6",             DecimalType(38, 9), True),
    StructField("tier_price_gross_6",           DecimalType(38, 9), True),
    StructField("tier_price_net_7",             DecimalType(38, 9), True),
    StructField("tier_price_gross_7",           DecimalType(38, 9), True),
    StructField("tier_price_net_8",             DecimalType(38, 9), True),
    StructField("tier_price_gross_8",           DecimalType(38, 9), True),
    StructField("tier_price_net_9",             DecimalType(38, 9), True),
    StructField("tier_price_gross_9",           DecimalType(38, 9), True),
    StructField("tier_price_net_10",            DecimalType(38, 9), True),
    StructField("tier_price_gross_10",          DecimalType(38, 9), True),
    # Cost-calc parameters (aelpwah)
    StructField("recommended_price",            DecimalType(38, 9), True),
    StructField("no_rounding",                  BooleanType(),      True),
    StructField("component_price_min",          DecimalType(38, 9), True),
    StructField("component_price_max",          DecimalType(38, 9), True),
    StructField("cost_procurement",             DecimalType(38, 9), True),
    StructField("cost_procurement_is_pct",      BooleanType(),      True),
    StructField("cost_handling",                DecimalType(38, 9), True),
    StructField("cost_handling_is_pct",         BooleanType(),      True),
    StructField("cost_testing",                 DecimalType(38, 9), True),
    StructField("cost_testing_is_pct",          BooleanType(),      True),
    StructField("cost_overhead",                DecimalType(38, 9), True),
    StructField("cost_overhead_is_pct",         BooleanType(),      True),
    StructField("formula_type",                 StringType(),       True),
    StructField("formula_x",                    DecimalType(38, 9), True),
    StructField("formula_y",                    DecimalType(38, 9), True),
    StructField("shipping_cost_type",           StringType(),       True),
    StructField("shipping_cost_amt",            DecimalType(38, 9), True),
    StructField("gema_type",                    StringType(),       True),
    StructField("gema_value",                   DecimalType(38, 9), True),
    StructField("warranty_calc_item",           StringType(),       True),
    # Tier quantities (aelpwah)
    StructField("tier_qty_1",                   DecimalType(38, 9), True),
    StructField("tier_qty_2",                   DecimalType(38, 9), True),
    StructField("tier_qty_3",                   DecimalType(38, 9), True),
    StructField("tier_qty_4",                   DecimalType(38, 9), True),
    StructField("tier_qty_5",                   DecimalType(38, 9), True),
    StructField("tier_qty_6",                   DecimalType(38, 9), True),
    StructField("tier_qty_7",                   DecimalType(38, 9), True),
    StructField("tier_qty_8",                   DecimalType(38, 9), True),
    StructField("tier_qty_9",                   DecimalType(38, 9), True),
    StructField("tier_qty_10",                  DecimalType(38, 9), True),
    StructField("updated_at",                   TimestampType(),    True),
])



@dp.materialized_view(name="stg_fact_product_prices")
def stg_price():

    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Product Prices' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_PRODUCT_PRICES_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
                                SELECT date_control
                                FROM workspace.mention_dw.etl_fact_pipeline_config
                                WHERE table_name  = 'fact_product_prices'
                                        AND column_name = 'updated_at'
                                LIMIT 1
    """).first()["date_control"]

    p  = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aelpprs`").alias("p").where(f"Cast(updtime As Date) >= '{dateControl}'")
    w = F.broadcast(spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aelpwah`")).alias("w")
    vp = F.broadcast(
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.`veprpar`")
    ).alias("vp")
    

    return (
        p
        .join(
            w,
            on=(
                (F.col("p.anidnr")     == F.col("w.awidnr"))   &
                (F.col("p.anmankey")   == F.col("w.awmankey"))  &
                (F.col("p.anwaehrung") == F.col("w.awwaehrung"))
            ),
            how="left"
        )
        # veprpar: broadcasted lookup — 1 row per (price_id, client)
        .join(
            vp,
            on=(
                (F.col("p.anpid")    == F.col("vp.aapid"))   &
                (F.col("p.anmankey") == F.col("vp.aamankey"))
            ),
            how="left"
        )
       
        .select(

            F.col("anidnr").alias("nk_product_id"),

            # ── Natural keys ──────────────────────────────────────
            F.trim(F.coalesce(F.col("p.anwaehrung"), F.lit("EUR"))) .alias("currency"),
            F.col("p.anpid")                                        .alias("price_id"),

            # ── Price group metadata (veprpar) ────────────────────
            F.trim(F.coalesce(F.col("vp.aapbez"), F.lit("")))       .alias("price_label"),
            F.trim(F.coalesce(F.col("vp.aapbem"), F.lit("")))       .alias("price_group_desc"),
            F.coalesce(F.col("vp.aapbrucalc"), F.lit(False))        .alias("is_gross_calc"),
            F.coalesce(F.col("vp.aapwarnroh"), F.lit(0))            .alias("min_gross_profit_pct"),
            F.coalesce(F.col("vp.aapmaxnach"), F.lit(0))            .alias("max_discount_pct"),

            # ── Sales prices (aelpprs) ────────────────────────────
            F.coalesce(F.col("p.anvkpreis"),  F.lit(0))             .alias("price_net"),
            F.coalesce(F.col("p.anvkbrutt"),  F.lit(0))             .alias("price_gross"),
            F.coalesce(F.col("p.anvkproz"),   F.lit(0))             .alias("calc_pct"),
            F.when(F.coalesce(F.col("p.anprnull"),   F.lit(0)) > 0, True)
             .otherwise(False)                                       .alias("allow_zero_price"),
            F.when(F.coalesce(F.col("p.anminmaxbr"), F.lit(0)) > 0, True)
             .otherwise(False)                                       .alias("price_min_max_gross"),
            F.coalesce(F.col("p.anprmin"), F.lit(0))                .alias("price_min"),
            F.coalesce(F.col("p.anprmax"), F.lit(0))                .alias("price_max"),

            # Tier prices per price_id slot (aelpprs)
            F.coalesce(F.col("p.anvkstne1"),  F.lit(0))             .alias("tier_price_net_1"),
            F.coalesce(F.col("p.anvkstbr1"),  F.lit(0))             .alias("tier_price_gross_1"),
            F.coalesce(F.col("p.anvkstne2"),  F.lit(0))             .alias("tier_price_net_2"),
            F.coalesce(F.col("p.anvkstbr2"),  F.lit(0))             .alias("tier_price_gross_2"),
            F.coalesce(F.col("p.anvkstne3"),  F.lit(0))             .alias("tier_price_net_3"),
            F.coalesce(F.col("p.anvkstbr3"),  F.lit(0))             .alias("tier_price_gross_3"),
            F.coalesce(F.col("p.anvkstne4"),  F.lit(0))             .alias("tier_price_net_4"),
            F.coalesce(F.col("p.anvkstbr4"),  F.lit(0))             .alias("tier_price_gross_4"),
            F.coalesce(F.col("p.anvkstne5"),  F.lit(0))             .alias("tier_price_net_5"),
            F.coalesce(F.col("p.anvkstbr5"),  F.lit(0))             .alias("tier_price_gross_5"),
            F.coalesce(F.col("p.anvkstne6"),  F.lit(0))             .alias("tier_price_net_6"),
            F.coalesce(F.col("p.anvkstbr6"),  F.lit(0))             .alias("tier_price_gross_6"),
            F.coalesce(F.col("p.anvkstne7"),  F.lit(0))             .alias("tier_price_net_7"),
            F.coalesce(F.col("p.anvkstbr7"),  F.lit(0))             .alias("tier_price_gross_7"),
            F.coalesce(F.col("p.anvkstne8"),  F.lit(0))             .alias("tier_price_net_8"),
            F.coalesce(F.col("p.anvkstbr8"),  F.lit(0))             .alias("tier_price_gross_8"),
            F.coalesce(F.col("p.anvkstne9"),  F.lit(0))             .alias("tier_price_net_9"),
            F.coalesce(F.col("p.anvkstbr9"),  F.lit(0))             .alias("tier_price_gross_9"),
            F.coalesce(F.col("p.anvkstne10"), F.lit(0))             .alias("tier_price_net_10"),
            F.coalesce(F.col("p.anvkstbr10"), F.lit(0))             .alias("tier_price_gross_10"),

            # ── Cost-calc parameters (aelpwah) ────────────────────
            # NULL when product has no aelpwah row (simple products without
            # assembly/formula pricing). Consumers should treat NULL as "not configured".
            F.col("w.awempfvk")                                     .alias("recommended_price"),
            F.col("w.awkeinrund")                                   .alias("no_rounding"),
            F.col("w.awprmin")                                      .alias("component_price_min"),
            F.col("w.awprmax")                                      .alias("component_price_max"),
            F.col("w.awkoeink")                                     .alias("cost_procurement"),
            F.col("w.awkoeinkpr")                                   .alias("cost_procurement_is_pct"),
            F.col("w.awkohand")                                     .alias("cost_handling"),
            F.col("w.awkohandpr")                                   .alias("cost_handling_is_pct"),
            F.col("w.awkotest")                                     .alias("cost_testing"),
            F.col("w.awkotestpr")                                   .alias("cost_testing_is_pct"),
            F.col("w.awkokost")                                     .alias("cost_overhead"),
            F.col("w.awkokostpr")                                   .alias("cost_overhead_is_pct"),
            F.col("w.awformel")                                     .alias("formula_type"),
            F.col("w.awformelx")                                    .alias("formula_x"),
            F.col("w.awformely")                                    .alias("formula_y"),
            F.col("w.awovsandtp")                                   .alias("shipping_cost_type"),
            F.col("w.awovsandwt")                                   .alias("shipping_cost_amt"),
            F.col("w.awgematyp")                                    .alias("gema_type"),
            F.col("w.awgemawert")                                   .alias("gema_value"),
            F.trim(F.coalesce(F.col("w.awgarkalk"), F.lit("")))     .alias("warranty_calc_item"),

            # Tier quantities — shared across all price_id rows for the same
            # product/client/currency. Sourced from aelpwah, repeated here.
            F.col("w.awvkstaf1")                                    .alias("tier_qty_1"),
            F.col("w.awvkstaf2")                                    .alias("tier_qty_2"),
            F.col("w.awvkstaf3")                                    .alias("tier_qty_3"),
            F.col("w.awvkstaf4")                                    .alias("tier_qty_4"),
            F.col("w.awvkstaf5")                                    .alias("tier_qty_5"),
            F.col("w.awvkstaf6")                                    .alias("tier_qty_6"),
            F.col("w.awvkstaf7")                                    .alias("tier_qty_7"),
            F.col("w.awvkstaf8")                                    .alias("tier_qty_8"),
            F.col("w.awvkstaf9")                                    .alias("tier_qty_9"),
            F.col("w.awvkstaf10")                                   .alias("tier_qty_10"),
            F.col("p.updtime").alias("updated_at")
        )
        
    ).dropDuplicates(["nk_product_id", "price_id","price_label"])
