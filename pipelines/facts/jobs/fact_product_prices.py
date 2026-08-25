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

SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_product_prices"
LABEL = "Product Prices"

_HEADER_CLUSTER_COLS = ["sk_product_id","price_id","price_label"]

_HEADER_HASH_COLS = ["currency","price_group_desc","is_gross_calc","min_gross_profit_pct","max_discount_pct","price_net","price_gross","calc_pct","allow_zero_price","price_min_max_gross","price_min","price_max","tier_price_net_1","tier_price_gross_1","tier_price_net_2","tier_price_gross_2","tier_price_net_3","tier_price_gross_3","tier_price_net_4","tier_price_gross_4","tier_price_net_5","tier_price_gross_5","tier_price_net_6","tier_price_gross_6","tier_price_net_7","tier_price_gross_7","tier_price_net_8","tier_price_gross_8","tier_price_net_9","tier_price_gross_9","tier_price_net_10","tier_price_gross_10",
"recommended_price","no_rounding","component_price_min","component_price_max","cost_procurement","cost_procurement_is_pct","cost_handling","cost_handling_is_pct","cost_testing","cost_testing_is_pct","cost_overhead","cost_overhead_is_pct","formula_type","formula_x","formula_y","shipping_cost_type","shipping_cost_amt","gema_type","gema_value","warranty_calc_item","tier_qty_1","tier_qty_2","tier_qty_3","tier_qty_4","tier_qty_5","tier_qty_6","tier_qty_7","tier_qty_8","tier_qty_9","tier_qty_10","updated_at","dw_created_date"]

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



dateControl = spark.sql(f""" Select  date_control
                                From {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                                Where table_name  = 'fact_product_prices'
                                        AND column_name = 'updated_at'
                                Limit 1
                        """).first()["date_control"]

p  = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aelpprs`").alias("p").where(f"Cast(updtime As Date) >= '{dateControl}'")
w = F.broadcast(spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aelpwah`")).alias("w")
vp = F.broadcast(
    spark.read.table("`bigquery-udp_catalog`.`mention_data`.`veprpar`")
).alias("vp")

df = (
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
            F.trim(F.coalesce(F.col("p.anwaehrung"), F.lit("EUR"))) .alias("currency"),
            F.col("p.anpid")                                        .alias("price_id"),
            F.trim(F.coalesce(F.col("vp.aapbez"), F.lit("")))       .alias("price_label"),
            F.trim(F.coalesce(F.col("vp.aapbem"), F.lit("")))       .alias("price_group_desc"),
            F.coalesce(F.col("vp.aapbrucalc"), F.lit(False))        .alias("is_gross_calc"),
            F.coalesce(F.col("vp.aapwarnroh"), F.lit(0))            .alias("min_gross_profit_pct"),
            F.coalesce(F.col("vp.aapmaxnach"), F.lit(0))            .alias("max_discount_pct"),
            F.coalesce(F.col("p.anvkpreis"),  F.lit(0))             .alias("price_net"),
            F.coalesce(F.col("p.anvkbrutt"),  F.lit(0))             .alias("price_gross"),
            F.coalesce(F.col("p.anvkproz"),   F.lit(0))             .alias("calc_pct"),
            F.when(F.coalesce(F.col("p.anprnull"),   F.lit(0)) > 0, True)
             .otherwise(False)                                       .alias("allow_zero_price"),
            F.when(F.coalesce(F.col("p.anminmaxbr"), F.lit(0)) > 0, True)
             .otherwise(False)                                       .alias("price_min_max_gross"),
            F.coalesce(F.col("p.anprmin"), F.lit(0))                .alias("price_min"),
            F.coalesce(F.col("p.anprmax"), F.lit(0))                .alias("price_max"),
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
    )

dim_product = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product").select(
            "sk_product_id",
            "nk_product_id",
            "__START_AT",
            "__END_AT",
        )
    ).alias("prd")

df = (df.alias("stg").join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.updated_at") >= col("prd.__START_AT")) &
            (col("stg.updated_at") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    ).select(
        col("prd.sk_product_id"),
        col("stg.currency"),
        col("stg.price_id"),
        col("stg.price_label"),
        col("stg.price_group_desc"),
        col("stg.is_gross_calc"),
        col("stg.min_gross_profit_pct"),
        col("stg.max_discount_pct"),
        col("stg.price_net"),
        col("stg.price_gross"),
        col("stg.calc_pct"),
        col("stg.allow_zero_price"),
        col("stg.price_min_max_gross"),
        col("stg.price_min"),
        col("stg.price_max"),
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
    ).withColumn('row_hash',make_row_hash(_HEADER_HASH_COLS))).dropDuplicates(_HEADER_CLUSTER_COLS)

try:
    DeltaTable.forName(spark,SOURCE_HEADER_TABLE).alias("t").merge(df.alias("s"), "t.sk_product_id = s.sk_product_id"
                                                                   " And t.price_id = s.price_id"
                                                                   " And t.price_label = s.price_label").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={**{c: f"s.{c}" for c in _HEADER_HASH_COLS}, "row_hash": "s.row_hash"}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_product_prices MERGE completed.")
except Exception as e:
    initial_load(df,SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS) 
    print(e)





