import sys
import os
import argparse

_parser = argparse.ArgumentParser()
_parser.add_argument("--repo-root")
_parser.add_argument("--pipeline-catalog")
_parser.add_argument("--pipeline-schema")
_args, _ = _parser.parse_known_args()
_repo_root = _args.repo_root or os.getcwd()
sys.path.insert(0, _repo_root)
from pyspark.sql import functions as F
from utils.init_load import initial_load
from utils.itscope_schema import ITSCOPE_PRODUCT_SCHEMA

CATALOG = _args.pipeline_catalog or os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA  = _args.pipeline_schema  or os.getenv("PIPELINE_SCHEMA",  "mention_dw")

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.fact_marketplace_price_snapshot"
LABEL = "Marketplace Competitor Offer Snapshot (itscope)"

# Append-only periodic snapshot fact - every crawl is a new observation,
# we NEVER merge/dedup price or stock history here.
#
# NOTE: the top-level "primary" offer (price/stock/rank/aggregated fields)
# now lives as current-state columns on dim_product_marketplace instead of
# being duplicated here - this fact only carries per-competitor-offer
# history from the `supplierItems` array.
_CLUSTER_COLS = ["puid", "fetched_at"]

dateControl = spark.sql(f"""
    SELECT date_control
    FROM {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
    WHERE table_name  = 'fact_marketplace_price_snapshot'
      AND column_name = 'fetched_at'
    LIMIT 1
""").first()["date_control"]

bronze = (
    spark.read.table("`bigquery-udp_catalog`.itscope_raw.products")
    .where(f"Cast(fetched_at As Date) >= '{dateControl}'")
    .select(
        F.col("fetched_at"),
        F.col("source"),
        F.from_json(F.col("raw_json"), ITSCOPE_PRODUCT_SCHEMA).alias("j"),
    )
    .where(F.col("j.puid").isNotNull())
)

df = bronze.select(
    F.col("fetched_at"),
    F.col("source"),
    F.col("j.puid").alias("puid"),
    F.explode("j.supplierItems").alias("o"),
).select(
    F.col("fetched_at"),
    F.col("source"),
    F.col("puid"),
    F.col("o.id").alias("supplier_item_id"),
    F.col("o.ean").alias("ean"),
    F.col("o.manufacturerSKU").alias("manufacturer_sku"),
    F.col("o.supplierSKU").alias("supplier_sku"),
    F.col("o.supplierId").alias("supplier_id"),
    F.col("o.supplierName").alias("supplier_name"),
    F.col("o.manufacturerName").alias("offer_manufacturer_name"),
    F.col("o.productName").alias("offer_product_name"),
    F.col("o.longDescription").alias("offer_long_description"),
    F.col("o.conditionId").alias("condition_id"),
    F.col("o.conditionName").alias("condition_name"),
    F.col("o.eolProduct").alias("eol_product"),
    F.col("o.matchQuality").alias("match_quality"),
    F.col("o.eanValid").alias("ean_valid"),
    F.col("o.specialOffer").alias("special_offer"),
    F.col("o.price").alias("price"),
    F.col("o.priceCalc").alias("price_calc"),
    F.col("o.currencyCode").alias("currency_code"),
    F.col("o.priceCalcVat").alias("price_calc_vat"),
    F.col("o.priceLastUpdate").cast("timestamp").alias("price_last_update"),
    F.col("o.stockSupplierText").alias("stock_supplier_text"),
    F.col("o.stockStatus").alias("stock_status"),
    F.col("o.stockStatusText").alias("stock_status_text"),
    F.col("o.stock").alias("stock"),
    F.col("o.externalStock").alias("external_stock"),
    F.col("o.stockAvailabilityDate").cast("timestamp").alias("stock_availability_date"),
    F.col("o.lastStockUpdate").cast("timestamp").alias("last_stock_update"),
    F.col("o.contractTypeId").alias("contract_type_id"),
    F.col("o.contractTypeName").alias("contract_type_name"),
    F.col("o.grossDimX").alias("gross_dim_x"),
    F.col("o.grossDimY").alias("gross_dim_y"),
    F.col("o.grossDimZ").alias("gross_dim_z"),
    F.col("o.recommendedRetailPriceNet").alias("recommended_retail_price_net"),
).withColumn("dw_created_date", F.current_timestamp())

# --- link own listings (KOSATEC offers) back to internal dim_product ---
# is_own_listing flags any offer from your own company regardless of match
# success; sk_product_id is only populated when the supplier_sku actually
# matches a known product_number, so the two can be inspected separately
# to catch data-quality gaps (own listing present but unmatched).
OWN_SUPPLIER_NAME = "KOSATEC"

dim_product = F.broadcast(
    spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product")
    .select("sk_product_id", "product_number", "__START_AT", "__END_AT")
).alias("prd")

df = (
    df.alias("s")
    .withColumn("is_own_listing", F.col("s.supplier_name") == F.lit(OWN_SUPPLIER_NAME))
    .join(
        dim_product,
        on=(
            (F.trim(F.col("s.supplier_sku")) == F.trim(F.col("prd.product_number"))) &
            (F.col("s.supplier_name") == F.lit(OWN_SUPPLIER_NAME)) &
            (F.col("s.fetched_at") >= F.col("prd.__START_AT")) &
            (F.col("s.fetched_at") <  F.coalesce(F.col("prd.__END_AT"), F.lit("2099-12-31").cast("timestamp")))
        ),
        how="left",
    )
    .select("s.*", "prd.sk_product_id")
)

try:
    # append-only: never merge/dedup - every crawl is a new historical row
    if not spark.catalog.tableExists(TARGET_TABLE):
        raise Exception(f"{TARGET_TABLE} does not exist yet")
    df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    print(f"[DONE] {LABEL} append completed.")
except Exception as e:
    print(e)
    initial_load(df, TARGET_TABLE, _CLUSTER_COLS)
