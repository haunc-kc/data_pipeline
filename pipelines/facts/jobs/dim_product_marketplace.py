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
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.hashing import make_row_hash
from utils.init_load import initial_load
from utils.itscope_schema import ITSCOPE_PRODUCT_SCHEMA

CATALOG = _args.pipeline_catalog or os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA  = _args.pipeline_schema  or os.getenv("PIPELINE_SCHEMA",  "mention_dw")

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_product_marketplace"
LABEL = "Product Marketplace Dimension (itscope)"

# rank/qualification intentionally excluded here - they fluctuate per crawl
# and belong in fact_marketplace_price_snapshot instead.
_HASH_COLS = [
    "ean", "manufacturer_sku", "icecat_id", "e_class", "e_class_v7", "unspsc",
    "manufacturer_id", "manufacturer_name", "product_name", "short_description",
    "long_description", "product_type_id", "product_type_group_id",
    "product_type_group_name", "product_type_name",
    "attribute_type_id_1", "attribute_type_name_1",
    "attribute_type_id_2", "attribute_type_name_2",
    "attribute_type_id_3", "attribute_type_name_3",
    "attribute_type_id_4", "attribute_type_name_4",
    "attribute_type_id_5", "attribute_type_name_5",
    "attribute_value_1", "product_sub_type_id", "product_sub_type",
    "color_family_id", "color_family", "estimate_gross_weight",
    "value_added_tax_germany", "deeplink", "standard_html_datasheet",
    "standard_pdf_datasheet", "html_specs", "entry_date",
    "contract_type_id", "contract_type_name", "dw_updated_date",
]

_CLUSTER_COLS = ["puid"]

# Shares the watermark with fact_marketplace_price_snapshot (append-only,
# so its MAX(fetched_at) always advances correctly - dim upserts don't
# always bump fetched_at on matched/unchanged rows, so it can't reliably
# drive its own watermark).
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
        "fetched_at",
        F.from_json(F.col("raw_json"), ITSCOPE_PRODUCT_SCHEMA).alias("j"),
    )
)

parsed = bronze.select(
    F.col("j.puid").alias("puid"),
    F.col("fetched_at"),
    F.col("j.ean").alias("ean"),
    F.col("j.manufacturerSKU").alias("manufacturer_sku"),
    F.col("j.icecatId").alias("icecat_id"),
    F.col("j.eClass").alias("e_class"),
    F.col("j.eClassV7").alias("e_class_v7"),
    F.col("j.UNSPSC").alias("unspsc"),
    F.col("j.manufacturerId").alias("manufacturer_id"),
    F.col("j.manufacturerName").alias("manufacturer_name"),
    F.col("j.productName").alias("product_name"),
    F.col("j.shortDescription").alias("short_description"),
    F.col("j.longDescription").alias("long_description"),
    F.col("j.productTypeId").alias("product_type_id"),
    F.col("j.productTypeGroupId").alias("product_type_group_id"),
    F.col("j.productTypeGroupName").alias("product_type_group_name"),
    F.col("j.productTypeName").alias("product_type_name"),
    F.col("j.attributeTypeId1").alias("attribute_type_id_1"),
    F.col("j.attributeTypeName1").alias("attribute_type_name_1"),
    F.col("j.attributeTypeId2").alias("attribute_type_id_2"),
    F.col("j.attributeTypeName2").alias("attribute_type_name_2"),
    F.col("j.attributeTypeId3").alias("attribute_type_id_3"),
    F.col("j.attributeTypeName3").alias("attribute_type_name_3"),
    F.col("j.attributeTypeId4").alias("attribute_type_id_4"),
    F.col("j.attributeTypeName4").alias("attribute_type_name_4"),
    F.col("j.attributeTypeId5").alias("attribute_type_id_5"),
    F.col("j.attributeTypeName5").alias("attribute_type_name_5"),
    F.col("j.attributeValue1").alias("attribute_value_1"),
    F.col("j.productSubTypeId").alias("product_sub_type_id"),
    F.col("j.productSubType").alias("product_sub_type"),
    F.col("j.colorFamilyId").alias("color_family_id"),
    F.col("j.colorFamily").alias("color_family"),
    F.col("j.estimateGrossWeight").alias("estimate_gross_weight"),
    F.col("j.valueAddedTaxGermany").alias("value_added_tax_germany"),
    F.col("j.deeplink").alias("deeplink"),
    F.col("j.standardHtmlDatasheet").alias("standard_html_datasheet"),
    F.col("j.standardPdfDatasheet").alias("standard_pdf_datasheet"),
    F.col("j.htmlSpecs").alias("html_specs"),
    F.col("j.entryDate").cast("timestamp").alias("entry_date"),
    F.col("j.contractTypeId").alias("contract_type_id"),
    F.col("j.contractTypeName").alias("contract_type_name"),
).where(F.col("puid").isNotNull())

# keep only the latest crawl snapshot per product (SCD1 - overwrite semantics)
w = Window.partitionBy("puid").orderBy(F.col("fetched_at").desc())
df = (
    parsed
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("dw_updated_date", F.current_timestamp())
)
df = df.withColumn("row_hash", make_row_hash(_HASH_COLS))

try:
    DeltaTable.forName(spark, TARGET_TABLE).alias("t").merge(
        df.alias("s"), "t.puid = s.puid"
    ).whenMatchedUpdate(
        condition="t.row_hash != s.row_hash",
        set={**{c: f"s.{c}" for c in _HASH_COLS}, "row_hash": "s.row_hash"},
    ).whenNotMatchedInsertAll().execute()
    print(f"[DONE] {LABEL} MERGE completed.")
except Exception as e:
    print(e)
    initial_load(df, TARGET_TABLE, _CLUSTER_COLS)
