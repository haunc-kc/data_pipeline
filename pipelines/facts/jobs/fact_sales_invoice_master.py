import sys
import os
import argparse

_parser = argparse.ArgumentParser()
_parser.add_argument("--repo-root")
_args, _ = _parser.parse_known_args()
_repo_root = _args.repo_root or os.getcwd()
sys.path.insert(0, _repo_root)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from datetime import datetime, timezone
from utils.hashing import make_row_hash
from utils.init_load import initial_load

CATALOG       = os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA        = os.getenv("PIPELINE_SCHEMA",  "mention_dw")
SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_sales_invoice_header"
SOURCE_LINES_TABLE   = f"{CATALOG}.{SCHEMA}.fact_sales_invoice_lines"
MASTER_TEMP_TABLE    = f"{CATALOG}.{SCHEMA}.tmp_fact_sales_invoice_master"
LABEL = "Sales Invoices"



dateControl = spark.sql(f"""Select date_control
                            From {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                            Where table_name  = 'fact_sales_invoice_header'
                                    And column_name = 'invoice_date'
                            Limit 1
                        """).first()["date_control"]

rechkk_df = (spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechkk")
            .where(col("bsmankey") == 1)
            .where(col("bsstbear") == 0)
            .where(f"bsdatum >= '{dateControl}'")
            .alias("h")
)

rechkp_df = (
    spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechkp")
    .alias("p")
)

rechk2p_df = (spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechk2p")
                .withColumn("k2pfrei1", F.lower(trim(col("k2pfrei1"))))
                .dropDuplicates(["k2pbelid", "k2pfrei1"])
                .alias("p2")
)

master_df = (
    rechkk_df
    .join(rechkp_df,  col("h.bsbelid") == col("p.bpbelid"),   "left")
    .join(rechk2p_df, col("h.bsbelid") == col("p2.k2pbelid"), "left")
    .select(
        col("h.bsbelid").alias("nk_document_id"),
        col("h.bsbelnr").alias("nk_document_no"),
        col("h.bskundennr").alias("nk_customer_id"),
        col("h.bsbeakz").alias("nk_salesperson_id"),
        col("h.bszb").alias("nk_payment_term_id"),
        col("h.bsvartnr").alias("nk_shipping_method_id"),
        col("h.bsubelnr").alias("nk_source_order_id"),
        col("h.bsubeltyp").alias("nk_source_order_type"),
        col("h.bsdatum").cast("date").alias("invoice_date"),
        col("h.bsedatum").cast("date").alias("document_creation_date"),
        col("h.bsudatum").cast("date").alias("original_document_date"),
        col("h.bsliefdat").cast("date").alias("delivery_date"),
        col("h.bsbeltyp").alias("document_type_code"),
        col("h.bsgara").cast("string").alias("document_category_code"),
        F.lower(col("h.bsbelkz").cast("string")).alias("reference_document_indicator"),
        col("h.bssammstat").cast("string").alias("collection_status_code"),
        col("h.bsstatkz").alias("document_status"),
        when(col("h.bsonline") == True, True).otherwise(False).alias("is_online"),
        when(col("h.bsbeltyp") == "G",  True).otherwise(False).alias("is_credit_note"),
        col("h.bsvkpreis").alias("header_net_total"),
        col("h.bsbrutto").alias("header_gross_total"),
        col("h.bsmwst").alias("header_vat_total"),
        col("h.bsversend").alias("header_shipping_cost"),
        col("h.bsgezahlt").alias("amount_paid"),
        col("h.bsfaellig").alias("due_date"),
        col("h.bszahldat").alias("payment_date"),
        when(col("h.bsoffen") == "O", True).otherwise(False).alias("is_open"),
        when(
            col("h.bszahldat").isNotNull(),
            F.datediff(col("h.bszahldat").cast("date"), col("h.bsdatum").cast("date"))
        ).otherwise(None).alias("days_to_pay"),
        trim(coalesce(col("h.bsprojekt"), lit(""))).alias("project_code"),
        (
            col("h.bsvkpreis").cast("decimal(38,6)") /
            when(col("h.bsukurs") != 0, col("h.bsukurs")).otherwise(lit(1))
        ).alias("revenue"),
        col("h.bsekpreis").alias("purchase_amount"),
        (col("h.bsekpreis2") + col("h.bsgema") + col("h.bskoop")).alias("cost_amount"),
        (col("h.bsversend") + col("h.bsverpack") + col("h.bsmaut") - col("h.bsintkostv")).alias("shipping_amount"),

        col("p.bpposnr").alias("position_seq_no"),
        col("p.bpidnr").alias("nk_product_id"),
        trim(coalesce(col("p.bplager"), lit(""))).alias("warehouse"),
        col("p.bpmankey").alias("pos_mankey"),
        when(col("p.bpstorno") != " ", True).otherwise(False).alias("is_cancellation"),
        col("p.bpstueck").alias("quantity"),
        col("p.bpmgut").alias("quantity_returned"),
        col("p.bpvkpreis").alias("unit_sales_price_net"),
        col("p.bpvkbrutt").alias("unit_sales_price_gross"),
        col("p.bpekpreis").alias("unit_purchase_price"),
        col("p.bpekpreis2").alias("unit_purchase_price2"),
        col("p.bpekrein").alias("unit_purchase_price_net"),
        col("p.bprabatt").alias("discount_pct"),
        col("p.bprabatt2").alias("discount_pct2"),
        col("p.bprabproz").alias("doc_discount_pct"),
        col("p.bpmwst").alias("tax_rate_id"),
        col("p.bpwaehrung").alias("currency_code"),
        col("p.bpukurs").alias("exchange_rate"),
        col("p.bpueinh").alias("exchange_unit"),
        (col("p.bpstueck") * col("p.bpvkpreis")).alias("line_net_amount"),
        (col("p.bpstueck") * col("p.bpvkbrutt")).alias("line_gross_amount"),
        (col("p.bpstueck") * col("p.bpekpreis")).alias("line_purchase_amount"),
        when(
            F.lower(F.coalesce(col("p2.k2pfrei1"), lit(""))).isin("smartstock", "all-lager"),
            "Smart Stock"
        ).otherwise("Direct Business / Warehouse").alias("business_channel"),
    )
)

# Materialize: master_df feeds both header_df and lines_df below (each with its
# own dimension joins), and each branch is scanned again during the Delta MERGE.
# Without materializing, the rechkk/rechkp/rechk2p read+join chain gets
# recomputed multiple times. Serverless compute doesn't support .cache()/
# .persist(), so we write to a temp Delta table and read it back instead.
(
    master_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MASTER_TEMP_TABLE)
)
master_df = spark.read.table(MASTER_TEMP_TABLE)


# ═══════════════════════════════════════════════════════════════════════════════
# Dimension Lookups
# ═══════════════════════════════════════════════════════════════════════════════
dim_customer = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_customer")
                            .select(
                                "sk_customer_id", "nk_customer_id",
                                col("__START_AT").cast("date").alias("__START_AT"),
                                col("__END_AT").cast("date").alias("__END_AT"),
                            )
).alias("c")

dim_salesperson = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_salesperson")
                                .select(
                                    "sk_salesperson_id", "nk_salesperson_id",
                                    col("__START_AT").cast("date").alias("__START_AT"),
                                    col("__END_AT").cast("date").alias("__END_AT"),
                                )
).alias("s")

dim_product = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product")
                        .select(
                            "sk_product_id", "nk_product_id",
                            col("__START_AT").cast("date").alias("__START_AT"),
                            col("__END_AT").cast("date").alias("__END_AT"),
                        )
).alias("prd")


# ═══════════════════════════════════════════════════════════════════════════════
# Header: Transform & Merge
# ═══════════════════════════════════════════════════════════════════════════════
_HEADER_CLUSTER_COLS = ["nk_document_id"
                        ,"sk_customer_id", "sk_salesperson_id"
                        ,"document_creation_date"]

_HEADER_HASH_COLS = [ 
                        "nk_payment_term_id", "nk_shipping_method_id",
                        "nk_source_order_id", "nk_source_order_type",
                        # Dates
                        "invoice_date", "delivery_date",
                        "original_document_date",
                        # Document attributes
                        "document_type_code", "document_category_code",
                        "reference_document_indicator", "collection_status_code", "document_status",
                        "is_online", "is_credit_note",
                        # Header amounts
                        "header_net_total", "header_gross_total", "header_vat_total", "header_shipping_cost",
                        # Payment \u2014 most likely to change after invoice created
                        "amount_paid", "due_date", "payment_date", "is_open", "days_to_pay",
                        # Computed
                        "project_code", "revenue", "purchase_amount", "cost_amount",
                        "shipping_amount", "business_channel"
]

header_df = (
    master_df
    .alias("m")
    .join(dim_customer,
        (col("m.nk_customer_id") == col("c.nk_customer_id")) &
        (col("m.invoice_date")   >= col("c.__START_AT")) &
        (col("m.invoice_date")   <  coalesce(col("c.__END_AT"), lit("2099-12-31").cast("date"))),
        "left")
    .join(dim_salesperson,
        (col("m.nk_salesperson_id") == col("s.nk_salesperson_id")) &
        (col("m.invoice_date")      >= col("s.__START_AT")) &
        (col("m.invoice_date")      <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date"))),
        "left")
    .select(
        col("m.nk_document_id"),
        col("c.sk_customer_id"),
        col("s.sk_salesperson_id"),
        col("m.nk_document_no"),
        col("m.nk_payment_term_id"),
        col("m.nk_shipping_method_id"),
        col("m.nk_source_order_id"),
        col("m.nk_source_order_type"),
        col("m.invoice_date"),
        col("m.document_creation_date"),
        col("m.original_document_date"),
        col("m.delivery_date"),
        col("m.document_type_code"),
        col("m.document_category_code"),
        col("m.reference_document_indicator"),
        col("m.collection_status_code"),
        col("m.document_status"),
        col("m.is_online"),
        col("m.is_credit_note"),
        col("m.header_net_total"),
        col("m.header_gross_total"),
        col("m.header_vat_total"),
        col("m.header_shipping_cost"),
        col("m.amount_paid"),
        col("m.due_date"),
        col("m.payment_date"),
        col("m.is_open"),
        col("m.days_to_pay"),
        col("m.project_code"),
        col("m.revenue"),
        col("m.purchase_amount"),
        col("m.cost_amount"),
        col("m.shipping_amount"),
        col("m.business_channel"),
        F.current_timestamp().alias("dw_updated_date"),
    ).withColumn("row_hash", make_row_hash(_HEADER_HASH_COLS))
).dropDuplicates(_HEADER_CLUSTER_COLS)


_HEADER_SET_COLS = (["sk_customer_id", "sk_salesperson_id"]  + _HEADER_HASH_COLS + ["row_hash", "dw_updated_date"])

try:
    DeltaTable.forName(spark, SOURCE_HEADER_TABLE).alias("t").merge(
        header_df.alias("s"),
        ("t.nk_document_id = s.nk_document_id "
        " And t.nk_document_no = s.nk_document_no "
        " And t.sk_customer_id = s.sk_customer_id"
        " And t.sk_salesperson_id = s.sk_salesperson_id"
        " And t.document_creation_date = s.document_creation_date")
    ).whenMatchedUpdate(
        condition="t.row_hash != s.row_hash",         
        set={c: f"s.{c}" for c in _HEADER_SET_COLS}
    ).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_sales_invoice_header MERGE completed.")
except Exception as e:
    print(e)
    initial_load(header_df, SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS)


print("[INFO] Merging fact_sales_invoice_lines ...")

_LINES_CLUSTER_COLS = ["nk_document_id","nk_document_no","position_seq_no","sk_product_id"]
_LINES_HASH_COLS = [
    "warehouse",
    "quantity", "quantity_returned",
    # Prices
    "unit_sales_price_net", "unit_sales_price_gross",
    "unit_purchase_price", "unit_purchase_price2", "unit_purchase_price_net",
    # Discounts
    "discount_pct", "discount_pct2", "doc_discount_pct",
    # Tax & currency
    "tax_rate_id", "currency_code", "exchange_rate", "exchange_unit",
    # Computed line amounts
    "line_net_amount", "line_gross_amount", "line_purchase_amount",
    # Status
    "is_cancellation", "pos_mankey",
]

lines_df = (
    master_df
    .alias("m")
    
    .join(dim_product,
        (col("m.nk_product_id") == col("prd.nk_product_id")) &
        (col("m.invoice_date")  >= col("prd.__START_AT")) &
        (col("m.invoice_date")  <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date"))),
        "left")
    .select(
        
        col("m.nk_document_id"),
        col("m.nk_document_no"),
        col("m.position_seq_no"),
        
        col("prd.sk_product_id"),
        
        col("m.warehouse"),
        col("m.pos_mankey"),
        col("m.is_cancellation"),
        col("m.quantity"),
        col("m.quantity_returned"),
        col("m.unit_sales_price_net"),
        col("m.unit_sales_price_gross"),
        col("m.unit_purchase_price"),
        col("m.unit_purchase_price2"),
        col("m.unit_purchase_price_net"),
        col("m.discount_pct"),
        col("m.discount_pct2"),
        col("m.doc_discount_pct"),
        col("m.tax_rate_id"),
        col("m.currency_code"),
        col("m.exchange_rate"),
        col("m.exchange_unit"),
        col("m.line_net_amount"),
        col("m.line_gross_amount"),
        col("m.line_purchase_amount"),
        
        F.current_timestamp().alias("dw_updated_date"),
    )
).dropDuplicates(_LINES_CLUSTER_COLS)

lines_df = lines_df.withColumn("row_hash", make_row_hash(_LINES_HASH_COLS))


_LINES_SET_COLS = _LINES_HASH_COLS + ["row_hash", "dw_updated_date"]

try:
    DeltaTable.forName(spark, SOURCE_LINES_TABLE).alias("t").merge(
        lines_df.alias("s"),
        ("t.nk_document_id = s.nk_document_id "
         " And t.nk_document_no = s.nk_document_no "
         " And t.position_seq_no = s.position_seq_no "
         " And t.sk_product_id = s.sk_product_id")
    ).whenMatchedUpdate(
        condition="t.row_hash != s.row_hash",         
        set={c: f"s.{c}" for c in _LINES_SET_COLS}
    ).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_sales_invoice_lines MERGE completed.")
except Exception as e:
    print(e)
    initial_load(lines_df, SOURCE_LINES_TABLE, _LINES_CLUSTER_COLS)

spark.sql(f"DROP TABLE IF EXISTS {MASTER_TEMP_TABLE}")


