import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.."))

from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from datetime import datetime, timezone
from utils.hashing import make_row_hash
from utils.init_load import initial_load


CATALOG       = os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA        = os.getenv("PIPELINE_SCHEMA",  "mention_dw")
SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_sales_order_header"
SOURCE_LINES_TABLE   = f"{CATALOG}.{SCHEMA}.fact_sales_order_lines"
LABEL = "Sales Orders"

_HEADER_CLUSTER_COLS = ["nk_document_id","sk_customer_id","sk_salesperson_id","transaction_date"]
_LINES_CLUSTER_COLS = ["nk_document_id","nk_document_no","sk_product_id", "transaction_date"]

_HEADER_HASH_COLS = ["document_type_code","document_category_code","reference_document_indicator","collection_status_code","document_status","document_online","nk_payment_term","nk_shipping_method","nk_source_doc_id","nk_source_doc_type","project_code",
"distributor_code","revenue","cost_amount","dw_created_date"]

_LINES_HASH_COLS = [ "warehouse","is_cancellation","quantity","quantity_returned","unit_sales_price_net","unit_sales_price_gross",
                    "unit_purchase_price","unit_purchase_price2","unit_purchase_price_net","discount_pct","discount_pct2",
                    "doc_discount_pct","tax_rate_id","currency_code","exchange_rate","exchange_unit","dw_created_date"]

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

dateControl = spark.sql(f""" SELECT date_control FROM {CATALOG}.{SCHEMA}.etl_fact_pipeline_config WHERE table_name  = 'fact_sales_order_header'
          AND column_name = 'transaction_date'
        LIMIT 1
    """).first()["date_control"]

bestkk_df = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.`bestkk`")
        .where(
            (col("bsmankey") == 1) &
            (col("bsstbear") == 0)
        )
        .where(f"Cast(bsdatum As Date) >= '{dateControl}'")
    )

bestkp_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`bestkp`")

master_df = bestkk_df.join(bestkp_df, on=bestkk_df["bsbelid"] == bestkp_df["bpbelid"], how="inner").select(
                bestkk_df["bsbelid"].cast("bigint").alias("nk_document_id"),
                bestkk_df["bsbelnr"].cast("bigint").alias("nk_document_no"),
                bestkp_df["bpposnr"].cast("bigint").alias("position_seq_no"),
                bestkk_df["bskundennr"].cast("bigint").alias("nk_customer_id"),
                bestkp_df["bpidnr"].cast("bigint").alias("nk_product_id"),
                bestkk_df["bsbeakz"].cast("bigint").alias("nk_salesperson_id"),
                bestkp_df["bplager"].cast("string").alias("warehouse"),
                bestkk_df["bsdatum"].cast("date").alias("transaction_date"),
                bestkp_df["bptermin"].cast("date").alias("delivery_date"),
                bestkk_df["bsbeltyp"].cast("string").alias("document_type_code"),
                bestkk_df["bsgara"].cast("string").alias("document_category_code"),
                F.lower(col("bsbelkz").cast("string")).alias("reference_document_indicator"),
                bestkk_df["bssammstat"].cast("string").alias("collection_status_code"),
                bestkk_df["bsstatkz"].cast("string").alias("document_status"),
                bestkk_df["bsonline"].cast("boolean").alias("document_online"),
                bestkk_df["bszb"].cast("bigint").alias("nk_payment_term"),
                bestkk_df["bsvartnr"].cast("bigint").alias("nk_shipping_method"),
                bestkk_df["bsubelnr"].cast("bigint").alias("nk_source_doc_id"),
                bestkk_df["bsubeltyp"].cast("string").alias("nk_source_doc_type"),
                trim(coalesce(bestkk_df["bsprojekt"].cast("string"), lit(""))).alias("project_code"),
                trim(coalesce(bestkk_df["bsprov"].cast("string"),    lit(""))).alias("distributor_code"),
                when(bestkp_df["bpstorno"] != " ", True).otherwise(False).alias("is_cancellation"),
                bestkp_df["bpstueck"].cast("decimal(38, 9)").alias("quantity"),
                bestkp_df["bpmgut"].cast("decimal(38, 9)").alias("quantity_returned"),
                bestkp_df["bpvkpreis"].cast("decimal(38, 9)").alias("unit_sales_price_net"),
                bestkp_df["bpvkbrutt"].cast("decimal(38, 9)").alias("unit_sales_price_gross"),
                bestkp_df["bpekpreis"].cast("decimal(38, 9)").alias("unit_purchase_price"),
                bestkp_df["bpekpreis2"].cast("decimal(38, 9)").alias("unit_purchase_price2"),
                bestkp_df["bpekrein"].cast("decimal(38, 9)").alias("unit_purchase_price_net"),
                bestkp_df["bprabatt"].cast("decimal(38, 9)").alias("discount_pct"),
                bestkp_df["bprabatt2"].cast("decimal(38, 9)").alias("discount_pct2"),
                bestkp_df["bprabproz"].cast("decimal(38, 9)").alias("doc_discount_pct"),
                bestkp_df["bpmwst"].cast("bigint").alias("tax_rate_id"),
                bestkp_df["bpwaehrung"].cast("string").alias("currency_code"),
                coalesce(bestkp_df["bpukurs"].cast("decimal(38, 9)"), lit(1)).alias("exchange_rate"),
                coalesce(bestkp_df["bpueinh"].cast("decimal(38, 9)"), lit(1)).alias("exchange_unit"),
                (
                    bestkk_df["bsvkpreis"].cast("decimal(38, 6)") /
                    F.when(bestkk_df["bsukurs"] != 0, bestkk_df["bsukurs"]).otherwise(lit(1))
                ).alias("revenue"),
                (bestkk_df["bsekpreis2"] + bestkk_df["bsgema"] + bestkk_df["bskoop"])
                    .cast("decimal(38, 7)").alias("cost_amount"),
)


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

order_header_df = (master_df.alias("m")
                    .join(dim_customer.alias("c"),
                        on=(col("m.nk_customer_id") == col("c.nk_customer_id")) &
                            (col("m.transaction_date") >= col("c.__START_AT")) &
                            (col("m.transaction_date") < coalesce(col("c.__END_AT"), lit("2099-12-31").cast("date"))),
                        how="left")
                    .join(dim_salesperson.alias("sp"), 
                        on=(col("m.nk_salesperson_id") == col("sp.nk_salesperson_id")) &
                            (col("m.transaction_date") >= col("sp.__START_AT")) &
                            (col("m.transaction_date") < coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date"))),
                        how="left")
                .select(
                        col("m.nk_document_id"),
                        col("m.nk_document_no"),
                        col("c.sk_customer_id"),
                        col("sp.sk_salesperson_id"),
                        col("m.transaction_date"),
                        col("m.document_type_code"),
                        col("m.document_category_code"),
                        col("m.reference_document_indicator"),
                        col("m.collection_status_code"),
                        col("m.document_status"),
                        col("m.document_online"),
                        col("m.nk_payment_term"),
                        col("m.nk_shipping_method"),
                        col("m.nk_source_doc_id"),
                        col("m.nk_source_doc_type"),
                        col("m.project_code"),
                        col("m.distributor_code"),
                        col("m.revenue"),
                        col("m.cost_amount"),
                        F.current_timestamp().alias("dw_created_date")
                ).withColumn('row_hash', make_row_hash(_HEADER_HASH_COLS))
                .dropDuplicates(_HEADER_CLUSTER_COLS + ["nk_document_no"])
)
                
_HEADER_SET_COLS = _HEADER_HASH_COLS + ["row_hash"]
try:
    (DeltaTable.forName(spark, SOURCE_HEADER_TABLE).alias("t")
     .merge(order_header_df.alias("s"), "t.nk_document_id = s.nk_document_id"
                                        " And t.nk_document_no = s.nk_document_no"
                                        " And t.sk_customer_id = s.sk_customer_id"
                                        " And t.sk_salesperson_id = s.sk_salesperson_id"
                                        " And t.transaction_date = s.transaction_date")
     .whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _HEADER_SET_COLS})
     .whenNotMatchedInsertAll().execute())
    print("[DONE] fact_sales_invoice_header MERGE completed.")
except Exception: 
    initial_load(order_header_df, SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS)

order_lines_df = (master_df.alias("m").join(dim_product,
        (col("m.nk_product_id") == col("prd.nk_product_id")) &
        (col("m.transaction_date")  >= col("prd.__START_AT")) &
        (col("m.transaction_date")  <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date"))),
        "left")
        .select( col("m.nk_document_id"),
        col("m.nk_document_no"),
        col("m.position_seq_no"),
        col("prd.sk_product_id"),
        col("m.warehouse"),
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
        F.current_timestamp().alias("dw_created_date"),
        col("m.transaction_date")
        ).withColumn('row_hash', make_row_hash(_LINES_HASH_COLS))
        .dropDuplicates(_LINES_CLUSTER_COLS + ["position_seq_no"])
)

_LINES_SET_COLS = _LINES_HASH_COLS + ["row_hash"]
try:
    (DeltaTable.forName(spark, SOURCE_LINES_TABLE).alias("t")
     .merge(order_lines_df.alias("s"), "t.nk_document_id = s.nk_document_id"
                                        " And t.nk_document_no = s.nk_document_no"
                                        " And t.position_seq_no = s.position_seq_no"
                                        " And t.sk_product_id = s.sk_product_id"
                                        " And t.transaction_date = s.transaction_date")
     .whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _LINES_SET_COLS})
     .whenNotMatchedInsertAll().execute())
    print("[DONE] fact_sales_invoice_lines MERGE completed.")
except Exception: 
    initial_load(order_lines_df, SOURCE_LINES_TABLE, _LINES_CLUSTER_COLS)
        
    


















