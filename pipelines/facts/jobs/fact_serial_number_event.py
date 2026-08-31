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
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from utils.hashing import make_row_hash
from datetime import datetime, timezone
from utils.init_load import initial_load

CATALOG       = _args.pipeline_catalog or os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA        = _args.pipeline_schema  or os.getenv("PIPELINE_SCHEMA",  "mention_dw")
SOURCE_TABLE  = f"{CATALOG}.{SCHEMA}.fact_serial_number_event"
LABEL = "Serial Number"

_HEADER_CLUSTER_COLS = ["nk_event_id","serial_number","nk_party_id","nk_document_id"]
_HEADER_HASH_COLS = ["house_serial_number","quantity","is_batch","is_outbound","movement_direction","party_type_code","party_type"
                    ,"nk_party_id","warranty_code","warranty_months","extended_warranty_code","extended_warranty_months","supplier_warranty_code","supplier_warranty_months","is_cancelled","cancellation_editor","error_code","error_text"
                    ,"delivery_note_no","is_external","remark_1","remark_2","serial_addition_1","serial_addition_2"
                    ]



dateControl = spark.sql(f"""Select date_control
                            From {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                            Where table_name  = 'fact_serial_number_event'
                                    And column_name = 'event_date'
                            Limit 1
                        """).first()["date_control"]


serien_df = (spark.read.table("`bigquery-udp_catalog`.`mention_data`.serien")
            # .where(col("sedatum") >= dateControl)
            .where(f"Cast(sedatum As Date) >= '{dateControl}'")
    ).select(
            col("seserid")                                       .alias("nk_event_id"),
            col("seidnr")                                        .alias("nk_product_id"),
            trim(coalesce(col("selager"),  lit("")))             .alias("warehouse"),
            trim(coalesce(col("sesernr"),  lit("")))             .alias("serial_number"),
            trim(coalesce(col("sehaunr"),  lit("")))             .alias("house_serial_number"),
            col("seserac")                                       .alias("quantity"),
            col("sechargen")                                     .alias("is_batch"),
            col("seaus")                                         .alias("is_outbound"),
            when(col("seaus") == True,  lit("OUT"))
            .otherwise(lit("IN"))                                   .alias("movement_direction"),
            trim(coalesce(col("sekltyp"), lit("")))              .alias("party_type_code"),
            when(col("sekltyp") == "K", lit("Customer"))
            .when(col("sekltyp") == "L", lit("Supplier"))
            .otherwise(lit("Unknown"))                              .alias("party_type"),
            col("sekdnummer")                                    .alias("nk_party_id"),
            col("sebelid")                                       .alias("nk_document_id"),
            trim(coalesce(col("sebeltyp"), lit("")))             .alias("document_type_code"),
            col("sebelnr")                                       .alias("document_no"),
            col("seposnr")                                       .alias("position_no"),
            when(col("sebeltyp").isin("F", "G", "M", "O"), lit("Sales Invoice"))
            .when(col("sebeltyp").isin("A", "B", "L"),      lit("Sales Order"))
            .when(col("sebeltyp").isin("E"),                 lit("Purchase Order"))
            .when(col("sebeltyp").isin("W"),                 lit("Goods Receipt"))
            .when(col("sebeltyp").isin("R"),                 lit("Supplier Invoice"))
            .otherwise(lit("Other"))                                .alias("document_category"),
            col("sedatum")   .cast("date")                       .alias("event_date"),
            col("seekredat") .cast("date")                       .alias("created_at"),
            col("semhdatum") .cast("date")                       .alias("best_before_date"),
            col("selfgadat") .cast("date")                       .alias("warranty_expiry_date"),
            col("sestdatum") .cast("date")                       .alias("cancellation_date"),

            trim(coalesce(col("segarkenn"),  lit("")))           .alias("warranty_code"),
            coalesce(col("segarmon"),  lit(0))                   .alias("warranty_months"),
            trim(coalesce(col("seergkenn"),  lit("")))           .alias("extended_warranty_code"),
            coalesce(col("seergmon"),  lit(0))                   .alias("extended_warranty_months"),
            trim(coalesce(col("selfgkenn"),  lit("")))           .alias("supplier_warranty_code"),
            coalesce(col("selfgmon"),  lit(0))                   .alias("supplier_warranty_months"),
            when(col("sestorno") == "J", lit(True))
            .otherwise(lit(False))                                  .alias("is_cancelled"),
            col("sestbear")                                      .alias("cancellation_editor"),
            trim(coalesce(col("sekztext"), lit("")))             .alias("error_code"),
            trim(coalesce(col("setext"),   lit("")))             .alias("error_text"),
            trim(coalesce(col("selbeleg"), lit("")))             .alias("delivery_note_no"),
            col("sebea")                                         .alias("nk_responsible_employee_id"),
            col("sefremd")                                       .alias("is_external"),
            trim(coalesce(col("sebem1"),   lit("")))             .alias("remark_1"),
            trim(coalesce(col("sebem2"),   lit("")))             .alias("remark_2"),
            trim(coalesce(col("seserz1"),  lit("")))             .alias("serial_addition_1"),
            trim(coalesce(col("seserz2"),  lit("")))             .alias("serial_addition_2"))

dim_product = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product").select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")).alias("prd")

dim_customer = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_customer").select("sk_customer_id", "nk_customer_id", "__START_AT", "__END_AT")).alias("cust")

dim_supplier = F.broadcast(spark.read  .table(f"{CATALOG}.{SCHEMA}.dim_supplier").select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")).alias("sp")

dim_salesperson = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_salesperson").select("sk_salesperson_id","nk_salesperson_id",
        col("__START_AT").cast("date").alias("__START_AT"),
        col("__END_AT").cast("date").alias("__END_AT"),
    )
).alias("s")

df = (serien_df.alias("se").join(dim_product,
                            on=(
                                (col("se.nk_product_id") == col("prd.nk_product_id")) &
                                (col("se.event_date") >= col("prd.__START_AT")) &
                                (col("se.event_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
                            ),
                            how="left")
       
        .join(dim_customer,
                on=(
                    (col("se.party_type_code") == lit("K")) &
                    (col("se.nk_party_id") == col("cust.nk_customer_id")) &
                    (col("se.event_date") >= col("cust.__START_AT")) &
                    (col("se.event_date") <  coalesce(col("cust.__END_AT"), lit("2099-12-31").cast("date")))
                ),
                how="left")
        .join(dim_supplier,
                on=(
                    (col("se.party_type_code") == lit("L")) &
                    (col("se.nk_party_id") == col("sp.nk_supplier_id")) &
                    (col("se.event_date") >= col("sp.__START_AT")) &
                    (col("se.event_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
                ),
                how="left")
        .join(dim_salesperson,
                on=(
                    (col("se.nk_responsible_employee_id") == col("s.nk_salesperson_id")) &
                    (col("se.event_date") >= col("s.__START_AT")) &
                    (col("se.event_date") <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
                ),
                how="left")
        
).select(col("prd.sk_product_id"),
        col("cust.sk_customer_id"),
        col("sp.sk_supplier_id"),
        col("se.nk_event_id"),
        col("se.nk_party_id"),
        col("se.nk_document_id"),
        col("nk_responsible_employee_id"),
        col("s.sk_salesperson_id"),
        col("se.serial_number"),
        col("se.warehouse"),
        col("se.house_serial_number"),
        col("se.quantity"),
        col("se.is_batch"),
        col("se.is_outbound"),
        col("se.movement_direction"),
        col("se.party_type_code"),
        col("se.party_type"),
        col("se.document_type_code"),
        col("se.document_category"),
        col("se.document_no"),
        col("se.position_no"),
        col("se.event_date"),
        col("se.created_at"),
        col("se.best_before_date"),
        col("se.warranty_expiry_date"),
        col("se.cancellation_date"),
        col("se.warranty_code"),
        col("se.warranty_months"),
        col("se.extended_warranty_code"),
        col("se.extended_warranty_months"),
        col("se.supplier_warranty_code"),
        col("se.supplier_warranty_months"),
        col("se.is_cancelled"),
        col("se.cancellation_editor"),
        col("se.is_external"),
        col("se.error_code"),
        col("se.error_text"),
        col("se.delivery_note_no"),
        col("se.remark_1"),
        col("se.remark_2"),
        col("se.serial_addition_1"),
        col("se.serial_addition_2"),
        F.current_timestamp().alias("dw_created_date"),
    ).withColumn("row_hash", make_row_hash(_HEADER_HASH_COLS))

_HEADER_SET_COLS = (_HEADER_HASH_COLS + ["row_hash", "dw_updated_date"])
try:
    DeltaTable.forName(spark, SOURCE_TABLE).alias('t').merge(df.alias("s"),("t.nk_event_id = s.nk_event_id"
                                                                        " And t.sk_product_id = s.sk_product_id"
                                                                        " And t.warehouse = s.warehouse"
                                                                        " And t.house_serial_number = s.house_serial_number"
                                                                        " And t.nk_document_id = s.nk_document_id"
                                                                        " And t.created_at = s.created_at")
    ).whenMatchedUpdate(condition="t.row_hash != s.row_hash"
                        ,set={c: f"s.{c}" for c in _HEADER_SET_COLS}
                        ).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_sales_invoice_header MERGE completed.")
except Exception:
    initial_load(df, SOURCE_TABLE, _HEADER_CLUSTER_COLS)

