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
SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_supplier_invoice_header"
SOURCE_LINES_TABLE   = f"{CATALOG}.{SCHEMA}.fact_supplier_invoice_lines"
LABEL = "Supplier Invoices"

_HEADER_CLUSTER_COLS = ["sk_supplier_id","sk_salesperson_id","nk_document_id","invoice_date"]
_LINES_CLUSTER_COLS = ["nk_document_id","nk_document_no","position_seq_no","invoice_date"]


_HEADER_HASH_COLS = ["document_type","internal_document_no","entry_date","due_date","payment_date","vat_date","cancellation_date","export_date","amount_gross","amount_net","total_vat","shipping_cost","packaging_cost","insurance_cost","amount_paid","payment_amount","is_open","is_cancelled","payment_block","payment_method","cash_discount_days","cash_discount_pct","currency_code","exchange_rate","exchange_unit","client_currency","reference_text","remark","note","project","branch","payables_account","cancellation_editor","dw_created_date"]

_LINES_HASH_COLS = ["sk_product_id","goods_receipt_no","goods_receipt_type","line_status","quantity_billed","unit_price","line_amount","currency_code","exchange_rate","exchange_unit","is_cancelled","document_type","dw_created_date"]


dateControl = spark.sql(f""" SELECT date_control 
                        FROM {CATALOG}.{SCHEMA}.etl_fact_pipeline_config 
                        WHERE table_name  = 'fact_supplier_invoice_header'
                            AND column_name = 'invoice_date'
                        LIMIT 1
                    """).first()["date_control"]

rechlk_df   = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechlk")
        .where(f"Cast(lrdatum As Date) >= '{dateControl}'")
        # .where(col("lrdatum") >= dateControl)
    )
rechlkwe_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechlkwe")
master_df = (rechlk_df.alias("h")
                .join(rechlkwe_df.alias("lp"),
                        col("h.lrbelid") == col("lp.lrwbelid"),
                        how = "left"
            )
        .select(
            col("h.lrbelid")                                    .alias("nk_document_id"),
            col("h.lrbelegnr")                                  .alias("nk_document_no"),
            col("h.lrbelegtyp")                                 .alias("document_type"),
            col("h.lrliefnr")                                   .alias("nk_supplier_id"),
            col("h.lrbeakz")                                    .alias("nk_salesperson_id"),
            col("h.lrintbelnr")                                 .alias("internal_document_no"),
            col("h.lrdatum")    .cast("date")                   .alias("invoice_date"),
            col("h.lreingabe")  .cast("date")                   .alias("entry_date"),
            col("h.lrfaellig")  .cast("date")                   .alias("due_date"),
            col("h.lrzahldat")  .cast("date")                   .alias("payment_date"),
            col("h.lrustdatum") .cast("date")                   .alias("vat_date"),
            col("h.lrstdatum")  .cast("date")                   .alias("cancellation_date"),
            col("h.lrexpdat")   .cast("date")                   .alias("export_date"),
            coalesce(col("h.lrbrutto"),  lit(0))                .alias("amount_gross"),
            coalesce(col("h.lrnetto"),   lit(0))                .alias("amount_net"),
            coalesce(col("h.lrmwst"),    lit(0))                .alias("total_vat"),
            coalesce(col("h.lrversend"), lit(0))                .alias("shipping_cost"),
            coalesce(col("h.lrverpack"), lit(0))                .alias("packaging_cost"),
            coalesce(col("h.lrversich"), lit(0))                .alias("insurance_cost"),
            coalesce(col("h.lrgezahlt"), lit(0))                .alias("amount_paid"),
            coalesce(col("h.lrzahlung"), lit(0))                .alias("payment_amount"),
            F.when(col("h.lroffen") == "J", lit(True))
             .otherwise(lit(False))                             .alias("is_open"),
            F.when(col("h.lrstbear") > 0, lit(True))
             .otherwise(lit(False))                             .alias("is_cancelled"),
            trim(coalesce(col("h.lropk"),    lit("")))          .alias("payment_block"),
            col("h.lrzahlkenn")                                 .alias("payment_method"),
            coalesce(col("h.lrsktage"), lit(0))                 .alias("cash_discount_days"),
            coalesce(col("h.lrskproz"), lit(0))                 .alias("cash_discount_pct"),
            trim(coalesce(col("h.lrwaehrung"), lit("EUR")))     .alias("currency_code"),
            coalesce(col("h.lrukurs"), lit(1))                  .alias("exchange_rate"),
            coalesce(col("h.lrueinh"), lit(1))                  .alias("exchange_unit"),
            trim(coalesce(col("h.lrmandwaeh"), lit("EUR")))     .alias("client_currency"),
            trim(coalesce(col("h.lrtext"),    lit("")))         .alias("reference_text"),
            trim(coalesce(col("h.lrbem"),     lit("")))         .alias("remark"),
            trim(coalesce(col("h.lrhinweis"), lit("")))         .alias("note"),
            trim(coalesce(col("h.lrprojekt"), lit("")))         .alias("project"),
            trim(coalesce(col("h.lrfil"),     lit("")))         .alias("branch"),
            col("h.lrkrekto")                                   .alias("payables_account"),
            col("h.lrstbear")                                   .alias("cancellation_editor"),
            col("lp.xsatzid")                                   .alias("nk_line_id"),
            col("lp.lrwposnr")                                  .alias("position_seq_no"),
            col("lp.lrwidnr")                                   .alias("nk_product_id"),
            col("lp.lrwmankey")                                 .alias("line_client_id"),
            col("lp.lrwbelnr")                                  .alias("goods_receipt_no"),
            trim(coalesce(col("lp.lrwbeltyp"), lit("")))        .alias("goods_receipt_type"),
            trim(coalesce(col("lp.lrwstatkz"), lit("")))        .alias("line_status"),
            coalesce(col("lp.lrwstueck"),  lit(0))              .alias("quantity_billed"),
            coalesce(col("lp.lrwlfpreis"), lit(0))              .alias("unit_price"),
            (coalesce(col("lp.lrwstueck"), lit(0)) *
             coalesce(col("lp.lrwlfpreis"), lit(0)))            .alias("line_amount"),
        )
    )

dim_supplier = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_supplier")
            .select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")
    ).alias("sp")
dim_salesperson = (spark.read.table(f"{CATALOG}.{SCHEMA}.dim_salesperson")
                .select("sk_salesperson_id", "nk_salesperson_id", "__START_AT", "__END_AT")
    ).alias("s")

supplier_invoice_header_df = master_df.alias("m").join(dim_supplier,
            on=(
                (col("m.nk_supplier_id") == col("sp.nk_supplier_id")) &
                (col("m.invoice_date") >= col("sp.__START_AT")) &
                (col("m.invoice_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
            ),
        how="left").join(
        dim_salesperson,
            on=(
                (col("m.nk_salesperson_id") == col("s.nk_salesperson_id")) &
                (col("m.invoice_date") >= col("s.__START_AT")) &
                (col("m.invoice_date") <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
            ),
        how="left"
        ).select(
            col("sp.sk_supplier_id"),
            col("s.sk_salesperson_id"),
            col("m.nk_document_id"),
            col("m.nk_document_no"),
            col("m.document_type"),
            col("m.internal_document_no"),
            col("m.invoice_date"),
            col("m.entry_date"),
            col("m.due_date"),
            col("m.payment_date"),
            col("m.vat_date"),
            col("m.cancellation_date"),
            col("m.export_date"),
            col("m.amount_gross"),
            col("m.amount_net"),
            col("m.total_vat"),
            col("m.shipping_cost"),
            col("m.packaging_cost"),
            col("m.insurance_cost"),
            col("m.amount_paid"),
            col("m.payment_amount"),
            col("m.is_open"),
            col("m.is_cancelled"),
            col("m.payment_block"),
            col("m.payment_method"),
            col("m.cash_discount_days"),
            col("m.cash_discount_pct"),
            col("m.currency_code"),
            col("m.exchange_rate"),
            col("m.exchange_unit"),
            col("m.client_currency"),
            col("m.reference_text"),
            col("m.remark"),
            col("m.note"),
            col("m.project"),
            col("m.branch"),
            col("m.payables_account"),
            col("m.cancellation_editor"),
            F.current_timestamp().alias("dw_created_date"),
        ).withColumn('row_hash',make_row_hash(_HEADER_HASH_COLS)).dropDuplicates(_HEADER_CLUSTER_COLS + ["document_type"])
try:
    DeltaTable.forName(spark, SOURCE_HEADER_TABLE).alias("t").merge(supplier_invoice_header_df.alias("s"), "t.sk_supplier_id = s.sk_supplier_id  And t.sk_salesperson_id = s.sk_salesperson_id And t.nk_document_id = s.nk_document_id And t.nk_document_no = s.nk_document_no And t.invoice_date = s.invoice_date").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _HEADER_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_supplier_invoice_header MERGE completed.")
except Exception as e:
    print(e)
    initial_load(supplier_invoice_header_df,SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS)


dim_product = (
        spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("prd")

supplier_invoice_lines =  master_df.alias("m").join(dim_product,
        on=(
            (col("m.nk_product_id") == col("prd.nk_product_id")) &
            (col("m.invoice_date") >= col("prd.__START_AT")) &
            (col("m.invoice_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    ).select(
        col("prd.sk_product_id"),
        col("m.nk_line_id"),
        col("m.nk_document_id"),
        col("m.nk_document_no"),
        col("m.position_seq_no"),

        col("m.goods_receipt_no"),
        col("m.goods_receipt_type"),
        col("m.line_status"),
        col("m.quantity_billed"),
        col("m.unit_price"),
        col("m.line_amount"),
        col("m.currency_code"),
        col("m.exchange_rate"),
        col("m.exchange_unit"),
        col("m.is_cancelled"),
        col("m.document_type"),
        col("invoice_date").alias("invoice_date"),
        F.current_timestamp().alias("dw_created_date")
        ).withColumn('row_hash', make_row_hash(_LINES_HASH_COLS)).dropDuplicates(_LINES_CLUSTER_COLS)
try:
    DeltaTable.forName(spark, SOURCE_LINES_TABLE).alias("t").merge(supplier_invoice_lines.alias("s"), "t.nk_document_id = s.nk_document_id And t.nk_document_no = s.nk_document_no And t.nk_line_id = s.nk_line_id And t.sk_product_id = s.sk_product_id And s.invoice_date = t.invoice_date").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _LINES_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_supplier_invoice_lines MERGE completed.")
except Exception as e: 
    print(e)
    initial_load(supplier_invoice_lines, SOURCE_LINES_TABLE, _LINES_CLUSTER_COLS)