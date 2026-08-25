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
SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_goods_receipt_lines"
LABEL = "Good Receipt"

_HEADER_CLUSTER_COLS = ["sk_product_id", "nk_receipt_id","sk_supplier_id","nk_document_id"]
_HEADER_HASH_COLS = ["warehouse","position_seq_no","receipt_date","expiry_date","best_before_date","receipt_status","stock_type","quantity_received","quantity_expected","is_defective","quantity_defective","serial_number","house_serial_number","package_no","position_uuid","container_id","serial_addition_1","serial_addition_2","warranty_code","warranty_months","extended_warranty_months","extended_warranty_code","supplier_warranty_months","supplier_warranty_code","source_document_no","source_document_type","delivery_note_no","receipt_remark","goods_receipt_no","storage_location","created_by_user","dw_created_date"]

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

dateControl = spark.sql(f"""
                                SELECT date_control
                                FROM {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                                WHERE table_name  = 'fact_goods_receipt_lines'
                                AND column_name = 'receipt_date'
                                LIMIT 1
                        """).first()["date_control"]

webestlp_df  = (spark.read.table("`bigquery-udp_catalog`.`mention_data`.webestlp")
                .where(f"Cast(lpdatum As Date) >= '{dateControl}'")
        ).alias("we").select(   
                    col("we.lpid")                                      .alias("nk_receipt_id"),
                    col("we.lpbelid")                                   .alias("nk_document_id"),
                    col("we.lpbelnr")                                   .alias("nk_document_no"),
                    col("we.lpposnr")                                   .alias("position_seq_no"),
                    col("we.lpmankey")                                  .alias("nk_client_id"),
                    col("we.lpliefnr")                                  .alias("nk_supplier_id"),
                    col("we.lpidnr")                                    .alias("nk_product_id"),
                    trim(coalesce(col("we.lplager"), lit("")))          .alias("warehouse"),
                    col("we.lpdatum")   .cast("date")                   .alias("receipt_date"),
                    col("we.lpedat")    .cast("date")                   .alias("expiry_date"),
                    col("we.lpmhdatum") .cast("date")                   .alias("best_before_date"),
                    col("we.lpstatus")                                  .alias("receipt_status"),
                    trim(coalesce(col("we.lpwekey"), lit("")))          .alias("stock_type"),
                    coalesce(col("we.lpmgist"),  lit(0))                .alias("quantity_received"),
                    coalesce(col("we.lpmgsoll"), lit(0))                .alias("quantity_expected"),
                    coalesce(col("we.lpdefekt"), lit(False))            .alias("is_defective"),
                    F.when(
                        coalesce(col("we.lpdefekt"), lit(False)),
                        coalesce(col("we.lpmgist"), lit(0))
                    ).otherwise(lit(0))                                 .alias("quantity_defective"),
                    trim(coalesce(col("we.lpsernr"),   lit("")))        .alias("serial_number"),
                    trim(coalesce(col("we.lphaunr"),   lit("")))        .alias("house_serial_number"),
                    trim(coalesce(col("we.lppaketnr"), lit("")))        .alias("package_no"),
                    trim(coalesce(col("we.lpposid"),   lit("")))        .alias("position_uuid"),
                    trim(coalesce(col("we.lpcontid"),  lit("")))        .alias("container_id"),
                    trim(coalesce(col("we.lpserz1"),   lit("")))        .alias("serial_addition_1"),
                    trim(coalesce(col("we.lpserz2"),   lit("")))        .alias("serial_addition_2"),
                    trim(coalesce(col("we.lpgarkenn"), lit("")))        .alias("warranty_code"),
                    coalesce(col("we.lpgarmon"), lit(0))                .alias("warranty_months"),
                    coalesce(col("we.lpergmon"), lit(0))                .alias("extended_warranty_months"),
                    trim(coalesce(col("we.lpergkenn"), lit("")))        .alias("extended_warranty_code"),
                    coalesce(col("we.lplfgmon"), lit(0))                .alias("supplier_warranty_months"),
                    trim(coalesce(col("we.lplfgkenn"), lit("")))        .alias("supplier_warranty_code"),
                    col("we.lpubelnr")                                  .alias("source_document_no"),
                    trim(coalesce(col("we.lpubeltyp"), lit("")))        .alias("source_document_type"),
                    trim(coalesce(col("we.lplbeleg"),  lit("")))        .alias("delivery_note_no"),
                    trim(coalesce(col("we.lpwebem"),   lit("")))        .alias("receipt_remark"),
                    trim(coalesce(col("we.lpwenr"),    lit("")))        .alias("goods_receipt_no"),
                    trim(coalesce(col("we.lplago1"),   lit("")))        .alias("storage_location"),
                    col("we.lpwsname")                                  .alias("created_by_user"),
        )

dim_supplier = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_supplier").select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")).alias("sp")

dim_product = F.broadcast(spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product").select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")).alias("prd")

df = webestlp_df.alias("stg").join(
        dim_supplier,
        on=(
            (col("stg.nk_supplier_id") == col("sp.nk_supplier_id")) &
            (col("stg.receipt_date") >= col("sp.__START_AT")) &
            (col("stg.receipt_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    ).join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.receipt_date") >= col("prd.__START_AT")) &
            (col("stg.receipt_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
        
    ).select(col("sk_supplier_id"),
                col("sk_product_id"),
                col("nk_receipt_id"),
                col("nk_document_id"),
                col("nk_document_no"),
                col("position_seq_no"),
                col("warehouse"),
                col("receipt_date"),
                col("expiry_date"),
                col("best_before_date"),
                col("receipt_status"),
                col("stock_type"),
                col("quantity_received"),
                col("is_defective"),
                col("quantity_expected"),
                col("quantity_defective"),
                col("serial_number"),
                col("house_serial_number"),
                col("package_no"),
                col("position_uuid"),
                col("container_id"),
                col("serial_addition_1"),
                col("serial_addition_2"),
                col("warranty_code"),
                col("warranty_months"),
                col("extended_warranty_months"),
                col("extended_warranty_code"),
                col("supplier_warranty_months"),
                col("supplier_warranty_code"),
                col("source_document_no"),
                col("source_document_type"),
                col("delivery_note_no"),
                col("receipt_remark"),
                col("goods_receipt_no"),
                col("storage_location"),
                col("created_by_user"),
               F.current_timestamp().alias("dw_created_date")
                ).withColumn("row_hash",make_row_hash(_HEADER_HASH_COLS)).dropDuplicates(["nk_document_id", "nk_document_no", "position_seq_no", "receipt_date", "sk_product_id", "sk_supplier_id"])

try:
    DeltaTable.forName(spark, SOURCE_HEADER_TABLE).alias("t").merge(df.alias("s"),"t.nk_document_id = s.nk_document_id"
                                                                    " And t.nk_document_no = s.nk_document_no"
                                                                    " And t.position_seq_no = s.position_seq_no"
                                                                    " And t.receipt_date = s.receipt_date"
                                                                    " And t.sk_product_id = s.sk_product_id"
                                                                    " And t.sk_supplier_id = s.sk_supplier_id").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _HEADER_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_good_receipt_lines MERGE completed.")
except Exception as e:
    print(e)
    initial_load(df,SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS) 





