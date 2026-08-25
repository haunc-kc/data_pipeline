import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.."))
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from delta.tables import DeltaTable
from datetime import datetime, timezone
from utils.hashing import make_row_hash
from utils.init_load import initial_load

CATALOG       = "workspace"
SCHEMA        = "mention_dw"
SOURCE_HEADER_TABLE  = f"{CATALOG}.{SCHEMA}.fact_purchase_order_header"
SOURCE_LINES_TABLE   = f"{CATALOG}.{SCHEMA}.fact_purchase_order_lines"
LABEL = "Purchase Orders"


_HEADER_CLUSTER_COLS = ["nk_document_id","sk_supplier_id","sk_salesperson_id","document_created_date"]

_LINES_CLUSTER_COLS = ["nk_document_id","nk_document_no","document_created_date","sk_product_id"]

_HEADER_HASH_COLS = ["document_type_code","document_status","sk_salesperson_id","created_date","original_document_date","completed_date","estimated_departure_date","estimated_arrival_date",
"actual_departure_date","actual_arrival_date","amount","purchase_price_total","total_vat","document_discount","shipping_cost","packaging_cost","insurance_cost","freight_handling","customs_cost","internal_procurement_cost","total_weight","header_currency",
"header_exchange_rate","header_exchange_unit","client_currency","delivery_terms","payment_terms","shipping_method","shipping_method_id","partial_delivery_allowed","down_payment_pct","is_blocked","answer_status","header_collective_status","price_enforcement","date_enforcement","auto_email","print_groups","cancellation_editor","cancellation_date","cancellation_comment","original_document_type","original_document_no","dunning_date_1","dunning_date_2","dunning_date_3","dunning_block",
"reference_text","note","pretext","additional_text","additional_xml","external_reference",
"project","project_yy","delivery_note_no", "created_by_user","created_by_time","branch",
"remark_1","remark_2","remark_3","remark_4","remark_5","dw_created_date"]

_LINES_HASH_COLS = ["line_id","position_seq_no","line_document_type","line_client_id","line_supplier_id","warehouse_code","exchange_item_id","delivery_date","line_timestamp","source_document_date","source_document_delivery_date","line_dunning_block_date","quantity","quantity_stock","quantity_stock2","quantity_reserved_stock","quantity_pre_invoiced","quantity_goods_receipt_stock","quantity_return","quantity_completed_returns","unit_purchase_price","unit_purchase_price2","unit_purchase_price_foreign","calc_purchase_price","line_vat_rate","line_document_discount","line_currency","line_exchange_rate","line_exchange_unit","line_client_currency","source_currency","source_exchange_rate","source_exchange_unit","surcharge0","surcharge1","surcharge2","surcharge3","customs_pct","freight_pct","line_shipping","line_packaging","line_insurance","line_internal_procurement_cost","bonus","manufacturer_discount","goods_receipt_surcharge","ear_costs","line_dunning_date_1","line_dunning_date_2","line_dunning_date_3","line_dunning_block","dunning_level","dunning_date","dunning_quantity","line_source_document_type","line_source_document_no","line_source_position_no","line_source_delivery_date_type","customer_document_type","customer_document_no","customer_document_customer_no","customer_document_position_no",

"customer_document_no2","customer_document_client_id","stock_assignment","component_flag","distributor","distributor_order_no","rma_no","delivery_date_type","line_collective_status","reservation_direct","additional_description","line_additional_text","line_additional_xml","hint_sales","old_position_no","position_uuid","contract_warehouse","line_purchase_amount","line_purchase_eur","delivery_time_days","dw_created_date"]

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

dateControl = spark.sql(f"""    Select  date_control
                                From {CATALOG}.{SCHEMA}.etl_fact_pipeline_config
                                Where table_name  = 'fact_purchase_order_header'
                                    And column_name = 'document_created_date'
                                Limit 1
                        """).first()["date_control"]
bestlk_df = (spark.read.table("`bigquery-udp_catalog`.`mention_data`.bestlk")
            .where(f"Cast(lbedatum As Date) >= '{dateControl}'"))
bestlp_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.bestlp")

master_df = (bestlk_df.alias("h")
            .join(bestlp_df.alias("p"), col("h.lbbelid") == col("p.lpbelid"), "left")  
            .select(
                # SECTION 1: DOCUMENT IDENTITY
                col("h.lbbelid").alias("nk_document_id"),
                col("h.lbbelnr").alias("nk_document_no"),
                col("h.lbbeltyp").alias("document_type_code"),
                col("h.lbstatkz").alias("document_status"),

                # SECTION 2: SUPPLIER
                col("h.lbliefnr").alias("nk_supplier_id"),
                col("h.lbbeakz").alias("nk_salesperson_id"),

                # SECTION 3: DATES
                col("h.lbdatum").cast("date").alias("created_date"),
                col("h.lbedatum").cast("date").alias("document_created_date"),
                col("h.lbudatum").cast("date").alias("original_document_date"),
                col("h.lberled").cast("date").alias("completed_date"),
                col("h.lbetd").cast("date").alias("estimated_departure_date"),
                col("h.lbeta").cast("date").alias("estimated_arrival_date"),
                col("h.lbatd").cast("date").alias("actual_departure_date"),
                col("h.lbata").cast("date").alias("actual_arrival_date"),

                # SECTION 4: HEADER FINANCIALS
                col("h.lbbetrag").alias("amount"),
                col("h.lbekpreis").alias("purchase_price_total"),
                col("h.lbmwst").alias("total_vat"),
                col("h.lbbelrab").alias("document_discount"),
                col("h.lbversand").alias("shipping_cost"),
                col("h.lbverpack").alias("packaging_cost"),
                col("h.lbversich").alias("insurance_cost"),
                col("h.lbfracht").alias("freight_handling"),
                col("h.lbzoll").alias("customs_cost"),
                col("h.lbversint").alias("internal_procurement_cost"),
                col("h.lbgewicht").alias("total_weight"),

                # SECTION 5: HEADER CURRENCY
                col("h.lbwaehrung").alias("header_currency"),
                col("h.lbukurs").alias("header_exchange_rate"),
                col("h.lbueinh").alias("header_exchange_unit"),
                col("h.lbmandwaeh").alias("client_currency"),

                # SECTION 7: HEADER T&C
                col("h.lbliefkond").alias("delivery_terms"),
                col("h.lbzb").alias("payment_terms"),
                col("h.lbversart").alias("shipping_method"),
                col("h.lbvartnr").alias("shipping_method_id"),
                col("h.lbteil").alias("partial_delivery_allowed"),
                col("h.lbanzproz").alias("down_payment_pct"),

                # SECTION 8: HEADER STATUS FLAGS
                col("h.lbsperr").alias("is_blocked"),
                col("h.lbantwort").alias("answer_status"),
                col("h.lbsammstat").alias("header_collective_status"),
                col("h.lbpreislim").alias("price_enforcement"),
                col("h.lbtermlim").alias("date_enforcement"),
                col("h.lbemauto").alias("auto_email"),
                col("h.lbkodruck").alias("print_groups"),

                # SECTION 9: CANCELLATION
                col("h.lbstbear").alias("cancellation_editor"),
                col("h.lbstdatum").cast("date").alias("cancellation_date"),
                col("h.lbstorntxt").alias("cancellation_comment"),

                # SECTION 10: SOURCE DOCUMENT
                col("h.lbubeltyp").alias("original_document_type"),
                col("h.lbubelnr").alias("original_document_no"),

                # SECTION 11: DUNNING
                col("h.lbmah1").cast("date").alias("dunning_date_1"),
                col("h.lbmah2").cast("date").alias("dunning_date_2"),
                col("h.lbmah3").cast("date").alias("dunning_date_3"),
                col("h.lbmahnsp").alias("dunning_block"),

                # SECTION 12: HEADER REFERENCES
                col("h.lbtext").alias("reference_text"),
                col("h.lbhinweis").alias("note"),
                col("h.lbvorsatzt").alias("pretext"),
                col("h.lbzusatzt").alias("additional_text"),
                col("h.lbzusatzxml").alias("additional_xml"),
                col("h.lbextref").alias("external_reference"),
                col("h.lbprojekt").alias("project"),
                col("h.lbprojyy").alias("project_yy"),
                col("h.lblsnr").alias("delivery_note_no"),
                col("h.lbwsname").alias("created_by_user"),
                col("h.lbwszeit").alias("created_by_time"),
                col("h.lbfil").alias("branch"),
                col("h.lbbem1").alias("remark_1"),
                col("h.lbbem2").alias("remark_2"),
                col("h.lbbem3").alias("remark_3"),
                col("h.lbbem4").alias("remark_4"),
                col("h.lbbem5").alias("remark_5"),

                # SECTION 13: LINE IDENTITY
                col("p.xsatzid").alias("line_id"),
                col("p.lpposnr").alias("position_seq_no"),
                col("p.lpbeltyp").alias("line_document_type"),
                col("p.lpmankey").alias("line_client_id"),
                col("p.lpliefnr").alias("line_supplier_id"),

                # SECTION 14: LINE PRODUCT & WAREHOUSE
                col("p.lpidnr").alias("nk_product_id"),
                F.trim(F.coalesce(col("p.lplager"), lit(""))).alias("warehouse_code"),
                col("p.lptauidnr").alias("exchange_item_id"),

                # SECTION 15: LINE DATES
                col("p.lptermin").cast("date").alias("delivery_date"),
                col("p.lptime").cast("date").alias("line_timestamp"),
                col("p.lpudatum").cast("date").alias("source_document_date"),
                col("p.lputermin").cast("date").alias("source_document_delivery_date"),
                col("p.lpmahnspb").cast("date").alias("line_dunning_block_date"),

                # SECTION 16: LINE QUANTITIES
                col("p.lpstueck").alias("quantity"),
                col("p.lpteilst").alias("quantity_stock"),
                col("p.lpbest2").alias("quantity_stock2"),
                col("p.lpresbst").alias("quantity_reserved_stock"),
                col("p.lpfakbst").alias("quantity_pre_invoiced"),
                col("p.lpbestwe").alias("quantity_goods_receipt_stock"),
                col("p.lprueck").alias("quantity_return"),
                col("p.lperled").alias("quantity_completed_returns"),

                # SECTION 17: LINE FINANCIALS
                col("p.lpekpreis").alias("unit_purchase_price"),
                col("p.lpek2we").alias("unit_purchase_price2"),
                col("p.lpekwae").alias("unit_purchase_price_foreign"),
                col("p.lpkalkek").alias("calc_purchase_price"),
                col("p.lpmwst").alias("line_vat_rate"),
                col("p.lpbelrab").alias("line_document_discount"),

                # SECTION 18: LINE CURRENCY
                col("p.lpwaehrung").alias("line_currency"),
                F.coalesce(col("p.lpukurs"), lit(1)).alias("line_exchange_rate"),
                F.coalesce(col("p.lpueinh"), lit(1)).alias("line_exchange_unit"),
                col("p.lpmandwaeh").alias("line_client_currency"),
                col("p.lpurwae").alias("source_currency"),
                col("p.lpurkurs").alias("source_exchange_rate"),
                col("p.lpureinh").alias("source_exchange_unit"),

                # SECTION 19: LINE COSTS & SURCHARGES
                F.coalesce(col("p.lpaufschl0"), lit(0)).alias("surcharge0"),
                F.coalesce(col("p.lpaufschl1"), lit(0)).alias("surcharge1"),
                F.coalesce(col("p.lpaufschl2"), lit(0)).alias("surcharge2"),
                F.coalesce(col("p.lpaufschl3"), lit(0)).alias("surcharge3"),
                F.coalesce(col("p.lpzoll"),     lit(0)).alias("customs_pct"),
                F.coalesce(col("p.lpfracht"),   lit(0)).alias("freight_pct"),
                F.coalesce(col("p.lpversand"),  lit(0)).alias("line_shipping"),
                F.coalesce(col("p.lpverpack"),  lit(0)).alias("line_packaging"),
                F.coalesce(col("p.lpversich"),  lit(0)).alias("line_insurance"),
                F.coalesce(col("p.lpversint"),  lit(0)).alias("line_internal_procurement_cost"),
                F.coalesce(col("p.lpbonus"),    lit(0)).alias("bonus"),
                F.coalesce(col("p.lprablag"),   lit(0)).alias("manufacturer_discount"),
                F.coalesce(col("p.lpwezuschl"), lit(0)).alias("goods_receipt_surcharge"),
                F.coalesce(col("p.lpearkost"),  lit(0)).alias("ear_costs"),

                # SECTION 20: LINE DUNNING
                col("p.lpmahn1").cast("date").alias("line_dunning_date_1"),
                col("p.lpmahn2").cast("date").alias("line_dunning_date_2"),
                col("p.lpmahn3").cast("date").alias("line_dunning_date_3"),
                col("p.lpmahnsp").alias("line_dunning_block"),
                col("p.lpmahnnr").alias("dunning_level"),
                col("p.lpmahndat").cast("date").alias("dunning_date"),
                col("p.lpmahnmg").alias("dunning_quantity"),

                # SECTION 21: LINE SOURCE DOCUMENT
                col("p.lpubeltyp").alias("line_source_document_type"),
                col("p.lpubelnr").alias("line_source_document_no"),
                col("p.lpuposnr").alias("line_source_position_no"),
                col("p.lputermart").alias("line_source_delivery_date_type"),

                # SECTION 22: DROP-SHIP LINK
                col("p.lpkdbeltyp").alias("customer_document_type"),
                col("p.lpkdbelnr").alias("customer_document_no"),
                col("p.lpkdnummer").alias("customer_document_customer_no"),
                col("p.lpkdposnr").alias("customer_document_position_no"),
                col("p.lpkdbelman").alias("customer_document_no2"),
                col("p.lpkdmankey").alias("customer_document_client_id"),

                # SECTION 23: LINE ATTRIBUTES
                col("p.lpbestand").alias("stock_assignment"),
                col("p.lpgrund").alias("component_flag"),
                F.trim(F.coalesce(col("p.lpdistrib"), lit(""))).alias("distributor"),
                F.trim(F.coalesce(col("p.lpbestnr"),  lit(""))).alias("distributor_order_no"),
                col("p.lprmanr").alias("rma_no"),
                col("p.lptermart").alias("delivery_date_type"),
                col("p.lpsammstat").alias("line_collective_status"),
                col("p.lpresdir").alias("reservation_direct"),
                col("p.lpzusben").alias("additional_description"),
                col("p.lpzusatzt").alias("line_additional_text"),
                col("p.lpzusatzxml").alias("line_additional_xml"),
                col("p.lphinwavk").alias("hint_sales"),
                col("p.lpposalt").alias("old_position_no"),
                col("p.lpposid").alias("position_uuid"),
                col("p.lpvertlag").alias("contract_warehouse"),

                # SECTION 24: COMPUTED
                (col("p.lpstueck") * col("p.lpekpreis")).alias("line_purchase_amount"),
                (
                    F.when(
                        F.coalesce(col("p.lpukurs"), lit(1)) != lit(0),
                        col("p.lpstueck") * col("p.lpekpreis")
                        / F.coalesce(col("p.lpukurs"), lit(1))
                        * F.coalesce(col("p.lpueinh"), lit(1))
                    ).otherwise(col("p.lpstueck") * col("p.lpekpreis"))
                ).alias("line_purchase_eur"),
                (
                    F.when(
                        col("p.lptermin").isNotNull() & col("h.lbdatum").isNotNull(),
                        F.datediff(col("p.lptermin").cast("date"), col("h.lbdatum").cast("date"))
                    ).otherwise(lit(None))
                ).alias("delivery_time_days"),
            )
    )

dim_salesperson = F.broadcast(
        spark.read.table(f"{CATALOG}.{SCHEMA}.dim_salesperson")
        .select(
            "sk_salesperson_id",
            "nk_salesperson_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
        ).alias("s")

dim_supplier = F.broadcast(
        spark.read.table(f"{CATALOG}.{SCHEMA}.dim_supplier")
        .select(
            "sk_supplier_id",
            "nk_supplier_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("spl")

purchase_header_df = (master_df.alias("m").join(dim_salesperson,on=(
                                                (col("m.nk_salesperson_id") == col("s.nk_salesperson_id")) &
                                                (col("m.document_created_date") >= col("s.__START_AT")) &
                                                (col("m.document_created_date") < coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
                                            ),
                                            how="left",
                                        )
                                        .join(dim_supplier,
                                                    on=(
                                                        (col("m.nk_supplier_id") == col("spl.nk_supplier_id")) &
                                                        (col("m.document_created_date") >= col("spl.__START_AT")) &
                                                        (col("m.document_created_date") < coalesce(col("spl.__END_AT"), lit("2099-12-31").cast("date")))
                                                    ),
                                                    how="left",
                                            )
                    .select(
                        col("nk_document_id"),
                        col("nk_document_no"),
                        col("document_type_code"),
                        col("document_status"),
                        col("sk_supplier_id"),
                        col("sk_salesperson_id"),
                        col("created_date"),
                        col("document_created_date"),
                        col("original_document_date"),
                        col("completed_date"),
                        col("estimated_departure_date"),
                        col("estimated_arrival_date"),
                        col("actual_departure_date"),
                        col("actual_arrival_date"),
                        col("amount"),
                        col("purchase_price_total"),
                        col("total_vat"),
                        col("document_discount"),
                        col("shipping_cost"),
                        col("packaging_cost"),
                        col("insurance_cost"),
                        col("freight_handling"),
                        col("customs_cost"),
                        col("internal_procurement_cost"),
                        col("total_weight"),
                        col("header_currency"),
                        col("header_exchange_rate"),
                        col("header_exchange_unit"),
                        col("client_currency"),
                        col("delivery_terms"),
                        col("payment_terms"),
                        col("shipping_method"),
                        col("shipping_method_id"),
                        col("partial_delivery_allowed"),
                        col("down_payment_pct"),
                        col("is_blocked"),
                        col("answer_status"),
                        col("header_collective_status"),
                        col("price_enforcement"),
                        col("date_enforcement"),
                        col("auto_email"),
                        col("print_groups"),
                        col("cancellation_editor"),
                        col("cancellation_date"),
                        col("cancellation_comment"),
                        col("original_document_type"),
                        col("original_document_no"),
                        col("dunning_date_1"),
                        col("dunning_date_2"),
                        col("dunning_date_3"),
                        col("dunning_block"),
                        col("reference_text"),
                        col("note"),
                        col("pretext"),
                        col("additional_text"),
                        col("additional_xml"),
                        col("external_reference"),
                        col("project"),
                        col("project_yy"),
                        col("delivery_note_no"),
                        col("created_by_user"),
                        col("created_by_time"),
                        col("branch"),
                        col("remark_1"),
                        col("remark_2"),
                        col("remark_3"),
                        col("remark_4"),
                        col("remark_5"),
                        F.current_timestamp().alias("dw_created_date")
                    ).withColumn('row_hash',make_row_hash(_HEADER_HASH_COLS))
                     .dropDuplicates(_HEADER_CLUSTER_COLS + ["nk_document_no","document_status","document_type_code","original_document_date","created_date"])
)

try:
    DeltaTable.forName(spark,SOURCE_HEADER_TABLE).alias("t").merge(purchase_header_df.alias("s"),
        "t.nk_document_id = s.nk_document_id"
        " And t.nk_document_no = s.nk_document_no"
        " And t.document_status = s.document_status"
        " And t.sk_supplier_id = s.sk_supplier_id"
        " And t.sk_salesperson_id = s.sk_salesperson_id"
        " And t.document_type_code = s.document_type_code"
        " And t.original_document_date = s.original_document_date"
        " And t.created_date = s.created_date"
        ).whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _HEADER_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_purchase_order_header MERGE completed.")
except Exception:
    initial_load(purchase_header_df,SOURCE_HEADER_TABLE, _HEADER_CLUSTER_COLS)


dim_product = F.broadcast(
        spark.read.table(f"{CATALOG}.{SCHEMA}.dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("prd")

purchase_lines_df = (master_df.alias("m").join(dim_product,
                                        (col("m.nk_product_id") == col("prd.nk_product_id")) &
                                        (col("m.document_created_date")  >= col("prd.__START_AT")) &
                                        (col("m.document_created_date")  <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date"))),
                    "left")
                     .select(   col("nk_document_id"),
                                col("nk_document_no"),
                                col("document_created_date"),
                                col("prd.sk_product_id"),
                                col("line_id"),
                                col("position_seq_no"),
                                col("line_document_type"),
                                col("line_client_id"),
                                col("line_supplier_id"),
                                col("warehouse_code"),
                                col("exchange_item_id"),
                                col("delivery_date"),
                                col("line_timestamp"),
                                col("source_document_date"),
                                col("source_document_delivery_date"),
                                col("line_dunning_block_date"),
                                col("quantity"),
                                col("quantity_stock"),
                                col("quantity_stock2"),
                                col("quantity_reserved_stock"),
                                col("quantity_pre_invoiced"),
                                col("quantity_goods_receipt_stock"),
                                col("quantity_return"),
                                col("quantity_completed_returns"),
                                col("unit_purchase_price"),
                                col("unit_purchase_price2"),
                                col("unit_purchase_price_foreign"),
                                col("calc_purchase_price"),
                                col("line_vat_rate"),
                                col("line_document_discount"),
                                col("line_currency"),
                                col("line_exchange_rate"),
                                col("line_exchange_unit"),
                                col("line_client_currency"),
                                col("source_currency"),
                                col("source_exchange_rate"),
                                col("source_exchange_unit"),
                                col("surcharge0"),
                                col("surcharge1"),
                                col("surcharge2"),
                                col("surcharge3"),
                                col("customs_pct"),
                                col("freight_pct"),
                                col("line_shipping"),
                                col("line_packaging"),
                                col("line_insurance"),
                                col("line_internal_procurement_cost"),
                                col("bonus"),
                                col("manufacturer_discount"),
                                col("goods_receipt_surcharge"),
                                col("ear_costs"),
                                col("line_dunning_date_1"),
                                col("line_dunning_date_2"),
                                col("line_dunning_date_3"),
                                col("line_dunning_block"),
                                col("dunning_level"),
                                col("dunning_date"),
                                col("dunning_quantity"),
                                col("line_source_document_type"),
                                col("line_source_document_no"),
                                col("line_source_position_no"),
                                col("line_source_delivery_date_type"),
                                col("customer_document_type"),
                                col("customer_document_no"),
                                col("customer_document_customer_no"),
                                col("customer_document_position_no"),
                                col("customer_document_no2"),
                                col("customer_document_client_id"),
                                col("stock_assignment"),
                                col("component_flag"),
                                col("distributor"),
                                col("distributor_order_no"),
                                col("rma_no"),
                                col("delivery_date_type"),
                                col("line_collective_status"),
                                col("reservation_direct"),
                                col("additional_description"),
                                col("line_additional_text"),
                                col("line_additional_xml"),
                                col("hint_sales"),
                                col("old_position_no"),
                                col("position_uuid"),
                                col("contract_warehouse"),
                                col("line_purchase_amount"),
                                col("line_purchase_eur"),
                                col("delivery_time_days"),
                                F.current_timestamp().alias("dw_created_date")  
                        ).withColumn('row_hash',make_row_hash(_LINES_HASH_COLS))
                    ).dropDuplicates(_LINES_CLUSTER_COLS + ["position_seq_no"])

try:
    DeltaTable.forName(spark, SOURCE_LINES_TABLE).alias("t").merge(purchase_lines_df.alias("s"),
                                                                   "t.nk_document_id = s.nk_document_id"
                                                                   " And t.nk_document_no = s.nk_document_no"
                                                                   " And t.position_seq_no = s.position_seq_no"
                                                                   " And t.document_created_date = s.document_created_date"
                                                                   " And t.sk_product_id = s.sk_product_id").whenMatchedUpdate(condition="t.row_hash != s.row_hash",
                        set={c: f"s.{c}" for c in _LINES_HASH_COLS}).whenNotMatchedInsertAll().execute()
    print("[DONE] fact_purchase_order_lines MERGE completed.")
except Exception as e:
    print(e)
    initial_load(purchase_lines_df,SOURCE_LINES_TABLE, _LINES_CLUSTER_COLS) 









