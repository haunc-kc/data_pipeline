from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType, IntegerType)

# Schema matches exactly the select() output below — used for the SKIP empty return
# No circular reference — pure in-memory, zero I/O, instant
_STG_PO_SCHEMA = StructType([
    # SECTION 1: DOCUMENT IDENTITY
    StructField("nk_document_id",                   LongType(),         True),
    StructField("nk_document_no",                   LongType(),         True),
    StructField("document_type_code",               StringType(),       True),
    StructField("document_status",                  StringType(),       True),
    # SECTION 2: SUPPLIER
    StructField("nk_supplier_id",                   LongType(),         True),
    StructField("nk_salesperson_id",                LongType(),         True),
    # SECTION 3: DATES
    StructField("created_date",                     DateType(),         True),
    StructField("document_created_date",            DateType(),         True),
    StructField("original_document_date",           DateType(),         True),
    StructField("completed_date",                   DateType(),         True),
    StructField("estimated_departure_date",         DateType(),         True),
    StructField("estimated_arrival_date",           DateType(),         True),
    StructField("actual_departure_date",            DateType(),         True),
    StructField("actual_arrival_date",              DateType(),         True),
    # SECTION 4: HEADER FINANCIALS
    StructField("amount",                           DecimalType(38, 6), True),
    StructField("purchase_price_total",             DecimalType(38, 6), True),
    StructField("total_vat",                        DecimalType(38, 6), True),
    StructField("document_discount",                DecimalType(38, 6), True),
    StructField("shipping_cost",                    DecimalType(38, 6), True),
    StructField("packaging_cost",                   DecimalType(38, 6), True),
    StructField("insurance_cost",                   DecimalType(38, 6), True),
    StructField("freight_handling",                 DecimalType(38, 6), True),
    StructField("customs_cost",                     DecimalType(38, 6), True),
    StructField("internal_procurement_cost",        DecimalType(38, 6), True),
    StructField("total_weight",                     DecimalType(38, 6), True),
    # SECTION 5: HEADER CURRENCY
    StructField("header_currency",                  StringType(),       True),
    StructField("header_exchange_rate",             DecimalType(38, 9), True),
    StructField("header_exchange_unit",             DecimalType(38, 9), True),
    StructField("client_currency",                  StringType(),       True),
    # SECTION 7: HEADER T&C
    StructField("delivery_terms",                   StringType(),       True),
    StructField("payment_terms",                    LongType(),         True),
    StructField("shipping_method",                  StringType(),       True),
    StructField("shipping_method_id",               LongType(),         True),
    StructField("partial_delivery_allowed",         StringType(),       True),
    StructField("down_payment_pct",                 DecimalType(38, 6), True),
    # SECTION 8: HEADER STATUS FLAGS
    StructField("is_blocked",                       StringType(),       True),
    StructField("answer_status",                    StringType(),       True),
    StructField("header_collective_status",         StringType(),       True),
    StructField("price_enforcement",                StringType(),       True),
    StructField("date_enforcement",                 StringType(),       True),
    StructField("auto_email",                       StringType(),       True),
    StructField("print_groups",                     StringType(),       True),
    # SECTION 9: CANCELLATION
    StructField("cancellation_editor",              LongType(),         True),
    StructField("cancellation_date",                DateType(),         True),
    StructField("cancellation_comment",             StringType(),       True),
    # SECTION 10: SOURCE DOCUMENT
    StructField("original_document_type",           StringType(),       True),
    StructField("original_document_no",             LongType(),         True),
    # SECTION 11: DUNNING
    StructField("dunning_date_1",                   DateType(),         True),
    StructField("dunning_date_2",                   DateType(),         True),
    StructField("dunning_date_3",                   DateType(),         True),
    StructField("dunning_block",                    StringType(),       True),
    # SECTION 12: HEADER REFERENCES
    StructField("reference_text",                   StringType(),       True),
    StructField("note",                             StringType(),       True),
    StructField("pretext",                          StringType(),       True),
    StructField("additional_text",                  StringType(),       True),
    StructField("additional_xml",                   StringType(),       True),
    StructField("external_reference",               StringType(),       True),
    StructField("project",                          StringType(),       True),
    StructField("project_yy",                       StringType(),       True),
    StructField("delivery_note_no",                 StringType(),       True),
    StructField("created_by_user",                  StringType(),       True),
    StructField("created_by_time",                  StringType(),       True),
    StructField("branch",                           StringType(),       True),
    StructField("remark_1",                         StringType(),       True),
    StructField("remark_2",                         StringType(),       True),
    StructField("remark_3",                         StringType(),       True),
    StructField("remark_4",                         StringType(),       True),
    StructField("remark_5",                         StringType(),       True),
    # SECTION 13: LINE IDENTITY
    StructField("line_id",                          LongType(),         True),
    StructField("position_no",                      LongType(),         True),
    StructField("line_document_type",               StringType(),       True),
    StructField("line_client_id",                   LongType(),         True),
    StructField("line_supplier_id",                 LongType(),         True),
    # SECTION 14: LINE PRODUCT & WAREHOUSE
    StructField("nk_product_id",                    LongType(),         True),
    StructField("warehouse_code",                   StringType(),       True),
    StructField("exchange_item_id",                 LongType(),         True),
    # SECTION 15: LINE DATES
    StructField("delivery_date",                    DateType(),         True),
    StructField("line_timestamp",                   DateType(),         True),
    StructField("source_document_date",             DateType(),         True),
    StructField("source_document_delivery_date",    DateType(),         True),
    StructField("line_dunning_block_date",          DateType(),         True),
    # SECTION 16: LINE QUANTITIES
    StructField("quantity",                         DecimalType(38, 9), True),
    StructField("quantity_stock",                   DecimalType(38, 9), True),
    StructField("quantity_stock2",                  DecimalType(38, 9), True),
    StructField("quantity_reserved_stock",          DecimalType(38, 9), True),
    StructField("quantity_pre_invoiced",            DecimalType(38, 9), True),
    StructField("quantity_goods_receipt_stock",     DecimalType(38, 9), True),
    StructField("quantity_return",                  DecimalType(38, 9), True),
    StructField("quantity_completed_returns",       DecimalType(38, 9), True),
    # SECTION 17: LINE FINANCIALS
    StructField("unit_purchase_price",              DecimalType(38, 9), True),
    StructField("unit_purchase_price2",             DecimalType(38, 9), True),
    StructField("unit_purchase_price_foreign",      DecimalType(38, 9), True),
    StructField("calc_purchase_price",              DecimalType(38, 9), True),
    StructField("line_vat_rate",                    LongType(),         True),
    StructField("line_document_discount",           DecimalType(38, 9), True),
    # SECTION 18: LINE CURRENCY
    StructField("line_currency",                    StringType(),       True),
    StructField("line_exchange_rate",               DecimalType(38, 9), True),
    StructField("line_exchange_unit",               DecimalType(38, 9), True),
    StructField("line_client_currency",             StringType(),       True),
    StructField("source_currency",                  StringType(),       True),
    StructField("source_exchange_rate",             DecimalType(38, 9), True),
    StructField("source_exchange_unit",             DecimalType(38, 9), True),
    # SECTION 19: LINE COSTS & SURCHARGES
    StructField("surcharge0",                       DecimalType(38, 9), True),
    StructField("surcharge1",                       DecimalType(38, 9), True),
    StructField("surcharge2",                       DecimalType(38, 9), True),
    StructField("surcharge3",                       DecimalType(38, 9), True),
    StructField("customs_pct",                      DecimalType(38, 9), True),
    StructField("freight_pct",                      DecimalType(38, 9), True),
    StructField("line_shipping",                    DecimalType(38, 9), True),
    StructField("line_packaging",                   DecimalType(38, 9), True),
    StructField("line_insurance",                   DecimalType(38, 9), True),
    StructField("line_internal_procurement_cost",   DecimalType(38, 9), True),
    StructField("bonus",                            DecimalType(38, 9), True),
    StructField("manufacturer_discount",            DecimalType(38, 9), True),
    StructField("goods_receipt_surcharge",          DecimalType(38, 9), True),
    StructField("ear_costs",                        DecimalType(38, 9), True),
    # SECTION 20: LINE DUNNING
    StructField("line_dunning_date_1",              DateType(),         True),
    StructField("line_dunning_date_2",              DateType(),         True),
    StructField("line_dunning_date_3",              DateType(),         True),
    StructField("line_dunning_block",               StringType(),       True),
    StructField("dunning_level",                    LongType(),         True),
    StructField("dunning_date",                     DateType(),         True),
    StructField("dunning_quantity",                 DecimalType(38, 9), True),
    # SECTION 21: LINE SOURCE DOCUMENT
    StructField("line_source_document_type",        StringType(),       True),
    StructField("line_source_document_no",          LongType(),         True),
    StructField("line_source_position_no",          LongType(),         True),
    StructField("line_source_delivery_date_type",   StringType(),       True),
    # SECTION 22: DROP-SHIP LINK
    StructField("customer_document_type",           StringType(),       True),
    StructField("customer_document_no",             LongType(),         True),
    StructField("customer_document_customer_no",    LongType(),         True),
    StructField("customer_document_position_no",    LongType(),         True),
    StructField("customer_document_no2",            LongType(),         True),
    StructField("customer_document_client_id",      LongType(),         True),
    # SECTION 23: LINE ATTRIBUTES
    StructField("stock_assignment",                 StringType(),       True),
    StructField("component_flag",                   StringType(),       True),
    StructField("distributor",                      StringType(),       True),
    StructField("distributor_order_no",             StringType(),       True),
    StructField("rma_no",                           LongType(),         True),
    StructField("delivery_date_type",               StringType(),       True),
    StructField("line_collective_status",           StringType(),       True),
    StructField("reservation_direct",               StringType(),       True),
    StructField("additional_description",           StringType(),       True),
    StructField("line_additional_text",             StringType(),       True),
    StructField("line_additional_xml",              StringType(),       True),
    StructField("hint_sales",                       StringType(),       True),
    StructField("old_position_no",                  LongType(),         True),
    StructField("position_uuid",                    StringType(),       True),
    StructField("contract_warehouse",               StringType(),       True),
    # SECTION 24: COMPUTED
    StructField("line_purchase_amount",             DecimalType(38, 9), True),
    StructField("line_purchase_eur",                DecimalType(38, 9), True),
    StructField("delivery_time_days",               IntegerType(),      True),
])


@dp.materialized_view(name="stg_purchase_order_master")  # materialized — 3 consumers share one BQ read
def stg_purchase_order_master():

    # SIGNAL: check etl_run_decision — if SKIP return empty immediately
    # Uses _STG_PO_SCHEMA — no table read, no circular reference, instant
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Purchase Orders' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_PO_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
        WHERE table_name  = 'fact_purchase_order_header'
          AND column_name = 'document_created_date'
        LIMIT 1
    """).first()["date_control"]

    # lbedatum = document_created_date — matches the control key column
    bestlk_df = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.bestlk")
        .where(f"Cast(lbedatum As Date) >= '{dateControl}'")
        # .where(col("lbedatum") >= dateControl)  # was lbudatum (original_document_date) — wrong column
    )
    bestlp_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.bestlp")

    return (
        bestlk_df.alias("h")
        .join(bestlp_df.alias("p"), col("h.lbbelid") == col("p.lpbelid"), "left")  # left — keeps headers with no lines
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
            col("p.lpposnr").alias("position_no"),
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
