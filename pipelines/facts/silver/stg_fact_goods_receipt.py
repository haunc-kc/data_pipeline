from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType, IntegerType)


_STG_GOODS_RECEIPT_SCHEMA = StructType([
    # Identity
    StructField("nk_receipt_id",            LongType(),          True),
    StructField("nk_document_id",           LongType(),          True),
    StructField("nk_document_no",           LongType(),          True),
    StructField("nk_position_no",           LongType(),          True),
    StructField("nk_client_id",             LongType(),          True),
    StructField("nk_supplier_id",           LongType(),          True),
    StructField("nk_product_id",            LongType(),          True),
    StructField("nk_warehouse_id",          StringType(),        True),
    # Dates
    StructField("receipt_date",             DateType(),          True),
    StructField("expiry_date",              DateType(),          True),
    StructField("best_before_date",         DateType(),          True),
    # Status
    StructField("receipt_status",           StringType(),        True),
    StructField("stock_type",               StringType(),        True),
    # Quantities
    StructField("quantity_received",        DecimalType(38, 9),  True),
    StructField("quantity_expected",        DecimalType(38, 9),  True),
    StructField("is_defective",             BooleanType(),       True),
    StructField("quantity_defective",       DecimalType(38, 9),  True),
    # Serial / Batch
    StructField("serial_number",            StringType(),        True),
    StructField("house_serial_number",      StringType(),        True),
    StructField("package_no",               StringType(),        True),
    StructField("position_uuid",            StringType(),        True),
    StructField("container_id",             StringType(),        True),
    StructField("serial_addition_1",        StringType(),        True),
    StructField("serial_addition_2",        StringType(),        True),
    # Warranty
    StructField("warranty_code",            StringType(),        True),
    StructField("warranty_months",          IntegerType(),       True),
    StructField("extended_warranty_months", IntegerType(),       True),
    StructField("extended_warranty_code",   StringType(),        True),
    StructField("supplier_warranty_months", IntegerType(),       True),
    StructField("supplier_warranty_code",   StringType(),        True),
    # Source document
    StructField("source_document_no",       LongType(),          True),
    StructField("source_document_type",     StringType(),        True),
    # References
    StructField("delivery_note_no",         StringType(),        True),
    StructField("receipt_remark",           StringType(),        True),
    StructField("goods_receipt_no",         StringType(),        True),
    StructField("storage_location",         StringType(),        True),
    StructField("created_by_user",          StringType(),        True),
    # PO enrichment
    StructField("po_supplier_id",           LongType(),          True),
    StructField("po_document_no",           LongType(),          True),
    StructField("po_order_date",            DateType(),          True),
    StructField("po_status",                StringType(),        True),
    StructField("po_document_type",         StringType(),        True),
    StructField("unit_purchase_price",      DecimalType(38, 9),  True),
    StructField("unit_purchase_price_eur",  DecimalType(38, 9),  True),
    StructField("currency_code",            StringType(),        True),
    StructField("exchange_rate",            DecimalType(38, 9),  True),
])


# @dp.materialized_view(name="stg_fact_goods_receipt")
@dp.temporary_view(name="stg_fact_goods_receipt")
def stg_goods_receipt():
    """
    Silver: Goods Receipt
    Source : webestlp (one row per receipt scan per PO line)
             stg_purchase_order_master (LEFT JOIN — enriches with PO header/line data)
    Grain  : one row per receipt scan (webestlp.lpid)
    """

    # SIGNAL: check etl_run_decision — if SKIP return empty immediately (no BQ read)
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Good Receipt' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_GOODS_RECEIPT_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
                                SELECT date_control
                                FROM workspace.mention_dw.etl_fact_pipeline_config
                                WHERE table_name  = 'fact_goods_receipt_lines'
                                AND column_name = 'receipt_date'
                                LIMIT 1
    """).first()["date_control"]

    webestlp_df  = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.webestlp")
        .where(f"Cast(lpdatum As Date) >= '{dateControl}'")
        # .where(col("lpdatum") >= dateControl)   # >= consistent with all other silver views
    )
    po_master_df = spark.read.table("stg_purchase_order_master")

   
    df = (
        webestlp_df.alias("we")
        .join(
            po_master_df.alias("po"),
            (col("we.lpbelid")  == col("po.nk_document_id")),
            "left"
        )
        .select(
            # ── Identity ─────────────────────────────────────
            col("we.lpid")                                      .alias("nk_receipt_id"),
            col("we.lpbelid")                                   .alias("nk_document_id"),
            col("we.lpbelnr")                                   .alias("nk_document_no"),
            col("we.lpposnr")                                   .alias("nk_position_no"),
            col("we.lpmankey")                                  .alias("nk_client_id"),
            col("we.lpliefnr")                                  .alias("nk_supplier_id"),
            col("we.lpidnr")                                    .alias("nk_product_id"),
            trim(coalesce(col("we.lplager"), lit("")))          .alias("nk_warehouse_id"),

            # ── Dates ────────────────────────────────────────
            col("we.lpdatum")   .cast("date")                   .alias("receipt_date"),
            col("we.lpedat")    .cast("date")                   .alias("expiry_date"),
            col("we.lpmhdatum") .cast("date")                   .alias("best_before_date"),

            # ── Status ───────────────────────────────────────
            col("we.lpstatus")                                  .alias("receipt_status"),
            trim(coalesce(col("we.lpwekey"), lit("")))          .alias("stock_type"),

            # ── Quantities ───────────────────────────────────
            coalesce(col("we.lpmgist"),  lit(0))                .alias("quantity_received"),
            coalesce(col("we.lpmgsoll"), lit(0))                .alias("quantity_expected"),
            coalesce(col("we.lpdefekt"), lit(False))            .alias("is_defective"),
            F.when(
                coalesce(col("we.lpdefekt"), lit(False)),
                coalesce(col("we.lpmgist"), lit(0))
            ).otherwise(lit(0))                                 .alias("quantity_defective"),

            # ── Serial / Batch ───────────────────────────────
            trim(coalesce(col("we.lpsernr"),   lit("")))        .alias("serial_number"),
            trim(coalesce(col("we.lphaunr"),   lit("")))        .alias("house_serial_number"),
            trim(coalesce(col("we.lppaketnr"), lit("")))        .alias("package_no"),
            trim(coalesce(col("we.lpposid"),   lit("")))        .alias("position_uuid"),
            trim(coalesce(col("we.lpcontid"),  lit("")))        .alias("container_id"),
            trim(coalesce(col("we.lpserz1"),   lit("")))        .alias("serial_addition_1"),
            trim(coalesce(col("we.lpserz2"),   lit("")))        .alias("serial_addition_2"),

            # ── Warranty ─────────────────────────────────────
            trim(coalesce(col("we.lpgarkenn"), lit("")))        .alias("warranty_code"),
            coalesce(col("we.lpgarmon"), lit(0))                .alias("warranty_months"),
            coalesce(col("we.lpergmon"), lit(0))                .alias("extended_warranty_months"),
            trim(coalesce(col("we.lpergkenn"), lit("")))        .alias("extended_warranty_code"),
            coalesce(col("we.lplfgmon"), lit(0))                .alias("supplier_warranty_months"),
            trim(coalesce(col("we.lplfgkenn"), lit("")))        .alias("supplier_warranty_code"),

            # ── Source document (Urbeleg) ─────────────────────
            col("we.lpubelnr")                                  .alias("source_document_no"),
            trim(coalesce(col("we.lpubeltyp"), lit("")))        .alias("source_document_type"),

            # ── References ───────────────────────────────────
            trim(coalesce(col("we.lplbeleg"),  lit("")))        .alias("delivery_note_no"),
            trim(coalesce(col("we.lpwebem"),   lit("")))        .alias("receipt_remark"),
            trim(coalesce(col("we.lpwenr"),    lit("")))        .alias("goods_receipt_no"),
            trim(coalesce(col("we.lplago1"),   lit("")))        .alias("storage_location"),
            col("we.lpwsname")                                  .alias("created_by_user"),

            # ── PO enrichment from stg_purchase_order_master ─
            # coalesce fallback to webestlp fields if PO already deleted
            coalesce(col("po.nk_supplier_id"),    col("we.lpliefnr")).alias("po_supplier_id"),
            coalesce(col("po.nk_document_no"),    col("we.lpbelnr")) .alias("po_document_no"),
            col("po.created_date")                                   .alias("po_order_date"),
            col("po.document_status")                                .alias("po_status"),
            col("po.document_type_code")                             .alias("po_document_type"),
            coalesce(col("po.unit_purchase_price"),    lit(0))       .alias("unit_purchase_price"),
            coalesce(col("po.line_purchase_eur"),      lit(0))       .alias("unit_purchase_price_eur"),
            coalesce(col("po.line_currency"),          lit("EUR"))   .alias("currency_code"),
            coalesce(col("po.line_exchange_rate"),     lit(1))       .alias("exchange_rate"),
        )
    )

    return df.dropDuplicates(["nk_receipt_id"])
