from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType, IntegerType)

# Schema matches exactly the select() output below — used for the SKIP empty return
# No circular reference — pure in-memory, zero I/O, instant
_STG_SUPPLIER_INVOICE_SCHEMA = StructType([
    # SECTION 1: HEADER IDENTITY
    StructField("nk_document_id",       LongType(),          True),
    StructField("nk_document_no",       LongType(),          True),
    StructField("document_type",        StringType(),        True),
    StructField("nk_supplier_id",       LongType(),          True),
    StructField("nk_salesperson_id",    LongType(),          True),
    StructField("internal_document_no", LongType(),          True),
    # SECTION 2: HEADER DATES
    StructField("invoice_date",         DateType(),          True),
    StructField("entry_date",           DateType(),          True),
    StructField("due_date",             DateType(),          True),
    StructField("payment_date",         DateType(),          True),
    StructField("vat_date",             DateType(),          True),
    StructField("cancellation_date",    DateType(),          True),
    StructField("export_date",          DateType(),          True),
    # SECTION 3: HEADER FINANCIALS
    StructField("amount_gross",         DecimalType(38, 6),  True),
    StructField("amount_net",           DecimalType(38, 6),  True),
    StructField("total_vat",            DecimalType(38, 6),  True),
    StructField("shipping_cost",        DecimalType(38, 6),  True),
    StructField("packaging_cost",       DecimalType(38, 6),  True),
    StructField("insurance_cost",       DecimalType(38, 6),  True),
    StructField("amount_paid",          DecimalType(38, 6),  True),
    StructField("payment_amount",       DecimalType(38, 6),  True),
    # SECTION 4: HEADER PAYMENT STATUS
    StructField("is_open",              BooleanType(),       True),
    StructField("is_cancelled",         BooleanType(),       True),
    StructField("payment_block",        StringType(),        True),
    StructField("payment_method",       StringType(),        True),
    StructField("cash_discount_days",   IntegerType(),       True),
    StructField("cash_discount_pct",    DecimalType(38, 6),  True),
    # SECTION 5: HEADER CURRENCY
    StructField("currency_code",        StringType(),        True),
    StructField("exchange_rate",        DecimalType(38, 9),  True),
    StructField("exchange_unit",        DecimalType(38, 9),  True),
    StructField("client_currency",      StringType(),        True),
    # SECTION 6: HEADER REFERENCES
    StructField("reference_text",       StringType(),        True),
    StructField("remark",               StringType(),        True),
    StructField("note",                 StringType(),        True),
    StructField("project",              StringType(),        True),
    StructField("branch",               StringType(),        True),
    StructField("payables_account",     LongType(),          True),
    StructField("cancellation_editor",  LongType(),          True),
    # SECTION 7: LINE IDENTITY
    StructField("nk_line_id",           LongType(),          True),
    StructField("position_no",          LongType(),          True),
    StructField("nk_product_id",        LongType(),          True),
    StructField("line_client_id",       LongType(),          True),
    # SECTION 8: LINE GOODS RECEIPT REFERENCE
    StructField("goods_receipt_no",     LongType(),          True),
    StructField("goods_receipt_type",   StringType(),        True),
    StructField("line_status",          StringType(),        True),
    # SECTION 9: LINE QUANTITIES & PRICE
    StructField("quantity_billed",      DecimalType(38, 9),  True),
    StructField("unit_price",           DecimalType(38, 9),  True),
    StructField("line_amount",          DecimalType(38, 9),  True),
])


@dp.materialized_view(name="stg_fact_supplier_invoice_master")  # name must match what Gold reads: "stg_supplier_invoice_master"
def stg_supplier_invoice_master():

    # SIGNAL: check etl_run_decision — if SKIP return empty immediately
    # Uses _STG_SUPPLIER_INVOICE_SCHEMA — no table read, no circular reference, instant
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Supplier Invoices' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_SUPPLIER_INVOICE_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
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

    return (
        rechlk_df.alias("h")
        .join(
            rechlkwe_df.alias("lp"),
            col("h.lrbelid") == col("lp.lrwbelid"),
            "left"   # LEFT — some invoices may have no line detail in rechlkwe
        )
        .select(
            # ── SECTION 1: HEADER IDENTITY ────────────────────
            col("h.lrbelid")                                    .alias("nk_document_id"),
            col("h.lrbelegnr")                                  .alias("nk_document_no"),
            col("h.lrbelegtyp")                                 .alias("document_type"),
            col("h.lrliefnr")                                   .alias("nk_supplier_id"),
            col("h.lrbeakz")                                    .alias("nk_salesperson_id"),
            col("h.lrintbelnr")                                 .alias("internal_document_no"),

            # ── SECTION 2: HEADER DATES ───────────────────────
            col("h.lrdatum")    .cast("date")                   .alias("invoice_date"),
            col("h.lreingabe")  .cast("date")                   .alias("entry_date"),
            col("h.lrfaellig")  .cast("date")                   .alias("due_date"),
            col("h.lrzahldat")  .cast("date")                   .alias("payment_date"),
            col("h.lrustdatum") .cast("date")                   .alias("vat_date"),
            col("h.lrstdatum")  .cast("date")                   .alias("cancellation_date"),
            col("h.lrexpdat")   .cast("date")                   .alias("export_date"),

            # ── SECTION 3: HEADER FINANCIALS ──────────────────
            coalesce(col("h.lrbrutto"),  lit(0))                .alias("amount_gross"),
            coalesce(col("h.lrnetto"),   lit(0))                .alias("amount_net"),
            coalesce(col("h.lrmwst"),    lit(0))                .alias("total_vat"),
            coalesce(col("h.lrversend"), lit(0))                .alias("shipping_cost"),
            coalesce(col("h.lrverpack"), lit(0))                .alias("packaging_cost"),
            coalesce(col("h.lrversich"), lit(0))                .alias("insurance_cost"),
            coalesce(col("h.lrgezahlt"), lit(0))                .alias("amount_paid"),
            coalesce(col("h.lrzahlung"), lit(0))                .alias("payment_amount"),

            # ── SECTION 4: HEADER PAYMENT STATUS ─────────────
            F.when(col("h.lroffen") == "J", lit(True))
             .otherwise(lit(False))                             .alias("is_open"),
            F.when(col("h.lrstbear") > 0, lit(True))
             .otherwise(lit(False))                             .alias("is_cancelled"),
            trim(coalesce(col("h.lropk"),    lit("")))          .alias("payment_block"),
            col("h.lrzahlkenn")                                 .alias("payment_method"),
            coalesce(col("h.lrsktage"), lit(0))                 .alias("cash_discount_days"),
            coalesce(col("h.lrskproz"), lit(0))                 .alias("cash_discount_pct"),

            # ── SECTION 5: HEADER CURRENCY ────────────────────
            trim(coalesce(col("h.lrwaehrung"), lit("EUR")))     .alias("currency_code"),
            coalesce(col("h.lrukurs"), lit(1))                  .alias("exchange_rate"),
            coalesce(col("h.lrueinh"), lit(1))                  .alias("exchange_unit"),
            trim(coalesce(col("h.lrmandwaeh"), lit("EUR")))     .alias("client_currency"),

            # ── SECTION 6: HEADER REFERENCES ─────────────────
            trim(coalesce(col("h.lrtext"),    lit("")))         .alias("reference_text"),
            trim(coalesce(col("h.lrbem"),     lit("")))         .alias("remark"),
            trim(coalesce(col("h.lrhinweis"), lit("")))         .alias("note"),
            trim(coalesce(col("h.lrprojekt"), lit("")))         .alias("project"),
            trim(coalesce(col("h.lrfil"),     lit("")))         .alias("branch"),
            col("h.lrkrekto")                                   .alias("payables_account"),
            col("h.lrstbear")                                   .alias("cancellation_editor"),

            # ── SECTION 7: LINE IDENTITY ──────────────────────
            col("lp.xsatzid")                                   .alias("nk_line_id"),
            col("lp.lrwposnr")                                  .alias("position_no"),
            col("lp.lrwidnr")                                   .alias("nk_product_id"),
            col("lp.lrwmankey")                                 .alias("line_client_id"),

            # ── SECTION 8: LINE GOODS RECEIPT REFERENCE ──────
            col("lp.lrwbelnr")                                  .alias("goods_receipt_no"),
            trim(coalesce(col("lp.lrwbeltyp"), lit("")))        .alias("goods_receipt_type"),
            trim(coalesce(col("lp.lrwstatkz"), lit("")))        .alias("line_status"),

            # ── SECTION 9: LINE QUANTITIES & PRICE ───────────
            coalesce(col("lp.lrwstueck"),  lit(0))              .alias("quantity_billed"),
            coalesce(col("lp.lrwlfpreis"), lit(0))              .alias("unit_price"),
            (coalesce(col("lp.lrwstueck"), lit(0)) *
             coalesce(col("lp.lrwlfpreis"), lit(0)))            .alias("line_amount"),
        )
    )
