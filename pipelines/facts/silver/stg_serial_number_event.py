from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, BooleanType, IntegerType, DecimalType)

# Schema matches exactly the select() output below — used for the SKIP empty return
# No circular reference — pure in-memory, zero I/O, instant
_STG_SERIAL_NUMBER_SCHEMA = StructType([
    # Identity
    StructField("nk_event_id",              LongType(),     True),
    StructField("nk_product_id",            LongType(),     True),
    StructField("nk_warehouse_id",          StringType(),   True),
    # Serial / Batch
    StructField("serial_number",            StringType(),   True),
    StructField("house_serial_number",      StringType(),   True),
    StructField("quantity",                 DecimalType(38, 9), True),
    StructField("is_batch",                 StringType(),   True),
    # Movement direction
    StructField("is_outbound",              BooleanType(),  True),
    StructField("movement_direction",       StringType(),   True),
    # Party type
    StructField("party_type_code",          StringType(),   True),
    StructField("party_type",               StringType(),   True),
    StructField("nk_party_id",              LongType(),     True),
    # Document reference
    StructField("nk_document_id",           LongType(),     True),
    StructField("document_type_code",       StringType(),   True),
    StructField("document_no",              LongType(),     True),
    StructField("position_no",              LongType(),     True),
    # Document category
    StructField("document_category",        StringType(),   True),
    # Dates
    StructField("event_date",               DateType(),     True),
    StructField("invoice_date",             DateType(),     True),
    StructField("best_before_date",         DateType(),     True),
    StructField("warranty_expiry_date",     DateType(),     True),
    StructField("cancellation_date",        DateType(),     True),
    # Warranty
    StructField("warranty_code",            StringType(),   True),
    StructField("warranty_months",          IntegerType(),  True),
    StructField("extended_warranty_code",   StringType(),   True),
    StructField("extended_warranty_months", IntegerType(),  True),
    StructField("supplier_warranty_code",   StringType(),   True),
    StructField("supplier_warranty_months", IntegerType(),  True),
    # Cancellation
    StructField("is_cancelled",             BooleanType(),  True),
    StructField("cancellation_editor",      LongType(),     True),
    # Error / Quality
    StructField("error_code",               StringType(),   True),
    StructField("error_text",               StringType(),   True),
    # References
    StructField("delivery_note_no",         StringType(),   True),
    StructField("nk_salesperson_id",        LongType(),     True),
    StructField("is_external",              BooleanType(),  True),
    StructField("remark_1",                 StringType(),   True),
    StructField("remark_2",                 StringType(),   True),
    StructField("serial_addition_1",        StringType(),   True),
    StructField("serial_addition_2",        StringType(),   True),
])


@dp.temporary_view(name="stg_serial_number_event")
def stg_serial_number_event():
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Serial Number' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_SERIAL_NUMBER_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
        WHERE table_name  = 'fact_serial_number_event'
          AND column_name = 'event_date'
        LIMIT 1
    """).first()["date_control"]

    serien_df = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.serien")
        # .where(col("sedatum") >= dateControl)
        .where(f"Cast(sedatum As Date) >= '{dateControl}'")
    )

    return (
        serien_df.alias("se")
        .select(
            # ── Identity ─────────────────────────────────────
            col("se.seserid")                                       .alias("nk_event_id"),
            col("se.seidnr")                                        .alias("nk_product_id"),
            trim(coalesce(col("se.selager"),  lit("")))             .alias("nk_warehouse_id"),

            # ── Serial / Batch ───────────────────────────────
            trim(coalesce(col("se.sesernr"),  lit("")))             .alias("serial_number"),
            trim(coalesce(col("se.sehaunr"),  lit("")))             .alias("house_serial_number"),
            col("se.seserac")                                       .alias("quantity"),
            col("se.sechargen")                                     .alias("is_batch"),

            # ── Movement direction ────────────────────────────
            col("se.seaus")                                         .alias("is_outbound"),
            when(col("se.seaus") == True,  lit("OUT"))
            .otherwise(lit("IN"))                                   .alias("movement_direction"),

            # ── Party type ───────────────────────────────────
            trim(coalesce(col("se.sekltyp"), lit("")))              .alias("party_type_code"),
            when(col("se.sekltyp") == "K", lit("Customer"))
            .when(col("se.sekltyp") == "L", lit("Supplier"))
            .otherwise(lit("Unknown"))                              .alias("party_type"),
            col("se.sekdnummer")                                    .alias("nk_party_id"),

            # ── Document reference ────────────────────────────
            col("se.sebelid")                                       .alias("nk_document_id"),
            trim(coalesce(col("se.sebeltyp"), lit("")))             .alias("document_type_code"),
            col("se.sebelnr")                                       .alias("document_no"),
            col("se.seposnr")                                       .alias("position_no"),

            # ── Document category ─────────────────────────────
            when(col("se.sebeltyp").isin("F", "G", "M", "O"), lit("Sales Invoice"))
            .when(col("se.sebeltyp").isin("A", "B", "L"),      lit("Sales Order"))
            .when(col("se.sebeltyp").isin("E"),                 lit("Purchase Order"))
            .when(col("se.sebeltyp").isin("W"),                 lit("Goods Receipt"))
            .when(col("se.sebeltyp").isin("R"),                 lit("Supplier Invoice"))
            .otherwise(lit("Other"))                                .alias("document_category"),

            # ── Dates ────────────────────────────────────────
            col("se.sedatum")   .cast("date")                       .alias("event_date"),
            col("se.seekredat") .cast("date")                       .alias("invoice_date"),
            col("se.semhdatum") .cast("date")                       .alias("best_before_date"),
            col("se.selfgadat") .cast("date")                       .alias("warranty_expiry_date"),
            col("se.sestdatum") .cast("date")                       .alias("cancellation_date"),

            # ── Warranty ─────────────────────────────────────
            trim(coalesce(col("se.segarkenn"),  lit("")))           .alias("warranty_code"),
            coalesce(col("se.segarmon"),  lit(0))                   .alias("warranty_months"),
            trim(coalesce(col("se.seergkenn"),  lit("")))           .alias("extended_warranty_code"),
            coalesce(col("se.seergmon"),  lit(0))                   .alias("extended_warranty_months"),
            trim(coalesce(col("se.selfgkenn"),  lit("")))           .alias("supplier_warranty_code"),
            coalesce(col("se.selfgmon"),  lit(0))                   .alias("supplier_warranty_months"),

            # ── Cancellation ──────────────────────────────────
            when(col("se.sestorno") == "J", lit(True))
            .otherwise(lit(False))                                  .alias("is_cancelled"),
            col("se.sestbear")                                      .alias("cancellation_editor"),

            # ── Error / Quality ───────────────────────────────
            trim(coalesce(col("se.sekztext"), lit("")))             .alias("error_code"),
            trim(coalesce(col("se.setext"),   lit("")))             .alias("error_text"),

            # ── References ───────────────────────────────────
            trim(coalesce(col("se.selbeleg"), lit("")))             .alias("delivery_note_no"),
            col("se.sebea")                                         .alias("nk_salesperson_id"),
            col("se.sefremd")                                       .alias("is_external"),
            trim(coalesce(col("se.sebem1"),   lit("")))             .alias("remark_1"),
            trim(coalesce(col("se.sebem2"),   lit("")))             .alias("remark_2"),
            trim(coalesce(col("se.seserz1"),  lit("")))             .alias("serial_addition_1"),
            trim(coalesce(col("se.seserz2"),  lit("")))             .alias("serial_addition_2"),
        )
        .dropDuplicates(["nk_event_id"])
    )
