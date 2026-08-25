from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType)

# Schema matches exactly the select() output below — used for the SKIP empty return
# No circular reference — pure in-memory, zero I/O, instant
_STG_ORDER_SCHEMA = StructType([
    StructField("nk_document_id",               LongType(),         True),
    StructField("nk_document_no",               LongType(),         True),
    StructField("position_seq_no",              LongType(),         True),
    StructField("nk_customer_id",               LongType(),         True),
    StructField("nk_product_id",                LongType(),         True),
    StructField("nk_salesperson_id",            LongType(),         True),
    StructField("nk_warehouse_id",              LongType(),         True),
    StructField("transaction_date",             DateType(),         True),
    StructField("delivery_date",                DateType(),         True),
    StructField("document_type_code",           StringType(),       True),
    StructField("document_category_code",       StringType(),       True),
    StructField("reference_document_indicator", StringType(),       True),
    StructField("collection_status_code",       StringType(),       True),
    StructField("document_status",              StringType(),       True),
    StructField("document_online",              BooleanType(),      True),
    StructField("nk_payment_term",              LongType(),         True),
    StructField("nk_shipping_method",           LongType(),         True),
    StructField("nk_source_doc_id",             LongType(),         True),
    StructField("nk_source_doc_type",           StringType(),       True),
    StructField("project_code",                 StringType(),       True),
    StructField("distributor_code",             StringType(),       True),
    StructField("is_cancellation",              BooleanType(),      True),
    StructField("quantity",                     DecimalType(38, 9), True),
    StructField("quantity_returned",            DecimalType(38, 9), True),
    StructField("unit_sales_price_net",         DecimalType(38, 9), True),
    StructField("unit_sales_price_gross",       DecimalType(38, 9), True),
    StructField("unit_purchase_price",          DecimalType(38, 9), True),
    StructField("unit_purchase_price2",         DecimalType(38, 9), True),
    StructField("unit_purchase_price_net",      DecimalType(38, 9), True),
    StructField("discount_pct",                 DecimalType(38, 9), True),
    StructField("discount_pct2",                DecimalType(38, 9), True),
    StructField("doc_discount_pct",             DecimalType(38, 9), True),
    StructField("tax_rate_id",                  LongType(),         True),
    StructField("currency_code",                StringType(),       True),
    StructField("exchange_rate",                DecimalType(38, 9), True),
    StructField("exchange_unit",                DecimalType(38, 9), True),
    StructField("revenue",                      DecimalType(38, 6), True),
    StructField("cost_amount",                  DecimalType(38, 7), True),
])


@dp.materialized_view()  # materialized so fact_sales_order_header + fact_sales_order_lines share one BQ read
def stg_fact_order_master():

    # SIGNAL: check etl_run_decision — if SKIP return empty immediately
    # Uses _STG_ORDER_SCHEMA — no table read, no circular reference, instant
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Sales Orders' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_ORDER_SCHEMA)
    except Exception:
        pass  # etl_run_decision not yet created on first run — proceed normally

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
        WHERE table_name  = 'fact_sales_order_header'
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

    df = bestkk_df.join(bestkp_df, on=bestkk_df["bsbelid"] == bestkp_df["bpbelid"], how="inner")

    df = df.select(
        bestkk_df["bsbelid"].cast("bigint").alias("nk_document_id"),
        bestkk_df["bsbelnr"].cast("bigint").alias("nk_document_no"),
        bestkp_df["bpposnr"].cast("bigint").alias("position_seq_no"),
        bestkk_df["bskundennr"].cast("bigint").alias("nk_customer_id"),
        bestkp_df["bpidnr"].cast("bigint").alias("nk_product_id"),
        bestkk_df["bsbeakz"].cast("bigint").alias("nk_salesperson_id"),
        bestkp_df["bplager"].cast("bigint").alias("nk_warehouse_id"),
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

    return df.dropDuplicates(["nk_document_id", "nk_document_no", "position_seq_no", "transaction_date"])
