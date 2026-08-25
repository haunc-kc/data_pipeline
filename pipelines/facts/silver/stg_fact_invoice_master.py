from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim, when
from pyspark.sql.types import (StructType, StructField,
    LongType, StringType, DateType, DecimalType, BooleanType, IntegerType)

_STG_INVOICE_SCHEMA = StructType([
    StructField("nk_document_id",               LongType(),         True),
    StructField("nk_document_no",               LongType(),         True),
    StructField("nk_customer_id",               LongType(),         True),
    StructField("nk_salesperson_id",            LongType(),         True),
    StructField("nk_payment_term_id",           LongType(),         True),
    StructField("nk_shipping_method_id",        LongType(),         True),
    StructField("nk_product_id",                LongType(),         True),
    StructField("nk_warehouse_id",              StringType(),       True),
    StructField("nk_source_order_id",           LongType(),         True),
    StructField("nk_source_order_type",         StringType(),       True),
    StructField("position_seq_no",              LongType(),         True),
    StructField("invoice_date",                 DateType(),         True),
    StructField("delivery_date",                DateType(),         True),
    StructField("document_type_code",           StringType(),       True),
    StructField("document_category_code",       StringType(),       True),
    StructField("reference_document_indicator", StringType(),       True),
    StructField("collection_status_code",       StringType(),       True),
    StructField("document_status",              StringType(),       True),
    StructField("is_online",                    BooleanType(),      True),
    StructField("is_credit_note",               BooleanType(),      True),
    StructField("header_net_total",             DecimalType(38, 6), True),
    StructField("header_gross_total",           DecimalType(38, 6), True),
    StructField("header_vat_total",             DecimalType(38, 6), True),
    StructField("header_shipping_cost",         DecimalType(38, 6), True),
    StructField("amount_paid",                  DecimalType(38, 6), True),
    StructField("due_date",                     DateType(),         True),
    StructField("payment_date",                 DateType(),         True),
    StructField("is_open",                      BooleanType(),      True),
    StructField("days_to_pay",                  IntegerType(),      True),
    StructField("project_code",                 StringType(),       True),
    StructField("pos_mankey",                   LongType(),         True),
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
    StructField("purchase_amount",              DecimalType(38, 6), True),
    StructField("line_net_amount",              DecimalType(38, 9), True),
    StructField("line_gross_amount",            DecimalType(38, 9), True),
    StructField("line_purchase_amount",         DecimalType(38, 9), True),
    StructField("revenue",                      DecimalType(38, 6), True),
    StructField("cost_amount",                  DecimalType(38, 9), True),
    StructField("shipping_amount",              DecimalType(38, 9), True),
    StructField("business_channel",             StringType(),       True),
])



@dp.materialized_view()  
def stg_fact_invoice_master():

    
    try:
        decision = spark.sql("""
            SELECT decision FROM workspace.mention_dw.etl_run_decision
            WHERE label = 'Sales Invoices' LIMIT 1
        """).first()
        if decision and decision["decision"] == "SKIP":
            return spark.createDataFrame([], schema=_STG_INVOICE_SCHEMA)
    except Exception:
        pass 

    dateControl = spark.sql("""
        SELECT date_control
        FROM workspace.mention_dw.etl_fact_pipeline_config
        WHERE table_name  = 'fact_sales_invoice_header'
          AND column_name = 'invoice_date'
        LIMIT 1
    """).first()["date_control"]

    rechkk_df = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechkk")
        .where(
            # # # (col("bsdatum")  >= dateControl) &
            (col("bsmankey") == 1) &
            (col("bsstbear") == 0)
        ).where(f"Cast(bsdatum As Date) >= '{dateControl}'")
    )
    rechkp_df   = spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechkp")
    rechk2p_df  = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.rechk2p")
        .withColumn("k2pfrei1", F.lower(trim(col("k2pfrei1"))))
        .dropDuplicates(["k2pbelid", "k2pfrei1"])
    )

    joined_df = (
        rechkk_df.alias("h")
        .join(rechkp_df.alias("p"),   col("h.bsbelid") == col("p.bpbelid"),   "left")
        .join(rechk2p_df.alias("p2"), col("h.bsbelid") == col("p2.k2pbelid"), "left")
    )

    result_df = joined_df.select(
        col("h.bsbelid").alias("nk_document_id"),
        col("h.bsbelnr").alias("nk_document_no"),
        col("h.bskundennr").alias("nk_customer_id"),
        col("h.bsbeakz").alias("nk_salesperson_id"),
        col("h.bszb").alias("nk_payment_term_id"),
        col("h.bsvartnr").alias("nk_shipping_method_id"),
        col("p.bpidnr").alias("nk_product_id"),
        trim(coalesce(col("p.bplager"), lit(""))).alias("nk_warehouse_id"),
        col("h.bsubelnr").alias("nk_source_order_id"),
        col("h.bsubeltyp").alias("nk_source_order_type"),
        col("p.bpposnr").alias("position_seq_no"),
        col("h.bsdatum").alias("invoice_date"),
        col("h.bsliefdat").alias("delivery_date"),
        col("h.bsbeltyp").alias("document_type_code"),
        col("h.bsgara").cast("string").alias("document_category_code"),
        F.lower(col("h.bsbelkz").cast("string")).alias("reference_document_indicator"),
        col("h.bssammstat").cast("string").alias("collection_status_code"),
        col("h.bsstatkz").alias("document_status"),
        when(col("h.bsonline") == True,  True).otherwise(False).alias("is_online"),
        when(col("h.bsbeltyp") == "G",   True).otherwise(False).alias("is_credit_note"),
        col("h.bsvkpreis").alias("header_net_total"),
        col("h.bsbrutto").alias("header_gross_total"),
        col("h.bsmwst").alias("header_vat_total"),
        col("h.bsversend").alias("header_shipping_cost"),
        col("h.bsgezahlt").alias("amount_paid"),
        col("h.bsfaellig").alias("due_date"),
        col("h.bszahldat").alias("payment_date"),
        when(col("h.bsoffen") == "O", True).otherwise(False).alias("is_open"),
        when(
            col("h.bszahldat").isNotNull(),
            F.datediff(col("h.bszahldat").cast("date"), col("h.bsdatum").cast("date"))
        ).otherwise(None).alias("days_to_pay"),
        trim(coalesce(col("h.bsprojekt"), lit(""))).alias("project_code"),
        col("p.bpmankey").alias("pos_mankey"),
        when(col("p.bpstorno") != " ", True).otherwise(False).alias("is_cancellation"),
        col("p.bpstueck").alias("quantity"),
        col("p.bpmgut").alias("quantity_returned"),
        col("p.bpvkpreis").alias("unit_sales_price_net"),
        col("p.bpvkbrutt").alias("unit_sales_price_gross"),
        col("p.bpekpreis").alias("unit_purchase_price"),
        col("p.bpekpreis2").alias("unit_purchase_price2"),
        col("p.bpekrein").alias("unit_purchase_price_net"),
        col("p.bprabatt").alias("discount_pct"),
        col("p.bprabatt2").alias("discount_pct2"),
        col("p.bprabproz").alias("doc_discount_pct"),
        col("p.bpmwst").alias("tax_rate_id"),
        col("p.bpwaehrung").alias("currency_code"),
        col("p.bpukurs").alias("exchange_rate"),
        col("p.bpueinh").alias("exchange_unit"),
        col("h.bsekpreis").alias("purchase_amount"),
        (col("p.bpstueck") * col("p.bpvkpreis")).alias("line_net_amount"),
        (col("p.bpstueck") * col("p.bpvkbrutt")).alias("line_gross_amount"),
        (col("p.bpstueck") * col("p.bpekpreis")).alias("line_purchase_amount"),
        (
            col("h.bsvkpreis").cast("decimal(38, 6)") /
            F.when(col("h.bsukurs") != 0, col("h.bsukurs")).otherwise(lit(1))
        ).alias("revenue"),
        (col("h.bsekpreis2") + col("h.bsgema") + col("h.bskoop")).alias("cost_amount"),
        # a.bsversend + a.bsverpack + a.bsmaut - a.bsintkostv
        (col("h.bsversend") + col("h.bsverpack") + col("h.bsmaut") - col("h.bsintkostv")).alias("shipping_amount"),
        when(
            F.lower(F.coalesce(col("p2.k2pfrei1"), lit(""))).isin("smartstock", "all-lager"),
            "Smart Stock"
        ).otherwise("Direct Business / Warehouse").alias("business_channel"),
    )
    return result_df.dropDuplicates(["nk_document_id","nk_document_no", "position_seq_no","document_category_code","invoice_date"])
