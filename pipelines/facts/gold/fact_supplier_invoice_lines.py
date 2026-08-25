from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for supplier invoice lines with SK lookups from SCD-2 dimensions",
    cluster_by=["sk_supplier_id", "sk_product_id"]
)
def _fact_supplier_invoice_lines():


    stg = (
        spark.read.table("stg_fact_supplier_invoice_master")
        .filter(col("nk_line_id").isNotNull())           # exclude invoices with no lines
        .dropDuplicates(["nk_document_id","nk_document_no","nk_product_id","nk_line_id","line_status","invoice_date"])                  # line grain
        .withColumn("invoice_date", col("invoice_date").cast("date"))
        .alias("stg")
    )

    dim_supplier = (
        spark.read.table("dim_supplier")
        .select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")
    ).alias("sp")

    dim_product = (
        spark.read.table("dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("prd")

    df = stg.join(
        dim_supplier,
        on=(
            (col("stg.nk_supplier_id") == col("sp.nk_supplier_id")) &
            (col("stg.invoice_date") >= col("sp.__START_AT")) &
            (col("stg.invoice_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    df = df.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.invoice_date") >= col("prd.__START_AT")) &
            (col("stg.invoice_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    return df.select(
        col("sp.sk_supplier_id"),
        col("prd.sk_product_id"),
        col("stg.nk_line_id"),
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("stg.position_no"),

        col("stg.goods_receipt_no"),
        col("stg.goods_receipt_type"),
        col("stg.line_status"),
        col("stg.quantity_billed"),
        col("stg.unit_price"),
        col("stg.line_amount"),
        col("stg.currency_code"),
        col("stg.exchange_rate"),
        col("stg.exchange_unit"),
        col("stg.is_cancelled"),
        col("stg.document_type"),
        F.current_timestamp().alias("dw_created_date"),
        col("invoice_date").alias("system_date")
    )
