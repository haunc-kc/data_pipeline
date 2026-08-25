from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for supplier invoice headers with SK lookups from SCD-2 dimensions",
    cluster_by=["sk_supplier_id", "invoice_date"]
)
def _fact_supplier_invoice_header():

    stg = (
        spark.read.table("stg_fact_supplier_invoice_master")
        .dropDuplicates(["nk_document_id","nk_document_no","invoice_date","document_type"])              
        .withColumn("invoice_date", col("invoice_date").cast("date"))
        .alias("stg")
    )

    dim_supplier = (
        spark.read.table("dim_supplier")
        .select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")
    ).alias("sp")

    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select("sk_salesperson_id", "nk_salesperson_id", "__START_AT", "__END_AT")
    ).alias("s")

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
        dim_salesperson,
        on=(
            (col("stg.nk_salesperson_id") == col("s.nk_salesperson_id")) &
            (col("stg.invoice_date") >= col("s.__START_AT")) &
            (col("stg.invoice_date") <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    return df.select(
        col("sp.sk_supplier_id"),
        col("s.sk_salesperson_id"),
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("stg.document_type"),
        col("stg.internal_document_no"),
        col("stg.invoice_date"),
        col("stg.entry_date"),
        col("stg.due_date"),
        col("stg.payment_date"),
        col("stg.vat_date"),
        col("stg.cancellation_date"),
        col("stg.export_date"),
        col("stg.amount_gross"),
        col("stg.amount_net"),
        col("stg.total_vat"),
        col("stg.shipping_cost"),
        col("stg.packaging_cost"),
        col("stg.insurance_cost"),
        col("stg.amount_paid"),
        col("stg.payment_amount"),
        col("stg.is_open"),
        col("stg.is_cancelled"),
        col("stg.payment_block"),
        col("stg.payment_method"),
        col("stg.cash_discount_days"),
        col("stg.cash_discount_pct"),
        col("stg.currency_code"),
        col("stg.exchange_rate"),
        col("stg.exchange_unit"),
        col("stg.client_currency"),
        col("stg.reference_text"),
        col("stg.remark"),
        col("stg.note"),
        col("stg.project"),
        col("stg.branch"),
        col("stg.payables_account"),
        col("stg.cancellation_editor"),
        F.current_timestamp().alias("dw_created_date"),
        
    )
