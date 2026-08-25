from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for invoice headers — one row per invoice document",
    cluster_by=["sk_customer_id"]
)
def _fact_sales_invoice_header():
    stg = (
        spark.read.table("stg_fact_invoice_master")
        .dropDuplicates(["nk_document_id","nk_document_no","invoice_date"])
        .withColumn("invoice_date", col("invoice_date").cast("date"))
        .withColumn("delivery_date", col("delivery_date").cast("date"))
        .alias("stg")
    ).dropDuplicates(["nk_document_id","nk_document_no","invoice_date","document_type_code"])

    dim_customer = (
        spark.read.table("dim_customer")
        .select(
            "sk_customer_id",
            "nk_customer_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("c")

    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select(
            "sk_salesperson_id",
            "nk_salesperson_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("s")

    df = stg.join(
        dim_customer,
        on=(
            (col("stg.nk_customer_id") == col("c.nk_customer_id")) &
            (col("stg.invoice_date") >= col("c.__START_AT")) &
            (col("stg.invoice_date") < coalesce(col("c.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )

    df = df.join(
        dim_salesperson,
        on=(
            (col("stg.nk_salesperson_id") == col("s.nk_salesperson_id")) &
            (col("stg.invoice_date") >= col("s.__START_AT")) &
            (col("stg.invoice_date") < coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )

    return df.select(
        # Keys
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("c.sk_customer_id"),
        col("s.sk_salesperson_id"),
        col("stg.nk_payment_term_id"),
        col("stg.nk_shipping_method_id"),
        col("stg.nk_source_order_id"),
        col("stg.nk_source_order_type"),
        # Dates
        col("stg.invoice_date"),
        col("stg.delivery_date"),
        # Document attributes
        col("stg.document_type_code"),
        col("stg.document_category_code"),
        col("stg.reference_document_indicator"),
        col("stg.collection_status_code"),
        col("stg.document_status"),
        col("stg.is_online"),
        col("stg.is_credit_note"),
        # Header amounts
        col("stg.header_net_total"),
        col("stg.header_gross_total"),
        col("stg.header_vat_total"),
        col("stg.header_shipping_cost"),
        # Payment
        col("stg.amount_paid"),
        col("stg.due_date"),
        col("stg.payment_date"),
        col("stg.is_open"),
        col("stg.days_to_pay"),
        # Other
        col("stg.project_code"),
        # Calculated metrics
        col("stg.purchase_amount"),
        col("stg.revenue"),
        col("stg.cost_amount"),
        col("stg.shipping_amount"),
        col("stg.business_channel"),
        F.current_timestamp().alias("dw_created_date")
    )
