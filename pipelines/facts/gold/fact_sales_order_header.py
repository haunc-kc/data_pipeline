from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.window import Window


@dp.materialized_view()
def _fact_sales_order_header():
    """
    Staging view with SCD Type-2 dimension lookups and duplicate detection.
    """
    stg = (
        spark.read.table("stg_fact_order_master")
        .withColumn("transaction_date", col("transaction_date").cast("date"))
        .alias("stg")
    ).dropDuplicates(["nk_document_id","nk_document_no","transaction_date","document_type_code"])

    # Reading dim tables creates the dependency graph → dims run before this fact
    dim_customer = (
        spark.read.table("dim_customer")
        .select("sk_customer_id", "nk_customer_id",
                 col("__START_AT").cast("date").alias("__START_AT"), 
                 col("__END_AT").cast("date").alias("__END_AT")
                 )
    ).alias("c")

    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select("sk_salesperson_id", "nk_salesperson_id"
                ,col("__START_AT").cast("date").alias("__START_AT")
                ,col("__END_AT").cast("date").alias("__END_AT"))
    ).alias("s")

    # Temporal join: match fact transaction_date to dim validity window
    df = stg.join(
        dim_customer,
        on=(
            (col("stg.nk_customer_id") == col("c.nk_customer_id")) &
            (col("stg.transaction_date") >= col("c.__START_AT")) &
            (col("stg.transaction_date") < coalesce(col("c.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )

    df = df.join(
        dim_salesperson,
        on=(
            (col("stg.nk_salesperson_id") == col("s.nk_salesperson_id")) &
            (col("stg.transaction_date") >= col("s.__START_AT")) &
            (col("stg.transaction_date") < coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )


    return df.select(
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("c.sk_customer_id"),
        col("s.sk_salesperson_id"),
        col("stg.transaction_date"),
        col("stg.document_type_code"),
        col("stg.document_category_code"),
        col("stg.reference_document_indicator"),
        col("stg.collection_status_code"),
        col("stg.document_status"),
        col("stg.document_online"),
        col("stg.nk_payment_term"),
        col("stg.nk_shipping_method"),
        col("stg.nk_source_doc_id"),
        col("stg.nk_source_doc_type"),
        col("stg.project_code"),
        col("stg.distributor_code"),
        col("stg.revenue"),
        col("stg.cost_amount"),
        F.current_timestamp().alias("dw_created_date")

    )



