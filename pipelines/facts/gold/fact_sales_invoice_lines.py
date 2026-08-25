from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for invoice lines",
    
)
def _fact_sales_invoice_lines():
    stg = (
        spark.read.table("stg_fact_invoice_master")
        .withColumn("invoice_date", col("invoice_date").cast("date"))
        .withColumn("delivery_date", col("delivery_date").cast("date"))
        .alias("stg")
    ).dropDuplicates(['nk_document_id','nk_document_no','nk_product_id','position_seq_no'])

    # Reading dim tables creates the dependency graph → dims run before this fact
  
    dim_product = (
        spark.read.table("dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("p")

  
    dim_warehouse = (
        spark.read.table("dim_warehouse")
        .select("sk_warehouse_id", "nk_warehouse_id", "__START_AT", "__END_AT")
    ).alias("w")


    df = stg.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("p.nk_product_id")) &
            (col("stg.invoice_date") >= col("p.__START_AT")) &
            (col("stg.invoice_date") < coalesce(col("p.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )


    df = df.join(
        dim_warehouse,
        on=(
            (col("stg.nk_warehouse_id") == col("w.nk_warehouse_id")) &
            (col("stg.invoice_date") >= col("w.__START_AT")) &
            (col("stg.invoice_date") < coalesce(col("w.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )

    return df.select(
        # Keys
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("p.sk_product_id"),
        col("w.sk_warehouse_id"),
    
        col("stg.position_seq_no"),
        col("stg.quantity"),
        col("stg.quantity_returned"),
        # Unit prices
        col("stg.unit_sales_price_net"),
        col("stg.unit_sales_price_gross"),
        col("stg.unit_purchase_price"),
        col("stg.unit_purchase_price2"),
        col("stg.unit_purchase_price_net"),
        # Discounts
        col("stg.discount_pct"),
        col("stg.discount_pct2"),
        col("stg.doc_discount_pct"),
        # Tax & currency
        col("stg.tax_rate_id"),
        col("stg.currency_code"),
        col("stg.exchange_rate"),
        col("stg.exchange_unit"),
      
        F.current_timestamp().alias("dw_created_date"),
        col("invoice_date").alias("system_date")
     
    )
