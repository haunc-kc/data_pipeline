from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for goods receipt lines with SK lookups from SCD-2 dimensions",
    cluster_by=["sk_supplier_id", "sk_product_id"]
)
def _fact_goods_receipt_lines():
   
    stg = (
        spark.read.table("stg_fact_goods_receipt")
        .withColumn("receipt_date", col("receipt_date").cast("date"))
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

    dim_warehouse = (
        spark.read.table("dim_warehouse")
        .select("sk_warehouse_id", "nk_warehouse_id")
    ).alias("wh")

   
    df = stg.join(
        dim_supplier,
        on=(
            (col("stg.nk_supplier_id") == col("sp.nk_supplier_id")) &
            (col("stg.receipt_date") >= col("sp.__START_AT")) &
            (col("stg.receipt_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    df = df.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.receipt_date") >= col("prd.__START_AT")) &
            (col("stg.receipt_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    # dim_warehouse — SCD-1, no temporal window needed
    df = df.join(
        dim_warehouse,
        on=(col("stg.nk_warehouse_id") == col("wh.nk_warehouse_id")),
        how="left"
    )

    return df.select(

        col("sp.sk_supplier_id"),
        col("prd.sk_product_id"),
        col("wh.sk_warehouse_id"),

        col("stg.nk_receipt_id"),
        col("stg.nk_document_id"),
        col("stg.nk_document_no"),
        col("stg.nk_position_no"),
        col("stg.nk_supplier_id"),
        col("stg.nk_product_id"),

        col("stg.receipt_date"),
        col("stg.expiry_date"),
        col("stg.best_before_date"),

        col("stg.receipt_status"),
        col("stg.stock_type"),


        col("stg.quantity_received"),
        col("stg.quantity_expected"),
        col("stg.is_defective"),
        col("stg.quantity_defective"),

        col("stg.serial_number"),
        col("stg.house_serial_number"),
        col("stg.package_no"),
        col("stg.position_uuid"),
        col("stg.container_id"),
        col("stg.serial_addition_1"),
        col("stg.serial_addition_2"),


        col("stg.warranty_code"),
        col("stg.warranty_months"),
        col("stg.extended_warranty_months"),
        col("stg.extended_warranty_code"),
        col("stg.supplier_warranty_months"),
        col("stg.supplier_warranty_code"),


        col("stg.source_document_no"),
        col("stg.source_document_type"),


        col("stg.delivery_note_no"),
        col("stg.receipt_remark"),
        col("stg.goods_receipt_no"),
        col("stg.storage_location"),
        col("stg.created_by_user"),


        col("stg.unit_purchase_price"),
        col("stg.unit_purchase_price_eur"),
        col("stg.currency_code"),
        col("stg.exchange_rate"),


        F.current_timestamp().alias("dw_created_date"),
    )
