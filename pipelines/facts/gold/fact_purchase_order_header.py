from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for purchase order header",
    cluster_by=["nk_document_id","nk_document_no"]
)
def _fact_purchase_order_header():
    
    stg = (
        spark.read.table("stg_purchase_order_master")
        .dropDuplicates(["nk_document_id","nk_document_no","document_created_date","created_date","document_type_code","document_status"])
        .withColumn("document_created_date", col("document_created_date").cast("date"))
        .alias("stg")
    )
    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select(
            "sk_salesperson_id",
            "nk_salesperson_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("s")
    dim_supplier = (
        spark.read.table("dim_supplier")
        .select(
            "sk_supplier_id",
            "nk_supplier_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("spl")

    df = stg.join(dim_salesperson,
        on=(
            (col("stg.nk_salesperson_id") == col("s.nk_salesperson_id")) &
            (col("stg.document_created_date") >= col("s.__START_AT")) &
            (col("stg.document_created_date") < coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )
    df = df.join(dim_supplier,
        on=(
            (col("stg.nk_supplier_id") == col("spl.nk_supplier_id")) &
            (col("stg.document_created_date") >= col("spl.__START_AT")) &
            (col("stg.document_created_date") < coalesce(col("spl.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )
 
    return df.select(
        col("nk_document_id"),
        col("nk_document_no"),
        col("document_type_code"),
        col("document_status"),
        col("sk_supplier_id"),
        col("sk_salesperson_id"),
        col("created_date"),
        col("document_created_date"),
        col("original_document_date"),
        col("completed_date"),
        col("estimated_departure_date"),
        col("estimated_arrival_date"),
        col("actual_departure_date"),
        col("actual_arrival_date"),
        col("amount"),
        col("purchase_price_total"),
        col("total_vat"),
        col("document_discount"),
        col("shipping_cost"),
        col("packaging_cost"),
        col("insurance_cost"),
        col("freight_handling"),
        col("customs_cost"),
        col("internal_procurement_cost"),
        col("total_weight"),
        col("header_currency"),
        col("header_exchange_rate"),
        col("header_exchange_unit"),
        col("client_currency"),
        col("delivery_terms"),
        col("payment_terms"),
        col("shipping_method"),
        col("shipping_method_id"),
        col("partial_delivery_allowed"),
        col("down_payment_pct"),
        col("is_blocked"),
        col("answer_status"),
        col("header_collective_status"),
        col("price_enforcement"),
        col("date_enforcement"),
        col("auto_email"),
        col("print_groups"),
        col("cancellation_editor"),
        col("cancellation_date"),
        col("cancellation_comment"),
        col("original_document_type"),
        col("original_document_no"),
        col("dunning_date_1"),
        col("dunning_date_2"),
        col("dunning_date_3"),
        col("dunning_block"),
        col("reference_text"),
        col("note"),
        col("pretext"),
        col("additional_text"),
        col("additional_xml"),
        col("external_reference"),
        col("project"),
        col("project_yy"),
        col("delivery_note_no"),
        col("created_by_user"),
        col("created_by_time"),
        col("branch"),
        col("remark_1"),
        col("remark_2"),
        col("remark_3"),
        col("remark_4"),
        col("remark_5"),
    )
