from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for purchase order header",
    cluster_by=["nk_document_id","nk_document_no"]
)
def _fact_purchase_order_lines():
    
    stg = (
        spark.read.table("stg_purchase_order_master")
        .dropDuplicates(["nk_document_id","nk_document_no","document_created_date","created_date","document_type_code","document_status"])
        .withColumn("document_created_date", col("document_created_date").cast("date"))
        .alias("stg")
    )
    

    dim_product = (
        spark.read.table("dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("prd")
    

    
    df = stg.join(
        dim_product.alias("p"),
        on=(
            (col("stg.nk_product_id") == col("p.nk_product_id")) &
            (col("stg.document_created_date") >= col("p.__START_AT")) &
            (col("stg.document_created_date") < coalesce(col("p.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left",
    )

    return df.select(
                        col("nk_document_id"),
                        col("nk_document_no"),
                        col("line_id"),
                        col("position_no"),
                        col("line_document_type"),
                        col("line_client_id"),
                        col("line_supplier_id"),
                        col("sk_product_id"),
                        col("warehouse_code"),
                        col("exchange_item_id"),
                        col("delivery_date"),
                        col("line_timestamp"),
                        col("source_document_date"),
                        col("source_document_delivery_date"),
                        col("line_dunning_block_date"),
                        col("quantity"),
                        col("quantity_stock"),
                        col("quantity_stock2"),
                        col("quantity_reserved_stock"),
                        col("quantity_pre_invoiced"),
                        col("quantity_goods_receipt_stock"),
                        col("quantity_return"),
                        col("quantity_completed_returns"),
                        col("unit_purchase_price"),
                        col("unit_purchase_price2"),
                        col("unit_purchase_price_foreign"),
                        col("calc_purchase_price"),
                        col("line_vat_rate"),
                        col("line_document_discount"),
                        col("line_currency"),
                        col("line_exchange_rate"),
                        col("line_exchange_unit"),
                        col("line_client_currency"),
                        col("source_currency"),
                        col("source_exchange_rate"),
                        col("source_exchange_unit"),
                        col("surcharge0"),
                        col("surcharge1"),
                        col("surcharge2"),
                        col("surcharge3"),
                        col("customs_pct"),
                        col("freight_pct"),
                        col("line_shipping"),
                        col("line_packaging"),
                        col("line_insurance"),
                        col("line_internal_procurement_cost"),
                        col("bonus"),
                        col("manufacturer_discount"),
                        col("goods_receipt_surcharge"),
                        col("ear_costs"),
                        col("line_dunning_date_1"),
                        col("line_dunning_date_2"),
                        col("line_dunning_date_3"),
                        col("line_dunning_block"),
                        col("dunning_level"),
                        col("dunning_date"),
                        col("dunning_quantity"),
                        col("line_source_document_type"),
                        col("line_source_document_no"),
                        col("line_source_position_no"),
                        col("line_source_delivery_date_type"),
                        col("customer_document_type"),
                        col("customer_document_no"),
                        col("customer_document_customer_no"),
                        col("customer_document_position_no"),
                        col("customer_document_no2"),
                        col("customer_document_client_id"),
                        col("stock_assignment"),
                        col("component_flag"),
                        col("distributor"),
                        col("distributor_order_no"),
                        col("rma_no"),
                        col("delivery_date_type"),
                        col("line_collective_status"),
                        col("reservation_direct"),
                        col("additional_description"),
                        col("line_additional_text"),
                        col("line_additional_xml"),
                        col("hint_sales"),
                        col("old_position_no"),
                        col("position_uuid"),
                        col("contract_warehouse"),
                        col("line_purchase_amount"),
                        col("line_purchase_eur"),
                        col("delivery_time_days"),
                        F.current_timestamp().alias("dw_created_date"),
                        col("original_document_date").alias("system_date")
                        

    )
