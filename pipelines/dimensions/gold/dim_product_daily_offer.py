from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Daily offer dimension — products on special offer with validity window",
    cluster_by=["sk_product_id"]
)
def dim_product_daily_offer():

    stg = (
        spark.read.table("stg_product_daily_offer")
        .withColumn("valid_from", col("valid_from").cast("date"))
        .alias("stg")
    )

    dim_product = (
        spark.read.table("dim_product")
        .select("sk_product_id", "product_number", "__START_AT", "__END_AT")
    ).alias("prd")

    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select("sk_salesperson_id", "nk_salesperson_id", "__START_AT", "__END_AT")
    ).alias("s")

   
    df = stg.join(
        dim_product,
        on=(
            (col("stg.product_number") == col("prd.product_number")) &
            (col("stg.valid_from") >= col("prd.__START_AT")) &
            (col("stg.valid_from") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )


    df = df.join(
        dim_salesperson,
        on=(
            (col("stg.nk_created_by") == col("s.nk_salesperson_id")) &
            (col("stg.valid_from") >= col("s.__START_AT")) &
            (col("stg.valid_from") <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    return df.select(

        col("prd.sk_product_id"),
        col("s.sk_salesperson_id"),
        col("stg.nk_offer_id"),
        col("stg.product_number"),
        col("stg.nk_created_by"),

        col("stg.valid_from"),
        col("stg.valid_to"),
        F.datediff(col("stg.valid_to"), col("stg.valid_from")).alias("offer_duration_days"),
        col("stg.comment"),

        F.when(
            F.current_date().between(col("stg.valid_from"), col("stg.valid_to")),
            lit(True)
        ).otherwise(lit(False))                                 .alias("is_currently_active"),

        col("stg.created_at"),
        F.current_timestamp()                                   .alias("dw_created_date"),
    )
