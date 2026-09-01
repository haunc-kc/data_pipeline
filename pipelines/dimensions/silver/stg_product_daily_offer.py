from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, trim


@dp.materialized_view(name="stg_product_daily_offer")
def stg_daily_offer():

    return (
        spark.read.table("`bigquery-udp_catalog`.`ktplogic`.dailyoffer")
        .select(
           
            trim(col("ID"))                                     .alias("nk_offer_id"),
            col("ItemNr")                                       .alias("product_number"),    
            col("CreatedBy")                                    .alias("nk_created_by"),    
            col("From")     .cast("date")                       .alias("valid_from"),
            col("To")       .cast("date")                       .alias("valid_to"),
            trim(coalesce(col("Comment"), lit("")))             .alias("comment"),
            col("Created")  .cast("timestamp")                  .alias("created_at"),
        )
    )
