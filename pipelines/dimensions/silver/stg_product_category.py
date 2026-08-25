from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="stg_product_category")
def stg_product_category():
    """
    Reads kokatar and prepares product category hierarchy attributes.
    Reads directly from BigQuery foreign catalog (no bronze layer needed).
    """
    k = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kokatar`")
    
    return (
        k.select(
              F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,k["kakat"]
                                ,F.coalesce(k["katext"], F.lit(""))
                                ,F.coalesce(k["kaobkat"], F.lit(""))
                                ,k["kaoaktiv"]                                
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_category_id"),
              
            
            F.trim(k["kakat"]).alias("nk_category_id"),
            F.trim(F.coalesce(k["katext"], F.lit(""))).alias("category_name"),
            F.trim(F.coalesce(k["kaobkat"], F.lit(""))).alias("parent_code"),
            F.when(F.trim(F.coalesce(k["kaobkat"], F.lit(""))) == "", 1)
            .otherwise(2)
            .alias("depth_level"),
            F.when(F.coalesce(k["kaoaktiv"], F.lit(0)) > 0, True)
            .otherwise(False)
            .alias("is_online"),
        )
    )
