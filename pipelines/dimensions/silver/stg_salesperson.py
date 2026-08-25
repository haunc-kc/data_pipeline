from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name = "stg_salesperson")
def stg_salesperson():
    return (spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kobea`").select(
         F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,F.col("kobnummer")
                                ,F.coalesce(F.col("kobname"), F.lit(""))
                                ,F.coalesce(F.col("kobvname"), F.lit(""))
                                ,F.coalesce(F.col("kobfunk"), F.lit(""))
                                ,F.coalesce(F.col("kobfil"), F.lit(""))
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_salesperson_id"),
        
        F.col("kobnummer").alias("nk_salesperson_id"),
        F.concat_ws(
            " ",
            F.trim(F.coalesce(F.col("kobname"), F.lit(""))),
            F.trim(F.coalesce(F.col("kobvname"), F.lit(""))),
        ).alias("full_name"),
        F.trim(F.coalesce(F.col("kobname"), F.lit(""))).alias("last_name"),
        F.trim(F.coalesce(F.col("kobvname"), F.lit(""))).alias("first_name"),
        F.trim(F.coalesce(F.col("kobemail"), F.lit(""))).alias("email"),
        F.trim(F.coalesce(F.col("kobfunk"), F.lit(""))).alias("department"),
        F.trim(F.coalesce(F.col("kobfil"), F.lit(""))).alias("branch_code"),
        # Inverted flag logic: kobarchiv = 1 means True, otherwise False
        F.when(F.col("kobarchiv") == 1, True)
        .otherwise(False)
        .alias("is_active"),
        F.coalesce(F.col("kobminroh"), F.lit(0)).alias("min_gross_profit"),
        )
        
    )
    

