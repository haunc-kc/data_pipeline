from pyspark import pipelines as dp 
from pyspark.sql import functions as F
from pyspark.sql.functions import col

SOURE_TABLE = "`bigquery-udp_catalog`.`mention_data`.`kovart`"

@dp.temporary_view(name="stg_shipping_method")
def stg_shipping_method():
    return (
        spark.table(SOURE_TABLE).where(col("KOVMANKEY") ==0)
        .select(
             F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,F.col("KOVNUMMER")
                                ,F.col("KOTEXT")
                                ,F.col("KOVSKENN")
                                ,F.col("KOVERSAND")                   
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_shipping_method_id"),
             
            F.col("KOVNUMMER").alias("nk_shipping_method_id"),
            F.col("KOTEXT").alias("shipping_method_name"),
            F.col("KOVSKENN").alias("shipping_type_flag"),
            F.col("KOVERSAND").alias("shipping_rate"),
            F.col("KOVPGFFID").alias("external_interface_id"),
            F.col("KOVPGCODE").alias("external_process_code"),
            F.col("KOVPGPOID").alias("external_position_id"),
            F.col("UPDTIME").alias("updated_at")
        )
    )