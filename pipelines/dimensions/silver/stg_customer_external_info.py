from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="stg_customer_external_info")
def stg_customer_external_info():
    kundenka_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kundenka`")
    kokatkd_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kokatkd`")
    return kundenka_df.join(kokatkd_df, kundenka_df["kkkat"] == kokatkd_df["kakat"]).select(
        kundenka_df["kknummer"].alias("nk_customer_id"),
        F.trim(F.coalesce(kundenka_df["kkkat"], F.lit('NONE'))).alias("customer_category_code"),
        F.trim(F.coalesce(kokatkd_df["katext"], F.lit(''))).alias("customer_category_label"),
        F.trim(F.coalesce(kokatkd_df["kaparent"], F.lit(''))).alias("parent_category"),
        kundenka_df["kklfdnr"].alias("category_seq_no")
    )