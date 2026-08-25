from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col

@dp.materialized_view(
    comment="customer external information",
    cluster_by=["sk_customer_id"]
)
def customer_external_info():
    stg = spark.read.table("stg_customer_external_info").alias("stg")
    dim_customer = (
        spark.read.table("dim_customer")
        .where("sk_customer_id IS NOT NULL")
        .select("sk_customer_id", "nk_customer_id", "__START_AT", "__END_AT")
        .alias("c")
    )
    df = stg.join(dim_customer, on = (col("stg.nk_customer_id") == col("c.nk_customer_id")), how="left")
    return df.select(
        "c.sk_customer_id",
        "customer_category_code",
        "customer_category_label",
        "parent_category",
        "category_seq_no",
        )
