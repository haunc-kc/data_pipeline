from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col

SOURE_TABLE = "`bigquery-udp_catalog`.`mention_data`.`kozb`"

@dp.materialized_view(
    name="stg_payment_term",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def stg_payment_term():
    return (
        spark.table(SOURE_TABLE).where(col("KZMANKEY") == 1)
        .select(
            F.regexp_replace(
                            F.md5(F.concat_ws("||"
                                ,F.col("KZZB")
                                ,F.col("KZTEXT1")
                                ,F.col("KZSKPROZ")
                                ,F.col("KZSKTAGE")
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_payment_term_id"),

            F.col("KZZB").alias("nk_payment_term_id"),
            F.col("KZTEXT1").alias("payment_term_name"),
            F.col("KZTEXT2").alias("payment_term_name_2"),
            F.col("KZTEXTA1").alias("payment_term_neutral_name"),
            F.col("KZTEXTA2").alias("payment_term_neutral_name_2"),
            F.col("KZTGREAL").alias("due_days_actual"),
            F.col("KZTGINTERN").alias("due_days_internal"),
            F.col("KZTGSTOP").alias("due_days_delivery_stop"),
            F.col("KZSKPROZ").alias("cash_discount_pct"),
            F.col("KZSKTAGE").alias("cash_discount_days"),
            F.col("KZZART").alias("payment_method_1"),
            F.col("KZNN").alias("payment_method_2"),
            F.col("KZVART").alias("nk_shipping_method_id"),
            F.col("kzmindbetr").alias("minimum_order_value"),
            F.col("kzraten").alias("installment_flag"),
            F.col("kzvorkasse").alias("prepayment_flag"),
            F.col("KZNOLIMIT").alias("no_limit_check_flag"),
            F.col("KZNOMAHN").alias("no_dunning_flag"),
            F.col("KZBEM").alias("remark"),
            F.col("updtime").cast("date").alias("updated_at")
        )
    )
