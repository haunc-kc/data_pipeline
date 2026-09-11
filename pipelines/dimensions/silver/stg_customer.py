from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.materialized_view(
    name="stg_customer",
    table_properties={"delta.feature.timestampNtz": "supported"}
)
def build_stg_customer():
    """
    Joins kunden + adressen and prepares customer dimension attributes.
    Reads directly from BigQuery foreign catalog (no bronze layer needed).
    """
    kd = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kunden`")
    ad = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`adressen`")
    kl = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kolandkz`")
    kka = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kundenan`").where(col("krakenn") != ' ').dropDuplicates(["kradnummer"])
    ad_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`adresspa`")

    dim_shipping = (spark.read.table("dim_shipping_method").where("__END_AT IS NULL").select("sk_shipping_method_id", "nk_shipping_method_id"))
    dim_payment_term = (spark.read.table("dim_payment_term").where("__END_AT IS NULL").select("sk_payment_term_id","nk_payment_term_id"))

    t1 = (
        ad_df.groupBy("PANUMMER")
        .agg(F.max("PALFDNR").alias("PALFDNR"))
    )

    ad_df_final = (
        ad_df.alias("t")
        .join(
            t1.alias("t1"),
            (F.col("t.PANUMMER") == F.col("t1.PANUMMER")) & (F.col("t.PALFDNR") == F.col("t1.PALFDNR")),
            "inner"
        )
        .select(
            F.col("t.PANUMMER"),
            F.col("t.PAEMAIL").alias("email"),
            F.col("t.PATELEFON1").alias("phone")
        )
    )


    df = (
        kd.join(ad, kd["kdadnummer"] == ad["adnummer"], "left")
        .join(kl, kl["lkzlandkz"] == ad["adlandkz"], "left")
        .join(kka, kka["kradnummer"] == ad["adnummer"], 'left')
        .join(ad_df_final, ad_df_final["PANUMMER"] == ad["adnummer"], 'left')
        .join(dim_shipping, dim_shipping["nk_shipping_method_id"] == kd["kdvart"], 'left')
        .join(dim_payment_term, dim_payment_term["nk_payment_term_id"] == kd["kdzbkenn"], "left")

        .select(
            F.regexp_replace(
                            F.md5(F.concat_ws("||",
                                kd["kdnummer"]
                                ,kd['kdbnummer']
                                ,F.coalesce(ad["adname1"], F.lit(""))
                                ,F.coalesce(ad["adname2"], F.lit(""))
                                ,F.coalesce(ad_df_final['phone'],F.lit(""))
                                ,F.coalesce(ad_df_final['email'],F.lit(""))
                                ,F.coalesce(ad["adstrasse"], F.lit(""))
                                ,F.coalesce(ad["adhausnr"], F.lit(""))
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_customer_id"),
            
            kd["kdnummer"].alias("nk_customer_id"),
            kd['kdbnummer'].alias("account_manager_id"),
            dim_shipping["sk_shipping_method_id"],
            dim_payment_term["sk_payment_term_id"],
            F.trim(F.concat_ws(" ",
                F.coalesce(ad["adname1"], F.lit("")),
                F.coalesce(ad["adname2"], F.lit(""))
            )).alias("customer_name"),
            F.trim(F.coalesce(ad["adname2"], F.lit(""))).alias("customer_name2"),
            F.trim(F.coalesce(kd["kdrating"], F.lit(""))).alias("customer_segment"),
            F.trim(F.coalesce(ad_df_final['phone'],F.lit(""))).alias("phone"),
            F.trim(F.coalesce(ad_df_final['email'],F.lit(""))).alias("email"),
            F.trim(F.coalesce(ad["adstrasse"], F.lit(""))).alias("street"),
            F.trim(F.coalesce(ad["adhausnr"], F.lit(""))).alias("house_number"),
            F.trim(F.coalesce(ad["adplz"], F.lit(""))).alias("postal_code"),
            F.trim(F.coalesce(ad["adort"], F.lit(""))).alias("city"),
            F.when(F.rtrim(ad["adlandkz"]).isin("D", ""), "Germany (DE)")
            .when(
                (F.rtrim(ad["adlandkz"]) != "D") & (F.rtrim(ad["adlandkz"]) != "") & (kl["lkzeuaus"] == 1),
                "EU (ex. DE)"
            ).when(
                (F.rtrim(ad["adlandkz"]) != "D") & (F.rtrim(ad["adlandkz"]) != "") & (kl["lkzeuaus"] == 0),
                "Non-EU/International"
            ).otherwise(None).alias("region"),
            F.trim(F.coalesce(ad["adlandkz"], F.lit(""))).alias("country_code"),
            ad["adgeolat"].alias("lat"),
            ad["adgeolng"].alias("lng"),
            F.trim(F.coalesce(kd["kdtyp"], F.lit(""))).alias("customer_type"),
            F.when(kd["kdarchiv"] == 0, True).otherwise(False).alias("is_active"),
            F.when(kd["kdliefsp"] > 0, True).otherwise(False).alias("has_delivery_block"),
            
            F.when(kd["kddsgvo"] > 0, True).otherwise(False).alias("gdpr_blocked"),
            F.trim(F.coalesce(kd["kdwaeh"], F.lit("EUR"))).alias("currency_code"),
            F.coalesce(kd["kdlimit"], F.lit(0)).alias("credit_limit"),
            F.coalesce(kd["kdrabatt"], F.lit(0)).alias("discount_pct"),
            kd["updtime"].alias("updtime"),
            kd["KDEINGABE"].alias("created_date"),
            ad["ADURL"].alias("url"),
            F.when(F.rtrim(ad["ADLAND"]) != "", ad["ADLAND"]).otherwise("Deutschland").alias("country"),
            F.when(col("krdirekt") == 1, True).otherwise(False).alias("is_direct_shipping")
        )
    )
    return df.dropDuplicates(['nk_customer_id'])

# @dp.materialized_view(name="_stg_customer")
# def _stg_customer():
#     return build_stg_customer()


# def stg_customer():
#     df = build_stg_customer()
#     key_columns = get_key_columns(spark, "mention_dw._stg_customer")
#     return df.dropDuplicates(key_columns)