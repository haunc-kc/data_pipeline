from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.materialized_view(name="stg_supplier_address")
def build_stg_supplier_address():
    lief_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.lief")
    liefan_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.liefan")
    adressen_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.adressen")
    kolandkz_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.kolandkz")
    adresspa_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.adresspa")   
    
    return (
        lief_df.alias("l")
        .join(
            liefan_df.alias("la"),
            (F.col("la.lanummer") == F.col("l.lfnummer")) &
            (F.col("la.lamankey") == F.col("l.lfmankey"))
        )
        .join(
            adressen_df.alias("a"),
            F.col("a.adnummer") == F.col("la.laadnummer")
        )
        .join(
            kolandkz_df.alias("lkz"),
            F.col("lkz.lkzlandkz") == F.col("a.adlandkz"),
            "left"
        )
        .join(
            adresspa_df.alias("ap"),
            (F.col("ap.panummer") == F.col("la.laadnummer")) &
            (F.col("ap.palfdnr") == F.lit(1)),
            "left"
        )
        .select(
            F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                               ,F.col("l.lfnummer")
                               ,F.col("la.lalfdnr")
                               ,F.col("a.adname1")
                               ,F.col("a.adname2")
                               ,F.col("a.adstrasse")
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_supplier_address_id"),
        
            
            F.col("l.lfnummer").alias("nk_supplier_id"),
            F.col("la.lalfdnr").alias("address_seq_no"),
            F.trim(F.col("la.laakenn")).alias("address_type_code"),
            F.when(F.trim(F.col("la.laakenn")) == "Z", F.lit("Payment address"))
            .when(F.trim(F.col("la.laakenn")) == "S", F.lit("Service address"))
            .when(F.trim(F.col("la.laakenn")) == "B", F.lit("Order address"))
            .otherwise(F.lit("Unknown")).alias("address_type"),
            F.when(F.col("la.lanoaktiv") == 0, F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("a.adname1"), F.lit("")),
                    F.coalesce(F.col("a.adname2"), F.lit(""))
                )
            ).alias("full_name"),
            F.trim(F.coalesce(F.col("a.adstrasse"), F.lit(""))).alias("street"),
            F.trim(F.coalesce(F.col("a.adhausnr"), F.lit(""))).alias("house_number"),
            F.trim(F.coalesce(F.col("a.adplz"), F.lit(""))).alias("postal_code"),
            F.trim(F.coalesce(F.col("a.adort"), F.lit(""))).alias("city"),
            F.trim(F.coalesce(F.col("a.adbstkz"), F.lit(""))).alias("state_code"),
            F.trim(F.coalesce(F.col("a.adlandkz"), F.lit(""))).alias("country_code"),
            F.trim(F.coalesce(F.col("a.adland"), F.lit(""))).alias("country_name_local"),
            F.trim(F.coalesce(F.col("lkz.lkzland"), F.lit(""))).alias("country_name"),
            F.col("a.adgeolat").alias("lat"),
            F.col("a.adgeolng").alias("lng"),
            F.trim(F.coalesce(F.col("a.adurl"), F.lit(""))).alias("website"),
            F.trim(F.coalesce(F.col("a.adiln"), F.lit(""))).alias("iln"),
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("ap.paname1"), F.lit("")),
                    F.coalesce(F.col("ap.paname2"), F.lit(""))
                )
            ).alias("contact_full_name"),
            F.trim(F.coalesce(F.col("ap.pafunk"), F.lit(""))).alias("contact_function"),
            F.trim(F.coalesce(F.col("ap.patelefon1"), F.lit(""))).alias("contact_phone_1"),
            F.trim(F.coalesce(F.col("ap.patelefon2"), F.lit(""))).alias("contact_phone_2"),
            F.trim(F.coalesce(F.col("ap.patelefon3"), F.lit(""))).alias("contact_mobile"),
            F.trim(F.coalesce(F.col("ap.pafax"), F.lit(""))).alias("contact_fax"),
            F.trim(F.coalesce(F.col("ap.paemail"), F.lit(""))).alias("contact_email"),
            F.trim(F.coalesce(F.col("ap.paspez"), F.lit(""))).alias("contact_specialization"),
            F.col("ap.pavip").alias("contact_is_vip")
        )
    ).dropDuplicates(['nk_supplier_id','address_seq_no','address_type_code'])
    

# @dp.materialized_view(name="_stg_supplier_address")
# def _stg_supplier_address():
#     return build_stg_supplier_address()

# @dp.materialized_view(name="stg_supplier_address")
# def stg_supplier_address():
#     key_columns = get_key_columns(spark, "_stg_supplier_address")
#     return build_stg_supplier_address().dropDuplicates(key_columns)