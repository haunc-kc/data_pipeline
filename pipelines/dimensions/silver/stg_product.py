from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window




@dp.temporary_view(name="stg_product")
def stg_product():
    """
    Joins ael + aelka + kokatar + koherst and prepares product dimension attributes.
    Reads directly from BigQuery foreign catalog (no bronze layer needed).
    """
    a = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`ael`")
    ak = (
        spark.read.table("`bigquery-udp_catalog`.`mention_data`.`aelka`")
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("KKIDNR").orderBy(F.asc("KKLFDNR"))
            )
        )
        .where(F.col("rn") == 1)
        .select("KKIDNR", "KKKAT")
    )
    kc = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`kokatar`")
    kh = spark.read.table("`bigquery-udp_catalog`.`mention_data`.`koherst`")
    return (
        a.join(ak, a["aridnr"] == ak["kkidnr"], "left")
        .join(kc, ak["kkkat"] == kc["kakat"], "left")
        .join(kh, a["arkzselekt"] == kh["kohkenn"], "left")
        .select(
            F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,F.coalesce(a["aranummer"], F.lit(""))
                                ,F.coalesce(a["arben"], F.lit(""))
                                ,F.coalesce(a["artyp"], F.lit(""))
                                ,F.coalesce(a["arherstnr"], F.lit(""))
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_product_id"),
            
            
            a["aridnr"].alias("nk_product_id"),
            F.trim(F.coalesce(a["aranummer"], F.lit(""))).alias("product_number"),

            F.trim(F.coalesce(a["arben"], F.lit(""))).alias("product_name"),
            F.substring(F.trim(F.coalesce(a["arerwben"], F.lit(""))), 1, 500).alias("product_name_ext"),
            F.trim(F.coalesce(a["arkatben"], F.lit(""))).alias("catalog_name"),

            F.trim(F.coalesce(a["artyp"], F.lit(""))).alias("product_type"),
            F.trim(F.coalesce(ak["kkkat"], F.lit(""))).alias("nk_category_id"),
            F.trim(F.coalesce(kc["katext"], kc["kaotext"], F.lit(""))).alias("category_name"),
            F.trim(F.coalesce(kc["kaobkat"], F.lit(""))).alias("parent_category"),
            F.trim(F.coalesce(a["arherstnr"], F.lit(""))).alias("manufacturer_no"),
            F.trim(F.coalesce(a["arkzselekt"], F.lit(""))).alias("manufacturer_code"),
            F.trim(F.coalesce(kh["kohtext"], a["arkzselekt"], F.lit("Unknown"))).alias("manufacturer_name"),
            F.trim(F.coalesce(a["areancode"], F.lit(""))).alias("ean_code"),
            F.coalesce(a["arme"], F.lit(0)).alias("unit_of_measure"),
            F.coalesce(a["arpe"], F.lit(1)).alias("price_unit"),
            F.coalesce(a["armwstsatz"], F.lit(0)).alias("tax_rate_id"),
            F.coalesce(a["ardimh"], F.lit(0)).alias("height"),
            F.coalesce(a["ardimw"], F.lit(0)).alias("width"),
            F.coalesce(a["ardiml"], F.lit(0)).alias("length"),
            F.coalesce(a["argewicht"], F.lit(0)).alias("weight"),
            F.trim(F.coalesce(a["arurland"], F.lit(""))).alias("country_of_origin"),
            F.coalesce(a["argarmon"], F.lit(0)).alias("warranty_months"),

            F.coalesce(a["arlagware"], F.lit(False)).alias("is_stock_item"),
            F.when(F.coalesce(a["arauslauf"], F.lit(0)) > 0, True).otherwise(False).alias("is_discontinued"),
            F.when(F.coalesce(a["arnoaktiv"], F.lit(0)) == 0, True).otherwise(False).alias("is_active"),
            F.when(F.coalesce(a["arsernr"], F.lit(0)) > 0, True).otherwise(False).alias("is_serial_tracked"),
            F.when(F.coalesce(a["arsernr"], F.lit(0)) > 0, True).otherwise(False).alias("has_serial_number"),
            F.coalesce(a["arnuronline"], F.lit(False)).alias("online_only"),
            F.coalesce(a["arkeinprov"], F.lit(False)).alias("no_commission"),

            F.coalesce(a["areingabe"], a["updtime"], F.lit("2000-01-01")).alias("created_date"),
        )
       
    ).dropDuplicates(["nk_product_id"])
