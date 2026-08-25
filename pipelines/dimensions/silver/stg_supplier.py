from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.materialized_view(name="stg_supplier")
def build_stg_supplier():
    lieft_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.lief")
    address_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.adressen")
    kolandkz_df = spark.read.table("`bigquery-udp_catalog`.`mention_data`.kolandkz")
    
    df = (lieft_df.alias("lf")
        .join(address_df.alias("ad"), F.col("lf.lfadnummer") == F.col("ad.adnummer"), "left")
        .join(kolandkz_df.alias("lkz"), F.col("ad.adlandkz") == F.col("lkz.lkzlandkz"), "left")
        .select(
             F.regexp_replace(
                            F.md5(F.concat_ws("||"      
                                ,F.col("lf.lfnummer")
                                ,F.col("ad.adname1")
                                ,F.col("ad.adname2")
                                ,F.col("ad.adstrasse")
                                ,F.col("ad.adhausnr")
                                ,F.col("ad.adurl")
                            )),
                            r"(.{8})(.{4})(.{4})(.{4})(.{12})",
                            r"$1-$2-$3-$4-$5"
                            ).alias("sk_supplier_id"),
            
            
            F.col("lf.lfnummer").alias("nk_supplier_id"),
            F.trim(F.concat_ws(" ", F.coalesce(F.col("ad.adname1"), F.lit("")), F.coalesce(F.col("ad.adname2"), F.lit("")))).alias("supplier_name"),
            F.trim(F.coalesce(F.col("ad.adstrasse"), F.lit(""))).alias("street"),
            F.trim(F.coalesce(F.col("ad.adhausnr"), F.lit(""))).alias("house_number"),
            F.trim(F.coalesce(F.col("ad.adplz"), F.lit(""))).alias("postal_code"),
            F.trim(F.coalesce(F.col("ad.adort"), F.lit(""))).alias("city"),
            F.trim(F.coalesce(F.col("ad.adbstkz"), F.lit(""))).alias("state_code"),
            F.trim(F.coalesce(F.col("ad.adlandkz"), F.lit(""))).alias("country_code"),
            F.trim(F.coalesce(F.col("ad.adland"), F.lit(""))).alias("country_name_local"),
            F.trim(F.coalesce(F.col("lkz.lkzland"), F.lit(""))).alias("country_name"),
            F.col("ad.adgeolat").alias("lat"),
            F.col("ad.adgeolng").alias("lng"),
            F.trim(F.coalesce(F.col("ad.adurl"), F.lit(""))).alias("website"),
            F.trim(F.coalesce(F.col("ad.adiln"), F.lit(""))).alias("iln"),

            F.trim(F.coalesce(F.col("lf.lfwae"), F.lit("EUR"))).alias("currency_code"),
            F.trim(F.coalesce(F.col("lf.lfzb"), F.lit(""))).alias("payment_terms"),
            F.col("lf.lfnetage").alias("payment_net_days"),
            F.col("lf.lfsktage").alias("cash_discount_days"),
            F.col("lf.lfskproz").alias("cash_discount_pct"),
            F.col("lf.lfzahlart").alias("payment_method_1"),
            F.col("lf.lfzbart").alias("payment_method_2"),
            F.col("lf.lffibkonto").alias("payables_account"),
            F.col("lf.lfkredit").alias("credit_line"),
            F.col("lf.lfkreditin").alias("credit_line_internal"),
            F.col("lf.lfkreditdt").cast("date").alias("credit_line_date"),
            F.col("lf.lfoprg").alias("open_items"),
            F.col("lf.lfoprgfael").alias("open_items_due"),
            F.col("lf.lfopdatum").cast("date").alias("open_items_date"),
            F.col("lf.lfanzproz").alias("down_payment_pct"),
            F.col("lf.lfmindwert").alias("minimum_order_value"),
            F.col("lf.lfmindzus").alias("minimum_order_surcharge"),
            F.col("lf.lfversand").alias("default_shipping_cost"),
            F.col("lf.lfverpack").alias("default_packaging_cost"),
            F.col("lf.lfversich").alias("default_insurance_cost"),

            F.trim(F.coalesce(F.col("lf.lfustid"), F.lit(""))).alias("vat_id"),
            F.trim(F.coalesce(F.col("lf.lfsteuernr"), F.lit(""))).alias("tax_no"),
            F.col("lf.lfmwstkz").alias("no_vat_flag"),
            F.col("lf.lfeu").alias("eu_vat_flag"),
            F.trim(F.coalesce(F.col("lf.lfweeenr"), F.lit(""))).alias("weee_no"),
            F.trim(F.coalesce(F.col("lf.lfci"), F.lit(""))).alias("creditor_id"),
            F.col("lf.lfdsgvo").alias("gdpr_flag"),

            F.trim(F.coalesce(F.col("lf.lfdistrib"), F.lit(""))).alias("distributor_code"),
            F.trim(F.coalesce(F.col("lf.lfsprach"), F.lit(""))).alias("language_code"),
            F.trim(F.coalesce(F.col("lf.lfliefkond"), F.lit(""))).alias("delivery_terms"),
            F.col("lf.lfvart").alias("shipping_method_id"),
            F.col("lf.lflager").alias("transport_warehouse"),
            F.col("lf.lftast").alias("appointment_type"),
            F.col("lf.lfpreislim").alias("price_enforcement"),
            F.col("lf.lftermlim").alias("date_enforcement"),
            F.col("lf.lfemauto").alias("auto_email"),
            F.col("lf.lfemformat").alias("email_format"),
            F.col("lf.lfemtrweg").alias("transport_route"),
            F.trim(F.coalesce(F.col("lf.lfememail"), F.lit(""))).alias("email_address"),
            F.trim(F.coalesce(F.col("lf.lfemailkz"), F.lit(""))).alias("email_text_code"),
            F.col("lf.lfgesber").alias("business_area"),

            F.trim(F.coalesce(F.col("lf.lfrmanr"), F.lit(""))).alias("rma_no"),
            F.col("lf.lfrmanrbis").cast("date").alias("rma_no_valid_to"),
            F.col("lf.lfrmaantr").alias("rma_request"),
            F.trim(F.coalesce(F.col("lf.lfrmaadr"), F.lit(""))).alias("rma_address"),
            F.trim(F.coalesce(F.col("lf.lfrmaerinemailkz"), F.lit(""))).alias("rma_return_email_code"),
            F.col("lf.lfkeinrs").alias("no_return_shipment"),
            F.col("lf.lfsammelrs").alias("collective_return"),

            F.trim(F.coalesce(F.col("lf.lfneoslfnr"), F.lit(""))).alias("neos_id"),
            F.trim(F.coalesce(F.col("lf.lfegislfnr"), F.lit(""))).alias("egis_id"),
            F.trim(F.coalesce(F.col("lf.lfcoplfnr"), F.lit(""))).alias("cop_id"),
            F.trim(F.coalesce(F.col("lf.lfvelolfnr"), F.lit(""))).alias("velo_id"),
            F.col("lf.lffremdid").alias("external_id"),
            F.trim(F.coalesce(F.col("lf.lfkdnummer"), F.lit(""))).alias("customer_no"),
            F.col("lf.lfmandnr").alias("client_customer_no"),

            
            F.when(F.col("lf.lfarchiv") == 0, F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            F.col("lf.lfart").alias("supplier_type"),
            F.col("lf.lfprofrg").alias("proforma_invoice"),
            F.col("lf.lfdirauto").alias("direct_delivery_auto"),

            F.col("lf.lfeingabe").cast("date").alias("created_date"),
            F.col("lf.lfletztbew").cast("date").alias("last_movement_date"),
            F.col("lf.lfletztaen").cast("date").alias("last_changed_date"),
            F.col("lf.updtime").alias("updtime"),
        )
    )

    return df
# @dp.materialized_view(name="_stg_supplier")
# def _stg_supplier():
#     return build_stg_supplier()

# @dp.materialized_view(name="stg_supplier")
# def stg_supplier():
#     key_columns = get_key_columns(spark, "_stg_supplier")
#     return build_stg_supplier().dropDuplicates(key_columns)
    