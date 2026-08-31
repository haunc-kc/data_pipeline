spark.sql("INSERT INTO dw_mention_serving.etl_inform (date_create) VALUES (current_timestamp())")
print("Done With UAT")
spark.sql("INSERT INTO dw_mention_serving.etl_inform_prd (date_create) VALUES (current_timestamp())")
print("Done With PROD")