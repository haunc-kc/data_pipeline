from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="stg_supplier_address_enriched")
def stg_supplier_address_enriched():
    stg = spark.read.table("stg_supplier_address")
    dim_sup = (
        spark.read.table("dim_supplier")
        .filter(F.col("__END_AT").isNull())
        .select("nk_supplier_id", "sk_supplier_id")
    )
    return stg.join(dim_sup, on="nk_supplier_id", how="left").dropDuplicates(["nk_supplier_id", "address_seq_no"])


dp.create_streaming_table(
    name="dim_supplier_address",
    comment="Supplier address dimension with SCD Type-2 history, auto-managed by Auto CDC from snapshot"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_supplier_address",
    source="stg_supplier_address_enriched",
    keys=["nk_supplier_id", "address_seq_no"],
    stored_as_scd_type=2,
    track_history_column_list=["address_seq_no","full_name","street"
    ]
)
