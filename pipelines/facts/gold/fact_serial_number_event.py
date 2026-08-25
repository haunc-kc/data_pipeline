from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit


@dp.materialized_view(
    comment="Fact table for serial number movement events with SK lookups from SCD-2 dimensions",
    cluster_by=["sk_product_id", "event_date"]
)
def _fact_serial_number_event():
   
    stg = (
        spark.read.table("stg_serial_number_event")
        .withColumn("event_date", col("event_date").cast("date"))
        .alias("stg")
    )

    dim_product = (
        spark.read.table("dim_product")
        .select("sk_product_id", "nk_product_id", "__START_AT", "__END_AT")
    ).alias("prd")

    dim_warehouse = (
        spark.read.table("dim_warehouse")
        .select("sk_warehouse_id", "nk_warehouse_id", "__START_AT", "__END_AT")
    ).alias("wh")

    dim_customer = (
        spark.read.table("dim_customer")
        .select("sk_customer_id", "nk_customer_id", "__START_AT", "__END_AT")
    ).alias("cust")

    dim_supplier = (
        spark.read.table("dim_supplier")
        .select("sk_supplier_id", "nk_supplier_id", "__START_AT", "__END_AT")
    ).alias("sp")

    dim_salesperson = (
        spark.read.table("dim_salesperson")
        .select(
            "sk_salesperson_id",
            "nk_salesperson_id",
            col("__START_AT").cast("date").alias("__START_AT"),
            col("__END_AT").cast("date").alias("__END_AT"),
        )
    ).alias("s")
    # temporal join — product version at event_date
    df = stg.join(
        dim_product,
        on=(
            (col("stg.nk_product_id") == col("prd.nk_product_id")) &
            (col("stg.event_date") >= col("prd.__START_AT")) &
            (col("stg.event_date") <  coalesce(col("prd.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    df = df.join(
        dim_warehouse,
        on=(
            (col("stg.nk_warehouse_id") == col("wh.nk_warehouse_id")) &
            (col("stg.event_date") >= col("wh.__START_AT")) &
            (col("stg.event_date") <  coalesce(col("wh.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    
   
    df = df.join(
        dim_customer,
        on=(
            (col("stg.party_type_code") == lit("K")) &
            (col("stg.nk_party_id") == col("cust.nk_customer_id")) &
            (col("stg.event_date") >= col("cust.__START_AT")) &
            (col("stg.event_date") <  coalesce(col("cust.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )   

    # temporal join — supplier (only resolves when party_type = Supplier)
    df = df.join(
        dim_supplier,
        on=(
            (col("stg.party_type_code") == lit("L")) &
            (col("stg.nk_party_id") == col("sp.nk_supplier_id")) &
            (col("stg.event_date") >= col("sp.__START_AT")) &
            (col("stg.event_date") <  coalesce(col("sp.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )
    df = df.join(
        dim_salesperson,
        on=(
            (col("stg.nk_salesperson_id") == col("s.nk_salesperson_id")) &
            (col("stg.event_date") >= col("s.__START_AT")) &
            (col("stg.event_date") <  coalesce(col("s.__END_AT"), lit("2099-12-31").cast("date")))
        ),
        how="left"
    )

    return df.select(
        # ── Surrogate keys ────────────────────────────────
        col("prd.sk_product_id"),
        col("wh.sk_warehouse_id"),
        col("cust.sk_customer_id"),       # NULL when party_type = Supplier
        col("sp.sk_supplier_id"),          # NULL when party_type = Customer

        # ── Natural keys ──────────────────────────────────
        col("stg.nk_event_id"),
        col("stg.nk_party_id"),
        col("stg.nk_document_id"),
        col("s.sk_salesperson_id"),

        # ── Serial / Batch ────────────────────────────────
        col("stg.serial_number"),
        col("stg.house_serial_number"),
        col("stg.quantity"),
        col("stg.is_batch"),

        # ── Movement ──────────────────────────────────────
        col("stg.is_outbound"),
        col("stg.movement_direction"),
        col("stg.party_type_code"),
        col("stg.party_type"),

        # ── Document reference ────────────────────────────
        col("stg.document_type_code"),
        col("stg.document_category"),
        col("stg.document_no"),
        col("stg.position_no"),

        # ── Dates ─────────────────────────────────────────
        col("stg.event_date"),
        col("stg.invoice_date"),
        col("stg.best_before_date"),
        col("stg.warranty_expiry_date"),
        col("stg.cancellation_date"),

        # ── Warranty ──────────────────────────────────────
        col("stg.warranty_code"),
        col("stg.warranty_months"),
        col("stg.extended_warranty_code"),
        col("stg.extended_warranty_months"),
        col("stg.supplier_warranty_code"),
        col("stg.supplier_warranty_months"),

        # ── Status ────────────────────────────────────────
        col("stg.is_cancelled"),
        col("stg.cancellation_editor"),
        col("stg.is_external"),
        col("stg.error_code"),
        col("stg.error_text"),

        # ── References ───────────────────────────────────
        col("stg.delivery_note_no"),
        col("stg.remark_1"),
        col("stg.remark_2"),
        col("stg.serial_addition_1"),
        col("stg.serial_addition_2"),

        # ── Audit ─────────────────────────────────────────
        F.current_timestamp().alias("dw_created_date"),
    )
