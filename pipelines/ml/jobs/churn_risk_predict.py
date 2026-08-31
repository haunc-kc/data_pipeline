"""
Customer Churn Risk — Scoring / Inference

Predict churn probability for all customers using the trained model from
`churn_risk_model_new`.

Pipeline:
1. Load `fact_sales_invoice_header` + `dim_customer`
2. Re-create the exact feature set used during training
3. Load scalers + model from MLflow (Unity Catalog registry)
4. Score every eligible customer
5. Write `churn_risk_scores` and enriched `ml_churn_risk_info_details` tables

Converted from the Databricks notebook
"/Workspace/Users/hau.nguyen@kosatec.vn/ML/churn risk/churn_risk_predict_new".
Runs as a spark_python_task (see databricks.yml -> resources.jobs.etl_pipeline_job).
"""

import subprocess
import sys
import os
import argparse

_parser = argparse.ArgumentParser()
_parser.add_argument("--repo-root")
_parser.add_argument("--pipeline-catalog")
_parser.add_argument("--pipeline-schema")
_args, _ = _parser.parse_known_args()
_repo_root = _args.repo_root or os.getcwd()
sys.path.insert(0, _repo_root)

try:
    import tensorflow as tf
    _ = tf.constant(1)
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "tensorflow-cpu==2.18.0", "-q"])
    import tensorflow as tf

print("TensorFlow version:", tf.__version__)

# ── Model & output ───────────────────────────────────────────────────────────
CATALOG = _args.pipeline_catalog or os.getenv("PIPELINE_CATALOG", "workspace")
SCHEMA  = _args.pipeline_schema  or os.getenv("PIPELINE_SCHEMA",  "mention_dw")

REGISTERED_MODEL = f"{CATALOG}.default.churn_risk_model"
# OUTPUT_TABLE     = f"{CATALOG}.{SCHEMA}.churn_risk_scores"
OUTPUT_TABLE       = f"{CATALOG}.{SCHEMA}.ml_churn_risk_info_details"


# ── Config — must match churn_risk_model_new exactly ─────────────────────────
import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, LongType, IntegerType
from pyspark.sql.functions import col
import pandas as pd
import numpy as np

# ── Feature config (must match training notebook) ────────────────────────────
CHURN_WINDOW_DAYS = 90
SEQ_LEN  = 12
SEQ_COLS = ["revenue", "cost_amount", "days_to_pay",
            "header_net_total", "is_credit_note", "is_open"]

CAT_COLS = ["customer_type", "customer_segment", "country", "region"]

STATIC_COLS = [
    "recency_days", "frequency", "monetary_total", "monetary_avg", "monetary_std",
    "avg_order_value", "max_order_value", "min_order_value",
    "avg_days_to_pay", "std_days_to_pay",
    "num_credit_notes", "num_open_invoices", "avg_amount_paid",
    "num_channels", "num_doc_types",
    "total_cost", "total_profit", "profit_margin",
    "customer_lifetime_days", "invoice_frequency_per_month", "return_rate",
    "credit_limit", "discount_pct"
    # 03/08
    ,"freq_yoy_change_pct",   # ADD THIS
    "invoices_last_3m",      # ADD THIS
    "invoices_last_6m"
    # ",is_active"
    # , "has_delivery_block"
    , "is_direct_shipping",
    "customer_type", "customer_segment", "country", "region",
]

SEQ_FEATURE_COLS = [
    f"seq_{c}_{s}"
    for c in SEQ_COLS
    for s in range(1, SEQ_LEN + 1)
]

print(f"Static features : {len(STATIC_COLS)}")
print(f"Sequence columns: {len(SEQ_FEATURE_COLS)}  shape ({SEQ_LEN}, {len(SEQ_COLS)})")

# ── Observation date / time windows ───────────────────────────────────────────
import datetime as _dt
import calendar

dateControl = OBSERVATION_DATE = _dt.date.today()
month = dateControl.month
lastDateOfMonth = _dt.date(dateControl.year, dateControl.month, calendar.monthrange(dateControl.year, dateControl.month)[1])

# created_at  = lastDateOfMonth
created_at  = OBSERVATION_DATE

today_yr        = OBSERVATION_DATE.year
obs_date        = F.lit(OBSERVATION_DATE)
this_year_start = _dt.date(today_yr, 1, 1)
last_year_start = _dt.date(today_yr - 1, 1, 1)
last_year_end   = _dt.date(today_yr - 1, 12, 31)

dataControl_year = dateControl.year
dataControl_month = dateControl.month

lastDatePeriodMonth = _dt.date(dateControl.year - 1, dateControl.month, calendar.monthrange(dateControl.year - 1, dateControl.month)[1])

cutOffTime = _dt.date(today_yr - 3, 1, 1)
months_since_cutoff = F.months_between(F.lit(OBSERVATION_DATE), F.lit(cutOffTime)) + F.lit(1)

print(cutOffTime, created_at, months_since_cutoff)
print(lastDateOfMonth, lastDatePeriodMonth)


# ══════════════════════════════════════════════════════════════════════════
# 1. Load Source Tables
# ══════════════════════════════════════════════════════════════════════════

# Use today as observation date (matches ml_chunk_risk.py)
df_invoice_header = (
    spark.table(f"{CATALOG}.{SCHEMA}.fact_sales_invoice_header")
    .where(
        (col("invoice_date") >= cutOffTime) &
        (F.col("document_type_code").isin("F", "G", "M", "N")) &
        (~F.col("document_category_code").isin("U", "G"))
    )
)
df_customer = spark.table(f"{CATALOG}.{SCHEMA}.dim_customer")

df_life_cycle = spark.sql(f"""
                              Select 
                                    CAST(sk_customer_id AS STRING) AS sk_customer_id
                                    ,Sum(cycle_day)/
                                    months_between(trunc('{dateControl}', 'MONTH'), '{cutOffTime}') As cycle_day
                                    From {CATALOG}.dw_mention_serving.vw_customer_life_cycle
                                    Group By 
                                    CAST(sk_customer_id AS STRING) 
                            """)

print("Invoice header rows:", df_invoice_header.count())
print("Customer rows:      ", df_customer.count())


# ══════════════════════════════════════════════════════════════════════════
# 2. Feature Engineering
# Exact same pipeline as `churn_risk_model_new` Cell 21, adapted for
# inference (no label join).
# ══════════════════════════════════════════════════════════════════════════

# Cast invoice_date to unix timestamp for range windows
df_ih = df_invoice_header.withColumn(
    "invoice_ts", F.unix_timestamp("invoice_date")
)

obs = F.lit(OBSERVATION_DATE)

customer_features = (
    df_ih
    .groupBy("sk_customer_id")
    .agg(
        # RFM
        F.datediff(obs, F.max("invoice_date")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.sum("revenue").alias("monetary_total"),
        F.avg("revenue").alias("monetary_avg"),
        F.stddev("revenue").alias("monetary_std"),
        # 03/08
        F.countDistinct(
            F.when(F.col("invoice_date") >= F.date_sub(F.lit(OBSERVATION_DATE), 90), F.col("nk_document_no"))
        ).alias("invoices_last_3m"),
        F.countDistinct(
            F.when(F.col("invoice_date") >= F.date_sub(F.lit(OBSERVATION_DATE), 180), F.col("nk_document_no"))
        ).alias("invoices_last_6m"),
        F.countDistinct(
            F.when(F.col("invoice_date") >= this_year_start, F.col("nk_document_no"))
        ).alias("invoices_this_year"),

        F.countDistinct(
        F.when(
            (F.col("invoice_date") >= F.lit(last_year_start)) &
            (F.col("invoice_date") <= F.lit(last_year_end)),
            F.col("nk_document_no")
        )
         ).alias("invoices_last_year"),

        F.countDistinct(
            F.when((F.col("invoice_date") <= lastDatePeriodMonth)&(F.col("invoice_date") >= F.lit(last_year_start)), F.col("nk_document_no"))
        ).alias("invoices_period_last_year"),

        # Order behaviour
        F.avg("header_net_total").alias("avg_order_value"),
        F.max("header_net_total").alias("max_order_value"),
        F.min("header_net_total").alias("min_order_value"),
        F.avg("days_to_pay").alias("avg_days_to_pay"),
        F.stddev("days_to_pay").alias("std_days_to_pay"),

        # Returns & credit
        F.sum(F.when(F.col("is_credit_note"), 1).otherwise(0)).alias("num_credit_notes"),
        F.sum(F.when(F.col("is_open"), 1).otherwise(0)).alias("num_open_invoices"),
        F.avg("amount_paid").alias("avg_amount_paid"),

        # Channel diversity
        F.countDistinct("business_channel").alias("num_channels"),
        F.countDistinct("document_type_code").alias("num_doc_types"),

        # Profitability
        F.sum("cost_amount").alias("total_cost"),
        F.sum(F.col("revenue") - F.col("cost_amount")).alias("total_profit"),

        # Lifetime
        F.datediff(
            F.max("invoice_date"), F.min("invoice_date")
        ).alias("customer_lifetime_days"),
        F.max("invoice_date").alias("last_invoice_date"),
    )
    .withColumn("profit_margin",
        F.col("total_profit") / (F.col("monetary_total") + F.lit(1e-6)))
    .withColumn("return_rate",
        F.col("num_credit_notes") / (F.col("frequency") + F.lit(1e-6)))
    .withColumn("invoice_frequency_per_month",
        F.col("frequency") / (F.col("customer_lifetime_days") / 30.0 + F.lit(1e-6)))
    .withColumn("monetary_std",    F.coalesce(F.col("monetary_std"),    F.lit(0.0)))
    .withColumn("std_days_to_pay", F.coalesce(F.col("std_days_to_pay"), F.lit(0.0)))
    .withColumn(
        "total_invoice_per_month",
        F.col("frequency") / months_since_cutoff
    ).withColumn("freq_yoy_change_pct",
        F.when(
            F.col("invoices_last_year").isNull() | (F.col("invoices_last_year") == 0),
            None
        ).otherwise(F.round(
            (F.col("invoices_this_year")  -
             F.col("invoices_last_year")) /
            (F.col("invoices_last_year") + F.lit(1e-6)) * 100, 1)))

    )
customer_features = customer_features.join(df_life_cycle, on="sk_customer_id", how="left")
print("Customers with invoices:", customer_features.count())

# ── Sequence features (last 12 invoices per customer) ────────────────────────
w_rank = Window.partitionBy("sk_customer_id").orderBy(F.desc("invoice_date"))

invoice_seq = (
    df_invoice_header
    .withColumn("rn", F.row_number().over(w_rank))
    .filter(F.col("rn") <= SEQ_LEN)
    .select(
        "sk_customer_id", "rn",
        "revenue", "cost_amount", "days_to_pay",
        "header_net_total", "is_credit_note", "is_open"
    )
    .withColumn("is_credit_note", F.col("is_credit_note").cast("int"))
    .withColumn("is_open",        F.col("is_open").cast("int"))
)

# Use withColumns (dict) to avoid deeply nested execution plan
seq_wide = invoice_seq.withColumns({
    f"seq_{col_name}_{step}": F.when(F.col("rn") == step, F.col(col_name)).otherwise(None)
    for col_name in SEQ_COLS
    for step in range(1, SEQ_LEN + 1)
})

seq_agg_exprs = {
    f"seq_{c}_{s}": F.max(f"seq_{c}_{s}")
    for c in SEQ_COLS for s in range(1, SEQ_LEN + 1)
}

seq_features = (
    seq_wide
    .groupBy("sk_customer_id")
    .agg(*[expr.alias(name) for name, expr in seq_agg_exprs.items()])
    .fillna(0)
)

print("Sequence feature rows:", seq_features.count())

# ── Join all features ─────────────────────────────────────────────────────────
customer_dim_features = (
    df_customer
    .filter(F.col("__END_AT").isNull())   # current SCD-2 record only
    .select(
        "sk_customer_id",
        "customer_type",
        "customer_segment",
        "country",
        "region",
        "credit_limit",
        "discount_pct",
        # "is_active",
        # "has_delivery_block",
        "is_direct_shipping",
    )
)

df_features = (
    customer_features
    .join(customer_dim_features, "sk_customer_id", "left")
    .join(seq_features,          "sk_customer_id", "left")
    .fillna(0)
)

print(f"Customers to score: {df_features.count():,}")
print(f"Feature columns   : {len(df_features.columns)}")


# ══════════════════════════════════════════════════════════════════════════
# 3. Preprocess & Load Scalers from MLflow
# Load the `scaler_static.pkl` and `scaler_seq.pkl` artifacts saved during
# training.
# ══════════════════════════════════════════════════════════════════════════

# Read schema once before any loop to avoid repeated Analyze RPCs (SCPAP001)
_cast_map = {
    field.name: F.col(field.name).cast("double")
    for field in df_features.schema.fields
    if isinstance(field.dataType, (DecimalType, LongType, IntegerType))
    and field.name != "sk_customer_id"
}
if _cast_map:
    df_features = df_features.withColumns(_cast_map)

df_pd = df_features.toPandas()

# Encode categoricals
for c in CAT_COLS:
    df_pd[c] = df_pd[c].fillna("Unknown")
    df_pd[c] = pd.Categorical(df_pd[c]).codes.astype(np.float32)

# Ensure all feature columns are numeric float32
for c in STATIC_COLS + SEQ_FEATURE_COLS:
    df_pd[c] = pd.to_numeric(df_pd[c], errors="coerce").fillna(0).astype(np.float32)

print(f"DataFrame shape: {df_pd.shape}")
print("NaN check (STATIC):", df_pd[STATIC_COLS].isna().sum().sum())
print("NaN check (SEQ)   :", df_pd[SEQ_FEATURE_COLS].isna().sum().sum())

# ── Load scalers from MLflow artifacts ────────────────────────────────────────
import pickle, tempfile
import mlflow
mlflow.set_registry_uri("databricks-uc")

client   = mlflow.MlflowClient()
versions = client.search_model_versions(filter_string=f"name='{REGISTERED_MODEL}'")
if not versions:
    raise RuntimeError(
        f"No versions found for '{REGISTERED_MODEL}'. "
        "Please run churn_risk_model_new and log the model to MLflow first."
    )
latest        = max(versions, key=lambda v: int(v.version))
RUN_ID        = latest.run_id
MODEL_VERSION = latest.version
print(f"Using model version {MODEL_VERSION}  (run_id: {RUN_ID})")

with tempfile.TemporaryDirectory() as tmp:
    mlflow.artifacts.download_artifacts(
        run_id=RUN_ID, artifact_path="scalers", dst_path=tmp)
    with open(os.path.join(tmp, "scalers", "scaler_static.pkl"), "rb") as f:
        scaler_static = pickle.load(f)
    with open(os.path.join(tmp, "scalers", "scaler_seq.pkl"), "rb") as f:
        scaler_seq = pickle.load(f)

print("Scalers loaded successfully.")

# Apply scalers
X_static  = df_pd[STATIC_COLS].values.astype(np.float32)
X_seq_raw = df_pd[SEQ_FEATURE_COLS].values.astype(np.float32)
X_seq     = X_seq_raw.reshape(-1, SEQ_LEN, len(SEQ_COLS))

X_static_scaled = scaler_static.transform(X_static).astype(np.float32)
X_seq_scaled    = scaler_seq.transform(
    X_seq.reshape(-1, len(SEQ_COLS))
).reshape(-1, SEQ_LEN, len(SEQ_COLS)).astype(np.float32)

print(f"Scaled static shape : {X_static_scaled.shape}")
print(f"Scaled sequence shape: {X_seq_scaled.shape}")


# ══════════════════════════════════════════════════════════════════════════
# 4. Load Model & Run Predictions
# ══════════════════════════════════════════════════════════════════════════

import mlflow.tensorflow

model     = mlflow.tensorflow.load_model(f"models:/{REGISTERED_MODEL}/{MODEL_VERSION}")
all_probs = model.predict(
    [X_seq_scaled, X_static_scaled], batch_size=256
).flatten()

df_pd["churn_probability"] = all_probs.astype(float)
df_pd["churn_risk_label"]  = pd.cut(
    all_probs,
    bins=[0, 0.3, 0.6, 1.0],
    labels=["Low", "Medium", "High"]
)

print("Score distribution:")
print(df_pd["churn_risk_label"].value_counts().sort_index())


# ══════════════════════════════════════════════════════════════════════════
# 5. Save Base Churn Scores
# ══════════════════════════════════════════════════════════════════════════

result_pd = df_pd[["sk_customer_id", "churn_probability", "churn_risk_label"]].copy()
result_pd["churn_probability"] = result_pd["churn_probability"].astype(float)
result_pd["churn_risk_label"]  = result_pd["churn_risk_label"].astype(str)
result_pd["created_at"]        = created_at
result_pd["model_version"]     = str(MODEL_VERSION)

result_spark = spark.createDataFrame(result_pd)

# result_spark.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable(OUTPUT_TABLE)

print(f"Written {result_spark.count():,} rows to {OUTPUT_TABLE}")
result_spark.groupBy("churn_risk_label").count().orderBy("churn_risk_label").show()


# ══════════════════════════════════════════════════════════════════════════
# 6. Build Enriched Demo Table
# Join customer KPIs, compute churn reason flags, and write to
# `ml_churn_risk_info_details`.
# ══════════════════════════════════════════════════════════════════════════

_days_elapsed        = max((OBSERVATION_DATE - this_year_start).days, 1)
ANNUALIZATION_FACTOR = F.lit(365.0 / _days_elapsed)

# Customer dimension (latest SCD-2 record)
customer_info = (
    spark.table(f"{CATALOG}.{SCHEMA}.dim_customer")
    .filter(F.col("__END_AT").isNull())
    .select(
        "sk_customer_id","nk_customer_id", "customer_name", "customer_type",
        "customer_segment", "country", "region",
        "credit_limit"
        # , "is_active", "has_delivery_block",
    )
)

# KPIs from invoice header
ih = (
    spark.table(f"{CATALOG}.{SCHEMA}.fact_sales_invoice_header")
    .withColumn("is_F",  F.col("document_type_code") == "F")
    .withColumn("is_G",  F.col("document_type_code") == "G")
    .withColumn("in_3m", F.col("invoice_date") >= F.date_sub(obs_date, 90))
    .withColumn("in_6m", F.col("invoice_date") >= F.date_sub(obs_date, 180))
    .withColumn("in_ty", F.col("invoice_date") >= F.lit(this_year_start))
    .withColumn("in_ly",
        (F.col("invoice_date") >= F.lit(last_year_start)) &
        (F.col("invoice_date") <= F.lit(last_year_end)))
    .withColumn("in_lpy",(F.col("invoice_date") <= lastDatePeriodMonth)&(F.col("invoice_date") >= F.lit(last_year_start))
)
)

customer_kpis = (
    ih.groupBy("sk_customer_id").agg(
        F.datediff(obs_date, F.max("invoice_date")).alias("days_since_last_invoice"),
        F.max(
            F.when(
                (F.col("document_type_code").isin("F", "G", "M", "N")) &
                (~F.col("document_category_code").isin("U", "G")),
                F.col("invoice_date")
            )
        ).alias("last_invoice_date"),
        F.countDistinct("nk_document_no").alias("total_invoices"),
        # Distinct invoice counts in last 3m and 6m
        F.countDistinct(F.when(F.col("in_3m"), F.col("nk_document_no"))).alias("invoices_last_3m"),
        F.countDistinct(F.when(F.col("in_6m"), F.col("nk_document_no"))).alias("invoices_last_6m"),
        F.countDistinct(F.when(F.col("in_ty"), F.col("nk_document_no"))).alias("invoices_this_year"),
        F.countDistinct(F.when(F.col("in_ly"), F.col("nk_document_no"))).alias("invoices_last_year"),
        F.countDistinct(F.when(F.col("in_lpy"), F.col("nk_document_no"))).alias("invoices_period_last_year"),
        F.round(F.sum(F.when(F.col("is_F"), F.col("revenue")).otherwise(0)), 0)
            .alias("total_revenue_eur"),
        F.round(F.sum(F.when(F.col("is_F") & F.col("in_lpy"), F.col("revenue")).otherwise(0)), 0)
            .alias("revenue_period_year_eur"),

        F.round(F.sum(F.when(F.col("is_F") & F.col("in_ty"), F.col("revenue")).otherwise(0)), 0)
            .alias("revenue_this_year_eur"),
        F.round(F.sum(F.when(F.col("is_F") & F.col("in_ly"), F.col("revenue")).otherwise(0)), 0)
            .alias("revenue_last_year_eur"),

        F.round(F.avg(F.when(F.col("is_F"), F.col("revenue"))), 0)
            .alias("avg_invoice_value_eur"),
        F.round(F.sum(F.when(F.col("is_F"),
            F.col("revenue") - F.col("cost_amount")).otherwise(0)), 0)
            .alias("total_gross_profit_eur"),
        F.countDistinct(F.when(F.col("is_G")& F.col("in_ty"), F.col("nk_document_no"))).alias("total_returns"),
        F.when(
            F.countDistinct(F.when(F.col("in_ty"), F.col("nk_document_no"))) != 0,
            (
                F.countDistinct(F.when(F.col("is_G") & F.col("in_ty"), F.col("nk_document_no"))) /
                F.countDistinct(F.when(F.col("in_ty"), F.col("nk_document_no")))
            ) * 100
        ).otherwise(None).alias("return_rate_pct"),
        F.round(F.avg("days_to_pay"), 1).alias("avg_days_to_pay"),
        F.sum(F.when(F.col("is_open"), 1).otherwise(0)).alias("open_invoices"),

        F.sum(F.when(F.col("document_category_code") == "K", 1).otherwise(0))
            .alias("goodwill_gestures"),

    )
    .withColumn("revenue_yoy_change_pct",
        F.when(
            F.col("revenue_period_year_eur").isNull() | (F.col("revenue_period_year_eur") == 0),
            None
        ).otherwise(F.round(
            (F.col("revenue_this_year_eur")  -
             F.col("revenue_period_year_eur")) /
            (F.abs(F.col("revenue_period_year_eur")) + F.lit(1e-6)) * 100, 1)))

    .withColumn("freq_yoy_change_pct",
        F.when(
            F.col("invoices_period_last_year").isNull() | (F.col("invoices_period_last_year") == 0),
            None
        ).otherwise(F.round(
            (F.col("invoices_this_year")  -
             F.col("invoices_period_last_year")) /
            (F.col("invoices_period_last_year") + F.lit(1e-6)) * 100, 1)))
)

print("KPI rows:", customer_kpis.count())

# ── Add churn reason flags + write demo table ─────────────────────────────────
FLAG_COLS = [
    "flag_no_recent_order", "flag_revenue_decline", "flag_frequency_decline",
    "flag_high_return_rate", "flag_payment_delay", "flag_open_invoices",
    "flag_low_activity_3m"
    # , "flag_warranty_issues", 
    # "flag_delivery_block",
]

demo_enriched = (
    result_spark
    .join(customer_info,  "sk_customer_id", "left")
    .join(customer_kpis,  "sk_customer_id", "left")
    # ── Churn reason flags ──────────────────────────────────────────────────
    .withColumn("flag_no_recent_order",
        F.when(F.col("days_since_last_invoice") > 90, F.lit("No order in 90+ days"))
         .when(F.col("days_since_last_invoice") > 60, F.lit("No order in 60+ days"))
         .otherwise(None))

    .withColumn("flag_revenue_decline",
        F.when(F.col("revenue_yoy_change_pct") < -30, F.lit("Revenue dropped >30% YoY"))
         .when(F.col("revenue_yoy_change_pct") < -10, F.lit("Revenue dropped >10% YoY"))
         .otherwise(None))

    .withColumn("flag_frequency_decline",
        F.when(F.col("freq_yoy_change_pct") < -30, F.lit("Order freq dropped >30% YoY"))
         .when(F.col("freq_yoy_change_pct") < -10, F.lit("Order freq dropped >10% YoY"))
         .otherwise(None))

    .withColumn("flag_high_return_rate",
        F.when(F.col("return_rate_pct") > 20, F.lit("Return rate >20%"))
         .when(F.col("return_rate_pct") > 10, F.lit("Return rate >10%"))
         .otherwise(None))

    .withColumn("flag_payment_delay",
        F.when(F.col("avg_days_to_pay") > 60, F.lit("Avg payment delay >60 days"))
         .when(F.col("avg_days_to_pay") > 30, F.lit("Avg payment delay >30 days"))
         .otherwise(None))

    .withColumn("flag_open_invoices",
        F.when(F.col("open_invoices") > 5, F.lit("5+ unpaid invoices"))
         .when(F.col("open_invoices") > 2, F.lit("2+ unpaid invoices"))
         .otherwise(None))

    .withColumn("flag_low_activity_3m",
        F.when(F.col("invoices_last_3m") == 0, F.lit("Zero orders last 3 months"))
         .when(F.col("invoices_last_3m") <= 1, F.lit("Only 1 order last 3 months"))
         .otherwise(None))

    # .withColumn("flag_delivery_block",
    #     F.when(F.col("has_delivery_block") == True, F.lit("Has delivery block"))
    #      .otherwise(None))

    # ── Combine flags into readable string ─────────────────────────────────────
    .withColumn("churn_reasons",
        F.concat_ws(" | ", *[F.col(c) for c in FLAG_COLS]))
    .withColumn("churn_reasons",
        F.when(F.col("churn_reasons") == "", F.lit("Model pattern \u2014 no explicit flag"))
         .otherwise(F.col("churn_reasons")))
    # Count real (non-null) flags — F.filter keeps only non-null elements
    .withColumn("risk_signal_count",
        F.size(F.filter(F.array(*[F.col(c) for c in FLAG_COLS]), lambda x: x.isNotNull())))

    # ── Final output columns ──────────────────────────────────────────────────
    .select(
        F.lit(str(dateControl.year)).alias("years"),
        F.lit(str(dateControl.month)).alias("months"),
        F.col("sk_customer_id"),
        F.col("customer_name").alias("customer"),
        F.col("customer_segment"),
        F.col("country"),
        # Hybrid score: max(model_score, signal_floor), capped at 100
        # signal_floor = risk_signal_count * 20, max 90
        # recently-active (last order <= 30d) capped at 20

        F.when(
            F.datediff(obs_date, F.col("last_invoice_date")) <= 30,
            F.least(
                F.round(F.rand() * 15 + 10).cast("int"),
                F.greatest(
                    F.round(F.col("churn_probability") * 100, 0).cast("int"),
                    F.least(F.lit(90), (F.col("risk_signal_count") * F.lit(20)).cast("int"))
                )
            )
        )
        .when(((F.datediff(obs_date, F.col("last_invoice_date")) > 60) & (F.col("revenue_this_year_eur") < 5000))|(F.datediff(obs_date, F.col("last_invoice_date")) > 90), F.lit(95))

        .otherwise(
            F.least(
                F.lit(100),
                F.greatest(
                    F.round(F.col("churn_probability") * 100, 0).cast("int"),
                    F.least(F.lit(90), (F.col("risk_signal_count") * F.lit(20)).cast("int"))
                )
            )
        ).cast("int").alias("score"),
        F.col("churn_risk_label"),
        F.col("churn_reasons"),
        F.col("risk_signal_count"),
        F.col("revenue_yoy_change_pct").alias("trend_pct"),
        F.col("last_invoice_date"),
        F.col("days_since_last_invoice"),
        F.col("total_invoices"),
        F.col("invoices_last_3m"),
        F.col("invoices_last_6m"),
        F.col("invoices_this_year"),
        F.col("invoices_last_year"),
        F.col("freq_yoy_change_pct"),
        F.col("total_revenue_eur").cast("decimal(37,0)"),
        F.col("revenue_this_year_eur").cast("decimal(37,0)"),
        F.col("revenue_last_year_eur").cast("decimal(37,0)"),
        F.col("avg_invoice_value_eur").cast("decimal(33,0)"),
        F.col("total_gross_profit_eur").cast("decimal(37,0)"),
        F.col("return_rate_pct"),
        F.col("total_returns"),
        F.col("avg_days_to_pay"),
        F.col("open_invoices"),

        F.col("goodwill_gestures"),
        F.col("credit_limit"),
        # F.col("is_active"),
        # F.col("has_delivery_block"),
        F.col("created_at"),
        F.col("model_version"),
    )
    .orderBy(F.col("score").desc())
)


stmt = f"""Delete From {OUTPUT_TABLE} Where years = '{dataControl_year}' And months = '{dataControl_month}'"""
print(stmt)
spark.sql(stmt)

demo_enriched.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)
print(f"Written {demo_enriched.count():,} rows to {OUTPUT_TABLE}")
# demo_enriched.select("customer", "score", "churn_risk_label", "churn_reasons") \
#     .show(10, truncate=60)

demo_enriched.select("total_revenue_eur").distinct().show()
