from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType, StringType, BooleanType,
)


@dp.materialized_view(
    name="dim_date",
    comment=(
        "Calendar dimension. Grain: one row per calendar day (2020-01-01 to 2030-12-31). "
        "Includes German national holidays, ISO week numbering, and fiscal calendar."
    )
)
def dim_date():
    """
    Generated calendar dimension — no OLTP source.
    Covers 2020-01-01 to 2030-12-31 plus an unknown member (date_key = -1).
    Fiscal calendar is currently equal to the calendar year.
    """

    # Date spine
    date_spine = spark.sql("""
        SELECT explode(sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS dt
    """)

    # German national holidays (add more if needed)
    holiday_dates = [
        "2020-01-01",
        "2020-12-25",
        "2020-12-26",
    ]

    holidays_df = (
        spark.createDataFrame([(d,) for d in holiday_dates], ["holiday_date"])
        .withColumn("holiday_date", F.to_date("holiday_date"))
    )

    # Spark dayofweek: 1=Sun, 2=Mon, ..., 7=Sat
    # ISO day of week: 1=Mon, ..., 7=Sun
    iso_dow = ((F.dayofweek("dt") + 5) % 7) + 1

    dim = (
        date_spine
        .join(holidays_df, date_spine["dt"] == holidays_df["holiday_date"], "left")
        .select(
            (F.year("dt") * 10000 + F.month("dt") * 100 + F.dayofmonth("dt"))
                .cast("int").alias("date_key"),
            F.col("dt").alias("full_date"),

            iso_dow.cast("int").alias("day_of_week"),
            F.date_format("dt", "EEEE").alias("day_name"),
            F.dayofmonth("dt").alias("day_of_month"),
            F.dayofyear("dt").alias("day_of_year"),

            F.weekofyear("dt").alias("week_of_year"),
            F.weekofyear("dt").alias("week_iso"),

            F.month("dt").alias("month_number"),
            F.date_format("dt", "MMMM").alias("month_name"),

            F.quarter("dt").alias("quarter"),
            F.year("dt").alias("year"),
            F.date_format("dt", "yyyy-MM").alias("year_month"),
            F.concat(F.year("dt").cast("string"), F.lit("-Q"), F.quarter("dt").cast("string")).alias("year_quarter"),

            F.when(iso_dow >= 6, F.lit(True)).otherwise(F.lit(False)).alias("is_weekend"),
            F.when(F.col("holiday_date").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_holiday_de"),

            F.year("dt").alias("fiscal_year"),
            F.quarter("dt").alias("fiscal_quarter"),
            F.month("dt").alias("fiscal_month"),
        )
    )

    unknown_schema = StructType([
        StructField("date_key",       IntegerType(), nullable=False),
        StructField("full_date",      DateType(),    nullable=True),
        StructField("day_of_week",    IntegerType(), nullable=False),
        StructField("day_name",       StringType(),  nullable=False),
        StructField("day_of_month",   IntegerType(), nullable=False),
        StructField("day_of_year",    IntegerType(), nullable=False),
        StructField("week_of_year",   IntegerType(), nullable=False),
        StructField("week_iso",       IntegerType(), nullable=False),
        StructField("month_number",   IntegerType(), nullable=False),
        StructField("month_name",     StringType(),  nullable=False),
        StructField("quarter",        IntegerType(), nullable=False),
        StructField("year",           IntegerType(), nullable=False),
        StructField("year_month",     StringType(),  nullable=False),
        StructField("year_quarter",   StringType(),  nullable=False),
        StructField("is_weekend",     BooleanType(), nullable=False),
        StructField("is_holiday_de",  BooleanType(), nullable=False),
        StructField("fiscal_year",    IntegerType(), nullable=False),
        StructField("fiscal_quarter", IntegerType(), nullable=False),
        StructField("fiscal_month",   IntegerType(), nullable=False),
    ])

    unknown = spark.createDataFrame([
        Row(
            date_key=-1,
            full_date=None,
            day_of_week=0,
            day_name="Unknown",
            day_of_month=0,
            day_of_year=0,
            week_of_year=0,
            week_iso=0,
            month_number=0,
            month_name="Unknown",
            quarter=0,
            year=0,
            year_month="0000-00",
            year_quarter="0000-Q0",
            is_weekend=False,
            is_holiday_de=False,
            fiscal_year=0,
            fiscal_quarter=0,
            fiscal_month=0,
        )
    ], schema=unknown_schema)

    return dim.unionByName(unknown)
