from pyspark.sql import functions as F


def make_row_hash(cols):
    return F.md5(
        F.concat_ws("|", *[
            F.coalesce(F.col(c).cast("string"), F.lit(""))
            for c in cols
        ])
    )
