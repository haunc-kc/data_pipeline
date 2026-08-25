from pyspark.sql import functions as F


def make_row_hash(cols):
    """MD5 hash of given columns — used for change detection in fact tables."""
    return F.md5(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols
    ]))


__all__ = ["make_row_hash"]
