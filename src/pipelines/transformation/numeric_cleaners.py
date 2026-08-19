import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Column


def cast_double(expr: Column) -> Column:
    return expr.cast(DoubleType())

def cast_only(column_name:str) -> Column:
    """
    Germany, Ireland, Spain
    """
    return cast_double(F.col(column_name))

def comma_decimal(column_name):
    """
    Spain datasets uses commas as decimal separators
    """

    return cast_double(
        F.regexp_replace(
            F.col(column_name),
            ",",
            "."
        )
    )