from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_empty(df: DataFrame, table_name: str) -> None:
    if df.limit(1).count() == 0:
        raise ValueError(
            f"{table_name} failed validation: table is empty"
        )


def assert_not_null(
    df: DataFrame,
    column_name: str,
    table_name: str,
) -> None:

    null_count = (
        df
        .filter(F.col(column_name).isNull())
        .count()
    )

    if null_count > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"{column_name} contains {null_count} null values"
        )


def assert_unique(
    df: DataFrame,
    column_name: str,
    table_name: str,
) -> None:

    duplicate_count = (
        df
        .groupBy(column_name)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"{column_name} contains "
            f"{duplicate_count} duplicate values"
        )

