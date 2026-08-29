from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_empty(
    df: DataFrame,
    table_name: str,
) -> None:

    if df.limit(1).count() == 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"table is empty"
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
            f"{column_name} contains "
            f"{null_count} null values"
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

def assert_column_lte(
    df: DataFrame,
    left_column: str,
    right_column: str,
    table_name: str,
) -> None:

    invalid_count = (
        df
        .filter(
            F.col(left_column) > F.col(right_column)
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"{left_column} is greater than "
            f"{right_column} in "
            f"{invalid_count} rows"
        )

def assert_unique_combination(
    df: DataFrame,
    columns: list[str],
    table_name: str,
) -> None:

    duplicate_count = (
        df
        .groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"duplicate combinations found for "
            f"{', '.join(columns)}: "
            f"{duplicate_count}"
        )

def assert_value_range(
    df: DataFrame,
    column_name: str,
    minimum: float | None,
    maximum: float | None,
    table_name: str,
) -> None:

    condition = F.lit(False)

    if minimum is not None:
        condition = condition | (
            F.col(column_name) < minimum
        )

    if maximum is not None:
        condition = condition | (
            F.col(column_name) > maximum
        )

    invalid_count = (
        df
        .filter(
            F.col(column_name).isNotNull()
            & condition
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"{column_name} contains "
            f"{invalid_count} values outside "
            f"the expected range "
            f"{minimum} to {maximum}"
        )