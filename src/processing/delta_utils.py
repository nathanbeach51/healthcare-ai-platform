from pathlib import Path
from typing import Optional
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_last_ingested_at(
    spark: SparkSession,
    delta_path: Path,
) -> Optional[datetime]:
    if not delta_path.exists():
        return None

    dataframe = (
        spark.read
        .format("delta")
        .load(str(delta_path))
    )

    return (
        dataframe
        .agg(
            F.max("ingested_at").alias(
                "last_ingested_at"
            )
        )
        .first()["last_ingested_at"]
    )

def merge_delta(
    spark: SparkSession,
    source: DataFrame,
    target_path: Path,
    merge_condition: str,
) -> None:
    if not target_path.exists():
        (
            source.write
            .format("delta")
            .mode("overwrite")
            .save(str(target_path))
        )

        return

    target = DeltaTable.forPath(
        spark,
        str(target_path),
    )

    (
        target.alias("target")
        .merge(
            source.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )