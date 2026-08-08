from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from processing.spark_session import create_spark_session


from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[3]

OBSERVATION_BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "bronze" / "observation"
)

OBSERVATION_SILVER_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "observation"
)

def write_silver_observations(
    observations: DataFrame,
) -> None:
    (
        observations.write
        .format("delta")
        .mode("overwrite")
        .save(str(OBSERVATION_SILVER_DELTA_PATH))
    )

def read_bronze_observations(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(OBSERVATION_BRONZE_DELTA_PATH))
    )

def deduplicate_observations(
    observations: DataFrame,
) -> DataFrame:
    latest_observation = (
        Window
        .partitionBy(
            "observation_id",
            "observation_code",
        )
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("ingested_at").desc(),
        )
    )

    return (
        observations
        .withColumn(
            "_row_number",
            F.row_number().over(latest_observation),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

def add_silver_metadata(
    observations: DataFrame,
) -> DataFrame:
    return observations.withColumn(
        "silver_processed_at",
        F.current_timestamp(),
    )

def validate_observations(
    observations: DataFrame,
) -> None:
    null_patient_count = (
        observations
        .filter(F.col("patient_id").isNull())
        .count()
    )

    null_code_count = (
        observations
        .filter(F.col("observation_code").isNull())
        .count()
    )

    duplicates = (
        observations
        .groupBy(
            "observation_id",
            "observation_code",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_patient_count:
        raise ValueError(
            f"Found {null_patient_count} observations with null patient IDs."
        )

    if null_code_count:
        raise ValueError(
            f"Found {null_code_count} observations with null codes."
        )

    if duplicates:
        raise ValueError(
            f"Found {duplicates} duplicate observation keys."
        )



def transform_simple_observations(
    bronze: DataFrame,
) -> DataFrame:
    return (
        bronze
        .filter(F.col("resource.valueQuantity").isNotNull())
        .select(
            F.col("resource.id").alias("observation_id"),

            F.regexp_extract(
                F.col("resource.subject.reference"),
                r"Patient/(.+)",
                1,
            ).alias("patient_id"),

            F.regexp_extract(
                F.col("resource.encounter.reference"),
                r"Encounter/(.+)",
                1,
            ).alias("encounter_id"),

            F.col("resource.code.coding")[0]["code"]
                .alias("observation_code"),

            F.col("resource.code.coding")[0]["display"]
                .alias("observation_display"),

            F.col("resource.effectiveDateTime")
                .cast("timestamp")
                .alias("observation_at"),

            F.col("resource.valueQuantity.value")
                .alias("value_numeric"),

            F.lit(None).cast("string")
                .alias("value_text"),

            F.col("resource.valueQuantity.unit")
                .alias("unit"),

            F.col("resource.status").alias("status"),

            F.col("resource.meta.lastUpdated")
                .cast("timestamp")
                .alias("source_last_updated"),

            F.col("_source_file").alias("source_file"),
            F.col("_ingested_at").alias("ingested_at"),
        )
    )


def transform_component_observations(
    bronze: DataFrame,
) -> DataFrame:
    exploded = (
        bronze
        .filter(F.col("resource.component").isNotNull())
        .select(
            "resource",
            "_source_file",
            "_ingested_at",
            F.explode("resource.component").alias("component"),
        )
    )

    return exploded.select(
        F.col("resource.id").alias("observation_id"),

        F.regexp_extract(
            F.col("resource.subject.reference"),
            r"Patient/(.+)",
            1,
        ).alias("patient_id"),

        F.regexp_extract(
            F.col("resource.encounter.reference"),
            r"Encounter/(.+)",
            1,
        ).alias("encounter_id"),

        F.col("component.code.coding")[0]["code"]
            .alias("observation_code"),

        F.col("component.code.coding")[0]["display"]
            .alias("observation_display"),

        F.col("resource.effectiveDateTime")
            .cast("timestamp")
            .alias("observation_at"),

        F.col("component.valueQuantity.value")
            .alias("value_numeric"),

        F.lit(None).cast("string")
            .alias("value_text"),

        F.col("component.valueQuantity.unit")
            .alias("unit"),

        F.col("resource.status").alias("status"),

        F.col("resource.meta.lastUpdated")
            .cast("timestamp")
            .alias("source_last_updated"),

        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def combine_observations(
    simple: DataFrame,
    components: DataFrame,
) -> DataFrame:
    return simple.unionByName(
        components,
        allowMissingColumns=True,
    )


def main() -> None:
    spark = create_spark_session("observation-silver")

    try:
        bronze = read_bronze_observations(spark)

        simple = transform_simple_observations(bronze)

        components = transform_component_observations(
            bronze
        )

        combined = combine_observations(
            simple,
            components,
        )

        current = deduplicate_observations(
            combined
        )

        silver = add_silver_metadata(
            current
        )

        validate_observations(
            silver
        )

        print(
            f"Bronze Observation snapshots: "
            f"{bronze.count()}"
        )

        print(
            f"Silver measurements: "
            f"{silver.count()}"
        )

        silver.select(
            "observation_id",
            "patient_id",
            "encounter_id",
            "observation_code",
            "observation_display",
            "observation_at",
            "value_numeric",
            "unit",
        ).show(50, truncate=False)

        write_silver_observations(
            silver
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
