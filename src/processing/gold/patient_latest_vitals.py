from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[3]

OBSERVATION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "observation"
)

PATIENT_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)

PATIENT_LATEST_VITALS_GOLD_PATH = (
    PROJECT_ROOT / "data" / "delta" / "gold" / "patient_latest_vitals"
)


VITAL_CODES = {
    "8302-2": "height_cm",
    "29463-7": "weight_kg",
    "39156-5": "bmi",
    "8867-4": "heart_rate",
    "9279-1": "respiratory_rate",
    "8480-6": "systolic_bp",
    "8462-4": "diastolic_bp",
}

def read_silver_observations(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(OBSERVATION_SILVER_PATH))
    )


def read_silver_patients(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(PATIENT_SILVER_PATH))
    )

def filter_vitals(
    observations: DataFrame,
) -> DataFrame:
    return observations.filter(
        F.col("observation_code").isin(
            list(VITAL_CODES.keys())
        )
    )

def get_latest_vitals(
    vitals: DataFrame,
) -> DataFrame:
    latest_window = (
        Window
        .partitionBy(
            "patient_id",
            "observation_code",
        )
        .orderBy(
            F.col("observation_at").desc_nulls_last()
        )
    )

    return (
        vitals
        .withColumn(
            "_row_number",
            F.row_number().over(latest_window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

def add_vital_name(
    vitals: DataFrame,
) -> DataFrame:
    mapping = F.create_map(
        *[
            item
            for code, name in VITAL_CODES.items()
            for item in (
                F.lit(code),
                F.lit(name),
            )
        ]
    )

    return vitals.withColumn(
        "vital_name",
        mapping[F.col("observation_code")],
    )

def pivot_vitals(
    vitals: DataFrame,
) -> DataFrame:
    return (
        vitals
        .groupBy("patient_id")
        .pivot(
            "vital_name",
            list(VITAL_CODES.values()),
        )
        .agg(
            F.first("value_numeric")
        )
    )

def build_patient_latest_vitals(
    patients: DataFrame,
    vitals: DataFrame,
) -> DataFrame:
    return (
        patients
        .join(
            vitals,
            on="patient_id",
            how="left",
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

def write_patient_latest_vitals(
    dataframe: DataFrame,
) -> None:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .save(str(PATIENT_LATEST_VITALS_GOLD_PATH))
    )

def main() -> None:
    spark = create_spark_session(
        "patient-latest-vitals-gold"
    )

    try:
        observations = read_silver_observations(spark)
        patients = read_silver_patients(spark)

        vitals = filter_vitals(observations)

        latest = get_latest_vitals(vitals)

        named = add_vital_name(latest)

        pivoted = pivot_vitals(named)

        patient_vitals = build_patient_latest_vitals(
            patients,
            pivoted,
        )

        patient_vitals.select(
            "patient_id",
            "birth_date",
            "gender",
            "height_cm",
            "weight_kg",
            "bmi",
            "heart_rate",
            "respiratory_rate",
            "systolic_bp",
            "diastolic_bp",
        ).show(truncate=False)

        write_patient_latest_vitals(
            patient_vitals
        )

        print(
            f"Gold patient vital rows: "
            f"{patient_vitals.count()}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()