from pathlib import Path
import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "silver"
    / "document_reference"
)

CHUNK_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_chunks"
)


def split_note_into_sections(
    note_text: str,
) -> list[dict]:

    if not note_text:
        return []

    section_pattern = re.compile(
        r"^(#{1,2})\s+(.+)$",
        re.MULTILINE,
    )

    matches = list(
        section_pattern.finditer(note_text)
    )

    sections = []

    for index, match in enumerate(matches):
        section_name = (
            match.group(2).strip()
        )

        content_start = match.end()

        if index + 1 < len(matches):
            content_end = (
                matches[index + 1].start()
            )
        else:
            content_end = len(note_text)

        section_text = (
            note_text[
                content_start:content_end
            ]
            .strip()
        )

        if not section_text:
            continue

        sections.append(
            {
                "section_name":
                    section_name,
                "text":
                    section_text,
            }
        )

    return sections


SECTION_SCHEMA = T.ArrayType(
    T.StructType(
        [
            T.StructField(
                "section_name",
                T.StringType(),
            ),
            T.StructField(
                "text",
                T.StringType(),
            ),
        ]
    )
)


split_sections_udf = F.udf(
    split_note_into_sections,
    SECTION_SCHEMA,
)


def get_last_processed_at(
    spark: SparkSession,
):
    if not DeltaTable.isDeltaTable(
        spark,
        str(CHUNK_PATH),
    ):
        return None

    chunks = (
        spark.read
        .format("delta")
        .load(str(CHUNK_PATH))
    )

    if (
        "source_silver_processed_at"
        not in chunks.columns
    ):
        print(
            "Existing chunk table does not "
            "contain incremental metadata. "
            "Running full migration pass."
        )
        return None

    return (
        chunks
        .agg(
            F.max(
                "source_silver_processed_at"
            ).alias("max_processed_at")
        )
        .first()
        ["max_processed_at"]
    )


def read_new_documents(
    spark: SparkSession,
    last_processed_at=None,
) -> DataFrame:

    documents = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH))
    )

    if last_processed_at is not None:
        documents = documents.filter(
            F.col("silver_processed_at")
            > last_processed_at
        )

    return documents


def create_chunks(
    documents: DataFrame,
) -> DataFrame:

    chunked = (
        documents
        .withColumn(
            "sections",
            split_sections_udf(
                F.col("clinical_note_text")
            ),
        )
        .select(
            "document_reference_id",
            "patient_id",
            "encounter_id",
            "document_date",
            "silver_processed_at",
            F.posexplode(
                "sections"
            ).alias(
                "chunk_index",
                "section",
            ),
        )
        .select(
            F.concat_ws(
                "-",
                F.col(
                    "document_reference_id"
                ),
                F.col(
                    "chunk_index"
                ),
            ).alias("chunk_id"),

            "document_reference_id",
            "patient_id",
            "encounter_id",
            "document_date",

            F.col(
                "section.section_name"
            ).alias(
                "section_name"
            ),

            "chunk_index",

            F.col(
                "section.text"
            ).alias(
                "chunk_text"
            ),

            F.col(
                "silver_processed_at"
            ).alias(
                "source_silver_processed_at"
            ),

            F.current_timestamp().alias(
                "chunk_processed_at"
            ),
        )
    )

    return chunked


def write_chunks(
    spark: SparkSession,
    chunks: DataFrame,
) -> None:

    if not DeltaTable.isDeltaTable(
        spark,
        str(CHUNK_PATH),
    ):
        (
            chunks.write
            .format("delta")
            .mode("overwrite")
            .save(str(CHUNK_PATH))
        )

        return

    target = DeltaTable.forPath(
        spark,
        str(CHUNK_PATH),
    )

    (
        target.alias("target")
        .merge(
            chunks.alias("source"),
            (
                "target.chunk_id = "
                "source.chunk_id"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:

    spark = create_spark_session(
        "chunk-clinical-notes"
    )

    try:
        last_processed_at = (
            get_last_processed_at(
                spark
            )
        )

        print(
            "Last processed Silver timestamp:",
            last_processed_at,
        )

        documents = read_new_documents(
            spark,
            last_processed_at,
        )

        document_count = documents.count()

        print(
            f"New/changed documents: "
            f"{document_count}"
        )

        if document_count == 0:
            print(
                "No new clinical notes "
                "to chunk."
            )
            return

        chunks = create_chunks(
            documents
        )

        chunk_count = chunks.count()

        print(
            f"Chunks generated: "
            f"{chunk_count}"
        )

        write_chunks(
            spark,
            chunks,
        )

        print(
            "Clinical note chunking "
            "complete."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()