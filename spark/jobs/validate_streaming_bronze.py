"""Validation de la table Delta Bronze alimentée par Kafka."""

import os
import sys

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ValidateStreamingBronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    path = os.getenv(
        "STREAMING_BRONZE_PATH",
        "/opt/spark-data/bronze_streaming/sensor_readings",
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        dataframe = spark.read.format("delta").load(path)
        row_count = dataframe.count()

        print("=" * 60)
        print("VALIDATION BRONZE STREAMING")
        print(f"Chemin : {path}")
        print(f"Nombre d'événements : {row_count}")
        print("=" * 60)

        dataframe.select(
            "event_id",
            "sensor_id",
            "machine_id",
            "sensor_type",
            "value",
            "unit",
            "event_timestamp",
            "kafka_partition",
            "kafka_offset",
        ).orderBy(
            "kafka_timestamp",
            ascending=False,
        ).show(10, truncate=False)

        if row_count == 0:
            raise ValueError(
                "La table Bronze Streaming existe, mais elle est vide."
            )

    except Exception as error:
        print(f"Échec de la validation : {error}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    