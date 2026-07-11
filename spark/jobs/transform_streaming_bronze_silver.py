"""
Pipeline Bronze Streaming vers Silver Streaming.

Le job :
- lit la table Delta Bronze comme un flux ;
- filtre les messages JSON invalides ;
- normalise les colonnes ;
- applique les règles métier des capteurs ;
- supprime les doublons avec watermark ;
- écrit les données propres dans Delta Silver.
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    trim,
    upper,
)
from pyspark.sql.streaming import StreamingQuery


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("streaming_bronze_to_silver")


def required_variable(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement '{name}' est absente."
        )

    return value


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IndustrialStreamingBronzeToSilver")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def read_bronze_stream(
    spark: SparkSession,
    bronze_path: str,
) -> DataFrame:
    return (
        spark.readStream
        .format("delta")
        .load(bronze_path)
    )


def clean_sensor_events(dataframe: DataFrame) -> DataFrame:
    cleaned = (
        dataframe
        .filter(col("event_id").isNotNull())
        .filter(col("sensor_id").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("event_timestamp").isNotNull())
        .filter(col("value").isNotNull())
        .withColumn(
            "sensor_type",
            upper(trim(col("sensor_type"))),
        )
        .withColumn(
            "unit",
            trim(col("unit")),
        )
        .filter(
            col("sensor_type").isin(
                "TEMPERATURE",
                "VIBRATION",
                "PRESSURE",
                "ENERGY",
            )
        )
    )

    valid_values = (
        (
            (col("sensor_type") == "TEMPERATURE")
            & col("value").between(-20.0, 180.0)
        )
        | (
            (col("sensor_type") == "VIBRATION")
            & col("value").between(0.0, 50.0)
        )
        | (
            (col("sensor_type") == "PRESSURE")
            & col("value").between(0.0, 40.0)
        )
        | (
            (col("sensor_type") == "ENERGY")
            & col("value").between(0.0, 2000.0)
        )
    )

    return (
        cleaned
        .filter(valid_values)
        .withWatermark(
            "event_timestamp",
            "10 minutes",
        )
        .dropDuplicates(
            ["event_id", "event_timestamp"]
        )
        .withColumn(
            "_silver_processed_at",
            current_timestamp(),
        )
        .withColumn(
    "_data_quality_status",
    lit("VALID"),
)
    )


def start_silver_stream(
    dataframe: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    return (
        dataframe.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .trigger(processingTime="5 seconds")
        .start(output_path)
    )


def main() -> None:
    logger = configure_logging()
    spark = None
    query = None

    try:
        bronze_path = required_variable(
            "STREAMING_BRONZE_PATH"
        )
        silver_path = required_variable(
            "STREAMING_SILVER_PATH"
        )
        checkpoint_path = required_variable(
            "STREAMING_SILVER_CHECKPOINT_PATH"
        )

        logger.info(
            "Démarrage Bronze Streaming vers Silver Streaming."
        )
        logger.info("Source : %s", bronze_path)
        logger.info("Destination : %s", silver_path)

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        bronze_stream = read_bronze_stream(
            spark=spark,
            bronze_path=bronze_path,
        )

        silver_stream = clean_sensor_events(
            bronze_stream
        )

        query = start_silver_stream(
            dataframe=silver_stream,
            output_path=silver_path,
            checkpoint_path=checkpoint_path,
        )

        logger.info(
            "Pipeline Silver Streaming actif."
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info(
            "Arrêt demandé par l'utilisateur."
        )

    except Exception:
        logger.exception(
            "Échec du pipeline Silver Streaming."
        )
        sys.exit(1)

    finally:
        if query is not None:
            query.stop()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
