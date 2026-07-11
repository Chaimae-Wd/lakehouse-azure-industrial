"""
Pipeline Kafka vers Bronze Delta avec Spark Structured Streaming.

Le job :
1. lit les événements du topic Kafka ;
2. conserve les métadonnées Kafka ;
3. analyse le message JSON ;
4. ajoute des métadonnées d'ingestion ;
5. écrit les événements dans Delta Lake ;
6. utilise un checkpoint pour reprendre après une interruption.
"""

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


EVENT_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            False,
        ),
        StructField(
            "sensor_id",
            IntegerType(),
            True,
        ),
        StructField(
            "machine_id",
            IntegerType(),
            True,
        ),
        StructField(
            "sensor_type",
            StringType(),
            True,
        ),
        StructField(
            "value",
            DoubleType(),
            True,
        ),
        StructField(
            "unit",
            StringType(),
            True,
        ),
        StructField(
            "event_timestamp",
            TimestampType(),
            True,
        ),
    ]
)


def configure_logging() -> logging.Logger:
    """Configurer les journaux."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("kafka_to_bronze_streaming")


def get_required_environment_variable(name: str) -> str:
    """Lire une variable obligatoire."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement '{name}' est absente."
        )

    return value


def create_spark_session() -> SparkSession:
    """Créer une session Spark avec Delta Lake."""

    return (
        SparkSession.builder
        .appName("IndustrialKafkaToBronze")
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


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Créer la source Kafka en streaming."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_kafka_events(
    kafka_dataframe: DataFrame,
) -> DataFrame:
    """Convertir le message Kafka en colonnes structurées."""

    decoded_dataframe = kafka_dataframe.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed_dataframe = decoded_dataframe.withColumn(
        "event",
        from_json(
            col("raw_value"),
            EVENT_SCHEMA,
        ),
    )

    return (
        parsed_dataframe
        .select(
            "kafka_key",
            "raw_value",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            col("event.event_id").alias("event_id"),
            col("event.sensor_id").alias("sensor_id"),
            col("event.machine_id").alias("machine_id"),
            col("event.sensor_type").alias("sensor_type"),
            col("event.value").alias("value"),
            col("event.unit").alias("unit"),
            col("event.event_timestamp").alias(
                "event_timestamp"
            ),
        )
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp(),
        )
    )


def start_delta_stream(
    dataframe: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    """Écrire le flux Kafka vers une table Delta."""

    return (
        dataframe.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start(output_path)
    )


def main() -> None:
    """Démarrer le pipeline streaming."""

    logger = configure_logging()
    spark = None
    query = None

    try:
        bootstrap_servers = (
            get_required_environment_variable(
                "KAFKA_BOOTSTRAP_SERVERS"
            )
        )

        topic = get_required_environment_variable(
            "KAFKA_TOPIC"
        )

        output_path = get_required_environment_variable(
            "STREAMING_BRONZE_PATH"
        )

        checkpoint_path = (
            get_required_environment_variable(
                "STREAMING_CHECKPOINT_PATH"
            )
        )

        logger.info(
            "Démarrage du pipeline Kafka vers Bronze."
        )
        logger.info(
            "Serveurs Kafka : %s",
            bootstrap_servers,
        )
        logger.info(
            "Topic Kafka : %s",
            topic,
        )
        logger.info(
            "Destination Delta : %s",
            output_path,
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        kafka_dataframe = read_kafka_stream(
            spark=spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )

        bronze_dataframe = parse_kafka_events(
            kafka_dataframe
        )

        query = start_delta_stream(
            dataframe=bronze_dataframe,
            output_path=output_path,
            checkpoint_path=checkpoint_path,
        )

        logger.info(
            "Pipeline streaming actif. "
            "Utilise Ctrl+C pour l'arrêter."
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info(
            "Arrêt demandé par l'utilisateur."
        )

    except Exception:
        logger.exception(
            "Échec du pipeline Kafka vers Bronze."
        )
        sys.exit(1)

    finally:
        if query is not None:
            query.stop()

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()