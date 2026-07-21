"""
Pipeline Batch PostgreSQL vers Bronze Delta Lake.

Ce job :
1. se connecte à PostgreSQL avec JDBC ;
2. lit les tables du schéma industrial ;
3. ajoute des métadonnées techniques ;
4. écrit chaque table dans la couche Bronze au format Delta ;
5. vérifie le nombre de lignes écrites.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit


TABLES = [
    "workshops",
    "machines",
    "technicians",
    "sensors",
    "production",
    "maintenance",
    "alerts",
]


def configure_logging() -> logging.Logger:
    """Configurer les journaux du pipeline."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("postgres_to_bronze")


def create_spark_session() -> SparkSession:
    """Créer une session Spark légère avec le support Delta Lake."""

    return (
        SparkSession.builder
        .appName("IndustrialPostgresToBronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def get_required_environment_variable(name: str) -> str:
    """Lire une variable obligatoire ou arrêter avec un message clair."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement obligatoire '{name}' est absente."
        )

    return value


def read_postgres_table(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    user: str,
    password: str,
) -> DataFrame:
    """Lire une table PostgreSQL dans un DataFrame Spark."""

    full_table_name = f"industrial.{table_name}"

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", full_table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def add_ingestion_metadata(
    dataframe: DataFrame,
    source_table: str,
    ingestion_id: str,
) -> DataFrame:
    """Ajouter des métadonnées techniques à la couche Bronze."""

    return (
        dataframe
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("postgresql"))
        .withColumn("_source_table", lit(source_table))
        .withColumn("_ingestion_id", lit(ingestion_id))
    )


def write_delta_table(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """
    Écrire le DataFrame dans Delta.

    Le mode overwrite est utilisé pour ce premier pipeline complet.
    Nous passerons à une ingestion incrémentale plus tard.
    """

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )


def validate_delta_table(
    spark: SparkSession,
    output_path: str,
    expected_count: int,
) -> int:
    """Relire la table Delta et vérifier le nombre de lignes."""

    actual_count = (
        spark.read
        .format("delta")
        .load(output_path)
        .count()
    )

    if actual_count != expected_count:
        raise ValueError(
            "Échec de validation : "
            f"{actual_count} lignes écrites au lieu de {expected_count}."
        )

    return actual_count


def main() -> None:
    """Exécuter le pipeline complet."""

    logger = configure_logging()
    start_time = time.perf_counter()

    spark = None

    try:
        postgres_host = get_required_environment_variable("POSTGRES_HOST")
        postgres_port = get_required_environment_variable("POSTGRES_PORT")
        postgres_db = get_required_environment_variable("POSTGRES_DB")
        postgres_user = get_required_environment_variable("POSTGRES_USER")
        postgres_password = get_required_environment_variable(
            "POSTGRES_PASSWORD"
        )
        bronze_root = get_required_environment_variable("BRONZE_PATH")

        jdbc_url = (
            f"jdbc:postgresql://{postgres_host}:"
            f"{postgres_port}/{postgres_db}"
        )

        ingestion_id = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )

        logger.info("Démarrage du pipeline PostgreSQL vers Bronze.")
        logger.info("Identifiant d'ingestion : %s", ingestion_id)

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        total_rows = 0

        for table_name in TABLES:
            logger.info("Lecture de la table industrial.%s", table_name)

            source_dataframe = read_postgres_table(
                spark=spark,
                jdbc_url=jdbc_url,
                table_name=table_name,
                user=postgres_user,
                password=postgres_password,
            )

            bronze_dataframe = add_ingestion_metadata(
                dataframe=source_dataframe,
                source_table=table_name,
                ingestion_id=ingestion_id,
            )

            table_output_path = f"{bronze_root}/{table_name}"

            logger.info(
                "Écriture Delta vers %s",
                table_output_path,
            )

            write_delta_table(
                dataframe=bronze_dataframe,
                output_path=table_output_path,
            )

            validated_count = (
                spark.read
                .format("delta")
                .load(table_output_path)
                .count()
            )

            total_rows += validated_count

            logger.info(
                "Table %s écrite et validée : %s lignes.",
                table_name,
                validated_count,
            )

            spark.catalog.clearCache()

           

        elapsed_time = time.perf_counter() - start_time

        logger.info(
            "Pipeline terminé avec succès : %s tables et %s lignes.",
            len(TABLES),
            total_rows,
        )
        logger.info(
            "Durée totale : %.2f secondes.",
            elapsed_time,
        )

    except Exception:
        logger.exception("Échec du pipeline PostgreSQL vers Bronze.")
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()