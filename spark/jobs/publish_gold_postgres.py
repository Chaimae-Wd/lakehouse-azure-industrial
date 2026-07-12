"""
Publication de la couche Gold Delta vers PostgreSQL.

Ce job :
1. lit les onze tables Gold ;
2. vérifie qu'elles ne sont pas vides ;
3. les publie dans le schéma PostgreSQL analytics ;
4. relit les tables via JDBC afin de contrôler les volumes.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Final

from pyspark.sql import DataFrame, SparkSession


GOLD_TABLES: Final[list[str]] = [
    "dim_workshops",
    "dim_machines",
    "dim_technicians",
    "dim_sensors",
    "fact_production",
    "fact_maintenance",
    "fact_alerts",
    "kpi_daily_production",
    "kpi_workshop_performance",
    "kpi_machine_performance",
    "kpi_maintenance_summary",
]


def configure_logging() -> logging.Logger:
    """Configurer les journaux du pipeline."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("publish_gold_postgres")


def required_environment_variable(name: str) -> str:
    """Lire une variable obligatoire."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement '{name}' est absente."
        )

    return value


def create_spark_session() -> SparkSession:
    """Créer une session Spark compatible avec Delta Lake."""

    return (
        SparkSession.builder
        .appName("PublishIndustrialGoldToPostgres")
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


def read_gold_table(
    spark: SparkSession,
    gold_root: str,
    table_name: str,
) -> DataFrame:
    """Lire une table Gold Delta."""

    return (
        spark.read
        .format("delta")
        .load(f"{gold_root}/{table_name}")
    )


def write_postgres_table(
    dataframe: DataFrame,
    jdbc_url: str,
    destination_table: str,
    user: str,
    password: str,
) -> None:
    """Publier un DataFrame dans PostgreSQL."""

    (
        dataframe.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", destination_table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .mode("overwrite")
        .save()
    )


def read_postgres_count(
    spark: SparkSession,
    jdbc_url: str,
    destination_table: str,
    user: str,
    password: str,
) -> int:
    """Relire une table PostgreSQL et retourner son volume."""

    dataframe = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", destination_table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return dataframe.count()


def main() -> None:
    """Exécuter la publication complète."""

    logger = configure_logging()
    start_time = time.perf_counter()
    spark = None

    try:
        gold_root = required_environment_variable("GOLD_PATH")
        postgres_host = required_environment_variable("POSTGRES_HOST")
        postgres_port = required_environment_variable("POSTGRES_PORT")
        postgres_db = required_environment_variable("POSTGRES_DB")
        postgres_user = required_environment_variable("POSTGRES_USER")
        postgres_password = required_environment_variable(
            "POSTGRES_PASSWORD"
        )
        analytics_schema = os.getenv(
            "ANALYTICS_SCHEMA",
            "analytics",
        )

        jdbc_url = (
            f"jdbc:postgresql://{postgres_host}:"
            f"{postgres_port}/{postgres_db}"
        )

        logger.info(
            "Démarrage de la publication Gold vers PostgreSQL."
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        total_rows = 0

        for table_name in GOLD_TABLES:
            logger.info(
                "Lecture de la table Gold %s.",
                table_name,
            )

            gold_dataframe = read_gold_table(
                spark=spark,
                gold_root=gold_root,
                table_name=table_name,
            )

            source_count = gold_dataframe.count()

            if source_count <= 0:
                raise ValueError(
                    f"La table Gold '{table_name}' est vide."
                )

            destination_table = (
                f"{analytics_schema}.{table_name}"
            )

            logger.info(
                "%s : publication de %s lignes vers %s.",
                table_name,
                source_count,
                destination_table,
            )

            write_postgres_table(
                dataframe=gold_dataframe,
                jdbc_url=jdbc_url,
                destination_table=destination_table,
                user=postgres_user,
                password=postgres_password,
            )

            published_count = read_postgres_count(
                spark=spark,
                jdbc_url=jdbc_url,
                destination_table=destination_table,
                user=postgres_user,
                password=postgres_password,
            )

            if published_count != source_count:
                raise ValueError(
                    f"Validation échouée pour {table_name} : "
                    f"{published_count} lignes publiées au lieu "
                    f"de {source_count}."
                )

            total_rows += published_count

            logger.info(
                "Validation réussie pour %s : %s lignes.",
                table_name,
                published_count,
            )

        elapsed_time = time.perf_counter() - start_time

        logger.info(
            "Publication Gold terminée avec succès."
        )
        logger.info(
            "Tables publiées : %s",
            len(GOLD_TABLES),
        )
        logger.info(
            "Total des lignes publiées : %s",
            total_rows,
        )
        logger.info(
            "Durée totale : %.2f secondes.",
            elapsed_time,
        )

    except Exception:
        logger.exception(
            "Échec de la publication Gold vers PostgreSQL."
        )
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()