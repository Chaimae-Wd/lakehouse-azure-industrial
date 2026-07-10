"""
Pipeline Bronze vers Silver.

Ce job :
1. lit les tables Delta de la couche Bronze ;
2. supprime les doublons ;
3. filtre les données invalides ;
4. standardise les chaînes de caractères ;
5. ajoute des métadonnées Silver ;
6. écrit les résultats au format Delta ;
7. valide les volumes écrits.
"""

import logging
import os
import sys
import time
from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    initcap,
    lower,
    trim,
    upper,
    when,
)


TABLE_PRIMARY_KEYS: Dict[str, str] = {
    "workshops": "workshop_id",
    "machines": "machine_id",
    "technicians": "technician_id",
    "sensors": "sensor_id",
    "production": "production_id",
    "maintenance": "maintenance_id",
    "alerts": "alert_id",
}


def configure_logging() -> logging.Logger:
    """Configurer les journaux du pipeline."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("bronze_to_silver")


def create_spark_session() -> SparkSession:
    """Créer une session Spark compatible avec Delta Lake."""

    return (
        SparkSession.builder
        .appName("IndustrialBronzeToSilver")
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


def get_required_environment_variable(name: str) -> str:
    """Lire une variable d’environnement obligatoire."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement '{name}' est absente."
        )

    return value


def read_delta_table(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """Lire une table Delta."""

    return (
        spark.read
        .format("delta")
        .load(path)
    )


def remove_duplicates(
    dataframe: DataFrame,
    primary_key: str,
) -> DataFrame:
    """Supprimer les doublons selon la clé primaire métier."""

    return dataframe.dropDuplicates([primary_key])


def add_silver_metadata(dataframe: DataFrame) -> DataFrame:
    """Ajouter la date de transformation Silver."""

    return dataframe.withColumn(
        "_silver_processed_at",
        current_timestamp(),
    )


def clean_workshops(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des ateliers."""

    return (
        dataframe
        .filter(col("workshop_id").isNotNull())
        .filter(col("workshop_name").isNotNull())
        .withColumn(
            "workshop_name",
            initcap(trim(col("workshop_name"))),
        )
        .withColumn(
            "location",
            initcap(trim(col("location"))),
        )
        .withColumn(
            "description",
            trim(col("description")),
        )
    )


def clean_machines(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des machines."""

    return (
        dataframe
        .filter(col("machine_id").isNotNull())
        .filter(col("machine_name").isNotNull())
        .filter(col("workshop_id").isNotNull())
        .withColumn(
            "machine_name",
            upper(trim(col("machine_name"))),
        )
        .withColumn(
            "machine_type",
            initcap(trim(col("machine_type"))),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
        .withColumn(
            "manufacturer",
            initcap(trim(col("manufacturer"))),
        )
        .filter(
            col("status").isin(
                "ACTIVE",
                "INACTIVE",
                "MAINTENANCE",
            )
        )
    )


def clean_technicians(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des techniciens."""

    return (
        dataframe
        .filter(col("technician_id").isNotNull())
        .filter(col("first_name").isNotNull())
        .filter(col("last_name").isNotNull())
        .withColumn(
            "first_name",
            initcap(trim(col("first_name"))),
        )
        .withColumn(
            "last_name",
            upper(trim(col("last_name"))),
        )
        .withColumn(
            "specialty",
            initcap(trim(col("specialty"))),
        )
        .withColumn(
            "email",
            lower(trim(col("email"))),
        )
        .withColumn(
            "phone",
            trim(col("phone")),
        )
    )


def clean_sensors(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des capteurs."""

    return (
        dataframe
        .filter(col("sensor_id").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("sensor_code").isNotNull())
        .withColumn(
            "sensor_code",
            upper(trim(col("sensor_code"))),
        )
        .withColumn(
            "sensor_type",
            upper(trim(col("sensor_type"))),
        )
        .withColumn(
            "measurement_unit",
            trim(col("measurement_unit")),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
        .filter(
            col("sensor_type").isin(
                "TEMPERATURE",
                "VIBRATION",
                "PRESSURE",
                "ENERGY",
            )
        )
        .filter(
            col("status").isin(
                "ACTIVE",
                "INACTIVE",
                "FAULTY",
            )
        )
        .filter(
            col("minimum_threshold").isNull()
            | col("maximum_threshold").isNull()
            | (
                col("minimum_threshold")
                < col("maximum_threshold")
            )
        )
    )


def clean_production(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table de production."""

    return (
        dataframe
        .filter(col("production_id").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("production_date").isNotNull())
        .filter(col("quantity_produced") >= 0)
        .filter(col("energy_consumption") >= 0)
        .filter(
            (col("operating_hours") >= 0)
            & (col("operating_hours") <= 24)
        )
        .filter(col("rejected_quantity") >= 0)
        .filter(
            col("rejected_quantity")
            <= col("quantity_produced")
        )
        .withColumn(
            "shift",
            upper(trim(col("shift"))),
        )
        .filter(
            col("shift").isin(
                "MORNING",
                "AFTERNOON",
                "NIGHT",
            )
        )
        .withColumn(
            "quality_rate",
            when(
                col("quantity_produced") > 0,
                (
                    col("quantity_produced")
                    - col("rejected_quantity")
                )
                / col("quantity_produced"),
            ).otherwise(None),
        )
        .withColumn(
            "energy_per_unit",
            when(
                col("quantity_produced") > 0,
                col("energy_consumption")
                / col("quantity_produced"),
            ).otherwise(None),
        )
    )


def clean_maintenance(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des maintenances."""

    return (
        dataframe
        .filter(col("maintenance_id").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("technician_id").isNotNull())
        .filter(col("start_time").isNotNull())
        .filter(
            col("end_time").isNull()
            | (col("end_time") >= col("start_time"))
        )
        .filter(col("cost") >= 0)
        .withColumn(
            "maintenance_type",
            upper(trim(col("maintenance_type"))),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
        .withColumn(
            "description",
            trim(col("description")),
        )
        .filter(
            col("maintenance_type").isin(
                "PREVENTIVE",
                "CORRECTIVE",
                "EMERGENCY",
            )
        )
        .filter(
            col("status").isin(
                "PLANNED",
                "IN_PROGRESS",
                "COMPLETED",
                "CANCELLED",
            )
        )
    )


def clean_alerts(dataframe: DataFrame) -> DataFrame:
    """Nettoyer la table des alertes."""

    return (
        dataframe
        .filter(col("alert_id").isNotNull())
        .filter(col("sensor_id").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("alert_timestamp").isNotNull())
        .withColumn(
            "alert_type",
            upper(trim(col("alert_type"))),
        )
        .withColumn(
            "severity",
            upper(trim(col("severity"))),
        )
        .withColumn(
            "status",
            upper(trim(col("status"))),
        )
        .withColumn(
            "message",
            trim(col("message")),
        )
        .filter(
            col("severity").isin(
                "LOW",
                "MEDIUM",
                "HIGH",
                "CRITICAL",
            )
        )
        .filter(
            col("status").isin(
                "OPEN",
                "ACKNOWLEDGED",
                "RESOLVED",
            )
        )
        .filter(
            col("resolved_at").isNull()
            | (
                col("resolved_at")
                >= col("alert_timestamp")
            )
        )
    )


CLEANING_FUNCTIONS: Dict[str, Callable[[DataFrame], DataFrame]] = {
    "workshops": clean_workshops,
    "machines": clean_machines,
    "technicians": clean_technicians,
    "sensors": clean_sensors,
    "production": clean_production,
    "maintenance": clean_maintenance,
    "alerts": clean_alerts,
}


def write_delta_table(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """Écrire une table Silver au format Delta."""

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )


def validate_written_table(
    spark: SparkSession,
    output_path: str,
    expected_count: int,
) -> int:
    """Relire la table écrite et valider son volume."""

    actual_count = (
        spark.read
        .format("delta")
        .load(output_path)
        .count()
    )

    if actual_count != expected_count:
        raise ValueError(
            f"Validation échouée pour {output_path} : "
            f"{actual_count} lignes au lieu de "
            f"{expected_count}."
        )

    return actual_count


def main() -> None:
    """Exécuter le pipeline Bronze vers Silver."""

    logger = configure_logging()
    start_time = time.perf_counter()
    spark = None

    try:
        bronze_root = get_required_environment_variable(
            "BRONZE_PATH"
        )
        silver_root = get_required_environment_variable(
            "SILVER_PATH"
        )

        logger.info(
            "Démarrage du pipeline Bronze vers Silver."
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        total_bronze_rows = 0
        total_silver_rows = 0

        for table_name, primary_key in TABLE_PRIMARY_KEYS.items():
            bronze_path = f"{bronze_root}/{table_name}"
            silver_path = f"{silver_root}/{table_name}"

            logger.info(
                "Lecture de la table Bronze %s",
                table_name,
            )

            bronze_dataframe = read_delta_table(
                spark=spark,
                path=bronze_path,
            )

            bronze_count = bronze_dataframe.count()
            total_bronze_rows += bronze_count

            logger.info(
                "%s : %s lignes Bronze.",
                table_name,
                bronze_count,
            )

            deduplicated_dataframe = remove_duplicates(
                dataframe=bronze_dataframe,
                primary_key=primary_key,
            )

            cleaned_dataframe = CLEANING_FUNCTIONS[
                table_name
            ](deduplicated_dataframe)

            silver_dataframe = add_silver_metadata(
                cleaned_dataframe
            )

            silver_count = silver_dataframe.count()
            rejected_count = bronze_count - silver_count

            logger.info(
                "%s : %s lignes Silver, %s rejetées.",
                table_name,
                silver_count,
                rejected_count,
            )

            write_delta_table(
                dataframe=silver_dataframe,
                output_path=silver_path,
            )

            validated_count = validate_written_table(
                spark=spark,
                output_path=silver_path,
                expected_count=silver_count,
            )

            total_silver_rows += validated_count

            logger.info(
                "Validation Silver réussie pour %s.",
                table_name,
            )

        elapsed_time = time.perf_counter() - start_time

        logger.info(
            "Pipeline terminé avec succès."
        )
        logger.info(
            "Lignes Bronze : %s",
            total_bronze_rows,
        )
        logger.info(
            "Lignes Silver : %s",
            total_silver_rows,
        )
        logger.info(
            "Lignes rejetées : %s",
            total_bronze_rows - total_silver_rows,
        )
        logger.info(
            "Durée totale : %.2f secondes.",
            elapsed_time,
        )

    except Exception:
        logger.exception(
            "Échec du pipeline Bronze vers Silver."
        )
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()