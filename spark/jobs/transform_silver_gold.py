"""
Pipeline Silver vers Gold.

Ce job construit :
- les dimensions analytiques ;
- les tables de faits ;
- les agrégations et KPI industriels ;
- les validations de volume et d'unicité.
"""

import logging
import os
import sys
import time
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    datediff,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
)


def configure_logging() -> logging.Logger:
    """Configurer les journaux du pipeline."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger("silver_to_gold")


def create_spark_session() -> SparkSession:
    """Créer une session Spark avec Delta Lake."""

    return (
        SparkSession.builder
        .appName("IndustrialSilverToGold")
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
    """Lire une variable d'environnement obligatoire."""

    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"La variable d'environnement '{name}' est absente."
        )

    return value


def read_delta_table(
    spark: SparkSession,
    root_path: str,
    table_name: str,
) -> DataFrame:
    """Lire une table Delta Silver."""

    return (
        spark.read
        .format("delta")
        .load(f"{root_path}/{table_name}")
    )


def write_delta_table(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """Écrire une table Gold au format Delta."""

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )


def add_gold_metadata(dataframe: DataFrame) -> DataFrame:
    """Ajouter une métadonnée de traitement Gold."""

    return dataframe.withColumn(
        "_gold_processed_at",
        current_timestamp(),
    )


def create_dimensions(
    silver_tables: Dict[str, DataFrame],
) -> Dict[str, DataFrame]:
    """Construire les dimensions du modèle analytique."""

    dim_workshops = (
        silver_tables["workshops"]
        .select(
            "workshop_id",
            "workshop_name",
            "location",
            "description",
        )
        .dropDuplicates(["workshop_id"])
    )

    dim_machines = (
        silver_tables["machines"]
        .join(
            dim_workshops.select(
                "workshop_id",
                "workshop_name",
                "location",
            ),
            on="workshop_id",
            how="left",
        )
        .select(
            "machine_id",
            "machine_name",
            "machine_type",
            "manufacturer",
            "installation_date",
            "status",
            "workshop_id",
            "workshop_name",
            "location",
        )
        .dropDuplicates(["machine_id"])
    )

    dim_technicians = (
        silver_tables["technicians"]
        .select(
            "technician_id",
            "first_name",
            "last_name",
            "specialty",
            "email",
            "phone",
            "hire_date",
        )
        .dropDuplicates(["technician_id"])
    )

    dim_sensors = (
        silver_tables["sensors"]
        .join(
            dim_machines.select(
                "machine_id",
                "machine_name",
                "workshop_id",
                "workshop_name",
            ),
            on="machine_id",
            how="left",
        )
        .select(
            "sensor_id",
            "sensor_code",
            "sensor_type",
            "measurement_unit",
            "minimum_threshold",
            "maximum_threshold",
            "status",
            "installation_date",
            "machine_id",
            "machine_name",
            "workshop_id",
            "workshop_name",
        )
        .dropDuplicates(["sensor_id"])
    )

    return {
        "dim_workshops": add_gold_metadata(dim_workshops),
        "dim_machines": add_gold_metadata(dim_machines),
        "dim_technicians": add_gold_metadata(dim_technicians),
        "dim_sensors": add_gold_metadata(dim_sensors),
    }


def create_fact_tables(
    silver_tables: Dict[str, DataFrame],
) -> Dict[str, DataFrame]:
    """Construire les tables de faits."""

    fact_production = (
        silver_tables["production"]
        .select(
            "production_id",
            "machine_id",
            "production_date",
            "shift",
            "quantity_produced",
            "rejected_quantity",
            "energy_consumption",
            "operating_hours",
            "quality_rate",
            "energy_per_unit",
        )
        .withColumn(
            "accepted_quantity",
            col("quantity_produced") - col("rejected_quantity"),
        )
        .withColumn(
            "production_year",
            date_format(col("production_date"), "yyyy").cast("int"),
        )
        .withColumn(
            "production_month",
            date_format(col("production_date"), "MM").cast("int"),
        )
    )

    fact_maintenance = (
        silver_tables["maintenance"]
        .select(
            "maintenance_id",
            "machine_id",
            "technician_id",
            "maintenance_type",
            "start_time",
            "end_time",
            "description",
            "cost",
            "status",
        )
        .withColumn(
            "maintenance_date",
            to_date(col("start_time")),
        )
        .withColumn(
            "duration_hours",
            when(
                col("end_time").isNotNull(),
                (
                    col("end_time").cast("long")
                    - col("start_time").cast("long")
                )
                / lit(3600.0),
            ),
        )
    )

    fact_alerts = (
        silver_tables["alerts"]
        .select(
            "alert_id",
            "sensor_id",
            "machine_id",
            "alert_timestamp",
            "alert_type",
            "severity",
            "measured_value",
            "threshold_value",
            "message",
            "status",
            "resolved_at",
        )
        .withColumn(
            "alert_date",
            to_date(col("alert_timestamp")),
        )
        .withColumn(
            "resolution_hours",
            when(
                col("resolved_at").isNotNull(),
                (
                    col("resolved_at").cast("long")
                    - col("alert_timestamp").cast("long")
                )
                / lit(3600.0),
            ),
        )
        .withColumn(
            "is_critical",
            when(
                col("severity") == "CRITICAL",
                lit(1),
            ).otherwise(lit(0)),
        )
    )

    return {
        "fact_production": add_gold_metadata(fact_production),
        "fact_maintenance": add_gold_metadata(fact_maintenance),
        "fact_alerts": add_gold_metadata(fact_alerts),
    }


def create_kpi_tables(
    silver_tables: Dict[str, DataFrame],
    dimensions: Dict[str, DataFrame],
    facts: Dict[str, DataFrame],
) -> Dict[str, DataFrame]:
    """Construire les indicateurs destinés aux analyses métiers."""

    machine_reference = dimensions["dim_machines"].select(
        "machine_id",
        "machine_name",
        "machine_type",
        "workshop_id",
        "workshop_name",
        "location",
    )

    production_enriched = facts["fact_production"].join(
        machine_reference,
        on="machine_id",
        how="left",
    )

    maintenance_enriched = facts["fact_maintenance"].join(
        machine_reference,
        on="machine_id",
        how="left",
    )

    alerts_enriched = facts["fact_alerts"].join(
        machine_reference,
        on="machine_id",
        how="left",
    )

    kpi_daily_production = (
        production_enriched
        .groupBy(
            "production_date",
            "workshop_id",
            "workshop_name",
        )
        .agg(
            spark_round(
                spark_sum("quantity_produced"),
                3,
            ).alias("total_quantity_produced"),
            spark_round(
                spark_sum("accepted_quantity"),
                3,
            ).alias("total_accepted_quantity"),
            spark_round(
                spark_sum("rejected_quantity"),
                3,
            ).alias("total_rejected_quantity"),
            spark_round(
                spark_sum("energy_consumption"),
                3,
            ).alias("total_energy_consumption"),
            spark_round(
                avg("quality_rate"),
                4,
            ).alias("average_quality_rate"),
            spark_round(
                avg("energy_per_unit"),
                4,
            ).alias("average_energy_per_unit"),
            count("production_id").alias("production_records"),
        )
    )

    kpi_workshop_performance = (
        production_enriched
        .groupBy(
            "workshop_id",
            "workshop_name",
            "location",
        )
        .agg(
            spark_round(
                spark_sum("quantity_produced"),
                3,
            ).alias("total_production"),
            spark_round(
                spark_sum("rejected_quantity"),
                3,
            ).alias("total_rejected"),
            spark_round(
                spark_sum("energy_consumption"),
                3,
            ).alias("total_energy"),
            spark_round(
                avg("quality_rate"),
                4,
            ).alias("average_quality_rate"),
            spark_round(
                avg("energy_per_unit"),
                4,
            ).alias("average_energy_per_unit"),
            spark_round(
                avg("operating_hours"),
                2,
            ).alias("average_operating_hours"),
        )
    )

    production_by_machine = (
        production_enriched
        .groupBy(
            "machine_id",
            "machine_name",
            "machine_type",
            "workshop_id",
            "workshop_name",
        )
        .agg(
            spark_round(
                spark_sum("quantity_produced"),
                3,
            ).alias("total_production"),
            spark_round(
                spark_sum("energy_consumption"),
                3,
            ).alias("total_energy"),
            spark_round(
                avg("quality_rate"),
                4,
            ).alias("average_quality_rate"),
            spark_round(
                avg("energy_per_unit"),
                4,
            ).alias("average_energy_per_unit"),
            spark_round(
                spark_sum("operating_hours"),
                2,
            ).alias("total_operating_hours"),
        )
    )

    maintenance_by_machine = (
        maintenance_enriched
        .groupBy("machine_id")
        .agg(
            count("maintenance_id").alias("maintenance_count"),
            spark_round(
                spark_sum("cost"),
                2,
            ).alias("total_maintenance_cost"),
            spark_round(
                avg("duration_hours"),
                2,
            ).alias("average_maintenance_duration_hours"),
        )
    )

    alerts_by_machine = (
        alerts_enriched
        .groupBy("machine_id")
        .agg(
            count("alert_id").alias("alert_count"),
            spark_sum("is_critical").alias("critical_alert_count"),
            spark_round(
                avg("resolution_hours"),
                2,
            ).alias("average_alert_resolution_hours"),
        )
    )

    kpi_machine_performance = (
        production_by_machine
        .join(
            maintenance_by_machine,
            on="machine_id",
            how="left",
        )
        .join(
            alerts_by_machine,
            on="machine_id",
            how="left",
        )
        .fillna(
            {
                "maintenance_count": 0,
                "total_maintenance_cost": 0.0,
                "alert_count": 0,
                "critical_alert_count": 0,
            }
        )
    )

    kpi_maintenance_summary = (
        maintenance_enriched
        .groupBy(
            "maintenance_date",
            "workshop_id",
            "workshop_name",
            "maintenance_type",
            "status",
        )
        .agg(
            count("maintenance_id").alias("intervention_count"),
            spark_round(
                spark_sum("cost"),
                2,
            ).alias("total_cost"),
            spark_round(
                avg("duration_hours"),
                2,
            ).alias("average_duration_hours"),
            spark_round(
                spark_min("duration_hours"),
                2,
            ).alias("minimum_duration_hours"),
            spark_round(
                spark_max("duration_hours"),
                2,
            ).alias("maximum_duration_hours"),
        )
    )

    return {
        "kpi_daily_production": add_gold_metadata(
            kpi_daily_production
        ),
        "kpi_workshop_performance": add_gold_metadata(
            kpi_workshop_performance
        ),
        "kpi_machine_performance": add_gold_metadata(
            kpi_machine_performance
        ),
        "kpi_maintenance_summary": add_gold_metadata(
            kpi_maintenance_summary
        ),
    }


def validate_unique_key(
    dataframe: DataFrame,
    key_column: str,
    table_name: str,
) -> None:
    """Vérifier l'unicité d'une clé dans une dimension ou un fait."""

    total_count = dataframe.count()

    distinct_count = (
        dataframe
        .select(key_column)
        .distinct()
        .count()
    )

    if total_count != distinct_count:
        raise ValueError(
            f"La clé {key_column} n'est pas unique "
            f"dans {table_name}."
        )


def persist_and_validate_tables(
    spark: SparkSession,
    tables: Dict[str, DataFrame],
    gold_root: str,
    logger: logging.Logger,
) -> int:
    """Écrire et valider toutes les tables Gold."""

    total_rows = 0

    unique_keys = {
        "dim_workshops": "workshop_id",
        "dim_machines": "machine_id",
        "dim_technicians": "technician_id",
        "dim_sensors": "sensor_id",
        "fact_production": "production_id",
        "fact_maintenance": "maintenance_id",
        "fact_alerts": "alert_id",
    }

    for table_name, dataframe in tables.items():
        output_path = f"{gold_root}/{table_name}"

        row_count = dataframe.count()

        if table_name in unique_keys:
            validate_unique_key(
                dataframe=dataframe,
                key_column=unique_keys[table_name],
                table_name=table_name,
            )

        logger.info(
            "%s : %s lignes à écrire.",
            table_name,
            row_count,
        )

        write_delta_table(
            dataframe=dataframe,
            output_path=output_path,
        )

        written_count = (
            spark.read
            .format("delta")
            .load(output_path)
            .count()
        )

        if written_count != row_count:
            raise ValueError(
                f"Validation échouée pour {table_name} : "
                f"{written_count} lignes écrites au lieu de "
                f"{row_count}."
            )

        logger.info(
            "Validation Gold réussie pour %s.",
            table_name,
        )

        total_rows += written_count

    return total_rows


def main() -> None:
    """Exécuter le pipeline Silver vers Gold."""

    logger = configure_logging()
    start_time = time.perf_counter()
    spark = None

    try:
        silver_root = get_required_environment_variable(
            "SILVER_PATH"
        )
        gold_root = get_required_environment_variable(
            "GOLD_PATH"
        )

        logger.info(
            "Démarrage du pipeline Silver vers Gold."
        )

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        source_table_names = [
            "workshops",
            "machines",
            "technicians",
            "sensors",
            "production",
            "maintenance",
            "alerts",
        ]

        silver_tables = {
            table_name: read_delta_table(
                spark=spark,
                root_path=silver_root,
                table_name=table_name,
            )
            for table_name in source_table_names
        }

        logger.info(
            "Création des dimensions analytiques."
        )
        dimensions = create_dimensions(silver_tables)

        logger.info(
            "Création des tables de faits."
        )
        facts = create_fact_tables(silver_tables)

        logger.info(
            "Création des KPI industriels."
        )
        kpi_tables = create_kpi_tables(
            silver_tables=silver_tables,
            dimensions=dimensions,
            facts=facts,
        )

        all_gold_tables = {
            **dimensions,
            **facts,
            **kpi_tables,
        }

        total_rows = persist_and_validate_tables(
            spark=spark,
            tables=all_gold_tables,
            gold_root=gold_root,
            logger=logger,
        )

        elapsed_time = time.perf_counter() - start_time

        logger.info(
            "Pipeline Silver vers Gold terminé avec succès."
        )
        logger.info(
            "Tables Gold créées : %s",
            len(all_gold_tables),
        )
        logger.info(
            "Total de lignes écrites : %s",
            total_rows,
        )
        logger.info(
            "Durée totale : %.2f secondes.",
            elapsed_time,
        )

    except Exception:
        logger.exception(
            "Échec du pipeline Silver vers Gold."
        )
        sys.exit(1)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
    