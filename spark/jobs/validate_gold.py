"""Valider la présence et le volume des tables Gold Delta."""

import os
import sys
from typing import Dict

from pyspark.sql import SparkSession


EXPECTED_TABLES = [
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


def create_spark_session() -> SparkSession:
    """Créer une session Spark compatible Delta Lake."""

    return (
        SparkSession.builder
        .appName("ValidateIndustrialGoldLayer")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    """Vérifier toutes les tables Gold."""

    gold_root = os.getenv(
        "GOLD_PATH",
        "/opt/spark-data/gold",
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    results: Dict[str, int] = {}

    try:
        for table_name in EXPECTED_TABLES:
            table_path = f"{gold_root}/{table_name}"

            dataframe = (
                spark.read
                .format("delta")
                .load(table_path)
            )

            row_count = dataframe.count()

            if row_count <= 0:
                raise ValueError(
                    f"La table Gold '{table_name}' est vide."
                )

            results[table_name] = row_count

            print(
                f"VALIDATION OK | {table_name} | "
                f"{row_count} lignes"
            )

        print("=" * 70)
        print(f"Tables Gold validées : {len(results)}")
        print(f"Total des lignes : {sum(results.values())}")
        print("COUCHE GOLD VALIDÉE AVEC SUCCÈS")
        print("=" * 70)

    except Exception as error:
        print(f"ÉCHEC DE LA VALIDATION GOLD : {error}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()