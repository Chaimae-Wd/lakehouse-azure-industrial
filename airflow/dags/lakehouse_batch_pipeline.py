"""
DAG principal du pipeline Lakehouse industriel.

Ordre :
1. Vérifier PostgreSQL ;
2. ingérer PostgreSQL vers Bronze ;
3. transformer Bronze vers Silver ;
4. construire la couche Gold ;
5. valider les tables Gold ;
6. publier Gold vers PostgreSQL.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def docker_container_command(container_name: str) -> str:
    """
    Démarrer un conteneur existant, attendre sa fin,
    afficher ses logs et vérifier son vrai code de sortie.
    """

    return (
        f"docker start {container_name} >/dev/null && "
        f"EXIT_CODE=$(docker wait {container_name}) && "
        f"docker logs {container_name} && "
        f'echo "Code de sortie {container_name}: $EXIT_CODE" && '
        f'test "$EXIT_CODE" -eq 0'
    )


with DAG(
    dag_id="industrial_lakehouse_batch_pipeline",
    description=(
        "Orchestration PostgreSQL vers Bronze, Silver, Gold "
        "et publication PostgreSQL"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="Africa/Casablanca",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "lakehouse",
        "spark",
        "delta",
        "industrial",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    check_postgres = BashOperator(
        task_id="check_postgres",
        bash_command=(
            "docker inspect "
            "--format='{{ '{{' }}.State.Health.Status{{ '}}' }}' "
            "ocp_postgres | grep healthy"
        ),
    )

    bronze_ingestion = BashOperator(
        task_id="postgres_to_bronze",
        bash_command=docker_container_command(
            "industrial_spark_batch"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    silver_transformation = BashOperator(
        task_id="bronze_to_silver",
        bash_command=docker_container_command(
            "industrial_spark_silver"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    gold_transformation = BashOperator(
        task_id="silver_to_gold",
        bash_command=docker_container_command(
            "industrial_spark_gold"
        ),
        execution_timeout=timedelta(minutes=40),
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=docker_container_command(
            "industrial_spark_validate_gold"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    publish_gold = BashOperator(
        task_id="publish_gold_to_postgres",
        bash_command=docker_container_command(
            "industrial_spark_publish_gold"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
        start
        >> check_postgres
        >> bronze_ingestion
        >> silver_transformation
        >> gold_transformation
        >> validate_gold
        >> publish_gold
        >> end
    )