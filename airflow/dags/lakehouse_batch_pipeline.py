"""
DAG principal du pipeline Lakehouse industriel.

Ordre :
1. Vérifier PostgreSQL ;
2. ingérer PostgreSQL vers Bronze ;
3. transformer Bronze vers Silver ;
4. construire la couche Gold ;
5. valider les tables Gold.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


PROJECT_DIRECTORY = "/opt/lakehouse-project"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="industrial_lakehouse_batch_pipeline",
    description=(
        "Orchestration PostgreSQL vers Bronze, Silver et Gold"
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
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker inspect "
            "--format='{{ '{{' }}.State.Health.Status{{ '}}' }}' "
            "ocp_postgres | grep healthy"
        ),
    )

    bronze_ingestion = BashOperator(
        task_id="postgres_to_bronze",
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker compose --profile batch "
            "run --rm spark-batch"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    silver_transformation = BashOperator(
        task_id="bronze_to_silver",
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker compose --profile silver "
            "run --rm spark-silver"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    gold_transformation = BashOperator(
        task_id="silver_to_gold",
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker compose --profile gold "
            "run --rm spark-gold"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker compose --profile validation "
            "run --rm spark-validate-gold"
        ),
        execution_timeout=timedelta(minutes=10),
    )


    publish_gold = BashOperator(
        task_id="publish_gold_to_postgres",
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            "docker compose --profile serving "
            "run --rm spark-publish-gold"
        ),
        execution_timeout=timedelta(minutes=15),
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