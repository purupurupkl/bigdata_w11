from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- CONFIGURATION ---
PROJECT_NETWORK = "star_schema_default"
SPARK_IMAGE = "spark:3.5.8-python3"
HOST_PROJECT_ROOT = os.environ.get(
    "AIRFLOW_HOST_PROJECT_ROOT",
    "/home/huy/Desktop/bigdata_code/star_schema",
)

# 1. Updated Packages: PostgreSQL driver only
SPARK_PACKAGES = (
    "com.mysql:mysql-connector-j:8.4.0,"
    "org.postgresql:postgresql:42.7.3"
)

# 2. Updated Conf: No Iceberg/S3A
SPARK_SUBMIT_CONF = (
    "--conf spark.jars.ivy=/tmp/.ivy2"
)

# 3. Updated Environment
SPARK_ENV = {
    "MYSQL_HOST": "mysql",
    "MYSQL_PORT": "3306",
    "MYSQL_DATABASE": "classicmodels",
    "MYSQL_USER": "etl",
    "MYSQL_PASSWORD": "etl",
    
    # PostgreSQL Data Warehouse
    "POSTGRES_DW_HOST": "postgres-dw",
    "POSTGRES_DW_PORT": "5432",
    "POSTGRES_DW_DB": "star_schema_dw",
    "POSTGRES_DW_USER": "warehouse_user",
    "POSTGRES_DW_PASSWORD": "warehouse_password",
    
    "SPARK_PACKAGES": SPARK_PACKAGES,
    "SPARK_SUBMIT_CONF": SPARK_SUBMIT_CONF,
}

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def spark_task(task_id: str, script_name: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=PROJECT_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        environment=SPARK_ENV,
        # Sửa lại command để parse env chuẩn hơn trong Docker
        command=(
            "bash -lc '/opt/spark/bin/spark-submit "
            f"--packages {SPARK_PACKAGES} "
            f"{SPARK_SUBMIT_CONF} "
            f"/opt/spark/jobs/{script_name}'"
        ),
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_ROOT}/etl/jobs",
                target="/opt/spark/jobs",
                type="bind",
                read_only=True,
            )
        ],
    )

with DAG(
    dag_id="classicmodels_mysql_to_postgres_star_schema", # Đổi tên DAG
    description="MySQL → Spark → Postgres star schema",
    start_date=datetime(2026, 5, 1),
    schedule=None, # Đổi thành None để test manual trước
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "postgres", "rdb"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Kiểm tra MySQL (Source)
    check_mysql_ready = BashOperator(
        task_id="check_mysql_ready",
        bash_command="""
        until mysqladmin ping -h mysql -uetl -petl --silent; do
            echo "Waiting for MySQL..."
            sleep 2
        done
        """,
    )

    # Kiểm tra Postgres (Warehouse) - Sửa lỗi command và đường dẫn
    check_dw_ready = BashOperator(
        task_id="check_dw_ready",
        bash_command="""
        until pg_isready -h postgres-dw -U warehouse_user -d star_schema_dw; do
            echo "Waiting for Postgres Data Warehouse..."
            sleep 2
        done
        """,
    )

    prepare_dw_schema = BashOperator(
    task_id="prepare_dw_schema",
    # Sử dụng psql trực tiếp thông qua docker exec
    bash_command=f"docker exec classicmodels-dw psql -U {SPARK_ENV['POSTGRES_DW_USER']} -d {SPARK_ENV['POSTGRES_DW_DB']} -c 'CREATE SCHEMA IF NOT EXISTS star_schema;'"
    )

    run_spark_etl = spark_task(
        task_id="run_spark_etl",
        script_name="build_star_schema.py",
    )

    validate_star_schema = spark_task(
        task_id="validate_star_schema",
        script_name="query_star_schema.py",
    )

    end = EmptyOperator(task_id="end")

    # Pipeline logic: Đợi cả 2 DB sẵn sàng rồi mới chạy Spark
    start >> [check_mysql_ready, check_dw_ready] >> prepare_dw_schema >> run_spark_etl >> validate_star_schema >> end