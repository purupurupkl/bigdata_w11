FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    docker.io \
    default-mysql-client \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupmod -g 999 docker || groupadd -g 999 docker
RUN usermod -aG docker airflow

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-docker==3.14.0