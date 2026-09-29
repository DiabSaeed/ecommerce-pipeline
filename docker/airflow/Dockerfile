FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    usermod -aG docker airflow

USER airflow

RUN pip install uv

RUN uv pip install --system --no-cache \
    apache-airflow-providers-docker \
    apache-airflow-providers-apache-spark==4.10.0 \
    apache-airflow-providers-snowflake