import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='ecommerce_batch_dbt_pipeline',
    default_args=default_args,
    description='Orchestrate Spark Batch Processing then dbt transformations',
    schedule_interval='@daily', 
    catchup=False,
) as dag:

    run_spark_batch = DockerOperator(
        task_id='run_spark_batch',
        image='ecommerce-datapipeline-spark:latest', 
        api_version='auto',
        auto_remove='force',
        mount_tmp_dir=False,
        command=(
    '/opt/spark/bin/spark-submit '
    '--packages '
    'net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5,'
    'net.snowflake:snowflake-jdbc:4.3.2 '
    '/opt/spark/jobs/spark_batch.py'
),
        docker_url='unix://var/run/docker.sock',
        network_mode='ecommerce-datapipeline_data_pipeline',
        mounts=[
            Mount(
                source='/home/diab_saeed/Projects/ecommerce-datapipeline/spark_jobs',
                target='/opt/spark/jobs',
                type='bind'
            )
        ],
        environment={
                    'SNOWFLAKE_ACCOUNT_SPARK': os.getenv('SNOWFLAKE_ACCOUNT_SPARK'),
                    'SNOWFLAKE_USER_SPARK': os.getenv('SNOWFLAKE_USER_SPARK'),
                    'SNOWFLAKE_PASSWORD_SPARK': os.getenv('SNOWFLAKE_PASSWORD_SPARK'),
                    'SNOWFLAKE_DATABASE_SPARK': os.getenv("SNOWFLAKE_DATABASE_SPARK", "ECOMMERCE_DB"),
                    'SNOWFLAKE_SCHEMA': os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
                    'SNOWFLAKE_WAREHOUSE_SPARK': os.getenv("SNOWFLAKE_WAREHOUSE_SPARK", "COMPUTE_WH"),
                    'SNOWFLAKE_ROLE_SPARK':os.getenv("SNOWFLAKE_ROLE_SPARK")
                }
    )


    run_dbt_models = DockerOperator(
        task_id='run_dbt_models',
        image='ecommerce-datapipeline-dbt:latest',
        api_version='auto',
        auto_remove='force',
        command='run --profiles-dir /usr/app/profiles',
        docker_url='unix://var/run/docker.sock',
        network_mode='ecommerce-datapipeline_data_pipeline',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/home/diab_saeed/Projects/ecommerce-datapipeline/dbt_project/Ecommerce',
                target='/usr/app/dbt',
                type='bind'
            ),
            Mount(
                source='/home/diab_saeed/Projects/ecommerce-datapipeline/dbt_project/profiles',
                target='/usr/app/profiles',
                type='bind'
            )
        ],
        environment={
            'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        }
    )


    run_spark_batch >> run_dbt_models