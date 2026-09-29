FROM python:3.10-slim

WORKDIR /usr/app/dbt

RUN pip install uv

RUN uv pip install --system --no-cache dbt-core dbt-snowflake 

ENTRYPOINT [ "dbt" ]