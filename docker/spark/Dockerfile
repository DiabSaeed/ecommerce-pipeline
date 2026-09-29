FROM spark:3.5.7-python3

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install uv

RUN uv pip install --system --no-cache \
    pandas

WORKDIR /opt/spark/jobs

USER 1000