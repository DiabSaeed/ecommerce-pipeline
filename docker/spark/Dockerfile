FROM spark:3.5.7-scala2.12-java17-python3-ubuntu

USER 0

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install uv

RUN uv pip install --system --no-cache \
    pandas

RUN mkdir -p /opt/spark/.ivy2 && \
    chown -R 1000:1000 /opt/spark/.ivy2

WORKDIR /opt/spark/jobs