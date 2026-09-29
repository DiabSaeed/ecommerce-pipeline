FROM python:3.10-slim

WORKDIR /app

RUN pip install uv
RUN uv pip install --system --no-cache confluent-kafka

CMD ["python", "-m", "scripts.main"]