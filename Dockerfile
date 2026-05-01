# Spark 3.5 + Python 3.11 + Java 17 baseline
FROM apache/spark:3.5.1-python3

USER root

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml ./
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY conf/ ./conf/

RUN pip install --no-cache-dir -e .

# Default: run end-to-end pipeline. Override with `docker run ... generate` etc.
ENTRYPOINT ["python", "-m", "pipeline.cli"]
CMD ["run"]
