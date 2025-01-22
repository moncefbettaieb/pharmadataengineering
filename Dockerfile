ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

RUN apt-get update && apt-get install -y \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PATH="/home/dbtuser/.local/bin:${PATH}"

RUN useradd -m dbtuser && chown -R dbtuser:dbtuser /app
USER dbtuser

COPY --chown=dbtuser:dbtuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=dbtuser:dbtuser . .

RUN mkdir -p /home/dbtuser/.dbt
COPY --chown=dbtuser:dbtuser profiles.yml /home/dbtuser/.dbt/

RUN dbt deps

RUN chmod +x /app/entrypoint.sh
RUN chmod +x /app/scripts/download_seeds.sh

ENTRYPOINT ["/app/entrypoint.sh"]