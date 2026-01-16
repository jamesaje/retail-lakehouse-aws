FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt Athena adapter
RUN pip install --no-cache-dir dbt-athena-community==1.9.5
