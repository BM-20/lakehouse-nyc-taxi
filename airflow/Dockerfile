# Extends the official Airflow image with this project's pipeline dependencies
# (PyIceberg, DuckDB, dbt-bigquery).
#
# IMPORTANT: PyIceberg's SQL catalog needs SQLAlchemy 2.0, but Airflow 2.10 pins
# SQLAlchemy 1.4 — installing the pipeline deps into Airflow's own environment
# upgrades SQLAlchemy and breaks Airflow's ORM. So we install them into an
# ISOLATED virtualenv (/opt/pipeline-venv). The DAG's BashOperators invoke this
# venv's python/dbt, leaving Airflow's own environment untouched.
#
# Bump AIRFLOW_VERSION / PYTHON_VERSION here if you want a different base.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
# Build from the base interpreter (not Airflow's env) so the venv is clean.
RUN python -m venv /opt/pipeline-venv
COPY requirements.txt /requirements.txt
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline-venv/bin/pip install --no-cache-dir -r /requirements.txt
# venv files are world-readable/executable, so the airflow user can run them.
USER airflow
