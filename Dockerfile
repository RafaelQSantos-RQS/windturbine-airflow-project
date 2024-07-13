ARG AIRFLOW_VERSION=2.9.2
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt