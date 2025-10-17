FROM apache/airflow:3.0.6
ADD requirements-dbt.txt requirements-dbt.txt
ADD requirements-airflow.txt requirements-airflow.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements-dbt.txt -r requirements-airflow.txt