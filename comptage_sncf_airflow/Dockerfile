FROM apache/airflow:3.3.2
COPY requirements.txt /
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt