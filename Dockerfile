FROM apache/airflow:2.3.0

COPY requirements.txt .
USER root
RUN apt-get update -y
RUN apt-get install vim -y

USER airflow
RUN pip install -r requirements.txt


RUN pip install dbt-snowflake==0.19.0 \
&& pip install rich