FROM apache/airflow:3.1.8

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install pyspark==4.1.1 delta-spark==4.2.0 streamlit plotly pandas pendulum
RUN pip install asyncpg psycopg2-binary

