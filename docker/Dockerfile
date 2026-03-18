FROM apache/airflow:3.1.8

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install pyspark delta-spark streamlit plotly pandas pendulum

