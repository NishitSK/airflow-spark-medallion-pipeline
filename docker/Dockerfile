FROM apache/airflow:3.1.8

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install pyspark==4.1.1 delta-spark==4.2.0 streamlit plotly pandas pendulum fpdf2 pyyaml boto3

# Pre-download Spark/Delta dependencies at build time and copy them to PySpark jars path
RUN python -c "import os, pyspark, glob, shutil; \
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.13:4.1.0 pyspark-shell'; \
from pyspark.sql import SparkSession; \
SparkSession.builder.getOrCreate().stop(); \
p = os.path.join(os.path.dirname(pyspark.__file__), 'jars'); \
[shutil.copy(j, p) for j in glob.glob(os.path.expanduser('~/.ivy2*/jars/*.jar'))]; \
shutil.rmtree(os.path.expanduser('~/.ivy2*'), ignore_errors=True)"


