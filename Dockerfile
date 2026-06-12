FROM apache/airflow:3.1.0-python3.11

USER root

RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars && \
    wget -q -O /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/jars/postgresql-42.7.3.jar

COPY log4j2.properties /opt/airflow/log4j2.properties
RUN chmod 644 /opt/airflow/log4j2.properties

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=/opt/airflow:/opt/airflow/src
ENV PYTHONUNBUFFERED=1
ENV SPARK_SUBMIT_OPTS="-Dlog4j2.configurationFile=/opt/airflow/log4j2.properties"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt