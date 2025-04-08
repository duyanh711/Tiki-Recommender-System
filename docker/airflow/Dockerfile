FROM apache/airflow:2.7.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION="3.4.1"
ARG HADOOP_VERSION="3.3"
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=python3

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN apt-get update && apt-get install -y curl && \
    curl -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz && \
    # Tạo thư mục logs và cấu hình cơ bản nếu cần
    mkdir -p ${SPARK_HOME}/logs && \
    chown -R airflow:root ${SPARK_HOME} && \
    apt-get remove -y curl && apt-get autoremove -y && apt-get clean

USER airflow