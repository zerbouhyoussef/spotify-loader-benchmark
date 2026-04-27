FROM python:3.10-slim

USER root

RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless wget curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV FLINK_VERSION=1.18.0
ENV FLINK_HOME=/opt/flink
ENV PATH="${FLINK_HOME}/bin:${PATH}"

RUN wget -q https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz && \
    tar -xzf flink-${FLINK_VERSION}-bin-scala_2.12.tgz && \
    mv flink-${FLINK_VERSION} ${FLINK_HOME} && \
    rm flink-${FLINK_VERSION}-bin-scala_2.12.tgz && \
    chmod +x ${FLINK_HOME}/bin/*.sh

RUN pip3 install --no-cache-dir apache-flink==1.18.0 kafka-python psycopg2-binary python-dotenv

RUN wget -q -P ${FLINK_HOME}/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy the Flink loader script and configuration
WORKDIR /opt/flink
COPY flink_loader.py .
COPY flink-conf.yaml ${FLINK_HOME}/conf/flink-conf.yaml