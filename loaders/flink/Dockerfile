FROM flink:1.18.0-scala_2.12-java11

USER root

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.18.0 kafka-python psycopg2-binary python-dotenv

# Download Kafka connector
RUN wget -q -P ${FLINK_HOME}/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy the Flink loader script and configuration
WORKDIR /opt/flink
COPY flink_loader.py .
COPY flink-conf.yaml ${FLINK_HOME}/conf/flink-conf.yaml