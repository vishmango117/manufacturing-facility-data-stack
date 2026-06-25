FROM python:3.10-slim

# Flink runtime + connectors
ENV FLINK_HOME=/opt/flink
ENV FLINK_VERSION=1.18.1
ENV PATH=$FLINK_HOME/bin:$PATH

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg python3-dev gcc g++ wget \
    && rm -rf /var/lib/apt/lists/*; \
    \
    # Install Flink runtime (download the binary distribution for aarch64)
    curl -fsSLO "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12-java11.tgz"; \
    tar xzf "flink-${FLINK_VERSION}-bin-scala_2.12-java11.tgz" -C /opt; \
    mv "/opt/flink-${FLINK_VERSION}" "$FLINK_HOME"; \
    rm "flink-${FLINK_VERSION}-bin-scala_2.12-java11.tgz"; \
    \
    # Install JDK for PyFlink native bridge (pemja)
    apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless; \
    rm -rf /var/lib/apt/lists/*; \
    \
    # Install PyFlink Python package
    pip3 install --no-cache-dir "apache-flink==${FLINK_VERSION}"

# Connector + format jars on the Flink classpath (/opt/flink/lib).
ENV FLINK_LIB=/opt/flink/lib
RUN set -eux; \
    cd "$FLINK_LIB"; \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar; \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.18.1/flink-sql-avro-confluent-registry-1.18.1.jar; \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-1.18.jar; \
    curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY flink/jobs /opt/flink/jobs

WORKDIR $FLINK_HOME
