FROM flink:1.18-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Kafka connector goes into Flink's own lib/ so it's on the classpath for
# every job automatically — no env.add_jars() needed at runtime.
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

USER flink