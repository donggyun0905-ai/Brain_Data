# Flink + PyFlink 2.2.1 (Python 3.12)
# 1주차 확인 항목: 기반 이미지 태그, 기반 OS의 Python 버전, Kafka 커넥터 JAR 버전 (docs/week1.md)
FROM flink:2.2.1-java17

ARG KAFKA_CONNECTOR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -c "import sys; assert sys.version_info[:2] in [(3,11),(3,12)], sys.version" && \
    python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir apache-flink==2.2.1 && \
    ln -sf /opt/venv/bin/python /usr/bin/python

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar "$KAFKA_CONNECTOR_URL"

ENV PYTHONPATH=/opt/databrain/src
