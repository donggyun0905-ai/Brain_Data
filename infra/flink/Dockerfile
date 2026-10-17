# Flink + PyFlink 2.2.1 (Python 3.12)
# 2026-10-17 확인: 기반 OS Ubuntu 24.04 (python3 = 3.12), 커넥터는 Flink 2.2 전용 5.0.0-2.2
FROM flink:2.2.1-java17

ARG KAFKA_CONNECTOR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

# Flink 2.x 공식 이미지는 기본 사용자가 flink(비 root)라 설치 동안만 root로 바꾼다
USER root

RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -c "import sys; assert sys.version_info[:2] in [(3,11),(3,12)], sys.version" && \
    python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir apache-flink==2.2.1

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar "$KAFKA_CONNECTOR_URL" && \
    chown flink:flink /opt/flink/lib/flink-sql-connector-kafka.jar

USER flink

# Flink는 PATH의 python으로 Python 작업을 띄운다. /usr/bin/python 심볼릭 링크로는
# 가상환경이 잡히지 않아(2026-10-17 확인) 가상환경을 PATH 맨 앞에 둔다
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/opt/databrain/src
