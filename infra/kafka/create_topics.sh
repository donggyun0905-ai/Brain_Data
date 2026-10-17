#!/bin/bash
# 토픽 생성. 파티션 키는 user_id (ADR-0004).
set -e
B=kafka:9092
T=/opt/kafka/bin/kafka-topics.sh
until $T --bootstrap-server $B --list >/dev/null 2>&1; do sleep 2; done

mk() { $T --bootstrap-server $B --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 ${3:+--config $3}; }

for p in "" "replay."; do                      # 일반 토픽 + 수면 재생용 토픽
  mk "${p}events.purchase" 3
  mk "${p}events.cart" 6
  mk "${p}events.view" 12
  mk "${p}agg.minute" 3
  mk "${p}agg.purchase" 3
done
mk control.rates 1 cleanup.policy=compact      # 마지막 비율만 남기면 됨
mk log.decisions 3
$T --bootstrap-server $B --list
