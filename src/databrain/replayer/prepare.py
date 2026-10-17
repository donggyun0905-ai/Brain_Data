"""
REES46 CSV → 시간순 Parquet 변환 (1회 실행).

    python -m databrain.replayer.prepare data/raw/2019-Oct.csv data/parquet/2019-10

- event_time 순으로 정렬해 날짜별 Parquet로 나눈다 → 재생기가 순서대로 빠르게 읽음
- REES46에는 이벤트 ID가 없고, 완전히 같은 행이 중복으로 들어 있다.
  그래서 행 내용으로 event_id를 만든다 → 같은 행은 같은 ID → 입구 정리 단계에서 중복 제거 가능
- remove_from_cart는 장바구니 토픽으로 보낸다 (중요 등급)
- 9GB급 CSV도 메모리에 다 올리지 않도록 DuckDB로 처리한다
"""
from __future__ import annotations

import sys
from pathlib import Path


def main(src: str, out_dir: str) -> None:  # pragma: no cover - 실제 파일 필요
    import duckdb
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                md5(concat_ws('|', event_time, event_type, product_id, user_id, user_session)) AS event_id,
                CAST(event_time AS TIMESTAMP) AS event_time,
                event_type,
                CASE WHEN event_type = 'purchase' THEN 'purchase'
                     WHEN event_type IN ('cart', 'remove_from_cart') THEN 'cart'
                     ELSE 'view' END AS lane,
                product_id, category_id,
                coalesce(category_code, 'etc') AS category_code,
                coalesce(brand, 'etc') AS brand,
                price, user_id, user_session,
                CAST(CAST(event_time AS TIMESTAMP) AS DATE) AS day
            FROM read_csv_auto('{src}')
            ORDER BY event_time
        ) TO '{out_dir}' (FORMAT PARQUET, PARTITION_BY (day), OVERWRITE_OR_IGNORE)
    """)
    print(con.execute(f"SELECT day, count(*) FROM read_parquet('{out_dir}/*/*.parquet', hive_partitioning=1) "
                      f"GROUP BY day ORDER BY day").fetchall())


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
