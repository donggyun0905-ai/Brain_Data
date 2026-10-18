"""실제 데이터 실측 (docs/week1.md '데이터' 항목). prepare로 만든 날짜별 Parquet을 읽는다.

    python tools/profile_data.py data/parquet/2019-11

재는 것
1. 행 수, 날짜 범위, 날짜별 행 수 (폭증한 날 찾기)
2. 이벤트 종류 비율 (기획서 가정: 조회 92%, 장바구니 6.5%, 구매 1.5%)
3. 완전히 같은 중복 행 수 (같은 event_id)
4. 사용자별 활동량: 상위 1%·0.1% 사용자가 조회의 몇 %를 만드는지 (혼합 샘플링 기준)
5. 초당 이벤트 수 분포 (실제 시간 기준 평균·p99·최대, 100배속이면 얼마인지)
"""
from __future__ import annotations

import json
import sys


def main(path: str, memory_limit: str = "1GB") -> None:
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{path}_duckdb_tmp'")   # 넘치는 집계는 디스크로
    con.execute(f"CREATE VIEW e AS SELECT * FROM read_parquet('{path}/*/*.parquet', hive_partitioning=1)")
    q = lambda sql: con.execute(sql).fetchall()
    out = {}

    n, t0, t1 = q("SELECT count(*), min(event_time), max(event_time) FROM e")[0]
    out["rows"], out["from"], out["to"] = n, str(t0), str(t1)
    out["per_day"] = {str(d): c for d, c in q("SELECT day, count(*) FROM e GROUP BY day ORDER BY day")}

    out["event_type_pct"] = {t: round(100 * c / n, 2) for t, c in q(
        "SELECT event_type, count(*) FROM e GROUP BY 1 ORDER BY 2 DESC")}
    out["lane_pct"] = {t: round(100 * c / n, 2) for t, c in q(
        "SELECT lane, count(*) FROM e GROUP BY 1 ORDER BY 2 DESC")}

    # 완전히 같은 행은 시각도 같다 → 날짜별로 세어 더해도 같고, 메모리는 1/30만 쓴다
    dup_rows = dup_ids = 0
    for d in out["per_day"]:
        r, i = q(f"SELECT coalesce(sum(c - 1), 0), count(*) FROM (SELECT event_id, count(*) c FROM "
                 f"read_parquet('{path}/day={d}/*.parquet') GROUP BY 1 HAVING count(*) > 1)")[0]
        dup_rows += int(r); dup_ids += i
    out["duplicate_extra_rows"], out["duplicated_ids"] = int(dup_rows), dup_ids
    out["duplicate_pct"] = round(100 * dup_rows / n, 3)

    users, = q("SELECT count(DISTINCT user_id) FROM e")[0]
    out["users"] = users
    share = q("""
        WITH u AS (SELECT user_id, count(*) c FROM e WHERE lane = 'view' GROUP BY 1),
             r AS (SELECT c, row_number() OVER (ORDER BY c DESC) rk, count(*) OVER () nu, sum(c) OVER () tot FROM u)
        SELECT round(100 * sum(c) FILTER (WHERE rk <= nu * 0.01) / max(tot), 1),
               round(100 * sum(c) FILTER (WHERE rk <= nu * 0.001) / max(tot), 1),
               max(c), quantile_cont(c, 0.5), quantile_cont(c, 0.99)
        FROM r""")[0]
    out["view_share_top1pct_users"], out["view_share_top01pct_users"] = share[0], share[1]
    out["views_per_user_max"], out["views_per_user_p50"], out["views_per_user_p99"] = share[2], share[3], share[4]

    eps = q("""
        WITH s AS (SELECT date_trunc('second', event_time) s, count(*) c FROM e GROUP BY 1)
        SELECT avg(c), quantile_cont(c, 0.99), max(c) FROM s""")[0]
    span = (t1 - t0).total_seconds()
    out["eps_mean_wall"] = round(n / span, 1)
    out["eps_busy_seconds_mean"], out["eps_p99"], out["eps_max"] = round(eps[0], 1), eps[1], eps[2]
    out["eps_at_100x_mean"] = round(100 * n / span)

    peak = q("""
        SELECT date_trunc('hour', event_time) h, count(*) c FROM e GROUP BY 1 ORDER BY 2 DESC LIMIT 5""")
    out["busiest_hours"] = {str(h): c for h, c in peak}
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(sys.argv[1], *sys.argv[2:3])
