"""
REES46 CSV → 시간순 Parquet 변환 (1회 실행).

    python -m databrain.replayer.prepare data/raw/2019-Nov.csv data/parquet/2019-11          # 전부
    python -m databrain.replayer.prepare data/raw/2019-Nov.csv data/parquet/2019-11 split    # 1단계만
    python -m databrain.replayer.prepare data/raw/2019-Nov.csv data/parquet/2019-11 sort 10  # 2단계, 10일치만

- event_time 순으로 정렬해 날짜별 Parquet로 나눈다 → 재생기가 순서대로 빠르게 읽음
- REES46에는 이벤트 ID가 없고, 완전히 같은 행이 중복으로 들어 있다.
  그래서 행 내용으로 event_id를 만든다 → 같은 행은 같은 ID → 입구 정리 단계에서 중복 제거 가능
- remove_from_cart는 장바구니 토픽으로 보낸다 (중요 등급)
- 메모리를 아끼려고 두 단계로 나눈다 (2026-10-18: 한 달 전체를 한 번에 정렬하다 PC 메모리 부족으로 중단됨)
  1단계 split: CSV를 정렬 없이 날짜별로만 나눠 '<출력>_staging'에 쓴다 (흘려 쓰기라 메모리 적게 씀)
  2단계 sort : 날짜 하나씩 시간순 정렬해 '<출력>/day=날짜/data_0.parquet'에 쓴다. 이미 된 날짜는 건너뜀
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path


def _connect(tmp: Path, memory_limit: str):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{tmp.as_posix()}'")
    return con


def split(src: str, out_dir: str, memory_limit: str = "1GB") -> None:  # pragma: no cover - 실제 파일 필요
    stage = Path(out_dir + "_staging")
    if (stage / "_DONE").exists():
        print("1단계 이미 끝남:", stage)
        return
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir(parents=True)
    con = _connect(stage.parent / "_duckdb_tmp", memory_limit)
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
        ) TO '{stage.as_posix()}' (FORMAT PARQUET, PARTITION_BY (day), OVERWRITE_OR_IGNORE)
    """)
    (stage / "_DONE").touch()
    print("1단계 끝:", sorted(p.name for p in stage.glob("day=*")))


def sort_days(out_dir: str, limit: int | None = None, memory_limit: str = "1GB") -> None:  # pragma: no cover
    stage, out = Path(out_dir + "_staging"), Path(out_dir)
    if not (stage / "_DONE").exists():
        raise SystemExit("1단계(split)가 끝나지 않았습니다")
    con = _connect(stage.parent / "_duckdb_tmp", memory_limit)
    todo = [d for d in sorted(stage.glob("day=*")) if not (out / d.name / "data_0.parquet").exists()]
    for d in todo[:limit]:
        (out / d.name).mkdir(parents=True, exist_ok=True)
        part = out / d.name / "data_0.parquet.part"      # 중간에 끊겨도 반쪽 파일이 '끝난 날짜'로 보이지 않게
        con.execute(f"""COPY (SELECT * FROM read_parquet('{d.as_posix()}/*.parquet') ORDER BY event_time)
                        TO '{part.as_posix()}' (FORMAT PARQUET)""")
        part.rename(out / d.name / "data_0.parquet")
        print("정렬 끝:", d.name, flush=True)
    left = [d for d in sorted(stage.glob("day=*")) if not (out / d.name / "data_0.parquet").exists()]
    if not left:
        shutil.rmtree(stage)
        shutil.rmtree(stage.parent / "_duckdb_tmp", ignore_errors=True)
        print(con.execute(f"SELECT day, count(*) FROM read_parquet('{out.as_posix()}/*/*.parquet', hive_partitioning=1) "
                          f"GROUP BY day ORDER BY day").fetchall())
    else:
        print("남은 날짜:", len(left))


def main(src: str, out_dir: str) -> None:  # pragma: no cover
    split(src, out_dir)
    sort_days(out_dir)


if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) >= 3 and a[2] == "split":
        split(a[0], a[1])
    elif len(a) >= 3 and a[2] == "sort":
        sort_days(a[1], int(a[3]) if len(a) > 3 else None)
    else:
        main(a[0], a[1])
