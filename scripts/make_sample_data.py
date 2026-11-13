#!/usr/bin/env python3
"""生成检修日志样本数据 events.parquet.

表结构: events(event_id VARCHAR, reported_at BIGINT, restored_at BIGINT)
  - reported_at / restored_at 为 UTC 秒(Unix epoch)
  - 故意掺入两类脏数据, 便于网页试跑筛错 SQL:
      1) 重号: 同一 event_id 出现多条记录
      2) 修复早于报障: restored_at < reported_at
"""

from __future__ import annotations

import argparse
import os
import random

import duckdb

BASE_EPOCH = 1754000000  # 约 2025-08-01 UTC, 仅作样本时间基准


def build_rows(n_rows: int, seed: int) -> list[tuple]:
    rng = random.Random(seed)
    rows: list[tuple] = []
    seen_ids: list[str] = []

    for i in range(n_rows):
        event_id = f"E{i:06d}"
        reported = BASE_EPOCH + rng.randint(0, 90 * 24 * 3600)      # 90 天内报障
        restored = reported + rng.randint(5 * 60, 3 * 24 * 3600)    # 5 分钟 ~ 3 天修复
        rows.append((event_id, reported, restored))
        seen_ids.append(event_id)

    # 1) 重号: 复制一批已有 event_id(时间不同), 约占 0.5%
    n_dup = max(10, n_rows // 200)
    for _ in range(n_dup):
        event_id = rng.choice(seen_ids)
        reported = BASE_EPOCH + rng.randint(0, 90 * 24 * 3600)
        restored = reported + rng.randint(5 * 60, 3 * 24 * 3600)
        rows.append((event_id, reported, restored))

    # 2) 修复早于报障, 约占 0.25%
    n_bad = max(10, n_rows // 400)
    for i in range(n_bad):
        event_id = f"E{n_rows + i:06d}"
        reported = BASE_EPOCH + rng.randint(1 * 24 * 3600, 90 * 24 * 3600)
        restored = reported - rng.randint(60, 2 * 24 * 3600)        # 修复反而更早
        rows.append((event_id, reported, restored))

    rng.shuffle(rows)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/events.parquet", help="输出 Parquet 路径")
    ap.add_argument("--rows", type=int, default=60000, help="正常记录行数")
    ap.add_argument("--seed", type=int, default=42, help="随机种子(可复现)")
    ap.add_argument("--force", action="store_true", help="已存在则覆盖")
    args = ap.parse_args()

    if os.path.exists(args.out) and not args.force:
        print(f"[seed] {args.out} 已存在, 跳过(用 --force 重新生成)")
        return

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    rows = build_rows(args.rows, args.seed)

    con = duckdb.connect()
    con.execute("CREATE TABLE events(event_id VARCHAR, reported_at BIGINT, restored_at BIGINT)")
    con.executemany("INSERT INTO events VALUES (?, ?, ?)", rows)
    con.execute(f"COPY events TO '{args.out}' (FORMAT PARQUET)")
    n = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    n_dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT event_id FROM events GROUP BY event_id "
        "HAVING COUNT(*) > 1)").fetchone()[0]
    n_bad = con.execute(
        "SELECT COUNT(*) FROM events WHERE restored_at < reported_at").fetchone()[0]
    con.close()
    print(f"[seed] 写出 {args.out}: 共 {n} 行, 其中重号 event_id {n_dup} 个, "
          f"修复早于报障 {n_bad} 行")


if __name__ == "__main__":
    main()
