#!/usr/bin/env python3
"""生成检修日志样本数据.

两种产物:
1. events.parquet —— 试查平台用的样本日志(6 万行, 含故意脏数据)
   表结构: events(event_id VARCHAR, reported_at BIGINT, restored_at BIGINT)
     - reported_at / restored_at 为 UTC 秒(Unix epoch)
     - 故意掺入两类脏数据:
         1) 重号: 同一 event_id 出现多条记录
         2) 修复早于报障: restored_at < reported_at

2. --make-batch-pair DIR —— 入仓批次演示用的一对文件:
     batch_before_correction.parquet  更正前(上游交来的带脏数据批次, 应被拦截)
     batch_after_correction.parquet   更正后(同批修正版, 规则零行, 可放行)
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
        event_id = f"B{i:06d}"
        reported = BASE_EPOCH + rng.randint(1 * 24 * 3600, 90 * 24 * 3600)
        restored = reported - rng.randint(60, 2 * 24 * 3600)        # 修复反而更早
        rows.append((event_id, reported, restored))

    rng.shuffle(rows)
    return rows


def write_parquet(rows: list[tuple], out: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE events(event_id VARCHAR, reported_at BIGINT, restored_at BIGINT)")
    con.executemany("INSERT INTO events VALUES (?, ?, ?)", rows)
    con.execute(f"COPY events TO '{out}' (FORMAT PARQUET)")
    n = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    n_dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT event_id FROM events GROUP BY event_id "
        "HAVING COUNT(*) > 1)").fetchone()[0]
    n_bad = con.execute(
        "SELECT COUNT(*) FROM events WHERE restored_at < reported_at").fetchone()[0]
    con.close()
    print(f"[seed] 写出 {out}: 共 {n} 行, 重号 event_id {n_dup} 个, "
          f"修复早于报障 {n_bad} 行")


def make_sample_events(out: str, rows: int, seed: int, force: bool) -> None:
    if os.path.exists(out) and not force:
        print(f"[seed] {out} 已存在, 跳过(用 --force 重新生成)")
        return
    write_parquet(build_rows(rows, seed), out)


def make_batch_pair(out_dir: str, rows: int, seed: int, force: bool) -> None:
    """同一批日志的更正前/更正后两个版本."""
    before_path = os.path.join(out_dir, "batch_before_correction.parquet")
    after_path = os.path.join(out_dir, "batch_after_correction.parquet")
    if (os.path.exists(before_path) and os.path.exists(after_path) and not force):
        print(f"[seed] {before_path} 与 {after_path} 均已存在, 跳过(--force 覆盖)")
        return

    rng = random.Random(seed + 7)
    before: list[tuple] = []
    for i in range(rows):
        event_id = f"EV{i:05d}"
        reported = BASE_EPOCH + rng.randint(0, 60 * 24 * 3600)
        restored = reported + rng.randint(5 * 60, 2 * 24 * 3600)
        before.append((event_id, reported, restored))

    # 上游原始批次里的两类错误
    dup_rows = []
    for i in range(8):                      # 8 个重号 event_id(各多一条)
        event_id = f"EV{rng.randrange(rows):05d}"
        reported = BASE_EPOCH + rng.randint(0, 60 * 24 * 3600)
        restored = reported + rng.randint(5 * 60, 24 * 3600)
        dup_rows.append((event_id, reported, restored))
    bad_rows = []
    for i in range(6):                      # 6 条修复早于报障
        event_id = f"BD{i:05d}"
        reported = BASE_EPOCH + rng.randint(1 * 24 * 3600, 60 * 24 * 3600)
        restored = reported - rng.randint(60, 12 * 3600)
        bad_rows.append((event_id, reported, restored))
    before.extend(dup_rows)
    before.extend(bad_rows)
    rng.shuffle(before)

    # 上游更正: 删掉重号的重复记录, 并把修复时间改到报障之后
    dup_keys = {(eid, rep, res) for eid, rep, res in dup_rows}
    after: list[tuple] = []
    for event_id, reported, restored in before:
        if (event_id, reported, restored) in dup_keys:
            continue                        # 重复记录直接删除
        if restored < reported:
            restored = reported + rng.randint(5 * 60, 24 * 3600)
        after.append((event_id, reported, restored))
    rng.shuffle(after)

    write_parquet(before, before_path)
    write_parquet(after, after_path)
    print(f"[seed] 更正前 {len(before)} 行 -> 更正后 {len(after)} 行 "
          f"(删除重号重复 {len(dup_rows)} 行, 修正时间倒挂 {len(bad_rows)} 行)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/events.parquet", help="试查样本 Parquet 路径")
    ap.add_argument("--rows", type=int, default=60000, help="正常记录行数")
    ap.add_argument("--seed", type=int, default=42, help="随机种子(可复现)")
    ap.add_argument("--force", action="store_true", help="已存在则覆盖")
    ap.add_argument("--make-batch-pair", metavar="DIR",
                    help="在 DIR 生成更正前/更正后入仓批次演示文件后退出")
    args = ap.parse_args()

    if args.make_batch_pair:
        make_batch_pair(args.make_batch_pair, min(args.rows, 5000),
                        args.seed, args.force)
        return
    make_sample_events(args.out, args.rows, args.seed, args.force)


if __name__ == "__main__":
    main()
