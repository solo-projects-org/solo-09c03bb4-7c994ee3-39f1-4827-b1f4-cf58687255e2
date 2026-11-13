"""检修日志筛错平台: SQL 试跑 + 入仓批次检查放行.

FastAPI + DuckDB:
- 只读 Parquet 挂载为 events 表(event_id, reported_at, restored_at, UTC 秒)
- 仅允许单条 SELECT / WITH 查询
- 全站最多 MAX_CONCURRENT 条并发查询(试查与批次检查共用), 超额直接 429(不排队)
- 查询可取消; 运行满 QUERY_TIMEOUT_SECONDS 秒自动终止
- 取消/超时只有在后台计算真正停止后才确认并腾出名额
- 取消与完成的竞态只有一个最终状态; 已取消/超时的查询不发布结果

批次放行流程:
- 网页可保存一条 SELECT 作为"现行规则", 每次保存产生新版本 rule_id
  (即使 SQL 改回原样也是新版本)
- 上传沿用 events 三列及类型的 Parquet 发起检查, 规则在该批文件上执行,
  返回零行且规则版本未变才可放行; 出错/有错行/取消/超时均不放行
- 规则保存与放行并发时, 以全局锁内的实际生效次序判断, 不沿用过期结论
- 放行后按批号可下载原文件、查看当时的规则与结论快照; 后续改规则不撤回旧批
- 批次文件与放行记录全部落在数据卷; 保存失败不会留下可下载的放行批
- 重启后未放行(检查中/待放行)批次一律标记为失败; 已放行批仍可下载
"""

from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Callable

import duckdb
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------- 配置

PARQUET_PATH = os.environ.get("EVENTS_PARQUET", "data/events.parquet")
DATA_DIR = Path(os.environ.get("APP_DATA_DIR", "data/appdata"))
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT_QUERIES", "2"))
QUERY_TIMEOUT_SECONDS = float(os.environ.get("QUERY_TIMEOUT_SECONDS", "10"))
MAX_ROWS = 200                 # 试查页面最多展示的行数
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))
JOB_TTL_SECONDS = 600          # 已完成内存任务结果保留时间
RULE_HISTORY_LIMIT = 50        # 规则历史最多保留版本数
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS_PER_QUERY", "2"))
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

REQUIRED_COLUMNS = [
    ("event_id", "VARCHAR"),
    ("reported_at", "BIGINT"),
    ("restored_at", "BIGINT"),
]

DEFAULT_RULE_SQL = """\
-- 入仓前筛错: 重号 或 修复早于报障 的记录, 退回上游更正
WITH checks AS (
  SELECT event_id, reported_at, restored_at,
         COUNT(*) OVER (PARTITION BY event_id) AS id_cnt
  FROM events
)
SELECT event_id,
       to_timestamp(reported_at) AS reported_at_utc,
       to_timestamp(restored_at) AS restored_at_utc,
       concat_ws('; ',
                 CASE WHEN id_cnt > 1 THEN 'duplicate_event_id' END,
                 CASE WHEN restored_at < reported_at THEN 'restored_before_reported' END
       ) AS issues
FROM checks
WHERE id_cnt > 1 OR restored_at < reported_at
ORDER BY event_id"""

# ---------------------------------------------------------------- SQL 校验


def _strip_leading_comments(sql: str) -> str:
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl == -1:
                return ""
            s = s[nl + 1:].lstrip()
        elif s.startswith("/*"):
            end = s.find("*/")
            if end == -1:
                return ""
            s = s[end + 2:].lstrip()
        else:
            return s


def _mask_literals(sql: str) -> str:
    """把字符串字面量/引号标识符/行注释的内容抹掉, 避免其中的分号被误判."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:   # '' / "" 转义
                        j += 2
                        continue
                    break
                j += 1
            out.append(" ")
            i = j + 1
        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i)
            i = n if nl == -1 else nl
        else:
            out.append(c)
            i += 1
    return "".join(out)


def validate_select(sql: str) -> str | None:
    """只允许单条 SELECT/WITH 语句; 返回错误信息或 None."""
    if not sql or not sql.strip():
        return "SQL 不能为空"
    masked = _mask_literals(sql).strip()
    # 允许末尾一个分号, 语句中间不允许出现分号(防止多语句)
    if masked.endswith(";"):
        masked = masked[:-1]
    if ";" in masked:
        return "只允许单条 SELECT 语句(语句中间不允许分号)"
    head = _strip_leading_comments(masked)
    m = re.match(r"([A-Za-z]+)", head)
    if not m or m.group(1).lower() not in ("select", "with"):
        return "只允许 SELECT / WITH 查询(只读)"
    return None


def jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


# ---------------------------------------------------------------- 查询任务


class QueryJob:
    """一条查询的完整生命周期(试查与批次检查共用).

    状态机(全部在 self.lock 内迁移, 终态唯一):
        running -> completed | failed | cancelled | timeout

    stop_reason 由取消/超时请求方先写入; 工作线程收尾时:
      - stop_reason 已设置 -> cancelled/timeout, 丢弃已算出的结果
      - 否则按执行结果 -> completed/failed
    因此"取消撞上完成"由锁的先后顺序决定唯一终态, 且被取消/超时的
    任务永远不会发布结果(结果只写入本任务对象, 不会覆盖其他查询)。
    """

    def __init__(self, manager: "QueryManager", sql: str, parquet_path: str,
                 on_finish: Callable[["QueryJob"], None] | None = None):
        self.manager = manager
        self.id = uuid.uuid4().hex[:12]
        self.sql = sql
        self.parquet_path = parquet_path
        self.on_finish = on_finish
        self.lock = threading.Lock()
        self.done = threading.Event()      # 后台计算完全停止后置位
        self.status = "running"
        self.stop_reason: str | None = None   # "cancelled" | "timeout"
        self.columns: list[str] | None = None
        self.rows: list[list] | None = None
        self.truncated = False
        self.row_count: int = 0            # 实际命中行数(含截断部分)
        self.error: str | None = None
        self.started_at = time.time()
        self.finished_at: float | None = None
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._timer: threading.Timer | None = None
        self._thread: threading.Thread | None = None

    # -- 生命周期 -------------------------------------------------

    def start(self) -> None:
        self._timer = threading.Timer(QUERY_TIMEOUT_SECONDS, self._on_timeout)
        self._timer.daemon = True
        self._thread = threading.Thread(target=self._worker, daemon=True,
                                        name=f"query-{self.id}")
        self._thread.start()
        self._timer.start()

    def _on_timeout(self) -> None:
        with self.lock:
            if self.status != "running" or self.stop_reason is not None:
                return
            self.stop_reason = "timeout"
        self._interrupt()

    def request_cancel(self) -> str:
        """请求取消; 阻塞到后台计算真正停止后才返回最终状态."""
        with self.lock:
            if self.status != "running":
                return self.status            # 已完成/失败/取消/超时: 终态唯一
            if self.stop_reason is None:
                self.stop_reason = "cancelled"
        self._interrupt()
        # 后台计算停止(并腾出名额)之后, 取消才算确认
        self.done.wait(timeout=QUERY_TIMEOUT_SECONDS + 30)
        with self.lock:
            return self.status

    def _interrupt(self) -> None:
        with self.lock:
            conn = self._conn
        if conn is not None:
            try:
                conn.interrupt()              # 只打断本查询的连接, 不影响其他查询
            except Exception:
                pass

    # -- 工作线程 -------------------------------------------------

    def _worker(self) -> None:
        conn = None
        try:
            conn = duckdb.connect(database=":memory:")
            conn.execute(f"SET threads={DUCKDB_THREADS}")
            conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
            # 只读 Parquet 挂载为 events 表(视图, 不复制数据)
            safe_path = self.parquet_path.replace("'", "''")
            conn.execute(
                f"CREATE VIEW events AS SELECT * FROM read_parquet('{safe_path}')"
            )
            with self.lock:
                self._conn = conn
                pre_stopped = self.stop_reason is not None
            if pre_stopped:
                return

            cur = conn.execute(self.sql)
            columns = [d[0] for d in cur.description]
            fetched = cur.fetchmany(MAX_ROWS + 1)   # 多取 1 行用于判断"超出 200 行"
            truncated = len(fetched) > MAX_ROWS
            rows = [[jsonable(v) for v in row] for row in fetched[:MAX_ROWS]]
            # 零行判定直接看抓取结果; 非零时 row_count 为已抓取行数(截断时为上限)
            row_count = len(fetched[:MAX_ROWS])

            with self.lock:
                if self.stop_reason is None:
                    # 取消/超时还没抢到锁 -> 本查询正常发布结果
                    self.columns = columns
                    self.rows = rows
                    self.truncated = truncated
                    self.row_count = row_count
                # 否则: 取消/超时已生效, 结果直接丢弃, 不发布
        except Exception as exc:  # 含 duckdb 中断异常
            with self.lock:
                if self.stop_reason is None and self.error is None:
                    self.error = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            with self.lock:
                # 后台计算已停止, 此刻才能确认终态
                if self.stop_reason is not None:
                    self.status = self.stop_reason
                elif self.error is not None:
                    self.status = "failed"
                else:
                    self.status = "completed"
                self.finished_at = time.time()
                self._conn = None
            if self._timer is not None:
                self._timer.cancel()
            # 计算停止、终态确认后才腾出名额
            self.manager.release_slot()
            # 先同步完成回调(检查结论落盘), 再置位 done:
            # 取消方从 done.wait() 返回时, 持久化结论一定已写好
            if self.on_finish is not None:
                try:
                    self.on_finish(self)
                except Exception:
                    pass
            self.done.set()

    # -- 序列化 -------------------------------------------------

    def snapshot(self) -> dict:
        with self.lock:
            now = time.time()
            snap = {
                "query_id": self.id,
                "status": self.status,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "elapsed_seconds": round(
                    (self.finished_at or now) - self.started_at, 3),
                "timeout_seconds": QUERY_TIMEOUT_SECONDS,
                "error": self.error,
            }
            if self.status == "completed":
                snap.update({
                    "columns": self.columns,
                    "rows": self.rows,
                    "row_count": self.row_count,
                    "truncated": self.truncated,
                    "max_rows": MAX_ROWS,
                })
            return snap


class QueryManager:
    """并发闸门: 全站最多 MAX_CONCURRENT 条(试查+检查共用), 不排队."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._slots = MAX_CONCURRENT
        self._jobs: dict[str, QueryJob] = {}

    def slots_available(self) -> int:
        with self._lock:
            return self._slots

    def submit(self, sql: str, parquet_path: str,
               on_finish: Callable[[QueryJob], None] | None = None) -> QueryJob | None:
        with self._lock:
            self._purge_locked()
            if self._slots <= 0:
                return None                    # 繁忙, 不排队
            self._slots -= 1
            job = QueryJob(self, sql, parquet_path, on_finish=on_finish)
            self._jobs[job.id] = job
        job.start()
        return job

    def release_slot(self) -> None:
        with self._lock:
            self._slots += 1

    def get(self, qid: str) -> QueryJob | None:
        with self._lock:
            return self._jobs.get(qid)

    def _purge_locked(self) -> None:
        cutoff = time.time() - JOB_TTL_SECONDS
        dead = [qid for qid, j in self._jobs.items()
                if j.status != "running" and (j.finished_at or 0) < cutoff]
        for qid in dead:
            del self._jobs[qid]


MANAGER = QueryManager()

# ---------------------------------------------------------------- 持久化存储


class Store:
    """规则与批次的持久化(数据卷), 进程内一把锁保证生效次序."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.batches_dir = root / "batches"
        self.tmp_dir = root / "tmp"
        self.rules_path = root / "rules.json"
        self.batches_index = root / "batches.json"
        self.lock = threading.RLock()
        self.root.mkdir(parents=True, exist_ok=True)
        self.batches_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.rules: dict = self._read_json(self.rules_path, default={
            "current_id": None, "history": []})
        self.batches: dict[str, dict] = self._read_json(
            self.batches_index, default={"batches": {}})["batches"]
        self._recover_on_startup()
        if self.rules.get("current_id") is None:
            # 首次启动写入默认规则, 保证演示上传即可检查
            self._save_rule_locked(DEFAULT_RULE_SQL)

    # -- 基础工具 -------------------------------------------------

    @staticmethod
    def _read_json(path: Path, default: dict) -> dict:
        if not path.exists():
            return json.loads(json.dumps(default))
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            # 损坏的索引不拖垮启动: 以空状态重建, 已放行批的文件仍在磁盘
            return json.loads(json.dumps(default))

    def _atomic_write_json(self, path: Path, payload: dict) -> None:
        """同卷临时文件 + os.replace 原子替换; 失败不留半成品."""
        tmp = self.tmp_dir / f".{path.name}.{uuid.uuid4().hex}.tmp"
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _recover_on_startup(self) -> None:
        """重启恢复: 未放行的批次(检查中/待放行)一律标记失败."""
        changed = False
        now = time.time()
        for batch in self.batches.values():
            if batch["status"] in ("uploaded", "checking", "ready"):
                batch["status"] = "failed"
                insp = batch.get("inspection")
                if insp and insp.get("status") == "running":
                    insp["status"] = "failed"
                    insp["error"] = "服务重启, 检查中断, 本批判定失败(需重新检查)"
                    insp["finished_at"] = now
                batch["failure_reason"] = "服务重启, 批次未完成放行, 判定失败"
                batch["updated_at"] = now
                changed = True
        if changed:
            self._atomic_write_json(self.batches_index, {"batches": self.batches})

    # -- 规则 -----------------------------------------------------

    def _save_rule_locked(self, sql: str) -> dict:
        history: list[dict] = self.rules.setdefault("history", [])
        version = (history[-1]["version"] + 1) if history else 1
        rule = {
            "rule_id": uuid.uuid4().hex[:16],
            "version": version,
            "sql": sql,
            "saved_at": time.time(),
        }
        history.append(rule)
        if len(history) > RULE_HISTORY_LIMIT:
            del history[:-RULE_HISTORY_LIMIT]
        self.rules["current_id"] = rule["rule_id"]
        self._atomic_write_json(self.rules_path, self.rules)
        return rule

    def save_rule(self, sql: str) -> dict:
        with self.lock:
            return self._save_rule_locked(sql)

    def current_rule(self) -> dict | None:
        with self.lock:
            return self.get_rule_locked(self.rules.get("current_id"))

    def get_rule_locked(self, rule_id: str | None) -> dict | None:
        if not rule_id:
            return None
        for rule in reversed(self.rules.get("history", [])):
            if rule["rule_id"] == rule_id:
                return rule
        return None

    def rule_snapshot(self) -> dict:
        with self.lock:
            cur = self.get_rule_locked(self.rules.get("current_id"))
            return {
                "current": None if cur is None else {
                    "rule_id": cur["rule_id"],
                    "version": cur["version"],
                    "sql": cur["sql"],
                    "saved_at": cur["saved_at"],
                },
                "history": [
                    {"rule_id": r["rule_id"], "version": r["version"],
                     "sql": r["sql"], "saved_at": r["saved_at"]}
                    for r in self.rules.get("history", [])
                ],
            }

    # -- 批次上传 -------------------------------------------------

    def create_batch(self, filename: str, tmp_path: Path, size: int) -> dict:
        """校验并固化上传文件; 任一步失败都清理, 不留下批次记录."""
        with self.lock:
            batch_id = uuid.uuid4().hex[:16]
            batch_dir = self.batches_dir / batch_id
            batch_dir.mkdir(parents=True, exist_ok=False)
            dest = batch_dir / "source.parquet"
            try:
                error = validate_parquet(tmp_path)
                if error:
                    raise ValueError(error)
                os.replace(tmp_path, dest)
                batch = {
                    "batch_id": batch_id,
                    "original_filename": filename,
                    "stored_path": str(dest),
                    "size_bytes": size,
                    "status": "uploaded",       # uploaded/checking/ready/released/failed
                    "created_at": time.time(),
                    "updated_at": time.time(),
                    "rule_id": None,
                    "inspection": None,
                    "release": None,
                    "failure_reason": None,
                }
                self.batches[batch_id] = batch
                # 索引写失败必须回滚内存与目录, 不能留下"幽灵批次"
                self._atomic_write_json(self.batches_index,
                                        {"batches": self.batches})
                return json_safe(batch)
            except Exception:
                self.batches.pop(batch_id, None)
                shutil.rmtree(batch_dir, ignore_errors=True)
                raise

    def get_batch(self, batch_id: str) -> dict | None:
        with self.lock:
            batch = self.batches.get(batch_id)
            return None if batch is None else json_safe(batch)

    def list_batches(self) -> list[dict]:
        with self.lock:
            return [json_safe(b) for bid, b in sorted(
                self.batches.items(), key=lambda kv: kv[1]["created_at"],
                reverse=True)]

    # -- 批次检查 -------------------------------------------------

    def begin_inspection(self, batch_id: str) -> tuple[dict | None, dict | None, str | None]:
        """返回 (batch, rule, error). 检查中/已放行的批次不允许重复发起."""
        with self.lock:
            batch = self.batches.get(batch_id)
            if batch is None:
                return None, None, "批次不存在"
            if batch["status"] in ("checking",):
                return None, None, "该批次已有检查在运行中"
            if batch["status"] == "released":
                return None, None, "该批次已放行, 不能重复检查"
            rule = self.get_rule_locked(self.rules.get("current_id"))
            if rule is None:
                return None, None, "尚无现行规则, 请先在上方保存一条 SELECT 规则"
            batch["status"] = "checking"
            batch["rule_id"] = rule["rule_id"]
            batch["inspection"] = {
                "query_id": None,
                "rule_id": rule["rule_id"],
                "rule_version": rule["version"],
                "status": "running",
                "started_at": time.time(),
                "finished_at": None,
                "elapsed_seconds": None,
                "row_count": None,
                "error": None,
            }
            batch["failure_reason"] = None
            batch["updated_at"] = time.time()
            self._atomic_write_json(self.batches_index, {"batches": self.batches})
            return json_safe(batch), json_safe(rule), None

    def attach_query_id(self, batch_id: str, query_id: str) -> None:
        with self.lock:
            batch = self.batches.get(batch_id)
            if batch and batch.get("inspection"):
                batch["inspection"]["query_id"] = query_id
                self._atomic_write_json(self.batches_index,
                                        {"batches": self.batches})

    def record_inspection_result(self, batch_id: str, job: QueryJob) -> None:
        """检查终态落盘(完成即持久化, 重启也不丢结论)."""
        with self.lock:
            batch = self.batches.get(batch_id)
            if batch is None or batch.get("inspection") is None:
                return
            insp = batch["inspection"]
            if insp["status"] != "running":
                return
            insp["status"] = job.status
            insp["finished_at"] = job.finished_at
            insp["elapsed_seconds"] = job.snapshot()["elapsed_seconds"]
            if job.status == "completed":
                insp["row_count"] = job.row_count
                if job.row_count == 0:
                    batch["status"] = "ready"
                else:
                    batch["status"] = "failed"
                    more = "以上" if job.truncated else ""
                    batch["failure_reason"] = (
                        f"规则命中 {job.row_count}{more} 行错行, 不放行")
            elif job.status == "failed":
                batch["status"] = "failed"
                insp["error"] = job.error
                batch["failure_reason"] = "规则执行出错, 不放行"
            elif job.status == "cancelled":
                batch["status"] = "failed"
                batch["failure_reason"] = "检查被取消, 不放行"
            elif job.status == "timeout":
                batch["status"] = "failed"
                batch["failure_reason"] = "检查超过 10 秒被终止, 不放行"
            batch["updated_at"] = time.time()
            self._atomic_write_json(self.batches_index, {"batches": self.batches})

    # -- 放行 -----------------------------------------------------

    def release(self, batch_id: str, claimed_rule_id: str | None) -> tuple[dict | None, str | None]:
        """按锁内实际生效次序判断; 规则被改过(任何保存, 含改回原文)即拒绝."""
        with self.lock:
            batch = self.batches.get(batch_id)
            if batch is None:
                return None, "批次不存在"
            if batch["status"] == "released":
                return json_safe(batch), None
            current = self.get_rule_locked(self.rules.get("current_id"))
            current_id = current["rule_id"] if current else None
            if claimed_rule_id is not None and claimed_rule_id != current_id:
                return None, ("现行规则已被修改(规则已保存为新版本), "
                              "本次放行拒绝, 请用新规则重新检查")
            if batch["status"] != "ready":
                reason = batch.get("failure_reason") or "批次尚未通过检查"
                return None, f"当前不可放行: {reason}"
            insp = batch.get("inspection") or {}
            if insp.get("status") != "completed" or insp.get("row_count") != 0:
                return None, "检查结论不是零行通过, 不放行"
            if insp.get("rule_id") != current_id:
                return None, ("检查所用规则已被修改(即使 SQL 改回原文也是新版本), "
                              "本次放行拒绝, 请重新检查")
            rule = self.get_rule_locked(insp["rule_id"]) or current
            release = {
                "released_at": time.time(),
                "rule_id": rule["rule_id"],
                "rule_version": rule["version"],
                "rule_sql": rule["sql"],
                "conclusion": {
                    "status": insp["status"],
                    "row_count": insp["row_count"],
                    "started_at": insp.get("started_at"),
                    "finished_at": insp.get("finished_at"),
                    "elapsed_seconds": insp.get("elapsed_seconds"),
                    "query_id": insp.get("query_id"),
                },
            }
            # 先记下旧状态; 落盘失败必须回滚内存, 绝不能留下可下载的"假放行"
            prev_status = batch["status"]
            prev_release = batch.get("release")
            prev_updated = batch.get("updated_at")
            batch["status"] = "released"
            batch["release"] = release
            batch["updated_at"] = time.time()
            try:
                # 放行记录原子落盘; 只有写成功, 放行才真正生效
                self._atomic_write_json(self.batches_index,
                                        {"batches": self.batches})
            except Exception:
                batch["status"] = prev_status
                batch["release"] = prev_release
                batch["updated_at"] = prev_updated
                raise
            return json_safe(batch), None

    def released_file(self, batch_id: str) -> tuple[Path, str] | None:
        with self.lock:
            batch = self.batches.get(batch_id)
            if batch is None or batch["status"] != "released":
                return None
            path = Path(batch["stored_path"])
            if not path.exists():
                return None
            return path, batch["original_filename"]


def json_safe(obj):
    return json.loads(json.dumps(obj, ensure_ascii=False))


def validate_parquet(path: Path) -> str | None:
    """必须沿用 events 三列及类型; 返回错误信息或 None."""
    conn = None
    try:
        conn = duckdb.connect(database=":memory:")
        safe = str(path).replace("'", "''")
        cols = conn.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE SELECT * "
            f"FROM read_parquet('{safe}'))").fetchall()
    except Exception as exc:
        return f"无法读取 Parquet 文件: {type(exc).__name__}: {exc}"
    finally:
        if conn is not None:
            conn.close()
    actual = [(name, typ.upper()) for name, typ in cols]
    expected = [(name, typ) for name, typ in REQUIRED_COLUMNS]
    if actual != expected:
        actual_desc = ", ".join(f"{n} {t}" for n, t in actual) or "空表/无列"
        expected_desc = ", ".join(f"{n} {t}" for n, t in expected)
        return (f"Parquet 结构不符合要求。需要且仅需要列 [{expected_desc}], "
                f"实际为 [{actual_desc}]")
    return None


STORE = Store(DATA_DIR)

# ---------------------------------------------------------------- HTTP API

app = FastAPI(title="检修日志筛错与入仓放行平台")


class QueryRequest(BaseModel):
    sql: str


class RuleRequest(BaseModel):
    sql: str


class ReleaseRequest(BaseModel):
    rule_id: str | None = None


# -- 试查(原功能) ----------------------------------------------


@app.post("/api/queries", status_code=201)
def submit_query(req: QueryRequest):
    err = validate_select(req.sql or "")
    if err:
        raise HTTPException(status_code=400, detail=err)
    job = MANAGER.submit(req.sql.strip(), PARQUET_PATH)
    if job is None:
        return JSONResponse(
            status_code=429,
            content={
                "detail": f"繁忙: 全站最多同时运行 {MAX_CONCURRENT} 条查询"
                          "(试查与批次检查共用名额), 当前名额已满, "
                          "请稍后再试(不排队)。",
                "slots_available": 0,
                "max_concurrent": MAX_CONCURRENT,
            },
        )
    return {"query_id": job.id, "status": job.status,
            "slots_available": MANAGER.slots_available(),
            "max_concurrent": MAX_CONCURRENT}


@app.get("/api/queries/{qid}")
def query_status(qid: str):
    job = MANAGER.get(qid)
    if job is None:
        raise HTTPException(status_code=404, detail="查询不存在或结果已过期")
    snap = job.snapshot()
    snap["slots_available"] = MANAGER.slots_available()
    snap["max_concurrent"] = MAX_CONCURRENT
    return snap


@app.post("/api/queries/{qid}/cancel")
def cancel_query(qid: str):
    job = MANAGER.get(qid)
    if job is None:
        raise HTTPException(status_code=404, detail="查询不存在或结果已过期")
    # 阻塞到后台计算真正停止、名额腾出后才返回
    final_status = job.request_cancel()
    snap = job.snapshot()
    snap["status"] = final_status
    snap["slots_available"] = MANAGER.slots_available()
    snap["max_concurrent"] = MAX_CONCURRENT
    return snap


# -- 现行规则 --------------------------------------------------


@app.get("/api/rules")
def get_rules():
    return STORE.rule_snapshot()


@app.put("/api/rules/current")
def save_rule(req: RuleRequest):
    err = validate_select(req.sql or "")
    if err:
        raise HTTPException(status_code=400, detail=err)
    rule = STORE.save_rule(req.sql.strip())
    return {"rule_id": rule["rule_id"], "version": rule["version"],
            "sql": rule["sql"], "saved_at": rule["saved_at"]}


# -- 批次: 上传 / 检查 / 放行 / 下载 ---------------------------


@app.post("/api/batches", status_code=201)
async def upload_batch(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="缺少上传文件名")
    if not file.filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail="只接受 .parquet 文件")
    batch_token = uuid.uuid4().hex
    tmp_path = STORE.tmp_dir / f"upload-{batch_token}.parquet"
    size = 0
    try:
        with tmp_path.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"文件超过上限 {MAX_UPLOAD_BYTES} 字节")
                out.write(chunk)
    except HTTPException:
        tmp_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400,
                            detail=f"接收上传文件失败: {exc}")
    safe_name = os.path.basename(file.filename)
    try:
        batch = STORE.create_batch(safe_name, tmp_path, size)
    except ValueError as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc))
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500,
                            detail=f"批次保存失败, 未留下任何放行批: {exc}")
    return batch


@app.get("/api/batches")
def list_batches():
    return {"batches": STORE.list_batches()}


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str):
    batch = STORE.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return batch


@app.post("/api/batches/{batch_id}/inspections", status_code=201)
def start_inspection(batch_id: str):
    batch, rule, error = STORE.begin_inspection(batch_id)
    if error:
        # 409 表示批次当前状态不允许该操作; 无规则属于前置条件缺失也归 409
        raise HTTPException(status_code=409, detail=error)

    def _on_finish(job: QueryJob) -> None:
        STORE.record_inspection_result(batch_id, job)

    job = MANAGER.submit(rule["sql"], batch["stored_path"], on_finish=_on_finish)
    if job is None:
        # 名额没抢到: 回滚批次为 uploaded, 不占用批次状态
        with STORE.lock:
            b = STORE.batches.get(batch_id)
            if b is not None and b["status"] == "checking":
                b["status"] = "uploaded"
                b["inspection"] = None
                b["updated_at"] = time.time()
                STORE._atomic_write_json(STORE.batches_index,
                                         {"batches": STORE.batches})
        return JSONResponse(
            status_code=429,
            content={
                "detail": f"繁忙: 全站最多同时运行 {MAX_CONCURRENT} 条查询"
                          "(试查与批次检查共用名额), 当前名额已满, "
                          "请稍后再试(不排队)。",
                "slots_available": 0,
                "max_concurrent": MAX_CONCURRENT,
            },
        )
    STORE.attach_query_id(batch_id, job.id)
    return {"batch_id": batch_id, "query_id": job.id,
            "rule_id": rule["rule_id"], "rule_version": rule["version"],
            "status": "running",
            "slots_available": MANAGER.slots_available(),
            "max_concurrent": MAX_CONCURRENT}


@app.post("/api/batches/{batch_id}/inspections/cancel")
def cancel_inspection(batch_id: str):
    batch = STORE.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    insp = batch.get("inspection") or {}
    qid = insp.get("query_id")
    if not qid:
        raise HTTPException(status_code=409, detail="该批次没有运行中的检查")
    job = MANAGER.get(qid)
    if job is None:
        # 内存任务已不在(如重启): 持久化状态应已在启动时标失败
        refreshed = STORE.get_batch(batch_id)
        return refreshed
    job.request_cancel()   # 阻塞到后台停止; on_finish 回调已同步落盘
    return STORE.get_batch(batch_id)


@app.post("/api/batches/{batch_id}/release")
def release_batch(batch_id: str, req: ReleaseRequest):
    try:
        batch, error = STORE.release(batch_id, req.rule_id)
    except OSError as exc:
        # 落盘失败, Store 已回滚内存状态: 该批不会变成可下载的放行批
        raise HTTPException(
            status_code=500,
            detail=f"放行记录保存失败, 本次放行未生效(未留下可下载放行批): {exc}")
    if error:
        raise HTTPException(status_code=409, detail=error)
    return batch


@app.get("/api/batches/{batch_id}/download")
def download_batch(batch_id: str):
    found = STORE.released_file(batch_id)
    if found is None:
        raise HTTPException(status_code=404,
                            detail="文件不存在或批次尚未放行(仅已放行批可下载)")
    path, original_name = found
    return FileResponse(
        path, filename=original_name, media_type="application/octet-stream")


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "parquet": PARQUET_PATH,
        "parquet_exists": os.path.exists(PARQUET_PATH),
        "data_dir": str(DATA_DIR),
        "slots_available": MANAGER.slots_available(),
        "max_concurrent": MAX_CONCURRENT,
        "timeout_seconds": QUERY_TIMEOUT_SECONDS,
        "max_rows": MAX_ROWS,
        "max_upload_bytes": MAX_UPLOAD_BYTES,
    }


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
