"""检修日志筛错 SQL 试跑平台.

FastAPI + DuckDB:
- 只读 Parquet 挂载为 events 表(event_id, reported_at, restored_at, UTC 秒)
- 仅允许单条 SELECT / WITH 查询
- 全站最多 MAX_CONCURRENT 条并发查询, 超额直接 429(繁忙, 不排队)
- 查询可取消; 运行满 QUERY_TIMEOUT_SECONDS 秒自动终止
- 取消/超时只有在后台计算真正停止后才确认并腾出名额
- 取消与完成的竞态只有一个最终状态; 已取消/超时的查询不发布结果
"""

from __future__ import annotations

import os
import re
import threading
import time
import uuid
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------- 配置

PARQUET_PATH = os.environ.get("EVENTS_PARQUET", "data/events.parquet")
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT_QUERIES", "2"))
QUERY_TIMEOUT_SECONDS = float(os.environ.get("QUERY_TIMEOUT_SECONDS", "10"))
MAX_ROWS = 200                 # 页面最多展示的行数
JOB_TTL_SECONDS = 600          # 已完成任务结果保留时间
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS_PER_QUERY", "2"))
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

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
    """一条查询的完整生命周期.

    状态机(全部在 self.lock 内迁移, 终态唯一):
        running -> completed | failed | cancelled | timeout

    stop_reason 由取消/超时请求方先写入; 工作线程收尾时:
      - stop_reason 已设置 -> cancelled/timeout, 丢弃已算出的结果
      - 否则按执行结果 -> completed/failed
    因此"取消撞上完成"由锁的先后顺序决定唯一终态, 且被取消/超时的
    任务永远不会发布结果(结果只写入本任务对象, 不会覆盖其他查询)。
    """

    def __init__(self, manager: "QueryManager", sql: str):
        self.manager = manager
        self.id = uuid.uuid4().hex[:12]
        self.sql = sql
        self.lock = threading.Lock()
        self.done = threading.Event()      # 后台计算完全停止后置位
        self.status = "running"
        self.stop_reason: str | None = None   # "cancelled" | "timeout"
        self.columns: list[str] | None = None
        self.rows: list[list] | None = None
        self.truncated = False
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
            safe_path = PARQUET_PATH.replace("'", "''")
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

            with self.lock:
                if self.stop_reason is None:
                    # 取消/超时还没抢到锁 -> 本查询正常发布结果
                    self.columns = columns
                    self.rows = rows
                    self.truncated = truncated
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
                    "row_count": len(self.rows or []),
                    "truncated": self.truncated,
                    "max_rows": MAX_ROWS,
                })
            return snap


class QueryManager:
    """并发闸门: 全站最多 MAX_CONCURRENT 条, 不排队."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._slots = MAX_CONCURRENT
        self._jobs: dict[str, QueryJob] = {}

    def slots_available(self) -> int:
        with self._lock:
            return self._slots

    def submit(self, sql: str) -> QueryJob | None:
        with self._lock:
            self._purge_locked()
            if self._slots <= 0:
                return None                    # 繁忙, 不排队
            self._slots -= 1
            job = QueryJob(self, sql)
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

# ---------------------------------------------------------------- HTTP API

app = FastAPI(title="检修日志筛错 SQL 试跑平台")


class QueryRequest(BaseModel):
    sql: str


@app.post("/api/queries", status_code=201)
def submit_query(req: QueryRequest):
    err = validate_select(req.sql or "")
    if err:
        raise HTTPException(status_code=400, detail=err)
    job = MANAGER.submit(req.sql.strip())
    if job is None:
        return JSONResponse(
            status_code=429,
            content={
                "detail": f"繁忙: 全站最多同时运行 {MAX_CONCURRENT} 条查询, "
                          "当前名额已满, 请稍后再试(不排队)。",
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


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "parquet": PARQUET_PATH,
        "parquet_exists": os.path.exists(PARQUET_PATH),
        "slots_available": MANAGER.slots_available(),
        "max_concurrent": MAX_CONCURRENT,
        "timeout_seconds": QUERY_TIMEOUT_SECONDS,
        "max_rows": MAX_ROWS,
    }


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
