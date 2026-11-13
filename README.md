# 检修日志筛错 SQL 试跑平台

检修日志入仓前，数据工程师在网页上试跑筛错 SQL，把**重号**或**修复早于报障**的记录退回上游更正。

- **FastAPI + DuckDB**：只读 Parquet 挂载为 `events` 表（`event_id`、`reported_at`、`restored_at`，时间为 UTC 秒）
- 网页可编辑、运行 **SELECT**（仅只读单条语句），显示**列名 + 前 200 行**，超出会注明
- **全站最多同时运行 2 条**，超额立即提示「繁忙」，**不排队**
- 查询**可取消**；运行满 **10 秒自动终止**
- 取消/超时都要等**后台计算真正停止**后才确认、才腾出名额，随后可立即再查
- 取消撞上完成由锁决定**唯一终态**；已取消/超时的查询**不发布结果**，也不会覆盖后续查询
- 每条查询使用**独立 DuckDB 连接**，两个窗口各自查询、取消互不影响

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `seed` 服务首次启动时生成样本日志 `data/events.parquet`（6 万行，含故意的重号与修复早于报障记录；已存在则跳过，`--force` 重新生成）
- `web` 服务以**只读**方式挂载 `./data` 并提供网页与 API

## 本地开发

```bash
pip install -r requirements.txt
python scripts/make_sample_data.py --out data/events.parquet --rows 60000
uvicorn app.main:app --reload --port 8000
```

## 网页使用

- 编辑 SQL 后点「▶ 运行」（或 Ctrl/Cmd+Enter）；运行中可点「✖ 取消当前查询」
- 示例按钮：筛错（重号∨修复早于报障）、重号检查、修复早于报障、大结果集（演示截断）、**慢查询（演示取消/10 秒超时）**
- 开两个浏览器窗口各跑一条慢查询，第三个并发请求会收到「繁忙」；取消其中一条，另一条照常运行

### 筛错 SQL（退回上游更正）

```sql
-- 重号 或 修复早于报障
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
ORDER BY event_id;
```

### 慢查询示例（用于试跑取消与超时）

```sql
SELECT sum(length(md5(CAST(a.event_id AS VARCHAR) || md5(CAST(b.event_id AS VARCHAR))))) AS checksum
FROM events a CROSS JOIN events b;
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queries` | 提交查询，返回 `query_id`；名额满返回 **429**（繁忙，不排队）；非 SELECT 返回 400 |
| GET | `/api/queries/{id}` | 查询状态：`running / completed / failed / cancelled / timeout`，完成时带 `columns`、`rows`（≤200 行）、`truncated` |
| POST | `/api/queries/{id}/cancel` | 取消；**阻塞到后台计算停止、名额腾出后**才返回最终状态 |
| GET | `/api/health` | 空闲名额、超时上限、数据文件是否存在 |

## 并发与终态语义

- 名额是计数闸门（默认 2，可用 `MAX_CONCURRENT_QUERIES` 调整），满员直接 429，不排队
- 每条查询一个工作线程 + 一个独立 DuckDB 连接 + 一个 10 秒定时器（`QUERY_TIMEOUT_SECONDS`）
- 取消/超时先置 `stop_reason` 再 `conn.interrupt()`（只打断本连接）；工作线程收尾时在锁内确认唯一终态并**丢弃结果**，随后才释放名额——所以取消/超时确认后一定可立即再查
- 取消与完成竞态：谁先抢到锁谁生效，终态唯一；已取消/超时的任务结果不发布，且结果按 `query_id` 隔离，不会覆盖后续查询

## 目录结构

```
app/main.py              FastAPI 应用与查询任务状态机
app/static/index.html    网页（编辑器、运行/取消、结果表格）
scripts/make_sample_data.py  样本日志生成器(可复现)
data/                    样本 Parquet(seed 服务生成, 只读挂载)
Dockerfile
docker-compose.yml
requirements.txt
```
