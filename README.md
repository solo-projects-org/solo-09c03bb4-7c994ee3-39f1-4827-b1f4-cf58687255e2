# 检修日志筛错与入仓放行平台

上游更正检修日志后，入仓值班员在网页上确认**整批通过现行筛错规则**，再交给装载作业。
平台在原有「SQL 试跑」之外，提供**规则版本化、批次检查、放行与留痕**的完整闭环。

- **FastAPI + DuckDB**：只读 Parquet 挂载为 `events` 表（`event_id`、`reported_at`、`restored_at`，UTC 秒）
- **SQL 试查（原试查照旧）**：网页编辑、运行单条 SELECT/WITH，显示列名 + 前 200 行，超出注明
- **现行规则**：网页保存一条 SELECT 作为现行规则，**每次保存都生成新版本**（即使 SQL 改回原文也是新版本）
- **批次检查**：上传沿用 `events` 三列及类型的 Parquet，在**该批文件**上执行现行规则；**返回零行才可放行**
- **共用名额**：试查与批次检查共用全站 **2 个并发名额**，支持**取消**与 **10 秒超时**；执行出错、查出错行、取消、超时**均不放行**
- **规则变动防护**：检查期间仍可保存新规则；一旦规则被改过，未放行批次即使结论仍为零行也**拒绝放行并提示重新检查**
- **并发判定**：规则保存与放行按全局锁内的**实际生效次序**判断，不沿用过期结论
- **留痕可追溯**：放行后按批号**下载原文件**、查看**当时的规则全文与检查结论快照**；后续改规则不撤回旧批
- **持久化与重启**：批次文件与放行记录存入数据卷；**保存失败不会留下可下载的放行批**；重启后未完成批标为失败，已放行批仍可下载

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `seed` 服务（幂等，已存在则跳过）在数据卷 `sample-data` 中生成：
  - `/data/events.parquet`：6 万行试查样本（含故意的重号与修复早于报障）
  - `/data/samples/batch_before_correction.parquet`：**更正前**批次（含 8 个重号、6 条时间倒挂，应被拦截）
  - `/data/samples/batch_after_correction.parquet`：**更正后**批次（同批修正版，规则零行，可放行）
- `web` 服务：
  - 样本目录 `/data` **只读**挂载
  - 应用数据目录 `/appdata` 以**可写数据卷**持久化（上传批次、规则、放行记录）

### 在网页上试做检查与放行

1. 打开「② 现行筛错规则」：默认已内置筛错规则（重号 ∨ 修复早于报障），可编辑后「保存为现行规则」
2. 打开「③ 批次检查与放行」，上传 `batch_before_correction.parquet` → 发起检查 → 命中错行、批次失败、不可放行
3. 上传 `batch_after_correction.parquet` → 发起检查 → **零行通过·待放行** → 「放行本批」
4. 放行后可「下载原文件」、展开「当时规则与结论」快照
5. 对任一待放行批次，先在规则页重新保存（哪怕 SQL 不变）再放行，会被拒绝并提示重新检查
6. 用慢查询规则可演示取消与 10 秒超时；开两个慢任务占满名额后，第三个请求（试查或检查）直接收到繁忙

## 本地开发

```bash
pip install -r requirements.txt
python scripts/make_sample_data.py --out data/events.parquet --rows 60000
python scripts/make_sample_data.py --make-batch-pair data/samples --rows 5000
APP_DATA_DIR=data/appdata EVENTS_PARQUET=data/events.parquet \
  uvicorn app.main:app --reload --port 8000
```

## 业务规则与状态语义

### 批次状态机

```
uploaded(待检查)
   │ 发起检查
   ▼
checking(检查中) ──取消 / 超时 / 执行出错──► failed(不放行)
   │ 完成且命中错行                          ▲
   ├──────────────────────────────────────► failed(不放行)
   │ 完成且零行
   ▼
ready(零行通过·待放行) ──放行落盘成功──► released(已放行, 可下载)
   │
   └─ 放行时发现规则已变更(任何新版本) ──► 409 拒绝, 提示重新检查
```

- 重启恢复：启动时把所有 `uploaded / checking / ready` 的批次一律置为 `failed`（检查中断），
  已 `released` 的批次及其快照、原文件不受影响
- 放行记录用「同卷临时文件 + `os.replace`」原子写；写失败会回滚内存状态，批次绝不会变成可下载的「假放行」
- 上传文件先落临时目录，校验 Parquet 结构通过后才原子移入批次目录；任何失败都清理，不产生批次记录

### 规则版本与并发

- 每次 `PUT /api/rules/current` 都追加一条历史并切换 `current_id`；版本号单调递增
- 检查在发起时绑定当时的 `rule_id`；放行时在同一把全局锁内比对**当前** `rule_id`
- 规则保存与放行并发：谁先拿到锁谁先生效
  - 保存先生效 → 放行判定为「规则已变更」，拒绝并要求重新检查
  - 放行先生效 → 放行成立，快照绑定旧规则；之后再改规则不影响该批
- 检查运行期间保存规则不影响正在跑的检查（用的是发起时的 SQL 副本），但该批结论将无法直接放行

### 查询名额与终态（试查 + 检查共用）

- 名额是计数闸门（默认 2，`MAX_CONCURRENT_QUERIES` 可调），满员直接 429，不排队
- 每条查询一个工作线程 + 独立 DuckDB 连接 + 10 秒定时器（`QUERY_TIMEOUT_SECONDS`）
- 取消/超时先置 `stop_reason` 再 `conn.interrupt()`（只打断本连接）；工作线程在锁内确认唯一终态并丢弃结果，
  随后才释放名额——确认取消/超时后一定可立即再查
- 检查终态由工作线程回调**同步落盘**，取消接口返回时持久化结论已写好

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queries` | 提交试查，返回 `query_id`；名额满 429；非 SELECT 400 |
| GET | `/api/queries/{id}` | 试查状态：`running/completed/failed/cancelled/timeout` |
| POST | `/api/queries/{id}/cancel` | 取消试查，阻塞到后台停止、名额腾出 |
| GET | `/api/rules` | 现行规则与保存历史 |
| PUT | `/api/rules/current` | 保存现行规则（产生新版本）；非 SELECT 400 |
| POST | `/api/batches` | 上传 Parquet 建批；结构不符/非 Parquet 400，且不留记录 |
| GET | `/api/batches` | 批次清单（含检查结论与放行快照） |
| GET | `/api/batches/{id}` | 单个批次详情 |
| POST | `/api/batches/{id}/inspections` | 发起检查；名额满 429（批次回滚为待检查）；状态冲突 409 |
| POST | `/api/batches/{id}/inspections/cancel` | 取消检查，批次判失败不放行 |
| POST | `/api/batches/{id}/release` | 放行；规则已变更/未零行通过/已失败均 409 |
| GET | `/api/batches/{id}/download` | 下载原文件（仅已放行批，否则 404） |
| GET | `/api/health` | 名额、超时上限、数据卷与样本文件状态 |

## 数据卷与目录结构

```
app/main.py                  FastAPI 应用: 查询状态机 + 规则/批次持久化
app/static/index.html        网页(试查 / 规则 / 批次放行 三个标签页)
scripts/make_sample_data.py  样本生成器(试查数据 + 更正前/后批次对)
data/                        本地开发样本目录(compose 中由命名卷承载)
docker-compose.yml           seed(造数) + web(服务), 两个命名卷
Dockerfile
requirements.txt
```

容器内应用数据卷 `/appdata` 布局：

```
/appdata/
├── rules.json               # 现行规则指针 + 历史版本(原子写)
├── batches.json             # 批次索引: 状态、检查结论、放行快照(原子写)
├── tmp/                     # 上传/写索引的临时文件
└── batches/<batch_id>/
    └── source.parquet       # 该批原始上传文件(放行后即下载此文件)
```
