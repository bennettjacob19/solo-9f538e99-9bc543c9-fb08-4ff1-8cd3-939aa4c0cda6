# 遥测网关 UDP 最大负载探测

小包能到回显端、大包却超时？本服务为值班员确认遥测网关**可用的最大 UDP 负载**：
FastAPI + asyncio 实现的探测 API 与本地回显端，默认探测范围 **576～1400 字节**，
二分搜索收敛，当次返回最大确认长度、各次探测结果及无法判定原因。

另提供**后台探测入口**：先落盘任务并返回探测编号，再开始发包；巡检途中容器退出、
值班员重启后，可用探测编号查到探测做到了哪一步。所有探测记录持久化在容器卷中。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

探测记录保存在 `probe-data` 卷（容器内 `/data/probes.db`）；`docker compose restart`
或重建容器后记录仍在，未结案的探测会被标为中断。

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

探测记录默认写入 `./data/probes.db`，可用 `PROBE_DB_PATH` 覆盖。

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## 值班流程（/docs 内即可操作）

1. **设置回显端行为**：`PUT /echo/config`
   - `{"mode": "normal", "drop_threshold": 1000}` — UDP 负载超过 1000 字节即丢弃；
   - `{"mode": "disconnect", "drop_threshold": null}` — 断联，丢弃一切报文；
   - 入参校验失败返回 422，**原配置保持不变**。
2. **发起探测**（两种入口共用同一套入参 `ProbeRequest` 与同一份存储）：

   ```json
   {"min_size": 576, "max_size": 1400, "timeout_ms": 500, "small_size": 64}
   ```

   - `POST /probe` — **同步**：发包并在本次 HTTP 响应中返回结论（`ProbeResult`）。
   - `POST /probe/background` — **后台**：先保存任务并立即返回 `202` 与 `probe_id`，
     再开始发包；响应本身不含结论（`max_confirmed_size` 恒为 `null`）。
     适合巡检/长时间探测：HTTP 断开、容器退出都不影响已落盘的进度。

     ```bash
     curl -s -X POST localhost:8000/probe/background \
       -H 'Content-Type: application/json' -d '{}'
     # {"probe_id":"…","status":"queued","detail_url":"/probe/…", …}
     ```
3. **查询进度 / 结论**：`GET /probe/{probe_id}`（进行中或已结束都可查）
   - `status`: `queued` / `running` / `ok` / `below_range` / `undetermined` /
     `invalidated` / `interrupted` / `write_failed` / `failed`
   - `attempts[]`: 逐次尝试，含 `seq`、`size`、`purpose`（`probe` 探测包 /
     `link-verify` 小包复核）、`registered_at`（发送前落盘时间）、`outcome`
     （`ok` / `timeout` / `send_error` / `pending` 进行中 / `unconfirmed` 未确认）、
     `rtt_ms`、`finished_at`。

   | 终态 `status` | 含义 | `max_confirmed_size` |
   |---|---|---|
   | `ok` | 已确认最大可用负载 | 确认值（等于 `max_size` 时 `reached_range_max=true`，实际上限可能更高） |
   | `below_range` | 链路正常，但范围下限都确认不了 | `null` |
   | `undetermined` | 大包失败后小包复核也失败，**链路不确定** | 此前已确认的下界（无则 `null`），**绝不把故障当作尺寸上限** |
   | `invalidated` | 探测期间回显端配置被修改，**本次结论作废** | `null` |
   | `interrupted` | 服务重启时探测尚未结案（已落盘记录保留） | `null`，且不自动重发 |
   | `write_failed` | 记录写入失败，探测已停止 | `null`，保留此前已落盘记录 |
   | `failed` | 探测未能发起（如目标地址不可达） | `null` |
4. 历史记录：`GET /probes`（最近结束 200 条 + 全部进行中）。

## 持久化与恢复规则

- **两个入口共用一份存储**：同步 `POST /probe` 与后台 `POST /probe/background`
  写入同一个 SQLite 数据库（容器卷 `/data/probes.db`）。
- **先保存任务再发包**：任务先以 `queued` 落盘并提交，之后才开始发包。
- **发送前落盘**：每次尝试（探测包与小包复核）先写入 `seq / size / purpose`
  并提交，**提交成功后才发包**；尝试结束后再落盘 `ok / timeout / send_error`。
- **终态最后落盘**：最后一条尝试结果落盘后，才提交整次探测的终态；
  **终态落盘成功后才发布最终结论**（同步响应 / 后台记录可查）。
- **写入失败即停**：任一次记录写入失败，立即停止本次探测、不再发包，
  尽力把本次探测标为 `write_failed` 并返回/记录"写入失败"，
  已落盘记录原样保留（源码路径：`app/store.py`、`app/prober.py`）。
- **重启恢复**：启动时把所有 `queued/running` 的探测标为 `interrupted`；
  已登记但没保存结果的尝试对外显示为 `unconfirmed`（不会臆测为成功或失败）；
  整次探测不给负载上限，也**不自动重发**。
- **保留策略**：只保留最近结束的 200 条探测（按结束时间，`PROBE_MAX_FINISHED`
  可调），所有 `queued/running` 的进行中记录一律保留。

## 判定规则与需求对应

- **禁止 IP 分片**：探测与回显 socket 均设置 `IP_MTU_DISCOVER/IP_PMTUDISC_DO`（DF=1），
  结果中 `df_enabled` 如实上报；超过路径 MTU 时发送直接失败（EMSGSIZE），按发送失败处理。
- **报文带本次探测编号**：每次运行生成 UUID，连同序号写入 28 字节报文头
  （`magic | UUID | seq | size | 填充`）；只有 magic/编号/序号/长度**全部对应**的回包才算成功。
- **大包失败小包复核**：大包超时或发送失败 → 立即用 `small_size`（默认 64B）复核链路；
  小包通 = 确为尺寸受限，继续二分；小包也不通 = `undetermined`，故障不记为尺寸上限。
- **配置改变结论作废**：回显端配置带单调递增的 `epoch`，探测开始/每步/结束比对，
  不一致即 `invalidated`。
- **并发与迟到回包隔离**：每次运行使用独立临时端口 socket 并 connect 到回显端，
  其他运行的回包在内核层就被过滤；迟到的本次回包按序号拒收并计入 `stray_replies`。
- **配置失败保留原状态**：配置先经 Pydantic 校验，通过才原子替换并递增 epoch。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `ECHO_HOST` | `0.0.0.0` | 回显端监听地址 |
| `ECHO_PORT` | `9999` | 回显端 UDP 端口 |
| `PROBE_TARGET_HOST` | `127.0.0.1` | 探测默认目标（入参可用 `target_host/target_port` 覆盖，指向真实网关） |
| `PROBE_DB_PATH` | `data/probes.db`（镜像内 `/data/probes.db`） | 探测记录数据库文件 |
| `PROBE_MAX_FINISHED` | `200` | 保留最近结束的探测条数（进行中不受限） |

## 项目结构

```
app/
  main.py        # FastAPI 路由与生命周期（同步 / 后台入口、重启恢复）
  echo_server.py # 本地回显端（阈值丢弃/断联，epoch）
  prober.py      # 二分探测引擎（DF、探测编号、小包复核、作废判定、落盘时序）
  store.py       # SQLite 探测记录存储（先落盘后发包、终态发布、保留策略）
  packets.py     # 探测报文编解码
  netutil.py     # DF 位设置
tests/
  test_api.py        # 原有同步探测端到端测试
  test_background.py # 后台入口、落盘顺序/写入失败、重启恢复、保留策略
Dockerfile / docker-compose.yml
```
