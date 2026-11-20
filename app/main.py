"""FastAPI 入口：探测 API + 本地回显端配置。

- ``GET  /``                    服务简介
- ``GET  /echo/config``         查看回显端当前配置与 epoch
- ``PUT  /echo/config``         设置丢弃阈值 / 断联（校验失败保留原状态）
- ``POST /probe``               同步探测：发包并在当次响应中返回结论
- ``POST /probe/background``    后台探测：先保存任务并返回探测编号，再开始发包
- ``GET  /probe/{probe_id}``    查看进行中或已结束的探测及逐次尝试
- ``GET  /probes``              探测记录列表

两种探测入口共用同一个持久化存储（容器卷），重启后未结案探测标记为中断。
交互式文档见 ``/docs``。
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator, Optional

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

from .echo_server import EchoConfig, EchoProtocol, EchoState
from .netutil import set_dont_fragment
from .prober import (
    AttemptDetail,
    ProbeDetail,
    ProbeRequest,
    ProbeResult,
    ProbeSummary,
    Prober,
)
from .store import ProbeStore, ProbeStoreError

logger = logging.getLogger(__name__)

ECHO_HOST = os.environ.get("ECHO_HOST", "0.0.0.0")
ECHO_PORT = int(os.environ.get("ECHO_PORT", "9999"))
PROBE_TARGET_HOST = os.environ.get("PROBE_TARGET_HOST", "127.0.0.1")
DEFAULT_DB_PATH = os.environ.get("PROBE_DB_PATH", "data/probes.db")
DEFAULT_MAX_FINISHED = int(os.environ.get("PROBE_MAX_FINISHED", "200"))


def _db_path() -> str:
    """在 lifespan 启动时读取，便于测试/部署按需覆盖。"""
    return os.environ.get("PROBE_DB_PATH", DEFAULT_DB_PATH)


def _max_finished() -> int:
    return int(os.environ.get("PROBE_MAX_FINISHED", DEFAULT_MAX_FINISHED))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state = EchoState()

    # 回显端 socket：设置 DF，回包同样禁止 IP 分片
    echo_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    echo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    set_dont_fragment(echo_sock)
    echo_sock.bind((ECHO_HOST, ECHO_PORT))
    echo_sock.setblocking(False)

    loop = asyncio.get_running_loop()
    echo_protocol = EchoProtocol(state)
    echo_transport, _ = await loop.create_datagram_endpoint(
        lambda: echo_protocol, sock=echo_sock
    )

    store = ProbeStore(_db_path(), max_finished=_max_finished())
    # 重启恢复：未结案的探测一律标记中断，不自动重发、不给负载上限
    interrupted = store.interrupt_open_probes()

    app.state.echo_state = state
    app.state.echo_protocol = echo_protocol
    app.state.prober = Prober(state, (PROBE_TARGET_HOST, ECHO_PORT))
    app.state.store = store
    app.state.interrupted_on_start = interrupted
    app.state.background_tasks = set()
    try:
        yield
    finally:
        for task in list(app.state.background_tasks):
            task.cancel()
        if app.state.background_tasks:
            await asyncio.gather(*app.state.background_tasks, return_exceptions=True)
        store.close()
        echo_transport.close()


app = FastAPI(
    title="遥测网关 UDP 最大负载探测",
    version="1.1.0",
    description=(
        "小包能到回显端、大包却超时？用本服务确认遥测网关可用的最大 UDP 负载。\n\n"
        "- 探测报文带 DF 位（禁止 IP 分片）与本次探测编号，只有对应回包才算成功；\n"
        "- 大包超时/发送失败先用小包复核，小包也失败则报告链路不确定；\n"
        "- 探测期间改动回显端配置会使本次结论作废；\n"
        "- **后台探测** `POST /probe/background`：先落盘任务并返回探测编号，再开始发包；\n"
        "  巡检中断/重启后可用 `GET /probe/{probe_id}` 查看做到哪一步；\n"
        "- 每次尝试发包前落盘序号/长度/用途，结束后落盘回包、超时或发送失败；\n"
        "  最后一条尝试与终态均落盘后才发布最终结论，记录写失败则停止探测；\n"
        "- 记录持久化在容器卷，重启后未结案探测标记为中断。"
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 视图转换
# ---------------------------------------------------------------------------


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:  # pragma: no cover - 存储损坏
        return None


def _to_detail(record: dict) -> ProbeDetail:
    probe = record["probe"]
    open_probe = probe["status"] in ("queued", "running")
    # 只有终态已确认落盘的结案探测，未保存结果的尝试才标“未确认”；
    # write_failed 时记录本身可能已不可信，仍按未确认展示，不臆测结果。
    attempts = []
    for a in record["attempts"]:
        if a["outcome"] is not None:
            outcome = a["outcome"]
        elif open_probe:
            # 已登记、结果尚未落盘：仍在尝试中
            outcome = "pending"
        else:
            # 已登记但没保存结果（如进程中断）：只标未确认
            outcome = "unconfirmed"
        attempts.append(
            AttemptDetail(
                seq=a["seq"],
                size=a["size"],
                purpose=a["purpose"],
                registered_at=_parse_dt(a["registered_at"]),
                outcome=outcome,
                rtt_ms=a["rtt_ms"],
                detail=a["detail"],
                finished_at=_parse_dt(a["finished_at"]),
            )
        )
    return ProbeDetail(
        probe_id=probe["probe_id"],
        mode=probe["mode"],
        status=probe["status"],
        request=ProbeRequest.model_validate_json(probe["request_json"]),
        target=probe["target"],
        created_at=_parse_dt(probe["created_at"]),
        started_at=_parse_dt(probe["started_at"]),
        finished_at=_parse_dt(probe["finished_at"]),
        duration_ms=probe["duration_ms"],
        df_enabled=(
            None if probe["df_enabled"] is None else bool(probe["df_enabled"])
        ),
        stray_replies=probe["stray_replies"] or 0,
        config_epoch_start=probe["config_epoch_start"],
        config_epoch_end=probe["config_epoch_end"],
        max_confirmed_size=probe["max_confirmed_size"],
        reached_range_max=bool(probe["reached_range_max"]),
        reason=probe["reason"],
        error=probe["error"],
        attempts=attempts,
    )


def _to_summary(probe: dict) -> ProbeSummary:
    return ProbeSummary(
        probe_id=probe["probe_id"],
        mode=probe["mode"],
        status=probe["status"],
        created_at=_parse_dt(probe["created_at"]),
        started_at=_parse_dt(probe["started_at"]),
        finished_at=_parse_dt(probe["finished_at"]),
        max_confirmed_size=probe["max_confirmed_size"],
        reason=probe["reason"],
    )


def _get_store(request: Request) -> ProbeStore:
    return request.app.state.store


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------


@app.get("/", summary="服务简介")
async def index() -> dict:
    return {
        "service": "telemetry-gateway-udp-probe",
        "docs": "/docs",
        "endpoints": {
            "config": ["GET /echo/config", "PUT /echo/config"],
            "probe": [
                "POST /probe",
                "POST /probe/background",
                "GET /probes",
                "GET /probe/{probe_id}",
            ],
        },
    }


@app.get("/echo/config", summary="查看回显端配置")
async def get_config(request: Request) -> dict:
    state: EchoState = request.app.state.echo_state
    proto: EchoProtocol = request.app.state.echo_protocol
    return {
        "epoch": state.epoch,
        "config": state.config,
        "stats": {"echoed": proto.echoed, "dropped": proto.dropped},
    }


@app.put("/echo/config", summary="设置丢弃阈值或断联")
async def put_config(cfg: EchoConfig, request: Request) -> dict:
    # 入参先经 Pydantic 校验；校验失败直接 422，不会进入这里，原状态保持不变
    state: EchoState = request.app.state.echo_state
    epoch = state.apply(cfg)
    return {"epoch": epoch, "config": state.config}


@app.post(
    "/probe",
    response_model=ProbeResult,
    summary="发起一次最大负载探测（同步返回结论）",
)
async def run_probe(req: ProbeRequest, request: Request) -> ProbeResult:
    prober: Prober = request.app.state.prober
    store: ProbeStore = _get_store(request)
    try:
        # 先落盘任务，再发包；写入失败则不起探测
        probe_id = prober.create_task(req, store, mode="sync")
    except ProbeStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"探测记录写入失败，未发起探测: {exc}",
        ) from exc
    try:
        return await prober.run(req, probe_id, store, mode="sync")
    except OSError as exc:
        # 目标地址无法解析/不可达等，探测未能发起（终态已尽力落盘）
        raise HTTPException(status_code=400, detail=f"无法发起探测: {exc}") from exc
    except ProbeStoreError as exc:
        # 终态无法落盘 -> 不发布结论
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"探测记录写入失败，未发布结论: {exc}",
        ) from exc


class BackgroundProbeResponse(BaseModel):
    probe_id: str
    status: str = Field("queued", description="任务已保存，等待/开始发包")
    mode: str = "background"
    detail_url: str
    max_confirmed_size: None = Field(None, description="后台探测不在此响应中给结论")
    message: str


@app.post(
    "/probe/background",
    response_model=BackgroundProbeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="后台探测：先保存任务并返回探测编号，再开始发包",
)
async def run_background_probe(
    req: ProbeRequest, request: Request
) -> BackgroundProbeResponse:
    prober: Prober = request.app.state.prober
    store: ProbeStore = _get_store(request)
    try:
        probe_id = prober.create_task(req, store, mode="background")
    except ProbeStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"探测记录写入失败，未发起探测: {exc}",
        ) from exc

    async def _runner() -> None:
        try:
            await prober.run(req, probe_id, store, mode="background")
        except OSError:
            logger.exception("后台探测 %s 无法发起（终态已尽力落盘）", probe_id)
        except Exception:  # pragma: no cover - 兜底，避免静默丢失
            logger.exception("后台探测 %s 异常结束", probe_id)

    task = asyncio.create_task(_runner())
    tasks: set[asyncio.Task] = request.app.state.background_tasks
    tasks.add(task)
    task.add_done_callback(tasks.discard)

    return BackgroundProbeResponse(
        probe_id=probe_id,
        detail_url=f"/probe/{probe_id}",
        message="任务已保存，后台开始发包；用 GET /probe/{probe_id} 查看进度与逐次尝试",
    )


@app.get(
    "/probe/{probe_id}",
    response_model=ProbeDetail,
    summary="查看进行中或已结束的探测及逐次尝试",
)
async def get_probe(probe_id: str, request: Request) -> ProbeDetail:
    store = _get_store(request)
    try:
        record = store.get_probe(probe_id)
    except ProbeStoreError as exc:
        raise HTTPException(status_code=500, detail=f"读取探测记录失败: {exc}") from exc
    if record is None:
        raise HTTPException(status_code=404, detail="probe_id 不存在或已被淘汰")
    return _to_detail(record)


@app.get(
    "/probes",
    response_model=list[ProbeSummary],
    summary="探测记录列表（最近结束 200 条 + 全部进行中）",
)
async def list_probes(request: Request) -> list[ProbeSummary]:
    store = _get_store(request)
    try:
        rows = store.list_probes()
    except ProbeStoreError as exc:
        raise HTTPException(status_code=500, detail=f"读取探测列表失败: {exc}") from exc
    return [_to_summary(row) for row in rows]
