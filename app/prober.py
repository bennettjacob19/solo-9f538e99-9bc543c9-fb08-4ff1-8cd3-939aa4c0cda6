"""探测引擎：在 [min_size, max_size] 内二分搜索可确认的最大 UDP 负载。

判定规则（与需求一一对应）：
- 每次探测运行使用独立 socket（临时端口）与独立探测编号（UUID），
  并发探测、迟到回包不会串入其他结论；
- 探测 socket 设置 DF 位，禁止 IP 分片；发送失败（如 EMSGSIZE）与超时
  同等对待；
- 大包超时或发送失败时，先用小包（small_size）复核链路：
  小包通 → 链路正常，确为尺寸受限，继续二分；
  小包也不通 → 链路不确定，立即终止，绝不把该失败记为尺寸上限；
- 探测期间回显端配置（epoch）变化 → 本次结论作废（invalidated）；
- 范围内全部确认 → 达到范围上限（reached_range_max）；
- 范围下限都确认不了（但链路正常）→ below_range。

持久化约束：
- 每次尝试先落盘 seq / size / purpose，提交成功后才发包；尝试结束后再落盘
  回包 / 超时 / 发送失败结果；
- 最后一次尝试结果与终态全部提交成功后才返回最终结论；
- 任一记录写入失败 → 立即停止本次探测，尽力记录 write_failed 终态并报告，
  已落盘记录原样保留。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .echo_server import EchoState
from .netutil import set_dont_fragment
from .packets import HEADER_SIZE, MAX_UDP_PAYLOAD, build_packet, parse_packet
from .store import (
    DEFAULT_WRITE_FAILED_REASON,
    ProbeStore,
    ProbeStoreError,
    utcnow,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API 模型
# ---------------------------------------------------------------------------


class ProbeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_size: int = Field(576, ge=HEADER_SIZE, le=MAX_UDP_PAYLOAD, description="探测范围下限（字节）")
    max_size: int = Field(1400, ge=HEADER_SIZE, le=MAX_UDP_PAYLOAD, description="探测范围上限（字节）")
    timeout_ms: int = Field(500, ge=20, le=30000, description="单次探测等待回包超时（毫秒）")
    small_size: int = Field(64, ge=HEADER_SIZE, le=MAX_UDP_PAYLOAD, description="链路复核小包长度（字节）")
    target_host: Optional[str] = Field(None, description="回显端地址，缺省为本地回显端")
    target_port: Optional[int] = Field(None, ge=1, le=65535, description="回显端端口，缺省为本地回显端")

    @model_validator(mode="after")
    def _check_range(self) -> "ProbeRequest":
        if self.min_size > self.max_size:
            raise ValueError("min_size 不能大于 max_size")
        return self


AttemptOutcome = Literal["ok", "timeout", "send_error"]
ProbeStatus = Literal[
    "ok", "below_range", "undetermined", "invalidated",
    "interrupted", "write_failed", "failed",
]


class AttemptOut(BaseModel):
    seq: int
    size: int
    purpose: Literal["probe", "link-verify"]
    outcome: AttemptOutcome
    rtt_ms: Optional[float] = None
    detail: Optional[str] = None


class AttemptDetail(BaseModel):
    seq: int
    size: int
    purpose: Literal["probe", "link-verify"]
    registered_at: datetime
    outcome: Literal["ok", "timeout", "send_error", "pending", "unconfirmed"]
    rtt_ms: Optional[float] = None
    detail: Optional[str] = None
    finished_at: Optional[datetime] = None


class ProbeResult(BaseModel):
    probe_id: str
    mode: Literal["sync", "background"] = Field("sync", description="sync=同步入口，background=后台入口")
    status: ProbeStatus
    max_confirmed_size: Optional[int] = Field(
        None, description="本次确认可用的最大 UDP 负载；undetermined 时为已确认下界，invalidated 时作废为 null"
    )
    reason: Optional[str] = Field(None, description="无法判定 / 结论作废 / 低于范围 / 中断等的原因")
    error: Optional[str] = Field(None, description="write_failed / failed 时的错误详情")
    reached_range_max: bool = False
    min_size: int
    max_size: int
    attempts: list[AttemptOut]
    stray_replies: int = Field(0, description="被丢弃的迟到/不对应回包数")
    config_epoch_start: int
    config_epoch_end: int
    df_enabled: bool = Field(..., description="探测 socket 是否成功设置 DF（禁止 IP 分片）")
    target: str
    started_at: datetime
    finished_at: datetime
    duration_ms: float


class ProbeDetail(BaseModel):
    """``GET /probe/{probe_id}``：进行中或已结束的探测及逐次尝试。"""

    probe_id: str
    mode: Literal["sync", "background"]
    status: Literal[
        "queued", "running", "ok", "below_range", "undetermined",
        "invalidated", "interrupted", "write_failed", "failed",
    ]
    request: ProbeRequest
    target: str
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    df_enabled: Optional[bool] = None
    stray_replies: int = 0
    config_epoch_start: Optional[int] = None
    config_epoch_end: Optional[int] = None
    max_confirmed_size: Optional[int] = None
    reached_range_max: bool = False
    reason: Optional[str] = None
    error: Optional[str] = None
    attempts: list[AttemptDetail] = Field(default_factory=list)


class ProbeSummary(BaseModel):
    probe_id: str
    mode: Literal["sync", "background"]
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    max_confirmed_size: Optional[int] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# 探测引擎
# ---------------------------------------------------------------------------


class _Inbound(asyncio.DatagramProtocol):
    """把收到的数据报/发送错误塞进队列，由探测协程自行匹配。

    注意：asyncio 的 DatagramTransport.sendto 不会把 OSError 抛给调用方，
    发送失败（如 EMSGSIZE）经 error_received 回调上报。
    """

    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.queue.put_nowait(("data", data))

    def error_received(self, exc: Exception) -> None:
        # 发送失败（EMSGSIZE 等）或连通型 UDP 的异步 ICMP 错误
        self.queue.put_nowait(("error", exc))


class _AttemptRecordFailure(Exception):
    def __init__(self, exc: ProbeStoreError) -> None:
        super().__init__(str(exc))
        self.exc = exc


class Prober:
    def __init__(self, state: EchoState, default_target: tuple[str, int]) -> None:
        self._state = state
        self._default_target = default_target

    # ------------------------------------------------------------------ 任务登记

    def resolve_target(self, req: ProbeRequest) -> tuple[str, int]:
        host, port = self._default_target
        if req.target_host is not None:
            host = req.target_host
        if req.target_port is not None:
            port = req.target_port
        return host, port

    def create_task(
        self,
        req: ProbeRequest,
        store: ProbeStore,
        *,
        mode: Literal["sync", "background"],
        probe_id: Optional[str] = None,
    ) -> str:
        """先保存任务（queued），返回探测编号。提交失败则抛出 ProbeStoreError。"""
        probe_id = probe_id or str(uuid.uuid4())
        host, port = self.resolve_target(req)
        store.create_probe(
            probe_id,
            mode=mode,
            request_json=req.model_dump_json(),
            min_size=req.min_size,
            max_size=req.max_size,
            timeout_ms=req.timeout_ms,
            small_size=req.small_size,
            target=f"{host}:{port}",
            created_at=utcnow(),
        )
        return probe_id

    # ------------------------------------------------------------------ 执行

    async def run(
        self,
        req: ProbeRequest,
        probe_id: str,
        store: ProbeStore,
        *,
        mode: Literal["sync", "background"] = "sync",
    ) -> ProbeResult:
        """执行已登记任务。

        记录写入失败、或任务已不处于 queued（例如重启恢复时被标记中断）
        时停止探测并返回 write_failed / 中断结果；后者绝不发包。
        """
        host, port = self.resolve_target(req)
        target = f"{host}:{port}"
        started_at = utcnow()
        t0 = time.monotonic()
        epoch_start = self._state.epoch
        attempts: list[AttemptOut] = []
        stray = 0
        seq = 0
        df_ok = False

        # 先确认任务仍是 queued：重启恢复后任务已被标记中断，不得重发
        try:
            record = store.get_probe(probe_id)
        except ProbeStoreError as exc:
            return self._write_failed_result(
                store, probe_id, mode, req, target, attempts, stray,
                epoch_start, None, started_at, t0, exc, published=False,
            )
        if record is None or record["probe"]["status"] != "queued":
            current = record["probe"]["status"] if record else "missing"
            return ProbeResult(
                probe_id=probe_id,
                mode=mode,
                status="interrupted" if current == "interrupted" else "failed",
                max_confirmed_size=None,
                reason=(
                    DEFAULT_INTERRUPTED_REASON
                    if current == "interrupted"
                    else f"探测任务当前状态为 {current}，未执行"
                ),
                error=None,
                reached_range_max=False,
                min_size=req.min_size,
                max_size=req.max_size,
                attempts=[],
                stray_replies=0,
                config_epoch_start=epoch_start,
                config_epoch_end=self._state.epoch,
                df_enabled=False,
                target=target,
                started_at=started_at,
                finished_at=utcnow(),
                duration_ms=0.0,
            )

        # 每次探测运行独立 socket（独立临时端口），并 connect 到回显端：
        # 其他运行的回包、无关数据报在内核层就被过滤，不会到达本 socket。
        loop = asyncio.get_running_loop()
        transport = None
        try:
            transport, proto = await loop.create_datagram_endpoint(
                _Inbound, remote_addr=(host, port)
            )
        except OSError as exc:
            reason = f"无法发起探测: {exc}"
            self._finish(
                store, probe_id, mode, req, target, attempts, stray,
                status="failed", reason=reason, error=str(exc),
                max_confirmed_size=None, reached_range_max=False,
                epoch_start=epoch_start, epoch_end=self._state.epoch,
                df_enabled=None,
                started_at=started_at, t0=t0,
            )
            raise

        sock = transport.get_extra_info("socket")
        df_ok = set_dont_fragment(sock) if sock is not None else False
        queue: asyncio.Queue[tuple[str, object]] = proto.queue

        try:
            store.mark_running(
                probe_id,
                started_at=started_at,
                df_enabled=df_ok,
                epoch_start=epoch_start,
            )
        except ProbeStoreError as exc:
            transport.close()
            return self._write_failed_result(
                store, probe_id, mode, req, target, attempts, stray,
                epoch_start, df_ok, started_at, t0, exc,
            )

        async def attempt(size: int, purpose: Literal["probe", "link-verify"]) -> AttemptOut:
            nonlocal seq, stray
            seq += 1
            my_seq = seq
            # 发送前落盘：序号、长度、用途。写入失败 -> 不发送。
            try:
                store.register_attempt(
                    probe_id,
                    seq=my_seq,
                    size=size,
                    purpose=purpose,
                    registered_at=utcnow(),
                )
            except ProbeStoreError as exc:
                raise _AttemptRecordFailure(exc) from exc
            payload = build_packet(uuid.UUID(probe_id), my_seq, size)
            t_send = time.monotonic()
            transport.sendto(payload)
            deadline = t_send + req.timeout_ms / 1000.0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    result = AttemptOut(
                        seq=my_seq, size=size, purpose=purpose, outcome="timeout"
                    )
                    break
                try:
                    kind, obj = await asyncio.wait_for(queue.get(), remaining)
                except asyncio.TimeoutError:
                    result = AttemptOut(
                        seq=my_seq, size=size, purpose=purpose, outcome="timeout"
                    )
                    break
                if kind == "error":
                    # 发送失败（如 EMSGSIZE：DF 下超过路径 MTU）
                    exc = obj
                    result = AttemptOut(
                        seq=my_seq, size=size, purpose=purpose,
                        outcome="send_error",
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                    break
                pkt = parse_packet(obj)  # type: ignore[arg-type]
                if (
                    pkt is not None
                    and pkt.session_id == uuid.UUID(probe_id)
                    and pkt.seq == my_seq
                    and pkt.size == size
                ):
                    rtt = (time.monotonic() - t_send) * 1000.0
                    result = AttemptOut(
                        seq=my_seq, size=size, purpose=purpose,
                        outcome="ok", rtt_ms=round(rtt, 3),
                    )
                    break
                # 迟到/不对应回包：丢弃不采信，继续等到超时为止
                stray += 1
            # 尝试结束后落盘结果；失败同样停止探测
            try:
                store.save_attempt_result(
                    probe_id,
                    seq=my_seq,
                    outcome=result.outcome,
                    rtt_ms=result.rtt_ms,
                    detail=result.detail,
                    finished_at=utcnow(),
                )
            except ProbeStoreError as exc:
                raise _AttemptRecordFailure(exc) from exc
            return result

        def epoch_broken() -> bool:
            return self._state.epoch != epoch_start

        def invalidated_result() -> tuple[str, str]:
            return (
                "invalidated",
                f"探测期间回显端配置发生变化（epoch {epoch_start} → {self._state.epoch}），本次结论作废",
            )

        status: str = "ok"
        reason: Optional[str] = None
        best: Optional[int] = None
        lo, hi = req.min_size, req.max_size
        record_failure: Optional[ProbeStoreError] = None

        try:
            while lo <= hi:
                if epoch_broken():
                    status, reason = invalidated_result()
                    best = None
                    break
                mid = (lo + hi) // 2
                try:
                    att = await attempt(mid, "probe")
                except _AttemptRecordFailure as exc:
                    record_failure = exc.exc
                    break
                attempts.append(att)
                if att.outcome == "ok":
                    best = mid
                    lo = mid + 1
                    continue
                # 大包失败（超时或发送失败）→ 小包复核链路
                try:
                    verify = await attempt(req.small_size, "link-verify")
                except _AttemptRecordFailure as exc:
                    record_failure = exc.exc
                    break
                attempts.append(verify)
                if verify.outcome == "ok":
                    hi = mid - 1  # 链路正常，确为尺寸受限
                    continue
                if epoch_broken():
                    status, reason = invalidated_result()
                    best = None
                else:
                    status = "undetermined"
                    reason = (
                        f"{mid} 字节探测{('超时' if att.outcome == 'timeout' else '发送失败')}后，"
                        f"{req.small_size} 字节小包复核亦失败（{verify.outcome}），"
                        "链路状态不确定，不能将该失败记为尺寸上限"
                    )
                    # 保留此前已确认的下界，但绝不把失败尺寸当作上限
                break

            if record_failure is None and status == "ok":
                if epoch_broken():
                    status, reason = invalidated_result()
                    best = None
                elif best is None:
                    status = "below_range"
                    reason = (
                        f"链路正常（{req.small_size} 字节小包复核通过），但范围下限 "
                        f"{req.min_size} 字节未获确认，可确认上限低于探测范围"
                    )
        finally:
            transport.close()

        if record_failure is not None:
            return self._write_failed_result(
                store, probe_id, mode, req, target, attempts, stray,
                epoch_start, df_ok, started_at, t0, record_failure,
            )

        # 最后一次尝试结果已落盘；提交终态成功后才发布结论
        return self._finish(
            store, probe_id, mode, req, target, attempts, stray,
            status=status, reason=reason,
            error=None,
            max_confirmed_size=best,
            reached_range_max=(status == "ok" and best == req.max_size),
            epoch_start=epoch_start,
            epoch_end=self._state.epoch,
            df_enabled=df_ok,
            started_at=started_at, t0=t0,
        )

    # ------------------------------------------------------------------ 收尾

    def _finish(
        self,
        store: ProbeStore,
        probe_id: str,
        mode: Literal["sync", "background"],
        req: ProbeRequest,
        target: str,
        attempts: list[AttemptOut],
        stray: int,
        *,
        status: str,
        reason: Optional[str],
        error: Optional[str],
        max_confirmed_size: Optional[int],
        reached_range_max: bool,
        epoch_start: int,
        epoch_end: int,
        df_enabled: Optional[bool],
        started_at: datetime,
        t0: float,
    ) -> ProbeResult:
        finished_at = utcnow()
        duration_ms = round((time.monotonic() - t0) * 1000.0, 3)
        try:
            store.finalize(
                probe_id,
                status=status,
                reason=reason,
                error=error,
                max_confirmed_size=max_confirmed_size,
                reached_range_max=reached_range_max,
                stray_replies=stray,
                config_epoch_start=epoch_start,
                config_epoch_end=epoch_end,
                df_enabled=df_enabled,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
            )
        except ProbeStoreError as exc:
            # 终态未落盘 -> 不发布结论，报告写入失败
            return self._write_failed_result(
                store, probe_id, mode, req, target, attempts, stray,
                epoch_end, df_enabled, started_at, t0, exc,
            )
        return ProbeResult(
            probe_id=probe_id,
            mode=mode,
            status=status,  # type: ignore[arg-type]
            max_confirmed_size=max_confirmed_size,
            reason=reason,
            error=error,
            reached_range_max=reached_range_max,
            min_size=req.min_size,
            max_size=req.max_size,
            attempts=attempts,
            stray_replies=stray,
            config_epoch_start=epoch_start,
            config_epoch_end=epoch_end,
            df_enabled=bool(df_enabled),
            target=target,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
        )

    def _write_failed_result(
        self,
        store: ProbeStore,
        probe_id: str,
        mode: Literal["sync", "background"],
        req: ProbeRequest,
        target: str,
        attempts: list[AttemptOut],
        stray: int,
        epoch_start: int,
        df_enabled: Optional[bool],
        started_at: datetime,
        t0: float,
        exc: ProbeStoreError,
        published: bool = True,
    ) -> ProbeResult:
        finished_at = utcnow()
        duration_ms = round((time.monotonic() - t0) * 1000.0, 3)
        error = str(exc)
        logger.error("探测 %s 记录写入失败，已停止: %s", probe_id, error)
        if published:
            try:
                store.mark_write_failed(
                    probe_id,
                    error=error,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                )
            except ProbeStoreError as mark_exc:
                logger.error("探测 %s 写入失败状态也未能落盘: %s", probe_id, mark_exc)
        return ProbeResult(
            probe_id=probe_id,
            mode=mode,
            status="write_failed",
            max_confirmed_size=None,
            reason=DEFAULT_WRITE_FAILED_REASON,
            error=error,
            reached_range_max=False,
            min_size=req.min_size,
            max_size=req.max_size,
            attempts=attempts,
            stray_replies=stray,
            config_epoch_start=epoch_start,
            config_epoch_end=self._state.epoch,
            df_enabled=bool(df_enabled),
            target=target,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
        )
