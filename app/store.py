"""探测任务的落盘存储（SQLite）。

两种入口（同步 ``POST /probe`` 与后台 ``POST /probe/background``）共用同一
数据库文件，容器通过卷挂载持久化。设计要点：

- **发送前落盘**：每次尝试（探测包或小包复核）先写入 seq / size / purpose 并
  提交，之后才真正发包；尝试结束再写入回包 / 超时 / 发送失败的结果。
- **终态最后落盘**：最后一次尝试结果写入后，才提交整次探测的终态。
- **写失败即停**：任何一次写入失败都会抛出 ``ProbeStoreError``，由探测引擎
  停止本次探测并报告写入失败；此前已提交的记录保持原样。
- **重启恢复**：启动时把所有未结案（queued/running）的探测标为 interrupted；
  没有结果行的尝试保持 outcome=NULL，对外显示为 unconfirmed，不自动重发。
- **保留策略**：仅保留最近结束的 ``max_finished`` 条（默认 200）探测，
  所有进行中的记录一律保留。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

MAX_FINISHED = 200

OPEN_STATUSES = ("queued", "running")
TERMINAL_STATUSES = (
    "ok",
    "below_range",
    "undetermined",
    "invalidated",
    "interrupted",
    "write_failed",
    "failed",
)
#: 对外可见的全部状态
ALL_STATUSES = OPEN_STATUSES + TERMINAL_STATUSES

DEFAULT_WRITE_FAILED_REASON = (
    "探测记录写入失败，已停止本次探测；此前已落盘的记录保留"
)
DEFAULT_INTERRUPTED_REASON = "服务重启，本次探测未结案，已标记为中断"


class ProbeStoreError(RuntimeError):
    """探测记录写入/读取失败。"""

    def __init__(self, message: str, probe_id: Optional[str] = None) -> None:
        super().__init__(message)
        self.probe_id = probe_id


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


SCHEMA = """
CREATE TABLE IF NOT EXISTS probes (
    probe_id            TEXT PRIMARY KEY,
    mode                TEXT NOT NULL,
    status              TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    min_size            INTEGER NOT NULL,
    max_size            INTEGER NOT NULL,
    timeout_ms          INTEGER NOT NULL,
    small_size          INTEGER NOT NULL,
    target              TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    started_at          TEXT,
    finished_at         TEXT,
    df_enabled          INTEGER,
    stray_replies       INTEGER NOT NULL DEFAULT 0,
    config_epoch_start  INTEGER,
    config_epoch_end    INTEGER,
    max_confirmed_size  INTEGER,
    reached_range_max   INTEGER NOT NULL DEFAULT 0,
    reason              TEXT,
    error               TEXT,
    duration_ms         REAL
);

CREATE TABLE IF NOT EXISTS attempts (
    probe_id       TEXT NOT NULL REFERENCES probes(probe_id) ON DELETE CASCADE,
    seq            INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    purpose        TEXT NOT NULL,
    registered_at  TEXT NOT NULL,
    outcome        TEXT,
    rtt_ms         REAL,
    detail         TEXT,
    finished_at    TEXT,
    PRIMARY KEY (probe_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_probes_status_finished
    ON probes(status, finished_at);
CREATE INDEX IF NOT EXISTS idx_attempts_probe_seq
    ON attempts(probe_id, seq);
"""


class ProbeStore:
    """线程安全的 SQLite 存储，单连接 + 互斥锁。"""

    def __init__(self, path: str, max_finished: int = MAX_FINISHED) -> None:
        self.path = path
        self.max_finished = max_finished
        if path not in ("", ":memory:"):
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        # isolation_level=None：由本类显式管理 BEGIN/COMMIT，保证提交即落盘
        self._conn = sqlite3.connect(
            path, check_same_thread=False, timeout=30.0, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON")
            if path not in ("", ":memory:"):
                # WAL + FULL：每次提交都 fsync，满足"发送前落盘"
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = FULL")
            self._conn.execute("PRAGMA busy_timeout = 30000")
            self._conn.executescript(SCHEMA)

    # ------------------------------------------------------------------ 事务

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except sqlite3.Error as exc:  # pragma: no cover - 极端 I/O 故障
                raise ProbeStoreError(f"无法开启数据库事务: {exc}") from exc
            try:
                yield self._conn
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            else:
                try:
                    self._conn.execute("COMMIT")
                except sqlite3.Error as exc:
                    try:
                        self._conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise ProbeStoreError(f"数据库提交失败: {exc}") from exc

    # ------------------------------------------------------------------ 写入

    def create_probe(
        self,
        probe_id: str,
        *,
        mode: str,
        request_json: str,
        min_size: int,
        max_size: int,
        timeout_ms: int,
        small_size: int,
        target: str,
        created_at: datetime,
    ) -> None:
        """保存任务（状态 queued）。返回前已提交，之后才允许发包。"""
        try:
            with self._transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO probes (
                        probe_id, mode, status, request_json,
                        min_size, max_size, timeout_ms, small_size, target,
                        created_at
                    ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        probe_id,
                        mode,
                        request_json,
                        min_size,
                        max_size,
                        timeout_ms,
                        small_size,
                        target,
                        iso(created_at),
                    ),
                )
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法保存探测任务: {exc}", probe_id) from exc

    def mark_running(
        self,
        probe_id: str,
        *,
        started_at: datetime,
        df_enabled: Optional[bool],
        epoch_start: int,
    ) -> None:
        try:
            with self._transaction() as conn:
                cur = conn.execute(
                    """
                    UPDATE probes
                       SET status = 'running',
                           started_at = ?,
                           df_enabled = ?,
                           config_epoch_start = ?
                     WHERE probe_id = ? AND status = 'queued'
                    """,
                    (
                        iso(started_at),
                        None if df_enabled is None else int(df_enabled),
                        epoch_start,
                        probe_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise ProbeStoreError(
                        f"探测 {probe_id} 不存在或状态已不是 queued", probe_id
                    )
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法标记探测运行中: {exc}", probe_id) from exc

    def register_attempt(
        self,
        probe_id: str,
        *,
        seq: int,
        size: int,
        purpose: str,
        registered_at: datetime,
    ) -> None:
        """发送前落盘：序号、长度、用途。提交成功后才可发包。"""
        try:
            with self._transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO attempts (
                        probe_id, seq, size, purpose, registered_at, outcome
                    ) VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (probe_id, seq, size, purpose, iso(registered_at)),
                )
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(
                f"无法登记第 {seq} 次尝试（发送前落盘失败）: {exc}", probe_id
            ) from exc

    def save_attempt_result(
        self,
        probe_id: str,
        *,
        seq: int,
        outcome: str,
        rtt_ms: Optional[float],
        detail: Optional[str],
        finished_at: datetime,
    ) -> None:
        try:
            with self._transaction() as conn:
                conn.execute(
                    """
                    UPDATE attempts
                       SET outcome = ?, rtt_ms = ?, detail = ?, finished_at = ?
                     WHERE probe_id = ? AND seq = ?
                    """,
                    (outcome, rtt_ms, detail, iso(finished_at), probe_id, seq),
                )
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(
                f"无法保存第 {seq} 次尝试结果: {exc}", probe_id
            ) from exc

    def finalize(
        self,
        probe_id: str,
        *,
        status: str,
        reason: Optional[str],
        error: Optional[str],
        max_confirmed_size: Optional[int],
        reached_range_max: bool,
        stray_replies: int,
        config_epoch_start: int,
        config_epoch_end: int,
        df_enabled: Optional[bool],
        started_at: datetime,
        finished_at: datetime,
        duration_ms: float,
    ) -> None:
        """提交终态。只有本次提交成功后，调用方才可发布最终结论。"""
        try:
            with self._transaction() as conn:
                cur = conn.execute(
                    """
                    UPDATE probes
                       SET status = ?,
                           reason = ?,
                           error = ?,
                           max_confirmed_size = ?,
                           reached_range_max = ?,
                           stray_replies = ?,
                           config_epoch_start = COALESCE(config_epoch_start, ?),
                           config_epoch_end = ?,
                           df_enabled = COALESCE(?, df_enabled),
                           started_at = COALESCE(started_at, ?),
                           finished_at = ?,
                           duration_ms = ?
                     WHERE probe_id = ?
                    """,
                    (
                        status,
                        reason,
                        error,
                        max_confirmed_size,
                        int(reached_range_max),
                        stray_replies,
                        config_epoch_start,
                        config_epoch_end,
                        None if df_enabled is None else int(df_enabled),
                        iso(started_at),
                        iso(finished_at),
                        duration_ms,
                        probe_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise ProbeStoreError(f"探测 {probe_id} 不存在", probe_id)
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法保存探测终态: {exc}", probe_id) from exc
        # 终态已提交；清理旧记录属于后台维护，失败不得影响已发布的结论
        try:
            self.prune_finished()
        except ProbeStoreError:
            pass

    def mark_write_failed(
        self,
        probe_id: str,
        *,
        error: str,
        started_at: datetime,
        finished_at: datetime,
        duration_ms: Optional[float],
    ) -> None:
        """尽力记录"写入失败"终态；本身失败时由调用方吞掉。"""
        try:
            with self._transaction() as conn:
                conn.execute(
                    """
                    UPDATE probes
                       SET status = 'write_failed',
                           reason = ?,
                           error = ?,
                           max_confirmed_size = NULL,
                           reached_range_max = 0,
                           started_at = COALESCE(started_at, ?),
                           finished_at = COALESCE(finished_at, ?),
                           duration_ms = COALESCE(duration_ms, ?)
                     WHERE probe_id = ?
                    """,
                    (
                        DEFAULT_WRITE_FAILED_REASON,
                        error,
                        iso(started_at),
                        iso(finished_at),
                        duration_ms,
                        probe_id,
                    ),
                )
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法记录写入失败状态: {exc}", probe_id) from exc
        try:
            self.prune_finished()
        except ProbeStoreError:
            pass

    def interrupt_open_probes(self, now: Optional[datetime] = None) -> int:
        """启动时调用：未结案探测 -> interrupted，不重发、不给负载上限。"""
        now = now or utcnow()
        count = 0
        try:
            with self._transaction() as conn:
                rows = conn.execute(
                    """
                    SELECT probe_id, started_at, created_at
                      FROM probes
                     WHERE status IN ('queued', 'running')
                    """
                ).fetchall()
                for row in rows:
                    base = row["started_at"] or row["created_at"]
                    duration: Optional[float] = None
                    if base:
                        try:
                            duration = round(
                                (now - datetime.fromisoformat(base)).total_seconds()
                                * 1000.0,
                                3,
                            )
                        except ValueError:  # pragma: no cover - 存储损坏
                            duration = None
                    conn.execute(
                        """
                        UPDATE probes
                           SET status = 'interrupted',
                               reason = COALESCE(reason, ?),
                               max_confirmed_size = NULL,
                               reached_range_max = 0,
                               finished_at = COALESCE(finished_at, ?),
                               duration_ms = COALESCE(duration_ms, ?)
                         WHERE probe_id = ?
                        """,
                        (
                            DEFAULT_INTERRUPTED_REASON,
                            iso(now),
                            duration,
                            row["probe_id"],
                        ),
                    )
                    count += 1
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法标记中断探测: {exc}") from exc
        self.prune_finished()
        return count

    def prune_finished(self) -> int:
        """只保留最近结束的 max_finished 条；进行中的记录永不删除。"""
        try:
            with self._transaction() as conn:
                before = conn.execute(
                    "SELECT COUNT(*) AS n FROM probes WHERE status NOT IN ('queued', 'running')"
                ).fetchone()["n"]
                conn.execute(
                    """
                    DELETE FROM probes
                     WHERE status NOT IN ('queued', 'running')
                       AND probe_id NOT IN (
                           SELECT probe_id
                             FROM probes
                            WHERE status NOT IN ('queued', 'running')
                            ORDER BY COALESCE(finished_at, created_at) DESC,
                                     rowid DESC
                            LIMIT ?
                       )
                    """,
                    (self.max_finished,),
                )
                after = conn.execute(
                    "SELECT COUNT(*) AS n FROM probes WHERE status NOT IN ('queued', 'running')"
                ).fetchone()["n"]
                return max(0, before - after)
        except ProbeStoreError:
            raise
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法清理历史探测记录: {exc}") from exc

    # ------------------------------------------------------------------ 读取

    def get_probe(self, probe_id: str) -> Optional[dict[str, Any]]:
        try:
            with self._lock:
                row = self._conn.execute(
                    "SELECT * FROM probes WHERE probe_id = ?", (probe_id,)
                ).fetchone()
                if row is None:
                    return None
                attempts = self._conn.execute(
                    "SELECT * FROM attempts WHERE probe_id = ? ORDER BY seq ASC",
                    (probe_id,),
                ).fetchall()
            return {"probe": dict(row), "attempts": [dict(a) for a in attempts]}
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法读取探测记录: {exc}", probe_id) from exc

    def list_probes(self) -> list[dict[str, Any]]:
        try:
            with self._lock:
                rows = self._conn.execute(
                    "SELECT * FROM probes ORDER BY created_at DESC, rowid DESC"
                ).fetchall()
            return [dict(r) for r in rows]
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法读取探测列表: {exc}") from exc

    def count_finished(self) -> int:
        try:
            with self._lock:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM probes WHERE status NOT IN ('queued', 'running')"
                ).fetchone()
            return int(row["n"])
        except sqlite3.Error as exc:
            raise ProbeStoreError(f"无法统计探测记录: {exc}") from exc

    # ------------------------------------------------------------------ 生命周期

    def close(self) -> None:
        with self._lock:
            self._conn.close()
