"""后台探测、持久化、重启恢复与保留策略测试。"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.prober import ProbeRequest
from app.store import ProbeStore, ProbeStoreError, utcnow


@pytest.fixture()
def db_path(tmp_path, monkeypatch):
    path = tmp_path / "probes.db"
    monkeypatch.setenv("PROBE_DB_PATH", str(path))
    return path


@pytest.fixture()
def client(db_path):
    with TestClient(app) as c:
        yield c


def set_config(client, **kw):
    r = client.put("/echo/config", json=kw)
    assert r.status_code == 200, r.text
    return r.json()["epoch"]


def wait_terminal(client, probe_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/probe/{probe_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        if body["status"] not in ("queued", "running"):
            return body
        time.sleep(0.05)
    raise AssertionError(f"探测 {probe_id} 未在 {timeout}s 内结束")


# ---------------------------------------------------------------- 后台入口


class TestBackgroundProbe:
    def test_openapi_and_docs(self, client):
        assert client.get("/docs").status_code == 200
        spec = client.get("/openapi.json").json()
        assert "/probe/background" in spec["paths"]
        assert "/probe" in spec["paths"]
        assert "/probe/{probe_id}" in spec["paths"]

    def test_returns_probe_id_before_sending(self, client):
        set_config(client, mode="normal", drop_threshold=1000)
        r = client.post("/probe/background", json={"timeout_ms": 150})
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["probe_id"]
        assert body["status"] == "queued"
        assert body["max_confirmed_size"] is None
        assert body["detail_url"] == f"/probe/{body['probe_id']}"

        # 任务已在数据库中（先落盘后发包）
        detail = client.get(f"/probe/{body['probe_id']}").json()
        assert detail["status"] in ("queued", "running", "ok")
        assert detail["mode"] == "background"

        final = wait_terminal(client, body["probe_id"])
        assert final["status"] == "ok"
        assert final["max_confirmed_size"] == 1000
        assert final["attempts"]
        seqs = [a["seq"] for a in final["attempts"]]
        assert seqs == sorted(seqs) == list(range(1, len(seqs) + 1))
        assert all(a["size"] >= 28 and a["purpose"] in ("probe", "link-verify")
                   for a in final["attempts"])
        # 探测与小包复核都记录了落盘时间
        assert all(a["registered_at"] for a in final["attempts"])
        assert all(a["outcome"] in ("ok", "timeout", "send_error") for a in final["attempts"])

    def test_background_probe_survives_http_request(self, client):
        set_config(client, mode="disconnect")
        probe_id = client.post(
            "/probe/background", json={"timeout_ms": 100}
        ).json()["probe_id"]
        final = wait_terminal(client, probe_id)
        assert final["status"] == "undetermined"
        assert final["max_confirmed_size"] is None

    def test_background_and_sync_share_store(self, client):
        set_config(client, mode="normal", drop_threshold=1200)
        sync = client.post("/probe", json={"timeout_ms": 100})
        assert sync.status_code == 200, sync.text
        sync_id = sync.json()["probe_id"]
        bg_id = client.post(
            "/probe/background", json={"timeout_ms": 100}
        ).json()["probe_id"]
        wait_terminal(client, bg_id)
        listing = client.get("/probes").json()
        ids = {p["probe_id"] for p in listing}
        assert {sync_id, bg_id} <= ids
        assert all("mode" in p for p in listing)

    def test_request_validation_same_as_probe(self, client):
        assert client.post(
            "/probe/background", json={"min_size": 1000, "max_size": 576}
        ).status_code == 422
        assert client.post(
            "/probe/background", json={"timeout_ms": 5}
        ).status_code == 422


# ---------------------------------------------------------------- 落盘顺序


class TestDurability:
    def test_attempt_registered_before_send_and_result_after(self, client, monkeypatch):
        store: ProbeStore = client.app.state.store
        calls: list[tuple[str, tuple, dict]] = []
        orig_register = store.register_attempt
        orig_save = store.save_attempt_result

        def register(*args, **kwargs):
            calls.append(("register", args, kwargs))
            return orig_register(*args, **kwargs)

        def save(*args, **kwargs):
            calls.append(("save", args, kwargs))
            return orig_save(*args, **kwargs)

        monkeypatch.setattr(store, "register_attempt", register)
        monkeypatch.setattr(store, "save_attempt_result", save)
        res = client.post("/probe", json={"timeout_ms": 100, "max_size": 600}).json()
        assert res["status"] == "ok"
        assert calls, "未观察到落盘调用"
        assert calls[0][0] == "register"
        kinds = [c[0] for c in calls]
        # 每次尝试都是先 register 再 save，且不交错
        assert kinds == ["register", "save"] * (len(kinds) // 2)

        rows = client.app.state.store.get_probe(res["probe_id"])
        assert rows is not None
        assert [a["seq"] for a in rows["attempts"]] == list(range(1, len(calls) // 2 + 1))
        assert all(a["registered_at"] for a in rows["attempts"])
        assert all(a["finished_at"] for a in rows["attempts"])

    def test_write_failure_before_send_stops_without_sending(self, client, monkeypatch):
        store: ProbeStore = client.app.state.store
        before = client.get("/echo/config").json()["stats"]

        def fail_register(*args, **kwargs):
            raise ProbeStoreError("磁盘不可写（测试注入）")

        monkeypatch.setattr(store, "register_attempt", fail_register)
        r = client.post("/probe", json={"timeout_ms": 100})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "write_failed"
        assert "写入失败" in body["reason"]
        assert "磁盘不可写" in body["error"]
        assert body["max_confirmed_size"] is None
        # 发送前落盘失败 -> 一个包都没发
        after = client.get("/echo/config").json()["stats"]
        assert after == before

        detail = client.get(f"/probe/{body['probe_id']}").json()
        assert detail["status"] == "write_failed"
        assert detail["max_confirmed_size"] is None

    def test_result_write_failure_keeps_registered_attempt_unconfirmed(
        self, client, monkeypatch
    ):
        store: ProbeStore = client.app.state.store
        orig_save = store.save_attempt_result

        def fail_save(*args, **kwargs):
            raise ProbeStoreError("结果落盘失败（测试注入）")

        monkeypatch.setattr(store, "save_attempt_result", fail_save)
        body = client.post("/probe", json={"timeout_ms": 100}).json()
        assert body["status"] == "write_failed"
        assert body["max_confirmed_size"] is None
        assert body["attempts"] == []  # 结果未落盘，不发布任何尝试结论

        detail = client.get(f"/probe/{body['probe_id']}").json()
        assert detail["status"] == "write_failed"
        assert len(detail["attempts"]) == 1
        att = detail["attempts"][0]
        assert att["seq"] == 1 and att["outcome"] == "unconfirmed"
        assert att["purpose"] == "probe"
        assert att["finished_at"] is None

        # 恢复后不会自动重发：尝试条数保持不变
        monkeypatch.setattr(store, "save_attempt_result", orig_save)
        time.sleep(0.1)
        again = client.get(f"/probe/{body['probe_id']}").json()
        assert len(again["attempts"]) == 1
        assert again["status"] == "write_failed"

    def test_finalize_failure_does_not_publish_conclusion(self, client, monkeypatch):
        store: ProbeStore = client.app.state.store

        def fail_finalize(*args, **kwargs):
            raise ProbeStoreError("终态落盘失败（测试注入）")

        monkeypatch.setattr(store, "finalize", fail_finalize)
        body = client.post("/probe", json={"timeout_ms": 100, "max_size": 600}).json()
        assert body["status"] == "write_failed"
        assert body["max_confirmed_size"] is None
        assert "写入失败" in body["reason"]


# ---------------------------------------------------------------- 重启恢复


class TestRestartRecovery:
    def test_open_probe_marked_interrupted_with_unconfirmed_attempt(self, db_path):
        with TestClient(app) as c:
            store: ProbeStore = c.app.state.store
            req = ProbeRequest()
            probe_id = c.app.state.prober.create_task(
                req, store, mode="background"
            )
            store.mark_running(
                probe_id,
                started_at=utcnow(),
                df_enabled=True,
                epoch_start=0,
            )
            # 已登记、发包前落盘成功，但结果没来得及保存
            store.register_attempt(
                probe_id,
                seq=1,
                size=req.min_size,
                purpose="probe",
                registered_at=utcnow(),
            )
            store.register_attempt(
                probe_id,
                seq=2,
                size=req.small_size,
                purpose="link-verify",
                registered_at=utcnow(),
            )

        # 重启：新 lifespan 使用同一数据库文件
        with TestClient(app) as c:
            assert c.app.state.interrupted_on_start == 1
            detail = c.get(f"/probe/{probe_id}").json()
            assert detail["status"] == "interrupted"
            assert detail["max_confirmed_size"] is None
            assert detail["reached_range_max"] is False
            assert "中断" in detail["reason"]
            assert len(detail["attempts"]) == 2
            assert [a["outcome"] for a in detail["attempts"]] == [
                "unconfirmed",
                "unconfirmed",
            ]
            assert all(a["finished_at"] is None for a in detail["attempts"])
            # 不自动重发：重启前后记录一致
            time.sleep(0.1)
            again = c.get(f"/probe/{probe_id}").json()
            assert len(again["attempts"]) == 2
            assert again["status"] == "interrupted"

    def test_interrupted_task_is_not_executed(self, db_path):
        store = ProbeStore(str(db_path))
        prober_req = ProbeRequest()
        # 用独立 store 造一个未结案任务，模拟重启后的状态
        store.create_probe(
            "11111111-1111-4111-8111-111111111111",
            mode="background",
            request_json=prober_req.model_dump_json(),
            min_size=prober_req.min_size,
            max_size=prober_req.max_size,
            timeout_ms=prober_req.timeout_ms,
            small_size=prober_req.small_size,
            target="127.0.0.1:9999",
            created_at=utcnow(),
        )
        store.interrupt_open_probes()
        store.close()

        with TestClient(app) as c:
            body = c.app.state.prober  # 仅确保 prober 已就绪
            assert body is not None
            detail = c.get("/probe/11111111-1111-4111-8111-111111111111").json()
            assert detail["status"] == "interrupted"
            assert detail["attempts"] == []

    def test_finished_probe_survives_restart(self, db_path):
        with TestClient(app) as c:
            res = c.post("/probe", json={"timeout_ms": 100}).json()
            probe_id = res["probe_id"]
        with TestClient(app) as c:
            assert c.app.state.interrupted_on_start == 0
            detail = c.get(f"/probe/{probe_id}").json()
            assert detail["status"] == "ok"
            assert detail["attempts"]
            assert all(
                a["outcome"] in ("ok", "timeout", "send_error")
                for a in detail["attempts"]
            )


# ---------------------------------------------------------------- 保留策略


class TestRetention:
    def _insert(self, store: ProbeStore, status: str, finished_at=None, n=1):
        ids = []
        for i in range(n):
            pid = str(uuid.uuid4())
            store.create_probe(
                pid,
                mode="sync",
                request_json=ProbeRequest().model_dump_json(),
                min_size=576,
                max_size=1400,
                timeout_ms=100,
                small_size=64,
                target="127.0.0.1:9999",
                created_at=utcnow(),
            )
            if status != "queued":
                store.mark_running(
                    pid, started_at=utcnow(), df_enabled=True, epoch_start=0
                )
            if status not in ("queued", "running"):
                store.finalize(
                    pid,
                    status=status,
                    reason=None,
                    error=None,
                    max_confirmed_size=None,
                    reached_range_max=False,
                    stray_replies=0,
                    config_epoch_start=0,
                    config_epoch_end=0,
                    df_enabled=True,
                    started_at=utcnow(),
                    finished_at=finished_at or utcnow(),
                    duration_ms=1.0,
                )
            ids.append(pid)
        return ids

    def test_keeps_latest_200_finished_and_all_open(self, db_path):
        store = ProbeStore(str(db_path), max_finished=200)
        base = datetime.now(timezone.utc)
        self._insert(store, "ok", base - timedelta(minutes=2), n=205)
        open_ids = self._insert(store, "running", n=3)
        store.prune_finished()

        assert store.count_finished() == 200
        rows = store.list_probes()
        open_in_list = [r for r in rows if r["status"] in ("queued", "running")]
        assert {r["probe_id"] for r in open_in_list} == set(open_ids)
        # 最近结束的记录都在
        latest = self._insert(store, "ok", base + timedelta(minutes=1), n=1)[0]
        assert store.get_probe(latest) is not None
        assert store.count_finished() == 200
        store.close()

    def test_finalize_triggers_pruning(self, db_path):
        store = ProbeStore(str(db_path), max_finished=2)
        self._insert(store, "ok", n=2)
        self._insert(store, "ok", n=1)
        assert store.count_finished() == 2
        store.close()

    def test_pruned_probe_returns_404(self, db_path):
        store = ProbeStore(str(db_path), max_finished=1)
        old_id = self._insert(store, "ok", n=1)[0]
        self._insert(store, "ok", n=1)
        assert store.get_probe(old_id) is None
        store.close()


def test_sync_probe_still_returns_conclusion(client):
    set_config(client, mode="normal", drop_threshold=1000)
    r = client.post("/probe", json={"timeout_ms": 150})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["max_confirmed_size"] == 1000
    assert body["mode"] == "sync"
