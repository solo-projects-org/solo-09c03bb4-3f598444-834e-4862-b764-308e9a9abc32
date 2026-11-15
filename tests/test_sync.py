"""数仓单消费者同步测试：按序交付、重发、确认、越序拒绝、失败/复用不出新交付、重启续传。"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector.db import AckOutOfOrderError, Database
from collector.service import CollectorService
from tests.fakes import FakeFetcher, FakePage
from tests.html_fixtures import index_html, notice_html


class _MiniRecord:
    def __init__(self, number, hash_, urls, batches, correction_targets):
        self.notice_number = number
        self.content_hash = hash_
        self.source_urls = urls
        self.titles = [f"title-{number}"]
        self.batches = batches
        self.correction_targets = correction_targets


class _MiniCrawl:
    def __init__(self, revision, notices, edges):
        self.site_revision = revision
        self.notices = notices
        self.edges = edges


def _crawl(revision: int, suffix: str = "") -> _MiniCrawl:
    notices = {
        "N1": _MiniRecord("N1", f"hash-n1-{revision}{suffix}",
                          ["http://x/n1"], [("B1", "http://x/n1")], []),
        "N2": _MiniRecord("N2", f"hash-n2-{revision}{suffix}",
                          ["http://x/n2"], [("B2", "http://x/n2")],
                          [("http://x/n1", "http://x/n2")]),
    }
    edges = [{"from_notice": "N2", "to_notice": "N1",
              "source_url": "http://x/n2", "target_url": "http://x/n1"}]
    return _MiniCrawl(revision, notices, edges)


def _publish(db: Database, revision: int, suffix: str = "") -> int:
    return db.publish_snapshot(_crawl(revision, suffix), "N2")["snapshot_id"]


def test_empty_before_any_publish(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    out = db.pull_snapshot()
    assert out["status"] == "empty"
    assert out["snapshot"] is None
    assert out["last_acked_snapshot_id"] is None
    assert db.sync_state()["pending_count"] == 0
    db.close()


def test_delivers_in_publish_order_with_full_snapshot(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    first = _publish(db, 1)
    second = _publish(db, 2)

    out = db.pull_snapshot()
    assert out["status"] == "delivered"
    assert out["delivered_snapshot_id"] == first
    snap = out["snapshot"]
    assert snap["snapshot_id"] == first
    assert {n["notice_number"] for n in snap["notices"]} == {"N1", "N2"}
    n2 = next(n for n in snap["notices"] if n["notice_number"] == "N2")
    assert n2["batches"] == [{"batch_number": "B2", "source_url": "http://x/n2"}]
    assert n2["corrects"] == ["N1"]
    assert n2["sources"][0]["url"] == "http://x/n2"
    assert snap["edges"] == [{"from_notice": "N2", "to_notice": "N1",
                              "source_url": "http://x/n2", "target_url": "http://x/n1"}]
    assert out["pending_count"] == 2  # 当前已交付 + 后面一张

    # 未确认前重拉：同一张，且不会越过它去交付第二张
    again = db.pull_snapshot()
    assert again["status"] == "delivered"
    assert again["redelivery"] is True
    assert again["snapshot"]["snapshot_id"] == first

    ack = db.ack_snapshot(first)
    assert ack["status"] == "acked"
    assert ack["last_acked_snapshot_id"] == first
    nxt = db.pull_snapshot()
    assert nxt["snapshot"]["snapshot_id"] == second
    assert nxt["redelivery"] is False
    assert db.ack_snapshot(second)["status"] == "acked"
    assert db.pull_snapshot()["status"] == "empty"
    db.close()


def test_duplicate_ack_is_harmless(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    sid = _publish(db, 1)
    db.pull_snapshot()
    assert db.ack_snapshot(sid)["status"] == "acked"
    repeat = db.ack_snapshot(sid)
    assert repeat["status"] == "already_acked"
    assert repeat["last_acked_snapshot_id"] == sid
    # 重复确认后仍可继续拉取下一张
    sid2 = _publish(db, 2)
    assert db.pull_snapshot()["snapshot"]["snapshot_id"] == sid2
    db.close()


def test_ack_out_of_order_is_rejected(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    first = _publish(db, 1)
    second = _publish(db, 2)

    # 尚未交付任何快照：确认被拒绝
    with pytest.raises(AckOutOfOrderError):
        db.ack_snapshot(first)

    db.pull_snapshot()  # 交付 first
    # 尝试确认后面那张（越序）被拒绝
    with pytest.raises(AckOutOfOrderError):
        db.ack_snapshot(second)
    # 确认当前交付的 first 成功
    assert db.ack_snapshot(first)["status"] == "acked"
    # 已推进游标后，再确认更早的快照被拒绝（尚未交付 second）
    with pytest.raises(AckOutOfOrderError):
        db.ack_snapshot(999)
    db.pull_snapshot()  # 交付 second
    # 再次确认第一张属于“重复确认”，无害而非越序
    assert db.ack_snapshot(first)["status"] == "already_acked"
    # 确认尚未交付的 third 仍然拒绝
    with pytest.raises(AckOutOfOrderError):
        db.ack_snapshot(999)
    db.close()


def test_every_acked_snapshot_redelivery_is_harmless(tmp_path):
    """ack 响应丢失后，消费者可能重试任意已确认快照，均应无害。"""
    db = Database(str(tmp_path / "s.db"))
    ids = [_publish(db, rev) for rev in (1, 2, 3)]
    for sid in ids:
        db.pull_snapshot()
        assert db.ack_snapshot(sid)["status"] == "acked"
    for sid in ids:
        assert db.ack_snapshot(sid)["status"] == "already_acked"
    # 非已发布快照（不存在）不能借重复确认之名通过
    with pytest.raises(AckOutOfOrderError):
        db.ack_snapshot(10_000)
    assert db.sync_state()["last_acked_snapshot_id"] == ids[-1]
    db.close()


def test_legacy_published_snapshots_are_delivered(tmp_path):
    path = str(tmp_path / "s.db")
    db = Database(path)
    legacy = [_publish(db, 1), _publish(db, 2)]
    db.close()

    # 重新打开（相当于消费者接入已有库）：已发布快照从最早一张开始可领
    db2 = Database(path)
    out = db2.pull_snapshot()
    assert out["snapshot"]["snapshot_id"] == legacy[0]
    assert db2.ack_snapshot(legacy[0])["status"] == "acked"
    assert db2.pull_snapshot()["snapshot"]["snapshot_id"] == legacy[1]
    db2.close()


def test_restart_mid_delivery_resumes_same_snapshot(tmp_path):
    path = str(tmp_path / "s.db")
    db = Database(path)
    first = _publish(db, 1)
    _publish(db, 2)
    db.pull_snapshot()
    db.close()  # 交付后、确认前进程退出

    db2 = Database(path)
    state = db2.sync_state()
    assert state["last_acked_snapshot_id"] is None
    assert state["delivered_snapshot_id"] == first
    again = db2.pull_snapshot()
    assert again["redelivery"] is True
    assert again["snapshot"]["snapshot_id"] == first
    assert db2.ack_snapshot(first)["status"] == "acked"
    # 继续交付下一张
    assert db2.pull_snapshot()["snapshot"]["snapshot_id"] == first + 1
    db2.close()


def test_failed_and_noop_collect_do_not_create_new_delivery(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    pages = {
        "/index.html": index_html([("N1", "/notice/n1.html")]),
        "/notice/n1.html": notice_html("N1", "公告", ["B1"]),
    }
    fetcher = FakeFetcher({1: {p: FakePage(h, 1) for p, h in pages.items()}}, 1)
    service = CollectorService(db, fetcher)

    first = asyncio.run(service.collect("N1"))
    assert first["status"] == "published"
    published = first["snapshot_id"]
    assert db.pull_snapshot()["snapshot"]["snapshot_id"] == published
    assert db.ack_snapshot(published)["status"] == "acked"

    # 同版同内容：noop，不产生新快照，也就没有新交付
    noop = asyncio.run(service.collect("N1"))
    assert noop["status"] == "noop"
    assert noop["snapshot_id"] == published
    assert db.pull_snapshot()["status"] == "empty"
    assert len(db.list_snapshots()) == 1

    # 链接失效：failed，不产生新快照
    fetcher.pages = {1: {"/index.html": FakePage(pages["/index.html"], 1)}}
    failed = asyncio.run(service.collect("N1"))
    assert failed["status"] == "failed"
    assert db.pull_snapshot()["status"] == "empty"
    assert len(db.list_snapshots()) == 1
    db.close()


def test_collect_interleaved_with_pull_and_ack(tmp_path):
    db = Database(str(tmp_path / "s.db"))
    first = _publish(db, 1)
    delivered = db.pull_snapshot()
    assert delivered["snapshot"]["snapshot_id"] == first

    # 交付未确认期间，新采集发布更晚的快照
    second = _publish(db, 2)
    still = db.pull_snapshot()
    assert still["snapshot"]["snapshot_id"] == first  # 不跳号、不漏图

    assert db.ack_snapshot(first)["status"] == "acked"
    nxt = db.pull_snapshot()
    assert nxt["snapshot"]["snapshot_id"] == second
    db.close()


def test_concurrent_pull_ack_threads_do_not_lose_or_fork(tmp_path):
    """并发拉取/确认共用一条连接时不得嵌套事务或漏图。"""
    import threading

    db = Database(str(tmp_path / "s.db"))
    ids = [_publish(db, rev) for rev in range(1, 6)]
    seen: list[int] = []
    errors: list[str] = []

    def worker() -> None:
        try:
            for _ in range(30):
                out = db.pull_snapshot()
                if out["status"] == "delivered":
                    seen.append(out["snapshot"]["snapshot_id"])
                    try:
                        db.ack_snapshot(out["snapshot"]["snapshot_id"])
                    except AckOutOfOrderError:
                        pass
                db.sync_state()
        except Exception as exc:  # pragma: no cover - 失败时给出诊断
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert set(seen) == set(ids)
    assert db.sync_state()["last_acked_snapshot_id"] == ids[-1]
    assert db.pull_snapshot()["status"] == "empty"
    db.close()
