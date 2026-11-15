"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CollectRequest(BaseModel):
    notice_number: str = Field(..., description="通报号，例如 2024-003", examples=["2024-003"])


class SourceView(BaseModel):
    url: str
    title: str | None = None
    fetched_at: str
    content_hash: str


class BatchView(BaseModel):
    batch_number: str
    source_url: str


class EdgeView(BaseModel):
    from_notice: str
    to_notice: str
    source_url: str
    target_url: str


class NoticeView(BaseModel):
    notice_number: str
    latest_snapshot_id: int
    site_revision: int
    content_hash: str
    sources: list[SourceView]
    batches: list[BatchView]
    corrects: list[str]
    corrected_by: list[str]


class AncestorView(BaseModel):
    notice_number: str
    depth: int
    via_url: str | None = None


class CollectResponse(BaseModel):
    run_id: int
    status: str
    snapshot_id: int | None = None
    site_revision: int | None = None
    seed_notice_number: str
    pages_fetched: int
    notices: int
    edges: int
    failure_reason: str | None = None
    conflict_detail: dict[str, Any] | None = None


# ---- 数仓消费者同步（拉取 / 确认） --------------------------------

class SyncNoticeView(BaseModel):
    notice_number: str
    content_hash: str
    site_revision: int
    sources: list[SourceView]
    batches: list[BatchView]
    corrects: list[str]
    corrected_by: list[str]


class SyncSnapshotView(BaseModel):
    """一张完整快照：通报、来源、批号与引用边一次交齐。"""
    snapshot_id: int
    site_revision: int
    root_notice_number: str
    content_hash: str
    created_at: str
    notices: list[SyncNoticeView]
    edges: list[EdgeView]


class SyncPullResponse(BaseModel):
    consumer_id: str
    status: str = Field(..., description="delivered（有新交付/重发同一张）或 empty（无可交付快照）")
    redelivery: bool = Field(..., description="true 表示上次交付尚未确认，本次为同一张快照的重发")
    snapshot: SyncSnapshotView | None = None
    last_acked_snapshot_id: int | None = Field(
        None, description="已确认到的快照 id；null 表示尚未确认任何快照")
    delivered_snapshot_id: int | None = None
    pending_count: int = Field(..., description="确认游标之后仍待交付（含当前已交付未确认）的已发布快照数")


class SyncAckRequest(BaseModel):
    snapshot_id: int = Field(..., description="要确认的快照 id，必须是当前已交付的那一张",
                             examples=[1])


class SyncAckResponse(BaseModel):
    consumer_id: str
    status: str = Field(..., description="acked（本次确认成功）或 already_acked（重复确认，无害）")
    acked_snapshot_id: int
    last_acked_snapshot_id: int | None
    delivered_snapshot_id: int | None
    pending_count: int


class SyncStateView(BaseModel):
    consumer_id: str
    last_acked_snapshot_id: int | None
    delivered_snapshot_id: int | None
    delivered_at: str | None
    acked_at: str | None
    pending_count: int
    published_snapshots: int
