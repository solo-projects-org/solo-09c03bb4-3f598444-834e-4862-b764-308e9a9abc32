"""FastAPI 入口：采集、关系/批号查询、运行与快照审计。

Swagger UI：启动后访问 /docs，可直接发起采集与查询。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .db import AckOutOfOrderError, Database
from .fetcher import HttpFetcher
from .models import (
    AncestorView,
    CollectRequest,
    CollectResponse,
    NoticeView,
    SyncAckRequest,
    SyncAckResponse,
    SyncPullResponse,
    SyncStateView,
)
from .service import CollectorService

DB_PATH = os.environ.get("COLLECTOR_DB", "/data/collector/collector.db")
SITE_BASE_URL = os.environ.get("SITE_BASE_URL", "http://site:8080")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Database(DB_PATH)
    app.state.fetcher = HttpFetcher(SITE_BASE_URL)
    app.state.service = CollectorService(app.state.db, app.state.fetcher)
    try:
        yield
    finally:
        await app.state.fetcher.aclose()
        app.state.db.close()


app = FastAPI(
    title="药品召回更正公告采集 API",
    version="1.0.0",
    description=(
        "提交通报号后，沿公告中的“更正自”链接递归抓取旧公告，"
        "按通报号合并不同 URL，保存来源 URL、批号与引用边。\n\n"
        "关键语义：\n"
        "- 整次采集必须落在同一站点版号（每页回传 X-Site-Revision）；"
        "跨版、同号异文、链接失效均整次不发布，查询继续返回旧图；\n"
        "- 循环链接按 URL 去重，不重复采集；\n"
        "- 并发采集时晚完成的旧版发布被 CAS 拒绝，不能覆盖新版；\n"
        "- 整张子图单事务原子写入，重启不会读到半张图。\n\n"
        "内置样例：2024-003（更正链 003→002→001）、2024-001（同号镜像合并）、"
        "CYCLE-A/CYCLE-B（循环引用）；站点 v2 制造同号异文冲突，v3 制造链接失效。\n\n"
        "数仓同步（单消费者）：POST /sync/pull 按发布顺序交付一张完整快照；"
        "未确认时重拉返回同一张；POST /sync/ack 只接受已交付的当前快照，"
        "重复确认幂等，越序确认 409。游标持久化，重启后可续传。"
    ),
    lifespan=lifespan,
    openapi_tags=[
        {"name": "采集", "description": "发起整次采集，失败一律不发布"},
        {"name": "查询", "description": "读取最新已发布图中的通报、批号与引用关系"},
        {"name": "数仓同步", "description": "单消费者的快照拉取与确认（至多一次未确认交付）"},
        {"name": "审计", "description": "运行历史与快照版本历史"},
        {"name": "运维", "description": "健康检查"},
    ],
)


@app.post("/collect", response_model=CollectResponse, tags=["采集"],
          summary="按通报号发起一次采集",
          responses={
              409: {"description": "跨版 / 同号异文 / 链接失效 / 旧版覆盖被拒绝，"
                                    "整次不发布，旧图保留"},
          })
async def collect(req: CollectRequest) -> JSONResponse:
    result = await app.state.service.collect(req.notice_number)
    status = result.pop("http_status")
    return JSONResponse(status_code=status, content=result)


@app.get("/notices/{notice_number}", response_model=NoticeView, tags=["查询"],
         summary="按通报号查询来源、批号与引用关系（读取最新已发布图）")
async def get_notice(notice_number: str) -> dict:
    data = app.state.db.get_notice(notice_number)
    if data is None:
        raise HTTPException(status_code=404,
                            detail=f"已发布图中不存在通报号 {notice_number}，"
                                   "若采集曾失败，请用 GET /runs 查看原因")
    return data


@app.get("/notices/{notice_number}/ancestors",
         response_model=list[AncestorView], tags=["查询"],
         summary="查询某通报号沿“更正自”链接可追溯到的全部旧公告")
async def get_ancestors(notice_number: str) -> list[dict]:
    data = app.state.db.get_ancestors(notice_number)
    if data is None:
        raise HTTPException(status_code=404, detail=f"通报号 {notice_number} 尚未发布")
    return data


@app.get("/graph", tags=["查询"], summary="查看最新已发布的整张子图（通报号节点+引用边）")
async def get_graph() -> dict:
    data = app.state.db.get_graph()
    if data is None:
        raise HTTPException(status_code=404, detail="尚无已发布快照")
    return data


@app.get("/runs", tags=["审计"], summary="采集运行历史（含失败原因与冲突明细）")
async def list_runs(limit: int = 50) -> list[dict]:
    return app.state.db.list_runs(limit)


@app.get("/snapshots", tags=["审计"], summary="快照版本历史（已发布快照不可变；staging 残留会在启动时清理）")
async def list_snapshots() -> list[dict]:
    return app.state.db.list_snapshots()


# ---- 数仓消费者同步 -------------------------------------------------

@app.post("/sync/pull", response_model=SyncPullResponse, tags=["数仓同步"],
          summary="拉取下一张完整快照（未确认时重拉返回同一张）",
          description=(
              "按发布顺序（snapshot id 升序）交付一张完整快照，包含通报、来源 URL、"
              "批号与引用边。\n\n"
              "- 存在未确认的已交付快照时，原样重发同一张（`redelivery=true`），"
              "不会跳过它去交付更新的快照；\n"
              "- 采集失败、同版复用（noop）不产生新快照，因此不会产生新交付；\n"
              "- 接入前已发布的快照同样会被交付；游标持久化，服务重启后继续。\n"
              "- 没有可交付快照时返回 `status=empty`。"))
async def sync_pull() -> dict:
    return app.state.db.pull_snapshot()


@app.post("/sync/ack", response_model=SyncAckResponse, tags=["数仓同步"],
          summary="确认已交付的当前快照",
          description=(
              "只接受当前已交付的那一张快照，确认后游标推进并可拉取下一张。\n\n"
              "- 重复确认任何已确认过的快照返回 `already_acked`（无害，便于 ack 响应丢失后重试）；\n"
              "- 确认未交付的、或尚未推进到的更晚快照返回 409（越序确认）。"),
          responses={409: {"description": "越序确认：快照未交付、已过期或超出当前游标"}})
async def sync_ack(req: SyncAckRequest) -> dict:
    try:
        return app.state.db.ack_snapshot(req.snapshot_id)
    except AckOutOfOrderError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/sync/state", response_model=SyncStateView, tags=["数仓同步"],
         summary="查看单消费者同步游标与待交付数量")
async def sync_state() -> dict:
    return app.state.db.sync_state()


@app.get("/healthz", tags=["运维"], summary="健康检查")
async def healthz() -> dict:
    return {"status": "ok"}
