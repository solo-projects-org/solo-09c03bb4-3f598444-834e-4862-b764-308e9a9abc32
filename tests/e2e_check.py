#!/usr/bin/env python3
"""端到端场景矩阵：对运行中的站点+采集 API 做全链路验证。"""
import json
import urllib.request

SITE = "http://127.0.0.1:18080"
API = "http://127.0.0.1:18000"


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name, cond, detail=""):
    print(("PASS" if cond else "FAIL"), "-", name, detail)
    assert cond, detail


# 确保从 v1 开始
call("POST", f"{SITE}/admin/version", {"version": "v1"})

# 1. 正常采集更正链
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-003"})
check("v1 采集更正链成功", st == 200 and body["status"] == "published",
      f"http={st} {body.get('failure_reason')}")
check("更正链抓到 3 通报 2 边", body["notices"] == 3 and body["edges"] == 2)

# 2. 循环引用
st, body = call("POST", f"{API}/collect", {"notice_number": "CYCLE-A"})
check("循环引用采集成功且不重复", st == 200 and body["pages_fetched"] == 3)
st, n = call("GET", f"{API}/notices/CYCLE-A")
check("CYCLE-A 同时有 corrects 与 corrected_by",
      n["corrects"] == ["CYCLE-B"] and n["corrected_by"] == ["CYCLE-B"])

# 3. 镜像合并与祖先
st, n1 = call("GET", f"{API}/notices/2024-001")
check("同号两 URL 合并为两个来源", len(n1["sources"]) == 2,
      str([s["url"] for s in n1["sources"]]))
st, anc = call("GET", f"{API}/notices/2024-003/ancestors")
check("祖先链 003→002→001", [a["notice_number"] for a in anc] == ["2024-002", "2024-001"])

# 4. 幂等
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-003"})
check("同版同内容幂等 noop", body["status"] == "noop")

# 5. v2 同号异文冲突，旧图保留
old_graph = call("GET", f"{API}/notices/2024-003")[1]
call("POST", f"{SITE}/admin/version", {"version": "v2"})
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-003"})
check("v2 同号异文返回 409", st == 409 and "同号异文" in body["failure_reason"])
check("冲突明细指向 2024-001", body["conflict_detail"]["notice_number"] == "2024-001")
still = call("GET", f"{API}/notices/2024-003")[1]
check("失败后查询仍为旧图", still["latest_snapshot_id"] == old_graph["latest_snapshot_id"])
# CYCLE-A 的图不应受影响
st, _ = call("GET", f"{API}/notices/CYCLE-A")
check("其他通报号的图不受失败采集影响", st == 200)

# 6. v3 链接失效
call("POST", f"{SITE}/admin/version", {"version": "v3"})
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-003"})
check("v3 链接失效返回 409", st == 409 and "链接失效" in body["failure_reason"])
check("失效明细含 404 状态", body["conflict_detail"]["status_code"] == 404)

# 7. 回到 v1 重新发布，CYCLE 图仍在
call("POST", f"{SITE}/admin/version", {"version": "v1"})
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-003"})
check("回到 v1 后重新发布成功", st == 200 and body["status"] == "published")
st, _ = call("GET", f"{API}/notices/CYCLE-A")
check("多图并存：CYCLE-A 依旧可查", st == 200)

# 8. 快照与运行审计
snaps = call("GET", f"{API}/snapshots")[1]
check("所有快照保持 published 不可变", all(s["status"] == "published" for s in snaps))
runs = call("GET", f"{API}/runs")[1]
statuses = {r["status"] for r in runs}
check("运行历史含 published 与 failed", {"published", "failed"} <= statuses, str(statuses))

# 9. 数仓单消费者同步：按发布顺序交付、未确认重拉同一张、确认推进游标
start_state = call("GET", f"{API}/sync/state")[1]
start_acked = start_state["last_acked_snapshot_id"]
if start_state["delivered_snapshot_id"] is not None:
    # 上次跑批中断在已交付未确认：重拉必须是同一张，先消费掉它
    st, resumed = call("POST", f"{API}/sync/pull")
    check("重启后重拉未确认快照", resumed["redelivery"] is True
          and resumed["snapshot"]["snapshot_id"] == start_state["delivered_snapshot_id"])
    call("POST", f"{API}/sync/ack",
         {"snapshot_id": start_state["delivered_snapshot_id"]})

delivered_ids = []
while True:
    st, body = call("POST", f"{API}/sync/pull")
    check("拉取返回 200", st == 200)
    if body["status"] == "empty":
        break
    sid = body["snapshot"]["snapshot_id"]
    check("快照含通报/来源/批号/边",
          "notices" in body["snapshot"] and "edges" in body["snapshot"])

    st, again = call("POST", f"{API}/sync/pull")
    check("未确认重拉仍为同一张",
          again["status"] == "delivered"
          and again["snapshot"]["snapshot_id"] == sid and again["redelivery"])

    st, ack = call("POST", f"{API}/sync/ack", {"snapshot_id": sid})
    check("确认当前已交付快照成功", st == 200 and ack["status"] == "acked")
    st, dup = call("POST", f"{API}/sync/ack", {"snapshot_id": sid})
    check("重复确认无害", st == 200 and dup["status"] == "already_acked")
    delivered_ids.append(sid)

expected_ids = sorted(s["id"] for s in call("GET", f"{API}/snapshots")[1]
                      if s["status"] == "published"
                      and (start_acked is None or s["id"] > start_acked))
check("交付按发布顺序且不跳号", delivered_ids == expected_ids,
      f"delivered={delivered_ids} expected={expected_ids}")

# 10. 新发布成为下一张；noop 不产生新交付；越序确认被拒
call("POST", f"{SITE}/admin/reload")  # 仅递增版号，内容不变
st, body = call("POST", f"{API}/collect", {"notice_number": "2024-004"})
check("reload 后新采集发布成功", st == 200 and body["status"] == "published")
st, pulled = call("POST", f"{API}/sync/pull")
check("新快照按序交付", pulled["snapshot"]["snapshot_id"] == body["snapshot_id"])

st, noop = call("POST", f"{API}/collect", {"notice_number": "2024-004"})
check("同版同内容 noop", noop["status"] == "noop")
st, repull = call("POST", f"{API}/sync/pull")
check("noop 不产生新交付", repull["snapshot"]["snapshot_id"] == body["snapshot_id"])

st, bad = call("POST", f"{API}/sync/ack", {"snapshot_id": body["snapshot_id"] + 999})
check("越序确认返回 409", st == 409, f"http={st} {bad}")

st, ack = call("POST", f"{API}/sync/ack", {"snapshot_id": body["snapshot_id"]})
check("确认最新快照", st == 200 and ack["last_acked_snapshot_id"] == body["snapshot_id"])
st, empty = call("POST", f"{API}/sync/pull")
check("确认后无可交付快照", empty["status"] == "empty")
state = call("GET", f"{API}/sync/state")[1]
check("同步游标与状态一致",
      state["delivered_snapshot_id"] is None and state["pending_count"] == 0, str(state))

check("同步游标与状态一致",
      state["delivered_snapshot_id"] is None and state["pending_count"] == 0, str(state))

print("\n全部 E2E 场景通过。")
