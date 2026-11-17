"""采集节点业务规则：状态流转、字段校验与筛选口径都收在这里。

工作终端侧职责（重试链路）：
- 提供节点快照（在线状态、心跳新鲜度、剩余容量、最近派发次数、公平权重）；
- 消费重试派发结果并更新最近派发次数，据此调整下一轮可调度结果；
- 接收最终处理结果：先落到工作终端侧清单，再由清单更新节点自身记录。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "worker"
REQUIRED_FIELDS = ["节点编号", "主机地址", "并发槽位数"]
STATUS_ORDER = ["草稿", "注册中", "在线", "离线"]
ACTION_RULES = {"提交注册": "注册中", "标记在线": "在线", "标记离线": "离线"}
NEGATIVE_ACTIONS = ["标记离线"]

# 工作终端侧清单（系统表）：最终处理结果先落到这里，再回写节点自身记录。
RESULT_TABLE = "workerresult"
# 重试进度表名与在途状态（调度侧写入，这里只读，用于容量校验）。
PROGRESS_TABLE = "retryprogress"
PROGRESS_ACTIVE = ("待消费", "已派发")

FRESHNESS_WINDOW_MINUTES = 60  # 心跳超过该窗口视为不新鲜，不参与调度

# 集群共享状态：负载快照版本被重试队列读取，用来识别新旧负载快照冲突。
CLUSTER_STATE: dict[str, Any] = {
    "snapshot_version": 1,
    "lock": threading.RLock(),
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


class WorkerService:
    # ---------- 基础查询与登记 ----------
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def find_by_code(self, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("节点编号", "")) == node_code:
                return row
        return None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["最近派发次数"] = 0
        entry["处理中"] = 0
        entry["成功回传"] = 0
        entry["失败回传"] = 0
        entry["下一轮可调度"] = "待评估"
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"工作节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集节点可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        with CLUSTER_STATE["lock"]:
            entry["status"] = target
            entry["pending"] = target not in ("在线", STATUS_ORDER[-1])
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            if target == "在线":
                entry["上次心跳时间"] = _now()
            # 节点上下线会改变负载快照，版本号递增供重试派发识别新旧快照。
            CLUSTER_STATE["snapshot_version"] += 1
            self._refresh_schedulable(entry)
        return entry, f"工作节点已{action}，负载快照版本升至 v{CLUSTER_STATE['snapshot_version']}"

    # ---------- 节点快照：新鲜度、容量、公平权重 ----------
    def node_freshness(self, row: dict[str, Any]) -> tuple[float, str]:
        """节点新鲜度：心跳距现在越近越新鲜，超过窗口记 0 分。"""
        if row.get("status") != "在线":
            return 0.0, "节点不在线"
        beat = _parse_ts(row.get("上次心跳时间"))
        if beat is None:
            return 0.0, "心跳时间缺失"
        age_minutes = max((datetime.now() - beat).total_seconds() / 60, 0)
        score = max(0.0, 1 - age_minutes / FRESHNESS_WINDOW_MINUTES)
        return round(score, 2), f"心跳滞后{int(age_minutes)}分钟"

    def _inflight(self, node_code: str) -> int:
        return sum(
            1
            for row in store.system_rows(PROGRESS_TABLE)
            if row.get("目标节点") == node_code and row.get("状态") in PROGRESS_ACTIVE
        )

    def schedulable_nodes(self) -> list[dict[str, Any]]:
        """负载快照：重试派发读取的节点新鲜度、容量与公平权重数据。"""
        with CLUSTER_STATE["lock"]:
            nodes: list[dict[str, Any]] = []
            for row in store.rows(MODULE):
                code = str(row.get("节点编号", ""))
                online = row.get("status") == "在线"
                freshness, freshness_note = self.node_freshness(row)
                slots = _to_int(row.get("并发槽位数"))
                inflight = self._inflight(code)
                capacity = max(slots - inflight, 0)
                recent = _to_int(row.get("最近派发次数"))
                weight = round(freshness * 10 / (1 + recent), 2) if online else 0.0
                if not online:
                    reason = "离线"
                elif freshness <= 0:
                    reason = "心跳超窗"
                elif capacity <= 0:
                    reason = "容量不足"
                else:
                    reason = "可调度"
                nodes.append({
                    "id": row.get("id"),
                    "节点编号": code,
                    "是否在线": online,
                    "新鲜度": freshness,
                    "新鲜度说明": freshness_note,
                    "并发槽位数": slots,
                    "在途数": inflight,
                    "剩余容量": capacity,
                    "最近派发次数": recent,
                    "公平权重": weight,
                    "可调度": reason == "可调度",
                    "不可调度原因": "" if reason == "可调度" else reason,
                })
            return nodes

    def _refresh_schedulable(self, row: dict[str, Any]) -> None:
        """根据最新负载重算单个节点的下一轮可调度结果。"""
        code = str(row.get("节点编号", ""))
        slots = _to_int(row.get("并发槽位数"))
        inflight = self._inflight(code)
        capacity = max(slots - inflight, 0)
        freshness, _ = self.node_freshness(row)
        recent = _to_int(row.get("最近派发次数"))
        weight = round(freshness * 10 / (1 + recent), 2) if row.get("status") == "在线" else 0.0
        if row.get("status") != "在线":
            row["下一轮可调度"] = f"不可调度（{row.get('status')}）"
        elif freshness <= 0:
            row["下一轮可调度"] = "不可调度（心跳超窗）"
        elif capacity <= 0:
            row["下一轮可调度"] = "不可调度（容量不足）"
        else:
            row["下一轮可调度"] = f"可调度（剩余容量{capacity}·公平权重{weight}）"

    # ---------- 工作终端侧：消费派发、回写最近派发次数 ----------
    def consume_dispatch(self, node_counts: dict[str, int], batch_no: str) -> list[dict[str, Any]]:
        """消费重试派发结果：累加最近派发次数，并据此调整下一轮可调度结果。"""
        with CLUSTER_STATE["lock"]:
            updated: list[dict[str, Any]] = []
            for code, count in node_counts.items():
                row = self.find_by_code(code)
                if row is None:
                    continue
                row["最近派发次数"] = _to_int(row.get("最近派发次数")) + count
                row["处理中"] = _to_int(row.get("处理中")) + count
                row["最近消费批次"] = batch_no
                self._refresh_schedulable(row)
                updated.append(dict(row))
            CLUSTER_STATE["snapshot_version"] += 1
            return updated

    def release_inflight(self, node_code: str, count: int = 1) -> None:
        """任务取消等场景释放在途占用，避免容量被幽灵占用。"""
        with CLUSTER_STATE["lock"]:
            row = self.find_by_code(node_code)
            if row is None:
                return
            row["处理中"] = max(_to_int(row.get("处理中")) - count, 0)
            self._refresh_schedulable(row)

    # ---------- 工作终端侧清单：先落清单，再更新自身记录 ----------
    def land_results(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """最终处理结果先同步到工作终端侧清单，再由清单更新节点自身记录。"""
        with CLUSTER_STATE["lock"]:
            table = store.system_rows(RESULT_TABLE)
            landed: list[dict[str, Any]] = []
            for item in items:
                record = dict(item)
                record["id"] = max((int(row.get("id", 0)) for row in table), default=0) + 1
                record["结果编号"] = f"WRES-{record['id']:04d}"
                record["接收时间"] = _now()
                table.append(record)
                landed.append(record)
            # 清单落库后，由清单回写节点自身记录（处理中、回传计数、可调度结果）。
            for record in landed:
                if record.get("处置") != "已接受":
                    continue
                row = self.find_by_code(str(record.get("节点编号", "")))
                if row is None:
                    continue
                row["处理中"] = max(_to_int(row.get("处理中")) - 1, 0)
                if record.get("处理结果") == "成功":
                    row["成功回传"] = _to_int(row.get("成功回传")) + 1
                else:
                    row["失败回传"] = _to_int(row.get("失败回传")) + 1
                row["最近回传时间"] = record["接收时间"]
                self._refresh_schedulable(row)
            CLUSTER_STATE["snapshot_version"] += 1
            return landed

    def list_results(self) -> list[dict[str, Any]]:
        """工作终端侧清单：重试进度按记录回溯的消费依据。"""
        return sorted(store.system_rows(RESULT_TABLE), key=lambda row: int(row.get("id", 0)), reverse=True)

    def refresh_schedulable_all(self) -> None:
        """重试进度消费完成后，按最新在途状态重算全部节点的下一轮可调度结果。"""
        with CLUSTER_STATE["lock"]:
            for row in store.rows(MODULE):
                self._refresh_schedulable(row)
