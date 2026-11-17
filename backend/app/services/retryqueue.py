"""重试队列业务规则：失败URL去重、跨节点派发、幂等与冲突处置都收在这里。

链路梳理：
1. 失败登记 → 去重重排：同一URL只保留一个有效排队位置，退避级别决定位置；
2. 跨节点派发：读取等待条目、节点新鲜度、最近派发次数，校验节点容量，
   生成重试进度与节点派发数据，派发顺序与公平依据逐条可解释、可回溯；
3. 工作终端侧消费派发结果并更新最近派发次数，下一轮可调度结果据此调整；
4. 最终处理结果先同步到工作终端侧清单，再由清单更新节点自身记录，
   重试进度以清单为消费依据推进；节点离线或任务取消后的迟到结果只留痕，
   不生成幽灵重试；
5. 并发与幂等：同一时刻只允许一个待消费批次，同一幂等键重放同一批次；
   负载快照变化后的冲突重排保留优先级，旧版本整组标记被覆盖、可回溯。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from app.services.worker import CLUSTER_STATE, WorkerService
from app.store import store

MODULE = "retryqueue"
PROGRESS_TABLE = "retryprogress"   # 重试进度（系统表，按记录回溯）
DISPATCH_TABLE = "nodedispatch"    # 节点派发数据（系统表）
IDEM_TABLE = "retryidem"           # 派发幂等键台账（系统表）

REQUIRED_FIELDS = ["条目编号", "失败URL", "失败原因码"]
STATUS_ORDER = ["草稿", "等待重试", "重试中", "重试成功", "最终失败", "已归并", "已取消"]
TERMINAL_STATUSES = {"重试成功", "最终失败", "已归并", "已取消"}
ACTION_RULES = {"加入队列": "等待重试", "发起重试": "重试中", "标记最终失败": "最终失败", "取消任务": "已取消"}
NEGATIVE_ACTIONS = ["标记最终失败", "取消任务"]

MAX_BACKOFF_LEVEL = 5
MAX_RETRY_COUNT = 5
FAILURE_TTL_HOURS = 72  # 失败时效：首次失败超过该时长不再派发

# 重试进度状态
PROGRESS_PENDING = "待消费"       # 已生成，待工作终端消费
PROGRESS_DISPATCHED = "已派发"    # 工作终端已消费，执行中
PROGRESS_SUCCESS = "重试成功"
PROGRESS_REQUEUED = "重新排队"
PROGRESS_SUPERSEDED = "被覆盖"
PROGRESS_CANCELLED = "已取消"
PROGRESS_ACTIVE = (PROGRESS_PENDING, PROGRESS_DISPATCHED)

SORTABLE_FIELDS = {"队列位置", "退避级别", "首次失败时间", "已重试次数", "条目编号"}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
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


class RetryqueueService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._worker = WorkerService()

    # ---------- 列表查询：按失败原因检索、筛选、排序 ----------
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        reason: str | None = None,
        sort: str | None = None,
        order: str = "asc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(MODULE))
        if keyword:
            rows = [
                row for row in rows
                if keyword in str(row.get("条目编号", "")) or keyword.lower() in str(row.get("失败URL", "")).lower()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if reason:
            rows = [row for row in rows if reason.upper() in str(row.get("失败原因码", "")).upper()]
        if sort in SORTABLE_FIELDS:
            reverse = order == "desc"

            def sort_key(row: dict[str, Any]) -> tuple[int, Any]:
                value = row.get(sort)
                if sort in ("退避级别", "已重试次数"):
                    return (0, _to_int(value))
                if sort == "队列位置":
                    # 无位置的记录始终排在最后，不随升降序翻转
                    return (1, 0) if value in (None, "") else (0, _to_int(value))
                return (0, str(value or ""))

            if sort == "队列位置":
                positioned = sorted((r for r in rows if r.get("队列位置") not in (None, "")),
                                    key=lambda r: _to_int(r.get("队列位置")), reverse=reverse)
                unpositioned = [r for r in rows if r.get("队列位置") in (None, "")]
                rows = positioned + unpositioned
            else:
                rows = sorted(rows, key=sort_key, reverse=reverse)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def locate(self, *, url: str | None = None, code: str | None = None) -> dict[str, Any] | None:
        """定位到具体条目：返回条目、唯一队列位置与前后相邻条目。"""
        with self._lock:
            self.rebuild_queue()
            rows = store.rows(MODULE)
            entry = None
            if code:
                entry = next((row for row in rows if str(row.get("条目编号", "")) == code), None)
            if entry is None and url:
                key = self._normalize_url(url)
                entry = next((row for row in rows if self._normalize_url(str(row.get("失败URL", ""))) == key
                              and row.get("status") not in TERMINAL_STATUSES), None)
                if entry is None:
                    entry = next((row for row in rows if self._normalize_url(str(row.get("失败URL", ""))) == key), None)
            if entry is None:
                return None
            queue = self._queue_rows()
            position = entry.get("队列位置")
            prev_entry = next((row for row in queue if _to_int(row.get("队列位置")) == _to_int(position) - 1), None)
            next_entry = next((row for row in queue if _to_int(row.get("队列位置")) == _to_int(position) + 1), None)
            return {
                "条目": dict(entry),
                "队列位置": position,
                "队列长度": len(queue),
                "前一条目": self._brief(prev_entry),
                "后一条目": self._brief(next_entry),
                "位置依据": entry.get("位置依据", ""),
            }

    @staticmethod
    def _brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {"条目编号": row.get("条目编号"), "失败URL": row.get("失败URL"), "队列位置": row.get("队列位置")}

    # ---------- 登记与动作 ----------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with self._lock:
            rows = store.rows(MODULE)
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            entry.update({field: str(values.get(field)).strip() for field in REQUIRED_FIELDS})
            entry.update({
                "status": STATUS_ORDER[0],
                "pending": True,
                "abnormal": False,
                "首次失败时间": str(values.get("首次失败时间") or _now()),
                "已重试次数": 0,
                "下次重试时间": "",
                "退避级别": _to_int(values.get("退避级别"), 0),
                "最终处置": "",
                "队列位置": None,
                "版本": 1,
                "位置依据": "",
                "归并到": "",
            })
            rows.append(entry)
            return dict(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"重试条目 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于重试队列可执行范围"
        if action == "取消任务":
            return self.cancel_entry(entry_id)
        with self._lock:
            target = ACTION_RULES[action]
            if entry.get("status") in TERMINAL_STATUSES:
                return None, f"条目已处于终态「{entry.get('status')}」，不能再{action}"
            entry["status"] = target
            entry["pending"] = target not in TERMINAL_STATUSES
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            entry["版本"] = _to_int(entry.get("版本")) + 1
            if target == "最终失败":
                entry["最终处置"] = entry.get("最终处置") or "人工标记"
                entry["队列位置"] = None
            if target == "重试中":
                entry["队列位置"] = None
            self.rebuild_queue()
            return dict(entry), f"重试条目已{action}"

    def cancel_entry(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """取消任务：释放唯一队列位置，关闭在途进度，此后不再出现新的重试进度。"""
        with self._lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"重试条目 {entry_id} 不存在或已归档"
            if entry.get("status") in TERMINAL_STATUSES:
                return None, f"条目已处于终态「{entry.get('status')}」，无需取消"
            entry["status"] = "已取消"
            entry["pending"] = False
            entry["abnormal"] = True
            entry["取消时间"] = _now()
            entry["队列位置"] = None
            entry["版本"] = _to_int(entry.get("版本")) + 1
            closed = 0
            for progress in store.system_rows(PROGRESS_TABLE):
                if progress.get("条目编号") == entry.get("条目编号") and progress.get("状态") in PROGRESS_ACTIVE:
                    if progress.get("状态") == PROGRESS_DISPATCHED:
                        self._worker.release_inflight(str(progress.get("目标节点", "")))
                    progress["状态"] = PROGRESS_CANCELLED
                    progress["覆盖原因"] = "任务已取消，迟到结果将按幽灵拒绝"
                    closed += 1
            self.rebuild_queue()
            suffix = f"，同时关闭 {closed} 条在途重试进度" if closed else ""
            return dict(entry), f"重试条目已取消{suffix}，后续不再生成新的重试进度"

    # ---------- 去重与唯一队列位置 ----------
    @staticmethod
    def _normalize_url(url: str) -> str:
        parts = urlsplit(url.strip().lower())
        host = parts.netloc or parts.path.split("/")[0]
        path = parts.path.rstrip("/") if parts.netloc else "/".join(parts.path.split("/")[1:])
        query = f"?{parts.query}" if parts.query else ""
        return f"{host}{path}{query}"

    def _queue_rows(self) -> list[dict[str, Any]]:
        return sorted(
            (row for row in store.rows(MODULE) if row.get("队列位置") not in (None, "")),
            key=lambda row: _to_int(row.get("队列位置")),
        )

    def rebuild_queue(self) -> list[dict[str, Any]]:
        """去重 + 重排：同一URL只保留一个有效排队位置，位置随退避级别移动。

        排序依据（可核对）：退避级别降序 → 首次失败时间升序 → 条目编号升序；
        超过失败时效的条目直接归档为最终失败，不再占用队列位置。
        """
        with self._lock:
            rows = store.rows(MODULE)
            now = datetime.now()
            for row in rows:
                if row.get("status") != "等待重试":
                    continue
                first_failed = _parse_ts(row.get("首次失败时间"))
                if first_failed and (now - first_failed) > timedelta(hours=FAILURE_TTL_HOURS):
                    row["status"] = "最终失败"
                    row["pending"] = False
                    row["abnormal"] = True
                    row["最终处置"] = "超时归档"
                    row["队列位置"] = None
                    row["位置依据"] = f"首次失败已超过{FAILURE_TTL_HOURS}小时失败时效，不再派发"
            waiting = [row for row in rows if row.get("status") == "等待重试"]
            groups: dict[str, list[dict[str, Any]]] = {}
            for row in waiting:
                groups.setdefault(self._normalize_url(str(row.get("失败URL", ""))), []).append(row)
            keepers: list[dict[str, Any]] = []
            for group in groups.values():
                group.sort(key=lambda row: (-_to_int(row.get("退避级别")), str(row.get("首次失败时间", "")), row.get("条目编号", "")))
                keeper = group[0]
                keeper["归并到"] = ""
                keepers.append(keeper)
                for duplicate in group[1:]:
                    duplicate["status"] = "已归并"
                    duplicate["pending"] = False
                    duplicate["队列位置"] = None
                    duplicate["归并到"] = keeper.get("条目编号")
                    duplicate["位置依据"] = f"与 {keeper.get('条目编号')} 同一URL，仅保留一个有效排队位置"
            keepers.sort(key=lambda row: (-_to_int(row.get("退避级别")), str(row.get("首次失败时间", "")), row.get("条目编号", "")))
            for index, keeper in enumerate(keepers, start=1):
                first_failed = _parse_ts(keeper.get("首次失败时间"))
                age_hours = int((now - first_failed).total_seconds() / 3600) if first_failed else 0
                keeper["队列位置"] = index
                keeper["位置依据"] = (
                    f"退避级别{keeper.get('退避级别')}·首次失败{keeper.get('首次失败时间')}"
                    f"·失败时效{age_hours}h未超{FAILURE_TTL_HOURS}h"
                )
            return keepers

    def queue_view(self) -> list[dict[str, Any]]:
        """唯一队列视图：去重重排后的有效排队位置。"""
        with self._lock:
            self.rebuild_queue()
            return [dict(row) for row in self._queue_rows()]

    # ---------- 退避级别：移动唯一队列位置 ----------
    def change_backoff(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"重试条目 {entry_id} 不存在或已归档"
            if entry.get("status") != "等待重试":
                return None, "只有等待重试的条目才能调整退避级别"
            old_level = _to_int(entry.get("退避级别"))
            old_position = entry.get("队列位置")
            if "退避级别" in values:
                new_level = _to_int(values.get("退避级别"), old_level)
            else:
                new_level = old_level + _to_int(values.get("调整"), 0)
            new_level = max(0, min(MAX_BACKOFF_LEVEL, new_level))
            if new_level == old_level:
                return dict(entry), f"退避级别保持 {old_level}，队列位置不变"
            entry["退避级别"] = new_level
            entry["版本"] = _to_int(entry.get("版本")) + 1
            self.rebuild_queue()
            new_position = entry.get("队列位置")
            entry["位置依据"] += f"；退避{old_level}→{new_level}，队列位置{old_position}→{new_position}，旧位置已释放"
            return dict(entry), f"退避级别 {old_level}→{new_level}，唯一队列位置 {old_position}→{new_position}"

    # ---------- 跨节点派发：生成重试进度与节点派发数据 ----------
    def _next_batch_no(self) -> str:
        serial = 0
        for row in store.system_rows(PROGRESS_TABLE):
            serial = max(serial, _to_int(str(row.get("批次号", "")).replace("BATCH-", "")))
        return f"BATCH-{serial + 1:04d}"

    def _find_idem(self, key: str) -> dict[str, Any] | None:
        for row in store.system_rows(IDEM_TABLE):
            if row.get("幂等键") == key:
                return row
        return None

    def _batch_view(self, batch_no: str) -> dict[str, Any]:
        progress = [dict(row) for row in store.system_rows(PROGRESS_TABLE) if row.get("批次号") == batch_no]
        dispatch = [dict(row) for row in store.system_rows(DISPATCH_TABLE) if row.get("批次号") == batch_no]
        version = progress[0].get("负载快照版本") if progress else None
        return {"批次号": batch_no, "负载快照版本": version, "进度记录": progress, "节点派发": dispatch}

    def plan_dispatch(self, *, idem_key: str, conflict: str = "拒绝") -> tuple[dict[str, Any] | None, bool, str]:
        """生成一轮重试派发：并发时只保留一个待消费批次，同一幂等键重放同一批次。

        conflict=拒绝：已有在途批次时直接拒绝；conflict=重排：基于最新负载快照
        重排，旧批次整组标记被覆盖（保留可回溯），新批次仍按队列优先级派发。
        """
        with self._lock:
            idem_key = idem_key.strip()
            if not idem_key:
                return None, False, "缺少幂等键，无法保证重复提交只生成一个重试进度"
            hit = self._find_idem(idem_key)
            if hit is not None:
                return self._batch_view(hit["批次号"]), True, f"幂等重放：返回已生成的批次 {hit['批次号']}，未重复派发"
            active = [row for row in store.system_rows(PROGRESS_TABLE) if row.get("状态") in PROGRESS_ACTIVE]
            if active and conflict != "重排":
                batch_no = active[0].get("批次号")
                return None, False, (
                    f"批次 {batch_no} 的重试进度仍在途，并发请求只保留一个重试进度；"
                    "如需基于最新负载快照重排，请以冲突处置=重排再次提交"
                )
            superseded = 0
            if active:
                # 冲突重排：旧批次整组标记被覆盖（保留可回溯），条目回到等待队列，
                # 已派发进度占用的终端容量一并释放，再按最新快照重排。
                old_batch = active[0].get("批次号")
                old_version = active[0].get("负载快照版本")
                for progress in active:
                    if progress.get("状态") == PROGRESS_DISPATCHED:
                        self._worker.release_inflight(str(progress.get("目标节点", "")))
                    progress["状态"] = PROGRESS_SUPERSEDED
                    progress["覆盖原因"] = (
                        f"负载快照 v{old_version}→v{CLUSTER_STATE['snapshot_version']}：节点状态变化，"
                        f"批次 {old_batch} 整组被覆盖，按队列优先级重排"
                    )
                    superseded += 1
                    stale = next((row for row in store.rows(MODULE)
                                  if row.get("条目编号") == progress.get("条目编号")), None)
                    if stale is not None and stale.get("status") == "重试中":
                        stale["status"] = "等待重试"
                        stale["pending"] = True
            keepers = self.rebuild_queue()
            snapshot = self._worker.schedulable_nodes()
            version = CLUSTER_STATE["snapshot_version"]
            schedulable = [node for node in snapshot if node.get("可调度")]
            if not keepers:
                return None, False, "等待重试队列为空，未生成重试进度"
            if not schedulable:
                reasons = "、".join(f"{node['节点编号']}({node['不可调度原因']})" for node in snapshot) or "无节点"
                return None, False, f"当前没有可调度节点：{reasons}"
            capacity = {node["节点编号"]: _to_int(node.get("剩余容量")) for node in schedulable}
            assignments: list[tuple[dict[str, Any], dict[str, Any], list[str]]] = []
            for entry in keepers:
                candidates = [node for node in schedulable if capacity[node["节点编号"]] > 0]
                if not candidates:
                    break  # 容量耗尽，剩余条目保留队列位置等待下一轮
                candidates.sort(key=lambda node: (-_to_float(node.get("公平权重")), node["节点编号"]))
                node = candidates[0]
                code = node["节点编号"]
                used = _to_int(node.get("并发槽位数")) - capacity[code] + 1
                capacity[code] -= 1
                reasons = [
                    f"队列位置{entry.get('队列位置')}（{entry.get('位置依据')}）",
                    (
                        f"节点{code}：新鲜度{node.get('新鲜度')}（{node.get('新鲜度说明')}），"
                        f"最近派发{node.get('最近派发次数')}次，公平权重{node.get('公平权重')}，"
                        f"容量校验{used}/{node.get('并发槽位数')}通过"
                    ),
                    f"失败时效：首次失败{entry.get('首次失败时间')}，未超{FAILURE_TTL_HOURS}h，允许派发",
                ]
                assignments.append((entry, node, reasons))
            if not assignments:
                return None, False, "节点容量不足，未生成重试进度"
            batch_no = self._next_batch_no()
            progress_table = store.system_rows(PROGRESS_TABLE)
            dispatch_table = store.system_rows(DISPATCH_TABLE)
            now_text = _now()
            per_node: dict[str, dict[str, Any]] = {}
            for entry, node, reasons in assignments:
                code = node["节点编号"]
                progress_id = max((int(row.get("id", 0)) for row in progress_table), default=0) + 1
                entry_progress_version = sum(
                    1 for row in progress_table if row.get("条目编号") == entry.get("条目编号")
                ) + 1
                progress_table.append({
                    "id": progress_id,
                    "进度编号": f"PROG-{progress_id:04d}",
                    "批次号": batch_no,
                    "条目编号": entry.get("条目编号"),
                    "失败URL": entry.get("失败URL"),
                    "目标节点": code,
                    "负载快照版本": version,
                    "进度版本": entry_progress_version,
                    "状态": PROGRESS_PENDING,
                    "公平依据": reasons,
                    "覆盖原因": "",
                    "生成时间": now_text,
                    "消费时间": "",
                })
                entry["status"] = "重试中"
                entry["队列位置"] = None
                entry["版本"] = _to_int(entry.get("版本")) + 1
                bucket = per_node.setdefault(code, {"node": node, "count": 0})
                bucket["count"] += 1
            for code, bucket in per_node.items():
                dispatch_id = max((int(row.get("id", 0)) for row in dispatch_table), default=0) + 1
                node = bucket["node"]
                dispatch_table.append({
                    "id": dispatch_id,
                    "派发编号": f"NDIS-{dispatch_id:04d}",
                    "批次号": batch_no,
                    "节点编号": code,
                    "派发条数": bucket["count"],
                    "容量校验": f"校验通过：在途{node.get('在途数')}+派发{bucket['count']}≤槽位{node.get('并发槽位数')}",
                    "快照最近派发次数": node.get("最近派发次数"),
                    "消费后最近派发次数": None,
                    "负载快照版本": version,
                    "公平权重": node.get("公平权重"),
                    "新鲜度": node.get("新鲜度"),
                    "状态": PROGRESS_PENDING,
                    "生成时间": now_text,
                })
            idem_table = store.system_rows(IDEM_TABLE)
            idem_table.append({"id": len(idem_table) + 1, "幂等键": idem_key, "批次号": batch_no, "创建时间": now_text})
            message = f"批次 {batch_no} 已生成 {len(assignments)} 条重试进度（负载快照 v{version}）"
            if superseded:
                message += f"，旧批次 {superseded} 条进度按冲突处置标记被覆盖并保留回溯"
            return self._batch_view(batch_no), False, message

    # ---------- 工作终端侧消费：更新最近派发次数 ----------
    def consume_dispatch(self) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            pending = [row for row in store.system_rows(PROGRESS_TABLE) if row.get("状态") == PROGRESS_PENDING]
            if not pending:
                return None, "没有待消费的重试进度，请先生成派发计划"
            batch_no = pending[0].get("批次号")
            counts: dict[str, int] = {}
            for progress in pending:
                counts[progress["目标节点"]] = counts.get(progress["目标节点"], 0) + 1
                progress["状态"] = PROGRESS_DISPATCHED
                progress["消费时间"] = _now()
            updated = self._worker.consume_dispatch(counts, str(batch_no))
            recent_map = {str(row.get("节点编号")): row.get("最近派发次数") for row in updated}
            for row in store.system_rows(DISPATCH_TABLE):
                if row.get("批次号") == batch_no:
                    row["状态"] = "已消费"
                    row["消费后最近派发次数"] = recent_map.get(str(row.get("节点编号")))
            summary = {
                "批次号": batch_no,
                "消费条数": len(pending),
                "节点更新": [
                    {
                        "节点编号": row.get("节点编号"),
                        "最近派发次数": row.get("最近派发次数"),
                        "处理中": row.get("处理中"),
                        "下一轮可调度": row.get("下一轮可调度"),
                    }
                    for row in updated
                ],
            }
            return summary, f"工作终端侧已消费批次 {batch_no}，最近派发次数与下一轮可调度结果已更新"

    # ---------- 结果回传：先落终端清单，再更新自身记录，最后进度消费 ----------
    def sync_results(self, results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
        with self._lock:
            progress_table = store.system_rows(PROGRESS_TABLE)
            result_table = self._worker.list_results()
            seen_keys = {str(row.get("结果幂等键")) for row in result_table}
            items: list[dict[str, Any]] = []
            replayed = 0
            for result in results:
                progress_no = str(result.get("进度编号") or "").strip()
                outcome = "成功" if str(result.get("处理结果") or "成功") == "成功" else "失败"
                idem_key = str(result.get("结果幂等键") or f"{progress_no}:{outcome}")
                if idem_key in seen_keys:
                    replayed += 1
                    continue
                seen_keys.add(idem_key)
                progress = next((row for row in progress_table if row.get("进度编号") == progress_no), None)
                ghost_reasons: list[str] = []
                entry = None
                node_code = ""
                if progress is None:
                    ghost_reasons.append("重试进度不存在")
                else:
                    node_code = str(progress.get("目标节点", ""))
                    entry = next((row for row in store.rows(MODULE)
                                  if row.get("条目编号") == progress.get("条目编号")), None)
                    if progress.get("状态") != PROGRESS_DISPATCHED:
                        ghost_reasons.append(f"进度状态为{progress.get('状态')}，结果迟到或重复")
                    if entry is not None and entry.get("status") == "已取消":
                        ghost_reasons.append("任务已取消")
                    node = self._worker.find_by_code(node_code)
                    if node is None or node.get("status") == "离线":
                        ghost_reasons.append("节点已离线")
                items.append({
                    "进度编号": progress_no,
                    "条目编号": (progress or {}).get("条目编号", ""),
                    "节点编号": node_code or str(result.get("节点编号") or ""),
                    "处理结果": outcome,
                    "失败原因码": str(result.get("失败原因码") or ""),
                    "处置": "幽灵拒绝" if ghost_reasons else "已接受",
                    "拒绝原因": "；".join(ghost_reasons),
                    "结果幂等键": idem_key,
                })
            if not items:
                return [], f"没有新的处理结果需要同步（幂等跳过 {replayed} 条）"
            # 第一步：结果先同步到工作终端侧清单，并由清单更新节点自身记录。
            landed = self._worker.land_results(items)
            # 第二步：重试进度以终端清单为消费依据推进。
            accepted = sum(1 for row in landed if row.get("处置") == "已接受")
            ghosts = sum(1 for row in landed if row.get("处置") == "幽灵拒绝")
            for record in landed:
                if record.get("处置") != "已接受":
                    continue
                progress = next((row for row in progress_table if row.get("进度编号") == record.get("进度编号")), None)
                entry = next((row for row in store.rows(MODULE)
                              if row.get("条目编号") == record.get("条目编号")), None)
                if progress is None or entry is None:
                    continue
                if record.get("处理结果") == "成功":
                    progress["状态"] = PROGRESS_SUCCESS
                    entry["status"] = "重试成功"
                    entry["pending"] = False
                    entry["abnormal"] = False
                    entry["最终处置"] = "自动成功"
                    entry["队列位置"] = None
                else:
                    progress["状态"] = PROGRESS_REQUEUED
                    entry["已重试次数"] = _to_int(entry.get("已重试次数")) + 1
                    entry["退避级别"] = min(_to_int(entry.get("退避级别")) + 1, MAX_BACKOFF_LEVEL)
                    if _to_int(entry.get("已重试次数")) >= MAX_RETRY_COUNT:
                        entry["status"] = "最终失败"
                        entry["pending"] = False
                        entry["abnormal"] = True
                        entry["最终处置"] = "重试次数耗尽"
                        entry["队列位置"] = None
                    else:
                        entry["status"] = "等待重试"
                        entry["pending"] = True
                        wait_minutes = 2 ** _to_int(entry.get("退避级别"))
                        entry["下次重试时间"] = (datetime.now() + timedelta(minutes=wait_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                        if record.get("失败原因码"):
                            entry["失败原因码"] = record.get("失败原因码")
                entry["版本"] = _to_int(entry.get("版本")) + 1
            self.rebuild_queue()
            # 进度消费完毕后，按最新在途状态重算终端侧下一轮可调度结果。
            self._worker.refresh_schedulable_all()
            message = f"已同步 {len(landed)} 条终端结果（接受 {accepted}、幽灵拒绝 {ghosts}）"
            if replayed:
                message += f"，幂等跳过 {replayed} 条重复回传"
            return landed, message

    def auto_results(self) -> list[dict[str, Any]]:
        """为全部在途进度构造回传结果（默认成功），供演示与联调使用。"""
        return [
            {"进度编号": row.get("进度编号"), "处理结果": "成功"}
            for row in store.system_rows(PROGRESS_TABLE)
            if row.get("状态") == PROGRESS_DISPATCHED
        ]

    # ---------- 回溯与统计 ----------
    def list_progress(
        self,
        *,
        batch: str | None = None,
        entry_code: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(store.system_rows(PROGRESS_TABLE))
        if batch:
            rows = [row for row in rows if row.get("批次号") == batch]
        if entry_code:
            rows = [row for row in rows if row.get("条目编号") == entry_code]
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        return sorted((dict(row) for row in rows), key=lambda row: int(row.get("id", 0)), reverse=True)

    def list_node_dispatch(self, *, batch: str | None = None) -> list[dict[str, Any]]:
        rows = list(store.system_rows(DISPATCH_TABLE))
        if batch:
            rows = [row for row in rows if row.get("批次号") == batch]
        return sorted((dict(row) for row in rows), key=lambda row: int(row.get("id", 0)), reverse=True)

    def list_worker_results(self) -> list[dict[str, Any]]:
        return self._worker.list_results()

    def node_snapshot(self) -> dict[str, Any]:
        return {
            "负载快照版本": CLUSTER_STATE["snapshot_version"],
            "节点": self._worker.schedulable_nodes(),
        }

    def stats(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        progress = store.system_rows(PROGRESS_TABLE)
        results = store.system_rows("workerresult")
        success = sum(1 for row in rows if row.get("status") == "重试成功")
        finished = sum(1 for row in rows if row.get("status") in ("重试成功", "最终失败"))
        active_batch = next((row.get("批次号") for row in progress if row.get("状态") in PROGRESS_ACTIVE), "无")
        return {
            "重试条目总数": len(rows),
            "等待重试数": sum(1 for row in rows if row.get("status") == "等待重试"),
            "重试中数": sum(1 for row in rows if row.get("status") == "重试中"),
            "重试成功率": f"{round(success / finished * 100)}%" if finished else "—",
            "幽灵拒绝数": sum(1 for row in results if row.get("处置") == "幽灵拒绝"),
            "在途批次": active_batch,
            "负载快照版本": CLUSTER_STATE["snapshot_version"],
        }
