"""重试队列接口：失败URL去重、跨节点派发、工作终端消费与结果回传的完整链路。

链路顺序：登记/去重重排 → 生成派发计划（幂等）→ 工作终端消费 → 结果回传落清单
→ 重试进度消费。静态子路径全部放在 ``/{entry_id}`` 之前声明，避免被路径参数截获。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.retryqueue import RetryqueueService

router = APIRouter(prefix="/api/retryqueue", tags=["重试队列"])

service = RetryqueueService()

LIST_FIELDS = ["条目编号", "失败URL", "失败原因码", "首次失败时间", "已重试次数", "下次重试时间", "退避级别", "最终处置"]
STATUSES = ["草稿", "等待重试", "重试中", "重试成功", "最终失败", "已归并", "已取消"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按条目编号或失败URL检索"),
    status: str | None = Query(default=None, description="草稿、等待重试、重试中、重试成功、最终失败、已归并、已取消"),
    reason: str | None = Query(default=None, description="按失败原因码筛选，如 HTTP_429、TIMEOUT"),
    sort: str | None = Query(default=None, description="排序字段：队列位置、退避级别、首次失败时间、已重试次数、条目编号"),
    order: str = Query(default="asc", description="asc 升序 / desc 降序"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按失败原因等条件检索、筛选、排序重试队列；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(
        keyword=keyword, status=status, reason=reason, sort=sort, order=order, page=page, size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/stats")
def stats() -> dict[str, Any]:
    """队列与链路统计：等待数、成功率、幽灵拒绝数、在途批次与负载快照版本。"""
    return service.stats()


@router.get("/queue")
def queue_view() -> dict[str, Any]:
    """唯一队列视图：去重重排后每个URL只保留一个有效排队位置。"""
    items = service.queue_view()
    return {"total": len(items), "items": items}


@router.get("/locate")
def locate_entry(
    url: str | None = Query(default=None, description="按失败URL定位"),
    code: str | None = Query(default=None, description="按条目编号定位"),
) -> dict[str, Any]:
    """定位到具体条目：返回队列位置、前后相邻条目与位置依据。"""
    found = service.locate(url=url, code=code)
    if found is None:
        raise HTTPException(status_code=404, detail="未找到匹配的重试条目")
    return found


@router.get("/progress")
def list_progress(
    batch: str | None = Query(default=None, description="按批次号过滤"),
    entry_code: str | None = Query(default=None, description="按条目编号过滤"),
    status: str | None = Query(default=None, description="按进度状态过滤"),
) -> dict[str, Any]:
    """重试进度回溯：每条记录携带负载快照版本、公平依据与覆盖原因。"""
    items = service.list_progress(batch=batch, entry_code=entry_code, status=status)
    return {"total": len(items), "items": items}


@router.get("/node-dispatch")
def list_node_dispatch(batch: str | None = Query(default=None, description="按批次号过滤")) -> dict[str, Any]:
    """节点派发数据回溯：容量校验、消费前后最近派发次数逐条可查。"""
    items = service.list_node_dispatch(batch=batch)
    return {"total": len(items), "items": items}


@router.get("/worker-results")
def list_worker_results() -> dict[str, Any]:
    """工作终端侧清单：最终处理结果的落点，重试进度的消费依据。"""
    items = service.list_worker_results()
    return {"total": len(items), "items": items}


@router.get("/node-snapshot")
def node_snapshot() -> dict[str, Any]:
    """当前负载快照：节点新鲜度、剩余容量、最近派发次数与公平权重。"""
    return service.node_snapshot()


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出重试队列清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "retryqueue", "total": total, "items": items}


@router.post("/rebuild")
def rebuild_queue() -> dict[str, Any]:
    """去重重排：同一URL只保留一个有效排队位置，超失败时效的条目归档。"""
    items = service.queue_view()
    return {"ok": True, "message": f"去重重排完成，唯一队列共 {len(items)} 个有效位置", "items": items}


@router.post("/dispatch")
def plan_dispatch(payload: EntryPayload) -> dict[str, Any]:
    """生成一轮跨节点派发：读取等待条目、节点新鲜度与最近派发次数，校验容量。

    并发时只保留一个待消费批次；同一幂等键重复提交返回同一批次；
    冲突处置=重排时旧批次整组标记被覆盖（保留优先级、可回溯）。
    """
    idem_key = str(payload.values.get("幂等键") or "").strip()
    conflict = str(payload.values.get("冲突处置") or "拒绝").strip()
    batch, replayed, message = service.plan_dispatch(idem_key=idem_key, conflict=conflict)
    if batch is None:
        return {"ok": False, "message": message, "batch": None, "replayed": False}
    return {"ok": True, "message": message, "batch": batch, "replayed": replayed}


@router.post("/consume")
def consume_dispatch() -> dict[str, Any]:
    """工作终端侧消费派发结果：更新最近派发次数，调整下一轮可调度结果。"""
    summary, message = service.consume_dispatch()
    if summary is None:
        return {"ok": False, "message": message, "summary": None}
    return {"ok": True, "message": message, "summary": summary}


@router.post("/results")
def sync_results(payload: EntryPayload) -> dict[str, Any]:
    """同步终端处理结果：先落工作终端侧清单，再由清单更新节点记录，最后进度消费。

    节点离线或任务取消后的迟到结果按幽灵拒绝留痕，不生成新的重试进度；
    同一结果幂等键重复提交直接跳过。results 为空时按全部在途进度自动回传成功。
    """
    raw = payload.values.get("results")
    results = raw if isinstance(raw, list) and raw else service.auto_results()
    if not results:
        return {"ok": False, "message": "没有在途重试进度需要回传结果", "items": []}
    landed, message = service.sync_results(results)
    return {"ok": True, "message": message, "items": landed}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重试条目明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重试条目 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条重试条目，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="重试条目已登记（草稿），执行「加入队列」后参与去重排位", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条重试条目执行加入队列、发起重试、标记最终失败、取消任务。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/backoff", response_model=ActionResult)
def change_backoff(entry_id: int, payload: EntryPayload) -> ActionResult:
    """调整退避级别：唯一队列位置随之移动，旧位置立即释放。"""
    entry, message = service.change_backoff(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
