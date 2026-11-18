"""数据流转编排 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.flow import FlowService, LegacyEditDenied

router = APIRouter(prefix="/api/flow", tags=["数据流转编排"])
service = FlowService()
LIST_FIELDS = ["管道编号", "管道名称", "起始节点", "终止节点", "中间节点", "流转条件", "并行数量", "管道状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


class LegacyTextPayload(BaseModel):
    text: str


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=flow.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流转管道 {entry_id} 不存在")
    return entry


@router.put("/{entry_id}/legacy-text")
def update_legacy_text(entry_id: int, payload: LegacyTextPayload) -> ActionResult:
    """自由文本依赖的唯一编辑入口；迁移冻结/切换后返回 409，保证只有一个可编辑来源。"""
    try:
        entry = service.update_legacy_text(entry_id, payload.text)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"流转管道 {entry_id} 不存在")
    except LegacyEditDenied as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return ActionResult(ok=True, message="自由文本依赖已更新", entry=entry)


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流转管道已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
