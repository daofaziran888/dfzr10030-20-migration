"""采集转换管道 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.migration import ConcurrentUpdateError
from app.migration.service import MigrationError
from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.transform import TransformService

router = APIRouter(prefix="/api/transform", tags=["采集转换管道"])
service = TransformService()
LIST_FIELDS = ["节点编号", "输入来源", "输出目标", "转换表达式", "执行顺序", "依赖节点", "失败策略", "节点状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


class WritebackPayload(BaseModel):
    expected_version: int
    producer: str | None = "transform-pipeline"


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
                             headers={"Content-Disposition": "attachment; filename=transform.csv"})


@router.get("/migration/{job_id}/artifacts")
def read_migration_artifacts(job_id: str) -> dict:
    """转换管道服务读取兼容流转拓扑，并按迁移关系清单选择节点表达式。"""
    try:
        return service.read_migration_artifacts(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"迁移作业 {job_id} 不存在")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/migration/{job_id}/writeback")
def writeback_artifacts(job_id: str, payload: WritebackPayload) -> dict:
    """同步回写迁移关系清单及兼容流转拓扑，并返回待复核清单。"""
    try:
        return service.writeback_artifacts(
            job_id, payload.expected_version, payload.producer or "transform-pipeline")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"迁移作业 {job_id} 不存在")
    except (MigrationError, ConcurrentUpdateError) as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/migration/{job_id}/flow-records")
def generate_flow_record(job_id: str) -> ActionResult:
    """读取同一份迁移关系清单及兼容流转拓扑后才生成流转管道记录。"""
    try:
        record = service.generate_flow_record(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"迁移作业 {job_id} 不存在")
    except (MigrationError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return ActionResult(ok=True, message="流转管道记录已生成", entry=record)


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"转换节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="转换节点已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
