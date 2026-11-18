"""依赖迁移路由：迁移作业编排、复核、切换/回滚、产物持久化、导出与审计。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.migration import ConcurrentUpdateError, ObjectLockedError
from app.migration.service import MigrationError, migration_service

router = APIRouter(prefix="/api/migration", tags=["依赖迁移"])


class CreateJobPayload(BaseModel):
    name: str
    cutover_strategy: str
    actor: str | None = "operator"


class VersionedPayload(BaseModel):
    expected_version: int
    actor: str | None = "operator"


class RecalcPayload(VersionedPayload):
    pass


class ResolvePayload(BaseModel):
    expected_version: int
    node_id: str | None = None
    actor: str | None = "reviewer"


class CutoverPayload(VersionedPayload):
    pipeline_id: int | None = None


class PublishPayload(VersionedPayload):
    producer: str | None = "transform-pipeline"


class UpgradePayload(BaseModel):
    target_version: int
    actor: str | None = "release-bot"


def _handle_error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=f"对象不存在：{exc.args[0]}")
    if isinstance(exc, (ConcurrentUpdateError, ObjectLockedError, MigrationError)):
        return HTTPException(status_code=409, detail=str(exc))
    raise exc


def csv_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("/node-catalog")
def node_catalog() -> dict[str, Any]:
    """新编排器已注册的真实节点目录，供待复核取舍。"""
    from app.migration.catalog import list_nodes
    nodes = list_nodes()
    return {"items": nodes, "total": len(nodes)}


@router.get("/jobs")
def list_jobs() -> dict[str, Any]:
    return {"items": migration_service.list_jobs(), "total": len(migration_service.list_jobs())}


@router.post("/jobs")
def create_job(payload: CreateJobPayload) -> dict[str, Any]:
    try:
        job = migration_service.create_job(payload.name, payload.cutover_strategy, payload.actor or "operator")
    except MigrationError as exc:
        raise _handle_error(exc)
    return {"ok": True, "entry": job}


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    try:
        return migration_service.get_detail(job_id)
    except KeyError as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/analyze")
def analyze(job_id: str, payload: VersionedPayload) -> dict[str, Any]:
    try:
        return migration_service.analyze_job(job_id, payload.expected_version, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/freeze")
def freeze(job_id: str, payload: VersionedPayload) -> dict[str, Any]:
    try:
        return migration_service.freeze_job(job_id, payload.expected_version, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/conflicts")
def detect_conflicts(job_id: str, payload: VersionedPayload) -> dict[str, Any]:
    try:
        return migration_service.detect_conflicts(job_id, payload.expected_version, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/recalculate")
def recalculate(job_id: str, payload: RecalcPayload) -> dict[str, Any]:
    try:
        return migration_service.recalculate(job_id, payload.expected_version, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.get("/jobs/{job_id}/pending")
def pending_review(job_id: str) -> dict[str, Any]:
    items = migration_service.pending_review(job_id)
    return {"items": items, "total": len(items)}


@router.post("/items/{item_id}/resolve")
def resolve_item(item_id: str, payload: ResolvePayload) -> dict[str, Any]:
    try:
        entry = migration_service.resolve_item(
            item_id, payload.expected_version, payload.node_id, payload.actor or "reviewer")
    except (KeyError, MigrationError, ConcurrentUpdateError, ObjectLockedError) as exc:
        raise _handle_error(exc)
    return {"ok": True, "entry": entry}


@router.post("/jobs/{job_id}/artifacts")
def publish_artifacts(job_id: str, payload: PublishPayload) -> dict[str, Any]:
    try:
        return migration_service.publish_artifacts(
            job_id, payload.expected_version, payload.producer or "transform-pipeline",
            payload.actor or "transform-pipeline")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/cutover")
def cutover(job_id: str, payload: CutoverPayload) -> dict[str, Any]:
    try:
        return migration_service.cutover(job_id, payload.expected_version,
                                         payload.pipeline_id, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError, ObjectLockedError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/rollback")
def rollback(job_id: str, payload: VersionedPayload) -> dict[str, Any]:
    try:
        return migration_service.rollback(job_id, payload.expected_version, payload.actor or "operator")
    except (KeyError, MigrationError, ConcurrentUpdateError) as exc:
        raise _handle_error(exc)


@router.post("/jobs/{job_id}/pipeline-records")
def generate_pipeline_record(job_id: str) -> dict[str, Any]:
    try:
        record = migration_service.generate_pipeline_record(job_id)
    except (KeyError, MigrationError) as exc:
        raise _handle_error(exc)
    return {"ok": True, "entry": record}


@router.get("/jobs/{job_id}/audit-logs")
def audit_logs(job_id: str, object_id: str | None = None) -> dict[str, Any]:
    return {"items": migration_service.audit_logs(job_id, object_id)}


@router.get("/pipelines/{pipeline_id}/edit-state")
def pipeline_edit_state(pipeline_id: int) -> dict[str, Any]:
    return migration_service.pipeline_edit_state(pipeline_id)


@router.post("/schema/upgrade")
def upgrade_schema(payload: UpgradePayload) -> dict[str, Any]:
    try:
        return migration_service.upgrade_schema(payload.target_version, payload.actor or "release-bot")
    except MigrationError as exc:
        raise _handle_error(exc)


@router.get("/jobs/{job_id}/export")
def export_evidence(job_id: str) -> StreamingResponse:
    """导出文件保留两份依据：原依赖文本 + 新节点关系，逐条可回查。"""
    try:
        detail = migration_service.get_detail(job_id)
    except KeyError as exc:
        raise _handle_error(exc)
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["管道编号", "片段序号", "原依赖文本", "文本片段", "匹配状态",
                     "节点标识", "节点名称", "复核状态", "冲突", "编辑来源", "读取方式"])
    for item in detail["items"]:
        writer.writerow([
            csv_cell(item["pipeline_code"]), item["ordinal"],
            csv_cell(item["original_text"]), csv_cell(item["fragment"]),
            csv_cell(item["match_status"]), csv_cell(item.get("matched_node_id") or ""),
            csv_cell(item.get("matched_node_name") or ""), csv_cell(item["review_status"]),
            "是" if item.get("conflict") else "否",
            csv_cell(item.get("edit_source", "")), csv_cell(item.get("read_mode", "")),
        ])
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=migration_evidence.csv"},
    )
