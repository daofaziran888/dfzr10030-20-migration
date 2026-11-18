"""迁移应用服务：编排迁移引擎与持久化仓库，承载状态机与跨服务协作。

链路分工（与需求对应）：
- 流转编排服务（flow）：提供存量自由文本、执行单编辑入口守卫；
- 转换管道服务（transform）：读取同一份清单+拓扑后回写持久化并生成流转管道记录；
- 本服务（migration）：迁移过程编排、冲突/复核、切换与回滚、审计。
"""
from __future__ import annotations

from typing import Any

from app.config import settings
from app.migration import (
    ConcurrentUpdateError,
    MigrationRepository,
    ObjectLockedError,
    get_repository,
)
from app.migration.engine import (
    CUTOVER_BATCH,
    CUTOVER_PIPELINE,
    EDIT_LEGACY,
    EDIT_RELATION,
    JOB_ANALYZED,
    JOB_CONFLICT,
    JOB_CREATED,
    JOB_CUTOVER,
    JOB_FROZEN,
    JOB_READY,
    JOB_ROLLED_BACK,
    MATCH_AMBIGUOUS,
    MATCH_EXACT,
    MATCH_UNMAPPED,
    READ_LEGACY,
    READ_RELATION,
    READ_SHADOW,
    REVIEW_CONFIRMED,
    REVIEW_PENDING,
    REVIEW_RESOLVED,
    VALID_STRATEGIES,
    engine,
    fingerprint,
)


class MigrationError(Exception):
    """业务规则冲突，路由层映射为 409。"""


class MigrationService:
    def __init__(self, repo: MigrationRepository | None = None) -> None:
        self._repo = repo
        self.engine = engine

    @property
    def repo(self) -> MigrationRepository:
        # 延迟解析：测试可通过 repository_session 注入内存库
        return self._repo or get_repository()

    # ---- 作业 -----------------------------------------------------------
    def create_job(self, name: str, cutover_strategy: str, actor: str = "operator") -> dict[str, Any]:
        if cutover_strategy not in VALID_STRATEGIES:
            raise MigrationError(f"切换边界必须是 {sorted(VALID_STRATEGIES)} 之一")
        job = self.repo.create_job(
            {"name": name, "cutover_strategy": cutover_strategy,
             "schema_version": 1, "pipeline_count": len(self.engine.load_legacy_pipelines())},
            actor,
        )
        return job

    def list_jobs(self) -> list[dict[str, Any]]:
        return self.repo.list_jobs()

    def get_job(self, job_id: str) -> dict[str, Any]:
        job = self.repo.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    def analyze_job(self, job_id: str, expected_version: int,
                    actor: str = "operator") -> dict[str, Any]:
        job = self.get_job(job_id)
        if job["status"] not in (JOB_CREATED, JOB_ANALYZED):
            raise MigrationError(f"作业处于 {job['status']}，不能重新解析")
        items = self.engine.analyze()
        self.repo.bulk_replace_items(job_id, items, actor, action="analyze")
        source_fp = fingerprint([(i["pipeline_id"], i["original_text"]) for i in items])
        updated = self.repo.update_job(
            job_id, expected_version,
            {"status": JOB_ANALYZED, "source_fingerprint": source_fp},
            "analyze", actor, {"fragments": len(items)},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    def freeze_job(self, job_id: str, expected_version: int,
                   actor: str = "operator") -> dict[str, Any]:
        """冻结存量文本：之后旧文本编辑入口关闭，进入影子读取。"""
        job = self.get_job(job_id)
        if job["status"] not in (JOB_ANALYZED, JOB_FROZEN, JOB_CONFLICT, JOB_ROLLED_BACK):
            raise MigrationError(f"作业处于 {job['status']}，不能冻结")
        items = self.repo.list_items(job_id)
        if not items:
            raise MigrationError("没有可冻结的迁移条目，请先解析存量管道")
        frozen = self.engine.freeze_baseline(items)
        self.repo.bulk_replace_items(job_id, frozen, actor, action="freeze")
        updated = self.repo.update_job(
            job_id, expected_version, {"status": JOB_FROZEN, "frozen": 1},
            "freeze", actor, {"items": len(frozen)},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    def detect_conflicts(self, job_id: str, expected_version: int,
                         actor: str = "operator") -> dict[str, Any]:
        job = self.get_job(job_id)
        if not job.get("frozen"):
            raise MigrationError("作业尚未冻结旧文本，无法做冲突检测")
        items = self.repo.list_items(job_id)
        marked = self.engine.detect_conflicts(items)
        self.repo.bulk_replace_items(job_id, marked, actor, action="detect_conflict")
        has_conflict = any(i["conflict"] for i in marked)
        updated = self.repo.update_job(
            job_id, expected_version,
            {"status": JOB_CONFLICT if has_conflict else JOB_FROZEN},
            "detect_conflict", actor, {"conflicts": sum(1 for i in marked if i["conflict"])},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    def recalculate(self, job_id: str, expected_version: int,
                    actor: str = "operator") -> dict[str, Any]:
        """按冲突边界重算：只重建冲突管道，非冲突管道复核结论保留。"""
        job = self.get_job(job_id)
        items = self.repo.list_items(job_id)
        if not any(i.get("conflict") for i in items):
            raise MigrationError("当前没有冲突管道，无需重算")
        rebuilt = self.engine.recalculate(items)
        self.repo.bulk_replace_items(job_id, rebuilt, actor, action="recalculate")
        updated = self.repo.update_job(
            job_id, expected_version, {"status": JOB_FROZEN},
            "recalculate", actor, {"rebuilt_pipelines": len({i["pipeline_id"] for i in items if i["conflict"]})},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    # ---- 人工复核/取舍 --------------------------------------------------
    def resolve_item(self, item_id: str, expected_version: int, node_id: str | None,
                     actor: str = "reviewer") -> dict[str, Any]:
        item = self.repo.get_item(item_id)
        if item is None:
            raise KeyError(item_id)
        if item.get("edit_source") == EDIT_RELATION:
            raise ObjectLockedError("该管道已切换到结构化关系，条目只读")
        valid = {n["node_id"]: n for n in self.engine.catalog}
        if node_id:
            if node_id not in valid:
                raise MigrationError(f"节点标识 {node_id} 不在编排器目录中")
            node = valid[node_id]
            mutations = {
                "matched_node_id": node_id, "matched_node_name": node["node_name"],
                "match_status": MATCH_EXACT, "review_status": REVIEW_RESOLVED,
                "_guard_edit_source": True,
            }
        else:
            # 复核确认无法映射：保留原文本，拓扑用占位节点，仍标记已处理
            mutations = {
                "matched_node_id": None, "matched_node_name": None,
                "match_status": MATCH_UNMAPPED, "review_status": REVIEW_RESOLVED,
                "_guard_edit_source": True,
            }
        return self.repo.update_item(item_id, expected_version, mutations, "resolve", actor,
                                     {"node_id": node_id})

    # ---- 产出清单/拓扑并持久化（转换管道服务回写） ----------------------
    def publish_artifacts(self, job_id: str, expected_version: int,
                          producer: str = "transform-pipeline",
                          actor: str = "transform-pipeline") -> dict[str, Any]:
        job = self.get_job(job_id)
        items = self.repo.list_items(job_id)
        if not items:
            raise MigrationError("迁移条目为空，无法生成清单与拓扑")
        pending = [i for i in items if i["review_status"] == REVIEW_PENDING]
        if pending:
            raise MigrationError(f"还有 {len(pending)} 条待复核片段，不能生成迁移产物")
        errors = self.engine.validate_relations(items)
        if errors:
            raise MigrationError("节点关系校验失败：" + "；".join(errors))
        manifest = self.engine.build_manifest(job_id, items, settings.relation_schema_version)
        topology = self.engine.build_topology(
            job_id, items, manifest["fingerprint"], settings.relation_schema_version)
        manifest_snapshot = self.repo.save_snapshot(
            job_id, "manifest", settings.relation_schema_version,
            manifest["fingerprint"], manifest, actor)
        topology_snapshot = self.repo.save_snapshot(
            job_id, "topology", settings.relation_schema_version,
            topology["fingerprint"], topology, actor)
        updated = self.repo.update_job(
            job_id, expected_version, {"status": JOB_READY},
            "publish_artifacts", producer,
            {"manifest": manifest_snapshot["id"], "topology": topology_snapshot["id"]},
        )
        return {"job": updated, "manifest": manifest_snapshot, "topology": topology_snapshot}

    # ---- 切换：两种成立边界 --------------------------------------------
    def cutover(self, job_id: str, expected_version: int, pipeline_id: int | None = None,
                actor: str = "operator") -> dict[str, Any]:
        job = self.get_job(job_id)
        if job["status"] not in (JOB_READY, JOB_CUTOVER):
            raise MigrationError(f"作业处于 {job['status']}，未生成可切换的迁移产物")
        items = self.repo.list_items(job_id)
        strategy = job["cutover_strategy"]
        if strategy == CUTOVER_BATCH and pipeline_id is not None:
            raise MigrationError("整批影子切换不接受单管道参数，必须统一切换")
        if strategy == CUTOVER_PIPELINE and pipeline_id is None:
            raise MigrationError("按管道粒度切换必须指定 pipeline_id")
        target_ids = {i["pipeline_id"] for i in items} if pipeline_id is None else {pipeline_id}
        if pipeline_id is not None and pipeline_id not in {i["pipeline_id"] for i in items}:
            raise MigrationError(f"管道 {pipeline_id} 不在迁移范围内")
        for item in items:
            if item["pipeline_id"] in target_ids:
                self.repo.update_item(
                    item["id"], item["version"],
                    {"read_mode": READ_RELATION, "edit_source": EDIT_RELATION,
                     "_guard_edit_source": False},
                    "cutover", actor, {"strategy": strategy},
                )
        all_relation = all(i["edit_source"] == EDIT_RELATION for i in
                           self.repo.list_items(job_id))
        updated = self.repo.update_job(
            job_id, expected_version,
            {"status": JOB_CUTOVER if (strategy == CUTOVER_BATCH or all_relation) else JOB_READY},
            "cutover", actor, {"strategy": strategy, "pipeline_id": pipeline_id},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    def rollback(self, job_id: str, expected_version: int, actor: str = "operator") -> dict[str, Any]:
        """回滚到冻结基线：编辑入口恢复为旧文本，清单/拓扑快照保留可查。"""
        job = self.get_job(job_id)
        items = self.repo.list_items(job_id)
        for item in items:
            self.repo.update_item(
                item["id"], item["version"],
                {"read_mode": READ_LEGACY, "edit_source": EDIT_LEGACY,
                 "current_text": item.get("frozen_text") or item["original_text"],
                 "conflict": 0, "conflict_reason": "", "_guard_edit_source": False},
                "rollback", actor, {"to": "frozen_baseline"},
            )
        updated = self.repo.update_job(
            job_id, expected_version, {"status": JOB_ROLLED_BACK, "frozen": 1},
            "rollback", actor, {"restored_items": len(items)},
        )
        return {"job": updated, "items": self.repo.list_items(job_id)}

    # ---- 转换管道服务：读同一份清单+拓扑后生成流转管道记录 --------------
    def generate_pipeline_record(self, job_id: str, actor: str = "transform-pipeline") -> dict[str, Any]:
        job = self.get_job(job_id)
        manifest = self.repo.latest_snapshot(job_id, "manifest")
        topology = self.repo.latest_snapshot(job_id, "topology")
        if manifest is None or topology is None:
            raise MigrationError("迁移关系清单或兼容流转拓扑尚未持久化")
        # 同一份依据：拓扑必须引用当前清单指纹，否则拒绝生成
        if topology["payload"]["manifest_fingerprint"] != manifest["fingerprint"]:
            raise MigrationError("清单与拓扑指纹不一致，拒绝生成流转管道记录")
        expressions = self._select_expressions(manifest["payload"])
        record = self.repo.save_pipeline_record(
            job_id, manifest["id"], topology["id"], manifest["fingerprint"], actor,
            {"job_id": job_id, "expressions": expressions,
             "pipeline_count": len(topology["payload"]["pipelines"]),
             "schema_version": manifest["schema_version"]},
        )
        return record

    def _select_expressions(self, manifest: dict[str, Any]) -> list[dict[str, Any]]:
        """转换管道服务读取兼容拓扑，按迁移关系清单选择节点表达式。"""
        expr_by_node = {n["node_id"]: n["expression"] for n in self.engine.catalog}
        selected = []
        for relation in manifest["relations"]:
            node_id = relation.get("matched_node_id")
            selected.append({
                "relation_id": relation["relation_id"],
                "pipeline_code": relation["pipeline_code"],
                "fragment": relation["fragment"],
                "node_id": node_id,
                "expression": expr_by_node.get(node_id) if node_id else None,
                "fallback": node_id is None,
            })
        return selected

    # ---- 查询与导出 -----------------------------------------------------
    def get_detail(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        items = self.repo.list_items(job_id)
        manifest = self.repo.latest_snapshot(job_id, "manifest")
        topology = self.repo.latest_snapshot(job_id, "topology")
        records = self.repo.list_pipeline_records(job_id)
        return {"job": job, "items": items, "manifest": manifest, "topology": topology,
                "pipeline_records": records}

    def pending_review(self, job_id: str) -> list[dict[str, Any]]:
        return self.repo.list_items(job_id, REVIEW_PENDING)

    def audit_logs(self, job_id: str | None = None,
                   object_id: str | None = None) -> list[dict[str, Any]]:
        return self.repo.list_audit_logs(job_id, object_id)

    def pipeline_edit_state(self, pipeline_id: int) -> dict[str, Any]:
        """给流转编排服务的单编辑入口守卫提供判定依据。"""
        for job in self.repo.list_jobs():
            for item in self.repo.list_items(job["id"]):
                if item["pipeline_id"] == pipeline_id:
                    return {"pipeline_id": pipeline_id, "job_id": job["id"],
                            "read_mode": item["read_mode"], "edit_source": item["edit_source"],
                            "frozen": bool(job.get("frozen")), "job_status": job["status"],
                            "cutover_strategy": job["cutover_strategy"]}
        return {"pipeline_id": pipeline_id, "job_id": None, "read_mode": READ_LEGACY,
                "edit_source": EDIT_LEGACY, "frozen": False, "job_status": None,
                "cutover_strategy": None}

    # ---- 版本升级 -------------------------------------------------------
    def upgrade_schema(self, target_version: int, actor: str = "release-bot") -> dict[str, Any]:
        """把迁移清单与兼容流转拓扑迁移到新版本，返回升级前后版本与可回滚信息。"""
        current = int(self.repo.get_meta("relation_schema_version") or "1")
        if target_version <= current:
            raise MigrationError(f"目标版本 {target_version} 不高于当前版本 {current}，无需升级")
        upgraded: list[dict[str, Any]] = []
        for job in self.repo.list_jobs():
            items = self.repo.list_items(job["id"])
            manifest = self.engine.build_manifest(job["id"], items, target_version)
            topology = self.engine.build_topology(
                job["id"], items, manifest["fingerprint"], target_version)
            m = self.repo.save_snapshot(job["id"], "manifest", target_version,
                                        manifest["fingerprint"], manifest, actor)
            t = self.repo.save_snapshot(job["id"], "topology", target_version,
                                        topology["fingerprint"], topology, actor)
            self.repo.update_job(job["id"], job["version"],
                                 {"schema_version": target_version}, "upgrade_schema", actor,
                                 {"from": current, "to": target_version})
            upgraded.append({"job_id": job["id"], "manifest": m["id"], "topology": t["id"]})
        self.repo.set_meta("relation_schema_version", str(target_version))
        return {"from_version": current, "to_version": target_version, "upgraded": upgraded}


class _LazyMigrationService:
    """模块级入口延迟到首次调用时解析仓库，保证测试注入的内存/临时库生效。"""

    def __getattr__(self, name: str) -> Any:
        return getattr(MigrationService(), name)


migration_service = _LazyMigrationService()
