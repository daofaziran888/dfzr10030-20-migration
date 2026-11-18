"""依赖迁移链路后端单元测试（仅标准库 unittest，内存/临时文件 SQLite）。

覆盖：解析匹配、冻结冲突、按冲突重算、两种切换边界、回滚、
乐观锁并发互斥、单编辑入口守卫、转换管道回写/同指纹生成、
持久化跨连接可查、版本升级与回滚证据、双依据导出、审计历史。
"""
from __future__ import annotations

import copy
import threading
import tempfile
import unittest
from pathlib import Path

from app.migration import (
    ConcurrentUpdateError,
    MigrationRepository,
    ObjectLockedError,
    repository_session,
)
from app.migration.engine import (
    CUTOVER_BATCH,
    CUTOVER_PIPELINE,
    MATCH_AMBIGUOUS,
    MATCH_EXACT,
    MATCH_UNMAPPED,
    REVIEW_PENDING,
    engine,
    split_fragments,
)
from app.migration.service import MigrationError, MigrationService
from app.services.flow import FlowService, LegacyEditDenied
from app.services.transform import TransformService
from app.store import store


class MigrationTestBase(unittest.TestCase):
    def setUp(self) -> None:
        # 文件库放在临时目录：并发线程/新连接共享同一份数据（:memory: 连接间不共享）
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "migration-test.db")
        self.repo = MigrationRepository(self.db_path)
        self.ctx = repository_session(self.repo)
        self.ctx.__enter__()
        self.svc = MigrationService()
        self.flow = FlowService()
        self.transform = TransformService()
        self._flow_backup = copy.deepcopy(store.rows("flow"))

    def tearDown(self) -> None:
        store._tables["flow"] = self._flow_backup
        self.ctx.__exit__(None, None, None)
        self.repo.close()
        self._tmp.cleanup()

    # ---- 公共流程工具 ---------------------------------------------------
    def _create_analyzed_job(self, strategy: str = CUTOVER_BATCH, name: str = "迁移作业"):
        job = self.svc.create_job(name, strategy)
        result = self.svc.analyze_job(job["id"], job["version"])
        return result["job"], result["items"]

    def _freeze_and_clean(self, strategy: str = CUTOVER_BATCH):
        """创建→解析→冻结→无冲突→复核全部待处理项，返回 (job, items)。"""
        job, items = self._create_analyzed_job(strategy)
        frozen = self.svc.freeze_job(job["id"], job["version"])
        job, items = frozen["job"], frozen["items"]
        checked = self.svc.detect_conflicts(job["id"], job["version"])
        job, items = checked["job"], checked["items"]
        for item in list(items):
            if item["review_status"] == REVIEW_PENDING:
                # 无法映射的片段复核为“确认无映射”，保留原文本走占位节点
                self.svc.resolve_item(item["id"], item["version"], None)
        return self.repo.get_job(job["id"]), self.repo.list_items(job["id"])

    def _frozen_clean_job_id(self, strategy: str = CUTOVER_BATCH) -> str:
        job, _ = self._freeze_and_clean(strategy)
        return job["id"]


class ParseAndMatchTests(MigrationTestBase):
    def test_split_fragments_supports_mixed_separators(self):
        self.assertEqual(
            split_fragments("采集入口, 清洗节点A，去重；临时步骤、入库/主转换"),
            ["采集入口", "清洗节点A", "去重", "临时步骤", "入库", "主转换"],
        )

    def test_match_statuses_exact_ambiguous_unmapped(self):
        self.assertEqual(engine.match_fragment("数据校验")["match_status"], MATCH_EXACT)
        self.assertEqual(engine.match_fragment("节点")["match_status"], MATCH_AMBIGUOUS)
        result = engine.match_fragment("手工临时步骤")
        self.assertEqual(result["match_status"], MATCH_UNMAPPED)
        self.assertIsNone(result["matched_node_id"])

    def test_analyze_keeps_original_text_and_node_identity(self):
        items = engine.analyze()
        self.assertGreaterEqual(len(items), 8)
        first = next(i for i in items if i["pipeline_code"] == "FLOW-0001" and i["ordinal"] == 0)
        self.assertEqual(first["fragment"], "采集入口")
        self.assertEqual(first["matched_node_id"], "NODE-INGEST-01")
        self.assertIn("采集入口", first["original_text"])
        # 未映射片段进入待复核
        unmapped = next(i for i in items if i["fragment"] == "手工临时步骤")
        self.assertEqual(unmapped["review_status"], REVIEW_PENDING)


class BatchLifecycleTests(MigrationTestBase):
    def test_full_batch_shadow_then_unified_cutover_and_rollback(self):
        job, items = self._freeze_and_clean(CUTOVER_BATCH)

        # 发布清单与拓扑并持久化
        published = self.svc.publish_artifacts(job["id"], job["version"])
        manifest, topology = published["manifest"], published["topology"]
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(topology["payload"]["manifest_fingerprint"], manifest["fingerprint"])
        job = published["job"]
        self.assertEqual(job["status"], "ready")

        # 兼容拓扑使用真实节点标识，未映射片段使用占位节点，拓扑仍完整可读
        all_node_ids = {n["id"] for p in topology["payload"]["pipelines"] for n in p["nodes"]}
        self.assertIn("NODE-INGEST-01", all_node_ids)
        self.assertTrue(any(nid.startswith("UNMAPPED-") for nid in all_node_ids))

        # 整批策略不允许单管道切换
        with self.assertRaises(MigrationError):
            self.svc.cutover(job["id"], job["version"], pipeline_id=1)

        cut = self.svc.cutover(job["id"], job["version"])
        job = cut["job"]
        self.assertEqual(job["status"], "cutover")
        self.assertTrue(all(i["edit_source"] == "relation" for i in cut["items"]))
        self.assertTrue(all(i["read_mode"] == "relation" for i in cut["items"]))

        # 切换后旧文本入口与条目复核入口都只读：单一可编辑来源
        with self.assertRaises(LegacyEditDenied):
            self.flow.update_legacy_text(1, "采集入口, 去重")
        with self.assertRaises(ObjectLockedError):
            self.svc.resolve_item(cut["items"][0]["id"], cut["items"][0]["version"], "NODE-DEDUP-01")

        # 回滚到冻结基线，历史快照保留
        rolled = self.svc.rollback(job["id"], job["version"])
        self.assertEqual(rolled["job"]["status"], "rolled_back")
        self.assertTrue(all(i["edit_source"] == "legacy" for i in rolled["items"]))
        self.assertIsNotNone(self.repo.latest_snapshot(job["id"], "manifest"))
        self.assertIsNotNone(self.repo.latest_snapshot(job["id"], "topology"))

    def test_pending_review_blocks_publish(self):
        job, items = self._create_analyzed_job()
        frozen = self.svc.freeze_job(job["id"], job["version"])
        with self.assertRaises(MigrationError):
            self.svc.publish_artifacts(frozen["job"]["id"], frozen["job"]["version"])


class PerPipelineCutoverTests(MigrationTestBase):
    def test_per_pipeline_cutover_keeps_single_editable_source(self):
        job, items = self._freeze_and_clean(CUTOVER_PIPELINE)
        published = self.svc.publish_artifacts(job["id"], job["version"])
        job = published["job"]

        # 按管道策略必须指定 pipeline_id
        with self.assertRaises(MigrationError):
            self.svc.cutover(job["id"], job["version"])

        pipeline_ids = sorted({i["pipeline_id"] for i in items})
        first, second, third = pipeline_ids
        cut1 = self.svc.cutover(job["id"], job["version"], pipeline_id=first)
        # 部分切换时作业仍为 ready，混合存在两种读取方式
        self.assertEqual(cut1["job"]["status"], "ready")
        switched = {i["pipeline_id"] for i in cut1["items"] if i["edit_source"] == "relation"}
        self.assertEqual(switched, {first})
        # 已切换管道旧文本入口关闭，未切换管道仍可编辑
        with self.assertRaises(LegacyEditDenied):
            self.flow.update_legacy_text(first, "采集入口")
        before = store.find("flow", second)["依赖文本"]
        self.flow.update_legacy_text(second, before + ", 去重")
        self.assertEqual(store.find("flow", second)["依赖文本"], before + ", 去重")

        job = cut1["job"]
        self.svc.cutover(job["id"], job["version"], pipeline_id=second)
        job = self.repo.get_job(job["id"])
        cut3 = self.svc.cutover(job["id"], job["version"], pipeline_id=third)
        # 全部切换完成才进入 cutover
        self.assertEqual(cut3["job"]["status"], "cutover")
        self.assertTrue(all(i["edit_source"] == "relation" for i in cut3["items"]))


class ConflictAndRecalculateTests(MigrationTestBase):
    def test_freeze_guard_blocks_legacy_edit_but_out_of_band_edit_is_detected(self):
        job, items = self._create_analyzed_job()
        frozen = self.svc.freeze_job(job["id"], job["version"])
        job = frozen["job"]

        # 冻结后通过流转编排服务修改旧文本被拒绝
        with self.assertRaises(LegacyEditDenied):
            self.flow.update_legacy_text(2, "采集入口, 去重")

        # 模拟绕过入口的存量侧修改（老系统直写）→ 冲突检测按管道边界命中
        store.find("flow", 2)["依赖文本"] = "清洗节点一, 去重, 入库节点"
        checked = self.svc.detect_conflicts(job["id"], job["version"])
        self.assertEqual(checked["job"]["status"], "conflict")
        conflict_pipelines = {i["pipeline_id"] for i in checked["items"] if i["conflict"]}
        self.assertEqual(conflict_pipelines, {2})
        self.assertTrue(all(i["conflict"] for i in checked["items"] if i["pipeline_id"] == 2))
        self.assertFalse(any(i["conflict"] for i in checked["items"] if i["pipeline_id"] != 2))

        # 带冲突不能发布
        with self.assertRaises(MigrationError):
            self.svc.publish_artifacts(checked["job"]["id"], checked["job"]["version"])

        # 按冲突边界重算：仅重建管道2，其他管道复核结论保留
        rebuilt = self.svc.recalculate(checked["job"]["id"], checked["job"]["version"])
        self.assertEqual(rebuilt["job"]["status"], "frozen")
        self.assertFalse(any(i["conflict"] for i in rebuilt["items"]))
        pipeline2 = [i for i in rebuilt["items"] if i["pipeline_id"] == 2]
        self.assertNotIn("手工临时步骤", [i["fragment"] for i in pipeline2])
        self.assertIn("入库节点", [i["fragment"] for i in pipeline2])


class ConcurrencyTests(MigrationTestBase):
    def test_stale_job_version_is_rejected(self):
        job, _ = self._create_analyzed_job()
        # 同一对象并发写：乐观锁只允许一次成功，余下拒绝。
        # 顺序提交同一 expected_version，等价于并发下后到者的版本必然过期。
        accepted, rejected = 0, 0
        for _ in range(4):
            try:
                self.svc.freeze_job(job["id"], job["version"])
                accepted += 1
            except ConcurrentUpdateError:
                rejected += 1
        self.assertEqual(accepted, 1)
        self.assertEqual(rejected, 3)
        self.assertEqual(self.repo.get_job(job["id"])["version"], job["version"] + 1)

    def test_write_lock_serializes_concurrent_threads(self):
        # 真实多线程：进程内同一把互斥锁串行化，所有写要么成功要么以版本冲突拒绝，
        # 不出现脏写/找不到对象；线程内显式绑定同一测试库（ContextVar 不跨线程传播）。
        job, _ = self._create_analyzed_job()
        results: list[str] = []

        def freeze() -> None:
            with repository_session(self.repo):
                svc = MigrationService()
                try:
                    svc.freeze_job(job["id"], job["version"])
                    results.append("ok")
                except (ConcurrentUpdateError, MigrationError):
                    # 乐观锁版本冲突，或首个线程已把作业推进到冻结态：均属“余下拒绝”
                    results.append("rejected")

        threads = [threading.Thread(target=freeze) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results.count("ok"), 1)
        self.assertEqual(results.count("rejected"), 3)
        # 无脏写：作业只从 v2 推进到 v3
        self.assertEqual(self.repo.get_job(job["id"])["version"], job["version"] + 1)

    def test_stale_item_version_is_rejected(self):
        job, items = self._freeze_and_clean()
        target = next(i for i in items if i["match_status"] == MATCH_UNMAPPED)
        self.svc.resolve_item(target["id"], target["version"], "NODE-DEDUP-01")
        with self.assertRaises(ConcurrentUpdateError):
            self.svc.resolve_item(target["id"], target["version"], "NODE-LOAD-01")


class TransformCollaborationTests(MigrationTestBase):
    def test_writeback_and_same_evidence_flow_record(self):
        job, items = self._freeze_and_clean()
        # 转换管道服务回写清单+拓扑，并带出待复核清单（此时应为空）
        writeback = self.transform.writeback_artifacts(job["id"], job["version"])
        self.assertEqual(writeback["pending_review"], [])
        job = writeback["job"]

        # 读取兼容拓扑并按清单选择节点表达式
        artifacts = self.transform.read_migration_artifacts(job["id"])
        self.assertTrue(all("expression" in e for e in artifacts["expressions"]))
        mapped = next(e for e in artifacts["expressions"] if e["node_id"] == "NODE-INGEST-01")
        self.assertEqual(mapped["expression"], "ingest(source)")

        # 读取同一份清单+拓扑后才生成流转管道记录
        record = self.transform.generate_flow_record(job["id"])
        self.assertEqual(record["fingerprint"], writeback["manifest"]["fingerprint"])
        self.assertEqual(len(self.repo.list_pipeline_records(job["id"])), 1)

    def test_fingerprint_mismatch_rejects_flow_record(self):
        job, items = self._freeze_and_clean()
        published = self.svc.publish_artifacts(job["id"], job["version"])
        job = published["job"]
        # 伪造一份引用错误清单指纹的拓扑快照
        self.repo.save_snapshot(
            job["id"], "topology", 2, "deadbeefdead",
            {"job_id": job["id"], "schema_version": 2,
             "manifest_fingerprint": "wrong-fingerprint", "pipelines": []},
            "troublemaker",
        )
        with self.assertRaises((MigrationError, ValueError)):
            self.transform.generate_flow_record(job["id"])


class PersistenceAndUpgradeTests(MigrationTestBase):
    def test_repository_persists_across_connections(self):
        # 复用当前库文件，写入后关闭再用全新连接打开，验证落盘可回查
        db_path = self.db_path
        job_id = self._frozen_clean_job_id()
        job = self.repo.get_job(job_id)
        self.svc.publish_artifacts(job_id, job["version"])
        self.repo.close()

        # 新连接读取同一文件：清单、拓扑、审计均可回查
        repo_b = MigrationRepository(db_path)
        with repository_session(repo_b):
            self.assertIsNotNone(repo_b.get_job(job_id))
            self.assertIsNotNone(repo_b.latest_snapshot(job_id, "manifest"))
            self.assertIsNotNone(repo_b.latest_snapshot(job_id, "topology"))
            logs = repo_b.list_audit_logs(job_id)
            actions = {log["action"] for log in logs}
            self.assertIn("analyze", actions)
            self.assertIn("freeze", actions)
            self.assertIn("persist_manifest", actions)
        repo_b.close()

    def test_upgrade_schema_keeps_old_snapshots_as_rollback_evidence(self):
        job, items = self._freeze_and_clean()
        self.svc.publish_artifacts(job["id"], job["version"])
        job = self.repo.get_job(job["id"])
        self.assertEqual(int(self.repo.get_meta("relation_schema_version")), 2)

        result = self.svc.upgrade_schema(3)
        self.assertEqual(result["from_version"], 2)
        self.assertEqual(result["to_version"], 3)
        self.assertEqual(int(self.repo.get_meta("relation_schema_version")), 3)

        snapshots = self.repo.list_snapshots(job["id"])
        versions = sorted({s["schema_version"] for s in snapshots})
        self.assertEqual(versions, [2, 3])  # 旧版本保留，回滚边界清晰
        latest_manifest = self.repo.latest_snapshot(job["id"], "manifest")
        self.assertEqual(latest_manifest["schema_version"], 3)

        # 不能降级/重复升级
        with self.assertRaises(MigrationError):
            self.svc.upgrade_schema(3)


class ExportAndAuditTests(MigrationTestBase):
    def test_export_contains_both_evidence_and_audit_trail_is_complete(self):
        from app.routers.migration import export_evidence

        job, items = self._freeze_and_clean()
        published = self.svc.publish_artifacts(job["id"], job["version"])
        job = published["job"]
        self.svc.cutover(job["id"], job["version"])

        response = export_evidence(job["id"])

        async def _read_body() -> str:
            chunks = [chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
                      async for chunk in response.body_iterator]
            return "".join(chunks)

        import asyncio
        body = asyncio.run(_read_body())
        self.assertIn("原依赖文本", body)
        self.assertIn("节点标识", body)
        self.assertIn("NODE-INGEST-01", body)
        self.assertIn("采集入口", body)

        logs = self.svc.audit_logs(job["id"])
        actions = [log["action"] for log in logs]
        for expected in ("create", "analyze", "freeze", "detect_conflict",
                         "persist_manifest", "persist_topology", "cutover"):
            self.assertIn(expected, actions)
        # 作业级状态审计都带版本前后值，历史可逐条回查
        job_logs = [log for log in logs if log["object_type"] == "migration_job"]
        versioned = [log for log in job_logs if log["to_version"] is not None]
        self.assertGreater(len(versioned), 0)
        self.assertTrue(all(log["from_version"] is None or log["to_version"] == log["from_version"] + 1
                            for log in versioned))
        # 回查单条对象的完整历史
        item_logs = self.svc.audit_logs(object_id=items[0]["id"])
        self.assertTrue(any(log["action"] in ("analyze", "freeze", "cutover") for log in item_logs))


if __name__ == "__main__":
    unittest.main(verbosity=2)
