"""采集转换管道 DAG 图服务：生成节点、边示例结构与流转规则。

迁移链路协作：
- 转换管道服务读取兼容流转拓扑，按迁移关系清单选择节点表达式；
- 回写迁移关系清单与兼容流转拓扑（持久化由 migration 仓库承担），并生成待复核清单；
- 读取同一份清单与拓扑（指纹一致）后才生成流转管道记录。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "transform"
REQUIRED_FIELDS = ["节点编号", "输入来源", "输出目标"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用节点": "已启用", "停用节点": "已停用"}
NEGATIVE_ACTIONS = ["停用节点"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _migration_service():
    from app.migration.service import migration_service
    return migration_service


class TransformService:
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            nodes.append({"id": int(row.get("id", i+1)), "name": str(row.get(list(row.keys())[1] if len(row)>1 else "name", "")), "status": str(row.get("status", STATUS_ORDER[0])),
                          **{k: row[k] for k in row if k not in ("id","status","pending","abnormal")}})
        for i in range(len(nodes)-1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i+1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id",0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"转换节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集转换管道可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"转换节点已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ---- 迁移链路：读取同一份清单+拓扑，选择节点表达式 ------------------
    def read_migration_artifacts(self, job_id: str) -> dict[str, Any]:
        svc = _migration_service()
        detail = svc.get_detail(job_id)
        manifest = detail.get("manifest")
        topology = detail.get("topology")
        if manifest is None or topology is None:
            raise ValueError("迁移关系清单或兼容流转拓扑尚未持久化")
        if topology["payload"]["manifest_fingerprint"] != manifest["fingerprint"]:
            raise ValueError("清单与拓扑指纹不一致，转换管道拒绝读取")
        # 读取兼容流转拓扑，按迁移关系清单选择节点表达式
        expressions = svc._select_expressions(manifest["payload"])
        return {"manifest": manifest, "topology": topology, "expressions": expressions}

    def writeback_artifacts(self, job_id: str, expected_version: int,
                            producer: str = "transform-pipeline") -> dict[str, Any]:
        """转换管道服务同步回写迁移关系清单及兼容流转拓扑并生成待复核清单。"""
        svc = _migration_service()
        result = svc.publish_artifacts(job_id, expected_version, producer=producer, actor=producer)
        pending = svc.pending_review(job_id)
        return {"manifest": result["manifest"], "topology": result["topology"],
                "pending_review": pending, "job": result["job"]}

    def generate_flow_record(self, job_id: str) -> dict[str, Any]:
        """读取同一份迁移关系清单及兼容流转拓扑后才生成流转管道记录。"""
        # 先读取并校验同一份依据
        self.read_migration_artifacts(job_id)
        return _migration_service().generate_pipeline_record(job_id)
