"""自由文本依赖迁移引擎。

职责（对应需求链路）：
- 读取存量流转管道（flow）的自由文本依赖，按逗号/中文逗号切分片段；
- 用新编排器节点目录把片段匹配成可校验的结构化节点关系，保留原文本与节点标识；
- 冻结存量文本形成影子读取基线，检测运营期间旧文本被修改带来的冲突；
- 产出迁移关系清单与兼容流转拓扑（同一指纹），供转换管道服务回写与逐条回查；
- 支持两种切换边界（整批影子后统一切换 / 按管道粒度切换）与按冲突边界重算、回滚。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.migration.catalog import NODE_CATALOG, normalize
from app.store import store

FLOW_MODULE = "flow"
LEGACY_TEXT_FIELD = "依赖文本"

# 作业状态机
JOB_CREATED = "created"
JOB_ANALYZED = "analyzed"
JOB_FROZEN = "frozen"
JOB_CONFLICT = "conflict"
JOB_READY = "ready"
JOB_CUTOVER = "cutover"
JOB_ROLLED_BACK = "rolled_back"

# 单条片段匹配结果
MATCH_EXACT = "exact"          # 唯一命中真实节点
MATCH_AMBIGUOUS = "ambiguous"  # 多个候选，需人工取舍
MATCH_UNMAPPED = "unmapped"    # 找不到对应节点，进入待复核

REVIEW_PENDING = "pending"
REVIEW_CONFIRMED = "confirmed"
REVIEW_RESOLVED = "resolved"

CUTOVER_BATCH = "batch_shadow"   # 先冻结旧文本→整批影子读取→统一切换
CUTOVER_PIPELINE = "per_pipeline"  # 按管道逐个切换并停用对应旧文本入口

VALID_STRATEGIES = {CUTOVER_BATCH, CUTOVER_PIPELINE}

EDIT_LEGACY = "legacy"
EDIT_RELATION = "relation"
READ_LEGACY = "legacy"
READ_SHADOW = "shadow"
READ_RELATION = "relation"


def split_fragments(text: str) -> list[str]:
    """旧管道依赖是任务名与逗号分隔片段，兼容中英文逗号、分号、空白。"""
    for sep in ("；", ";", "，", "、", "/", "|"):
        text = text.replace(sep, ",")
    return [part.strip() for part in text.split(",") if part.strip()]


def fingerprint(payload: Any) -> str:
    """清单/拓扑内容指纹：转换管道服务据此确认读到的是同一份产物。"""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class MigrationEngine:
    def __init__(self) -> None:
        self.catalog = list(NODE_CATALOG)
        self._index: list[tuple[dict[str, Any], set[str]]] = [
            (node, {normalize(node["node_name"]), *(normalize(a) for a in node.get("aliases", []))})
            for node in self.catalog
        ]

    # ---- 读取存量管道 ---------------------------------------------------
    def load_legacy_pipelines(self) -> list[dict[str, Any]]:
        rows = store.rows(FLOW_MODULE)
        pipelines = []
        for row in rows:
            pipelines.append({
                "pipeline_id": int(row["id"]),
                "pipeline_code": str(row.get("管道编号", "")),
                "pipeline_name": str(row.get("管道名称", "")),
                "legacy_text": str(row.get(LEGACY_TEXT_FIELD, "") or ""),
            })
        return pipelines

    # ---- 文本 → 节点匹配 -----------------------------------------------
    def match_fragment(self, fragment: str) -> dict[str, Any]:
        target = normalize(fragment)
        candidates: list[dict[str, Any]] = []
        for node, keys in self._index:
            if target in keys or any(target and target in key for key in keys):
                candidates.append(node)
        unique: dict[str, dict[str, Any]] = {n["node_id"]: n for n in candidates}
        if len(unique) == 1:
            node = next(iter(unique.values()))
            return {"match_status": MATCH_EXACT, "matched_node_id": node["node_id"],
                    "matched_node_name": node["node_name"], "candidates": []}
        if len(unique) > 1:
            return {"match_status": MATCH_AMBIGUOUS, "matched_node_id": None,
                    "matched_node_name": None,
                    "candidates": [{"node_id": n["node_id"], "node_name": n["node_name"]}
                                   for n in unique.values()]}
        return {"match_status": MATCH_UNMAPPED, "matched_node_id": None,
                "matched_node_name": None, "candidates": []}

    def analyze(self) -> list[dict[str, Any]]:
        """把每条存量管道的自由文本拆成迁移条目，逐片段保留两份依据。"""
        items: list[dict[str, Any]] = []
        for pipeline in self.load_legacy_pipelines():
            text = pipeline["legacy_text"]
            fragments = split_fragments(text)
            for ordinal, fragment in enumerate(fragments):
                match = self.match_fragment(fragment)
                needs_review = match["match_status"] != MATCH_EXACT
                items.append({
                    "pipeline_id": pipeline["pipeline_id"],
                    "pipeline_code": pipeline["pipeline_code"],
                    "pipeline_name": pipeline["pipeline_name"],
                    "ordinal": ordinal,
                    "original_text": text,
                    "fragment": fragment,
                    "matched_node_id": match["matched_node_id"],
                    "matched_node_name": match["matched_node_name"],
                    "match_status": match["match_status"],
                    "candidates": match["candidates"],
                    "review_status": REVIEW_PENDING if needs_review else REVIEW_CONFIRMED,
                    "conflict": 0,
                    "conflict_reason": "",
                    "frozen_text": "",
                    "current_text": text,
                    "read_mode": READ_LEGACY,
                    "edit_source": EDIT_LEGACY,
                })
        return items

    # ---- 冻结与冲突检测 -------------------------------------------------
    def freeze_baseline(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """冻结旧文本：记录冻结时刻原文，作为影子读取与冲突比对基线。"""
        frozen = []
        for item in items:
            cloned = dict(item)
            cloned["frozen_text"] = item.get("frozen_text") or item["original_text"]
            cloned["current_text"] = item.get("current_text") or item["original_text"]
            frozen.append(cloned)
        return frozen

    def detect_conflicts(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """运营在迁移完成前修改旧文本 → 冻结文本与当前文本不一致即冲突。

        冲突按管道边界聚合：同一管道任一片段所在原文变化，则该管道所有条目标记冲突，
        便于后续“按冲突边界重算”。检测前先从存量管道重读当前文本，
        保证捕获冻结之后运营侧的修改。
        """
        live_text = {p["pipeline_id"]: p["legacy_text"] for p in self.load_legacy_pipelines()}
        for item in items:
            if item["pipeline_id"] in live_text:
                item["current_text"] = live_text[item["pipeline_id"]]
        changed_pipeline_ids = set()
        for item in items:
            frozen_text = item.get("frozen_text") or item["original_text"]
            if item.get("current_text", frozen_text) != frozen_text:
                changed_pipeline_ids.add(item["pipeline_id"])
        result = []
        for item in items:
            cloned = dict(item)
            if cloned["pipeline_id"] in changed_pipeline_ids:
                cloned["conflict"] = 1
                cloned["conflict_reason"] = "冻结后存量自由文本被修改"
            result.append(cloned)
        return result

    def recalculate(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按冲突边界重算：只重建冲突管道的条目，非冲突管道的复核结论保留。"""
        fresh = self.analyze()
        fresh_by_pipeline: dict[int, list[dict[str, Any]]] = {}
        for item in fresh:
            fresh_by_pipeline.setdefault(item["pipeline_id"], []).append(item)
        conflict_pipeline_ids = {i["pipeline_id"] for i in items if i.get("conflict")}
        keep = [dict(i) for i in items if i["pipeline_id"] not in conflict_pipeline_ids]
        rebuilt: list[dict[str, Any]] = []
        for pipeline_id in conflict_pipeline_ids:
            for new_item in fresh_by_pipeline.get(pipeline_id, []):
                # 重算以当前文本为新基线，冲突解除，等待再次复核/确认
                new_item["frozen_text"] = new_item["current_text"] = new_item["original_text"]
                new_item["conflict"] = 0
                new_item["conflict_reason"] = ""
                rebuilt.append(new_item)
        return keep + rebuilt

    # ---- 迁移关系清单与兼容流转拓扑 ------------------------------------
    def build_manifest(self, job_id: str, items: list[dict[str, Any]],
                       schema_version: int) -> dict[str, Any]:
        relations = []
        for item in items:
            relations.append({
                "relation_id": f"REL-{item['pipeline_id']}-{item['ordinal']}",
                "pipeline_id": item["pipeline_id"],
                "pipeline_code": item["pipeline_code"],
                "ordinal": item["ordinal"],
                "original_text": item["original_text"],
                "fragment": item["fragment"],
                "matched_node_id": item.get("matched_node_id"),
                "matched_node_name": item.get("matched_node_name"),
                "match_status": item["match_status"],
                "review_status": item["review_status"],
                "conflict": item.get("conflict", 0),
                "edit_source": item.get("edit_source", EDIT_LEGACY),
            })
        manifest = {
            "job_id": job_id,
            "schema_version": schema_version,
            "relations": relations,
            "summary": {
                "total": len(relations),
                "exact": sum(1 for r in relations if r["match_status"] == MATCH_EXACT),
                "ambiguous": sum(1 for r in relations if r["match_status"] == MATCH_AMBIGUOUS),
                "unmapped": sum(1 for r in relations if r["match_status"] == MATCH_UNMAPPED),
                "conflicts": sum(1 for r in relations if r["conflict"]),
                "pending_review": sum(1 for r in relations if r["review_status"] == REVIEW_PENDING),
            },
        }
        manifest["fingerprint"] = fingerprint(manifest)
        return manifest

    def build_topology(self, job_id: str, items: list[dict[str, Any]],
                       manifest_fingerprint: str, schema_version: int) -> dict[str, Any]:
        """兼容流转拓扑：节点用真实节点标识，边按管道内顺序连接；

        未映射片段生成占位节点（unmapped），保证存量管道拓扑仍可完整读取。
        指纹与清单同源，转换管道服务可校验是否读取同一份产物。
        """
        grouped: dict[int, dict[str, Any]] = {}
        for item in items:
            bucket = grouped.setdefault(item["pipeline_id"], {
                "pipeline_id": item["pipeline_id"], "pipeline_code": item["pipeline_code"],
                "pipeline_name": item["pipeline_name"], "nodes": [], "edges": [],
            })
            if item.get("matched_node_id"):
                node_id, node_name = item["matched_node_id"], item["matched_node_name"]
                node_kind = "node"
            else:
                node_id = f"UNMAPPED-{item['pipeline_id']}-{item['ordinal']}"
                node_name = item["fragment"]
                node_kind = "unmapped"
            if not any(n["id"] == node_id for n in bucket["nodes"]):
                bucket["nodes"].append({"id": node_id, "name": node_name, "kind": node_kind,
                                        "fragment": item["fragment"]})
        for bucket in grouped.values():
            ordered = bucket["nodes"]
            for prev, curr in zip(ordered, ordered[1:]):
                bucket["edges"].append({"from": prev["id"], "to": curr["id"], "label": "依赖"})
        pipelines = [grouped[k] for k in sorted(grouped)]
        topology = {
            "job_id": job_id,
            "schema_version": schema_version,
            "manifest_fingerprint": manifest_fingerprint,
            "read_mode_policy": "shadow_during_migration",
            "pipelines": pipelines,
        }
        topology["fingerprint"] = fingerprint(topology)
        return topology

    # ---- 校验：节点关系必须指向真实目录节点且无环 -----------------------
    def validate_relations(self, items: list[dict[str, Any]]) -> list[str]:
        errors: list[str] = []
        valid_ids = {n["node_id"] for n in self.catalog}
        for item in items:
            if item["match_status"] == MATCH_EXACT and item.get("matched_node_id") not in valid_ids:
                errors.append(f"管道{item['pipeline_code']}片段「{item['fragment']}」节点标识不在目录中")
            if item.get("conflict"):
                errors.append(f"管道{item['pipeline_code']}存在未处理的文本冲突")
        # 拓扑无环校验（同一管道内边按序生成，这里防御性检查）
        for pipeline_id in {i["pipeline_id"] for i in items}:
            seen = [i for i in items if i["pipeline_id"] == pipeline_id and i.get("matched_node_id")]
            ids = [i["matched_node_id"] for i in seen]
            if len(ids) != len(set(ids)):
                errors.append(f"管道{pipeline_id}节点关系出现重复依赖，需人工确认")
        return errors


engine = MigrationEngine()
