"""数据流转编排 DAG 图服务：生成节点、边示例结构与流转规则。

迁移期约束（对应需求）：
- 任一管道在迁移后只能有一个可编辑依赖来源；旧文本被冻结或已切换到结构化关系后，
  对自由文本的写入一律拒绝（409），避免旧文本与新关系互相覆盖。
- 存量拓扑始终可读：get_graph 同时回显迁移状态、读取方式与编辑来源。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "flow"
REQUIRED_FIELDS = ["管道编号", "管道名称", "起始节点"]
LEGACY_TEXT_FIELD = "依赖文本"
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用管道": "已启用", "停用管道": "已停用"}
NEGATIVE_ACTIONS = ["停用管道"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _migration_service():
    # 延迟导入，避免 flow 与 migration 在模块加载期形成循环依赖
    from app.migration.service import migration_service
    return migration_service


class LegacyEditDenied(Exception):
    """迁移期间旧文本编辑入口已关闭。"""


class FlowService:
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            node = {"id": int(row.get("id", i + 1)),
                    "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                    "status": str(row.get("status", STATUS_ORDER[0])),
                    **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")}}
            try:
                node["migration"] = _migration_service().pipeline_edit_state(int(row["id"]))
            except Exception:  # 迁移库不可用时不阻断存量拓扑读取
                node["migration"] = None
            nodes.append(node)
        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def _guard_legacy_edit(self, entry_id: int) -> None:
        """单编辑入口守卫：冻结后/切换后拒绝旧文本写入。

        - 已切换到结构化关系的管道：旧文本入口一律停用；
        - 整批影子策略：冻结后所有管道旧文本入口关闭，只做影子读取；
        - 按管道粒度策略：仅已切换管道停用，未切换管道仍以旧文本为唯一可编辑来源。
        """
        state = _migration_service().pipeline_edit_state(entry_id)
        if not state.get("job_id"):
            return
        if state.get("edit_source") == "relation":
            raise LegacyEditDenied(
                f"管道 {entry_id} 已切换到结构化节点关系，旧文本入口已停用"
            )
        if state.get("frozen") and state.get("cutover_strategy") == "batch_shadow":
            raise LegacyEditDenied(
                f"管道 {entry_id} 处于整批影子迁移（{state.get('job_status')}），"
                "旧文本已冻结，编辑入口已关闭"
            )

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        if LEGACY_TEXT_FIELD in values:
            entry[LEGACY_TEXT_FIELD] = values.get(LEGACY_TEXT_FIELD)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def update_legacy_text(self, entry_id: int, text: str) -> dict[str, Any]:
        """运营修改自由文本依赖的唯一写入口，受迁移守卫保护。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise KeyError(entry_id)
        self._guard_legacy_edit(entry_id)
        entry[LEGACY_TEXT_FIELD] = text
        return entry

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流转管道 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据流转编排可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流转管道已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
