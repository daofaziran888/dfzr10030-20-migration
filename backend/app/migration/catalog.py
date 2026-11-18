"""新编排器已注册的流转节点目录。

存量自由文本里的任务名/逗号片段最终都要落到这里的真实节点标识上；
目录同时给出别名，供迁移引擎做归一化匹配。新关系保留节点 id 与名称，
保证“新关系可以逐条回查”。
"""
from __future__ import annotations

from typing import Any

NODE_CATALOG: list[dict[str, Any]] = [
    {
        "node_id": "NODE-INGEST-01",
        "node_name": "采集入口",
        "aliases": ["入口节点", "采集节点"],
        "expression": "ingest(source)",
    },
    {
        "node_id": "NODE-CLEAN-01",
        "node_name": "清洗节点A",
        "aliases": ["清洗A", "清洗节点一"],
        "expression": "clean(rule_a)",
    },
    {
        "node_id": "NODE-CLEAN-02",
        "node_name": "清洗节点B",
        "aliases": ["清洗B", "清洗节点二"],
        "expression": "clean(rule_b)",
    },
    {
        "node_id": "NODE-VALID-01",
        "node_name": "数据校验",
        "aliases": ["校验-01", "校验节点", "数据校验节点"],
        "expression": "validate(schema_v2)",
    },
    {
        "node_id": "NODE-DEDUP-01",
        "node_name": "去重节点",
        "aliases": ["去重", "数据去重"],
        "expression": "dedup(biz_key)",
    },
    {
        "node_id": "NODE-LOAD-01",
        "node_name": "入库节点",
        "aliases": ["装载节点", "数据入库"],
        "expression": "load(warehouse)",
    },
    {
        "node_id": "NODE-TRANSFORM-01",
        "node_name": "转换主节点",
        "aliases": ["主转换", "转换节点"],
        "expression": "transform(main_pipeline)",
    },
]


def normalize(text: str) -> str:
    """归一化片段：去空白与常见标点差异，便于别名/包含匹配。"""
    return "".join(str(text).lower().split()).replace("_", "").replace("-", "")


def list_nodes() -> list[dict[str, Any]]:
    return [dict(node) for node in NODE_CATALOG]
