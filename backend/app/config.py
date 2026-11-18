"""运行配置：端口、跨域、运行环境与迁移模块持久化设置。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "多源数据采集与RPA自动化平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 迁移清单与兼容流转拓扑使用 SQLite 持久化；:memory: 仅供单元测试。
    migration_db_path: str = field(
        default_factory=lambda: os.environ.get(
            "MIGRATION_DB_PATH",
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "migration.db"),
        )
    )
    # 结构化关系当前版本；升级流水线以此为目标版本做清单/拓扑版本迁移。
    relation_schema_version: int = field(
        default_factory=lambda: int(os.environ.get("RELATION_SCHEMA_VERSION", "2"))
    )


settings = Settings()
