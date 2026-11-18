"""迁移链路持久化层：迁移关系清单、兼容流转拓扑与审计历史的 SQLite 仓库。

设计要点：
- 只依赖标准库 sqlite3，不新增第三方依赖，契合仓库“克隆即可运行”的约定。
- 所有写操作在同一把进程内互斥锁内完成，并通过 version 乐观锁保证
  “同一对象同时操作仅一次成功，余下拒绝”。
- 清单与拓扑以快照行整体落盘，转换管道服务回写后可逐条回查、可重放。
- 每张表都带版本号与时间戳，配合 audit_log 实现“历史记录可查、回滚边界清晰”。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from app.config import settings

SCHEMA_VERSION = 2


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_id() -> str:
    return uuid.uuid4().hex


class ConcurrentUpdateError(Exception):
    """乐观锁版本不匹配：同一对象已有一次成功写入，本次操作必须拒绝。"""


class ObjectLockedError(Exception):
    """对象处于不可写状态（已冻结/已切换），写入入口已关闭。"""


_DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_jobs (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    cutover_strategy TEXT NOT NULL,
    status TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL DEFAULT '',
    schema_version INTEGER NOT NULL DEFAULT 1,
    frozen INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_items (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    pipeline_id INTEGER NOT NULL,
    pipeline_code TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    original_text TEXT NOT NULL,
    fragment TEXT NOT NULL,
    matched_node_id TEXT,
    matched_node_name TEXT,
    match_status TEXT NOT NULL,
    review_status TEXT NOT NULL,
    conflict INTEGER NOT NULL DEFAULT 0,
    conflict_reason TEXT NOT NULL DEFAULT '',
    frozen_text TEXT NOT NULL DEFAULT '',
    current_text TEXT NOT NULL DEFAULT '',
    read_mode TEXT NOT NULL DEFAULT 'legacy',
    edit_source TEXT NOT NULL DEFAULT 'legacy',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relation_snapshots (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    kind TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_records (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    manifest_snapshot_id TEXT NOT NULL,
    topology_snapshot_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    producer TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    job_id TEXT,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER,
    created_at TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_items_job ON migration_items(job_id);
CREATE INDEX IF NOT EXISTS idx_snap_job ON relation_snapshots(job_id);
CREATE INDEX IF NOT EXISTS idx_audit_object ON audit_logs(object_type, object_id);
"""


class MigrationRepository:
    """线程安全的 SQLite 仓库；单进程 FastAPI 下共享一个实例。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_DDL)
        self._init_meta()

    def _init_meta(self) -> None:
        now = utc_now()
        row = self._conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO schema_meta(key, value, updated_at) VALUES('schema_version', ?, ?)",
                (str(SCHEMA_VERSION), now),
            )
        self._conn.execute(
            "INSERT OR IGNORE INTO schema_meta(key, value, updated_at) VALUES('relation_schema_version', ?, ?)",
            (str(settings.relation_schema_version), now),
        )

    # ---- 通用工具 -------------------------------------------------------
    @staticmethod
    def _dump(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _audit(
        self,
        object_type: str,
        object_id: str,
        action: str,
        actor: str,
        from_version: int | None,
        to_version: int | None,
        detail: dict[str, Any],
        job_id: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO audit_logs(id, job_id, object_type, object_id, action, actor, "
            "from_version, to_version, created_at, detail) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                new_id(), job_id, object_type, object_id, action, actor,
                from_version, to_version, utc_now(), self._dump(detail),
            ),
        )

    # ---- schema 元数据 --------------------------------------------------
    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM schema_meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO schema_meta(key, value, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, value, utc_now()),
            )

    # ---- 迁移作业 -------------------------------------------------------
    def create_job(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        with self._lock:
            job_id = new_id()
            now = utc_now()
            row = {
                "id": job_id,
                "version": 1,
                "name": payload["name"],
                "cutover_strategy": payload["cutover_strategy"],
                "status": "created",
                "source_fingerprint": "",
                "schema_version": int(payload.get("schema_version", 1)),
                "frozen": 0,
                "created_at": now,
                "updated_at": now,
            }
            full = {**row, **payload}
            self._conn.execute(
                "INSERT INTO migration_jobs(id, version, name, cutover_strategy, status, "
                "source_fingerprint, schema_version, frozen, created_at, updated_at, payload) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (row["id"], row["version"], row["name"], row["cutover_strategy"], row["status"],
                 row["source_fingerprint"], row["schema_version"], row["frozen"], now, now,
                 self._dump(full)),
            )
            self._audit("migration_job", job_id, "create", actor, None, 1,
                        {"cutover_strategy": row["cutover_strategy"]}, job_id)
            return self.get_job(job_id)  # type: ignore[return-value]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT payload FROM migration_jobs WHERE id=?", (job_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    def list_jobs(self) -> list[dict[str, Any]]:
        rows = self._conn.execute("SELECT payload FROM migration_jobs ORDER BY created_at").fetchall()
        return [json.loads(r["payload"]) for r in rows]

    def update_job(self, job_id: str, expected_version: int, mutations: dict[str, Any],
                   action: str, actor: str, detail: dict[str, Any] | None = None) -> dict[str, Any]:
        """乐观锁更新：版本不符抛 ConcurrentUpdateError，冻结后禁写抛 ObjectLockedError。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT version, frozen, payload FROM migration_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"迁移作业 {job_id} 不存在")
            if row["version"] != expected_version:
                raise ConcurrentUpdateError(
                    f"作业版本 {expected_version} 已过期，当前版本 {row['version']}，本次写入拒绝"
                )
            payload = json.loads(row["payload"])
            if mutations.get("_guard_frozen") and row["frozen"] and action not in (
                "freeze", "detect_conflict", "recalculate", "cutover", "rollback",
            ):
                raise ObjectLockedError("存量自由文本已冻结，迁移期间旧文本编辑入口已关闭")
            next_version = row["version"] + 1
            now = utc_now()
            payload.update({k: v for k, v in mutations.items() if not k.startswith("_")})
            payload["version"] = next_version
            payload["updated_at"] = now
            self._conn.execute(
                "UPDATE migration_jobs SET version=?, status=COALESCE(?, status), "
                "frozen=COALESCE(?, frozen), source_fingerprint=COALESCE(?, source_fingerprint), "
                "schema_version=COALESCE(?, schema_version), updated_at=?, payload=? WHERE id=?",
                (next_version, mutations.get("status"), mutations.get("frozen"),
                 mutations.get("source_fingerprint"), mutations.get("schema_version"),
                 now, self._dump(payload), job_id),
            )
            self._audit("migration_job", job_id, action, actor, row["version"], next_version,
                        detail or mutations, job_id)
            return payload

    # ---- 迁移条目（逐片段，保留原文本与节点标识，可逐条回查） ------------
    def bulk_replace_items(self, job_id: str, items: list[dict[str, Any]], actor: str,
                           action: str = "analyze") -> None:
        with self._lock:
            now = utc_now()
            self._conn.execute("DELETE FROM migration_items WHERE job_id=?", (job_id,))
            for item in items:
                item = {**item, "id": item.get("id") or new_id(), "version": 1,
                       "created_at": now, "updated_at": now}
                self._conn.execute(
                    "INSERT INTO migration_items(id, job_id, version, pipeline_id, pipeline_code, "
                    "pipeline_name, ordinal, original_text, fragment, matched_node_id, "
                    "matched_node_name, match_status, review_status, conflict, conflict_reason, "
                    "frozen_text, current_text, read_mode, edit_source, created_at, updated_at, payload) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (item["id"], job_id, 1, item["pipeline_id"], item["pipeline_code"],
                     item["pipeline_name"], item["ordinal"], item["original_text"], item["fragment"],
                     item.get("matched_node_id"), item.get("matched_node_name"),
                     item["match_status"], item["review_status"], int(item.get("conflict", 0)),
                     item.get("conflict_reason", ""), item.get("frozen_text", ""),
                     item.get("current_text", ""), item.get("read_mode", "legacy"),
                     item.get("edit_source", "legacy"), now, now, self._dump(item)),
                )
            self._audit("migration_job", job_id, action, actor, None, None,
                        {"items": len(items)}, job_id)

    def list_items(self, job_id: str, review_status: str | None = None) -> list[dict[str, Any]]:
        if review_status:
            rows = self._conn.execute(
                "SELECT payload FROM migration_items WHERE job_id=? AND review_status=? "
                "ORDER BY pipeline_id, ordinal", (job_id, review_status)).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT payload FROM migration_items WHERE job_id=? ORDER BY pipeline_id, ordinal",
                (job_id,)).fetchall()
        return [json.loads(r["payload"]) for r in rows]

    def get_item(self, item_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT payload FROM migration_items WHERE id=?", (item_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    def update_item(self, item_id: str, expected_version: int, mutations: dict[str, Any],
                    action: str, actor: str, detail: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT version, job_id, edit_source, payload FROM migration_items WHERE id=?",
                (item_id,)).fetchone()
            if row is None:
                raise KeyError(f"迁移条目 {item_id} 不存在")
            if row["version"] != expected_version:
                raise ConcurrentUpdateError(
                    f"条目版本 {expected_version} 已过期，当前版本 {row['version']}，本次写入拒绝"
                )
            if mutations.get("_guard_edit_source") and row["edit_source"] == "relation":
                raise ObjectLockedError("该管道已切换到结构化关系，旧文本编辑入口已停用")
            payload = json.loads(row["payload"])
            next_version = row["version"] + 1
            now = utc_now()
            payload.update({k: v for k, v in mutations.items() if not k.startswith("_")})
            payload["version"] = next_version
            payload["updated_at"] = now
            self._conn.execute(
                "UPDATE migration_items SET version=?, matched_node_id=?, matched_node_name=?, "
                "match_status=?, review_status=?, conflict=?, conflict_reason=?, frozen_text=?, "
                "current_text=?, read_mode=?, edit_source=?, updated_at=?, payload=? WHERE id=?",
                (next_version, payload.get("matched_node_id"), payload.get("matched_node_name"),
                 payload["match_status"], payload["review_status"], int(payload.get("conflict", 0)),
                 payload.get("conflict_reason", ""), payload.get("frozen_text", ""),
                 payload.get("current_text", ""), payload.get("read_mode", "legacy"),
                 payload.get("edit_source", "legacy"), now, self._dump(payload), item_id),
            )
            self._audit("migration_item", item_id, action, actor, row["version"], next_version,
                        detail or mutations, row["job_id"])
            return payload

    # ---- 清单/拓扑快照 --------------------------------------------------
    def save_snapshot(self, job_id: str, kind: str, schema_version: int, fingerprint: str,
                      payload: dict[str, Any], actor: str) -> dict[str, Any]:
        with self._lock:
            snapshot_id = new_id()
            now = utc_now()
            record = {"id": snapshot_id, "job_id": job_id, "kind": kind,
                      "schema_version": schema_version, "fingerprint": fingerprint,
                      "created_at": now, "payload": payload}
            self._conn.execute(
                "INSERT INTO relation_snapshots(id, job_id, version, kind, schema_version, "
                "fingerprint, created_at, payload) VALUES(?,?,1,?,?,?,?,?)",
                (snapshot_id, job_id, kind, schema_version, fingerprint, now, self._dump(record)),
            )
            self._audit("relation_snapshot", snapshot_id, f"persist_{kind}", actor, None, 1,
                        {"fingerprint": fingerprint, "schema_version": schema_version}, job_id)
            return record

    def latest_snapshot(self, job_id: str, kind: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT payload FROM relation_snapshots WHERE job_id=? AND kind=? "
            "ORDER BY created_at DESC, id DESC LIMIT 1", (job_id, kind)).fetchone()
        return json.loads(row["payload"]) if row else None

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT payload FROM relation_snapshots WHERE id=?", (snapshot_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    def list_snapshots(self, job_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT payload FROM relation_snapshots WHERE job_id=? ORDER BY created_at", (job_id,)
        ).fetchall()
        return [json.loads(r["payload"]) for r in rows]

    # ---- 转换管道记录（读取同一份清单+拓扑后才允许生成） ----------------
    def save_pipeline_record(self, job_id: str, manifest_snapshot_id: str,
                             topology_snapshot_id: str, fingerprint: str, producer: str,
                             payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            record_id = new_id()
            now = utc_now()
            record = {"id": record_id, "job_id": job_id,
                      "manifest_snapshot_id": manifest_snapshot_id,
                      "topology_snapshot_id": topology_snapshot_id,
                      "fingerprint": fingerprint, "producer": producer,
                      "created_at": now, "payload": payload}
            self._conn.execute(
                "INSERT INTO pipeline_records(id, job_id, manifest_snapshot_id, "
                "topology_snapshot_id, fingerprint, producer, created_at, payload) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (record_id, job_id, manifest_snapshot_id, topology_snapshot_id, fingerprint,
                 producer, now, self._dump(record)),
            )
            self._audit("pipeline_record", record_id, "generate", producer, None, 1,
                        {"fingerprint": fingerprint}, job_id)
            return record

    def list_pipeline_records(self, job_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT payload FROM pipeline_records WHERE job_id=? ORDER BY created_at", (job_id,)
        ).fetchall()
        return [json.loads(r["payload"]) for r in rows]

    # ---- 审计历史 -------------------------------------------------------
    def list_audit_logs(self, job_id: str | None = None,
                        object_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM audit_logs WHERE 1=1"
        params: list[Any] = []
        if job_id:
            sql += " AND job_id=?"
            params.append(job_id)
        if object_id:
            sql += " AND object_id=?"
            params.append(object_id)
        sql += " ORDER BY created_at, id"
        rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.ProgrammingError:
                pass


_REPO_OVERRIDE: ContextVar[MigrationRepository | None] = ContextVar("migration_repo", default=None)
_SINGLETON: MigrationRepository | None = None
_SINGLETON_LOCK = threading.Lock()


def get_repository() -> MigrationRepository:
    """返回进程内共享仓库；测试可用 use_repository 注入临时内存库。"""
    global _SINGLETON
    override = _REPO_OVERRIDE.get()
    if override is not None:
        return override
    with _SINGLETON_LOCK:
        if _SINGLETON is None:
            _SINGLETON = MigrationRepository(settings.migration_db_path)
        return _SINGLETON


class repository_session:
    """测试辅助：临时把共享仓库切换为内存库，退出后还原。"""

    def __init__(self, repo: MigrationRepository) -> None:
        self.repo = repo
        self.token: Any = None

    def __enter__(self) -> MigrationRepository:
        self.token = _REPO_OVERRIDE.set(self.repo)
        return self.repo

    def __exit__(self, *exc: Any) -> None:
        _REPO_OVERRIDE.reset(self.token)
