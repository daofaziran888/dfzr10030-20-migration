#!/usr/bin/env bash
# 把迁移关系清单与兼容流转拓扑升级到新版本。
# 通过后端接口执行：旧版本快照整体保留，回滚边界清晰；失败不修改当前版本元数据。
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
TARGET_VERSION="${RELATION_SCHEMA_VERSION:-3}"

echo "==> 升级流转节点关系到 v${TARGET_VERSION}"
response="$(curl -fsS -X POST "${BASE_URL}/api/migration/schema/upgrade" \
  -H 'Content-Type: application/json' \
  -d "{\"target_version\": ${TARGET_VERSION}, \"actor\": \"release-bot\"}")"

echo "${response}"
echo "==> 升级完成；旧版本清单/拓扑快照保留，可通过审计日志与快照表回查"
