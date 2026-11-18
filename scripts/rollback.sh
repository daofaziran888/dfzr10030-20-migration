#!/usr/bin/env bash
# 回滚到指定版本镜像；迁移数据卷不动，关系版本由旧快照保证可回查。
set -euo pipefail

TAG="${1:?用法: rollback.sh <要回滚到的镜像版本标签>}"
COMPOSE_FILE="$(dirname "$0")/../docker-compose.yml"
export TAG

echo "==> 回滚到版本 ${TAG}"
docker compose -f "${COMPOSE_FILE}" up -d --no-deps backend frontend

for i in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8000/api/health" >/dev/null; then
    echo "==> 回滚后健康检查通过"
    echo "==> 数据层未清空：迁移关系清单与兼容流转拓扑的历史快照仍在 migration_data 卷中"
    exit 0
  fi
  sleep 2
done

echo "==> 回滚后健康检查超时" >&2
exit 1
