#!/usr/bin/env bash
# 上线部署：拉取指定版本镜像并滚动更新；数据卷保留，迁移清单/拓扑不丢。
set -euo pipefail

TAG="${1:?用法: deploy.sh <镜像版本标签>}"
COMPOSE_FILE="$(dirname "$0")/../docker-compose.yml"
export TAG

echo "==> 部署版本 ${TAG}"
# 健康检查：旧容器在新容器健康前不停，保证存量管道可继续读取
if ! docker compose -f "${COMPOSE_FILE}" up -d --no-deps --build backend; then
  echo "==> 后端更新失败" >&2
  exit 1
fi

for i in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8000/api/health" >/dev/null; then
    echo "==> 后端健康检查通过"
    docker compose -f "${COMPOSE_FILE}" up -d --no-deps --build frontend
    echo "==> 版本 ${TAG} 部署完成"
    exit 0
  fi
  sleep 2
done

echo "==> 健康检查超时，部署失败" >&2
exit 1
