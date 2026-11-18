.PHONY: install backend frontend test backend-test frontend-build

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm ci

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# 后端迁移链路单元测试（标准库 unittest，无需额外依赖）
backend-test:
	cd backend && .venv/bin/python -m unittest discover -s tests -v

# 前端类型检查与生产构建
frontend-build:
	cd frontend && npm run typecheck && npm run build

# 本地全量校验
test: backend-test frontend-build
