FROM python:3.12-slim
WORKDIR /srv/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# 迁移清单与兼容流转拓扑的持久化目录（compose 用命名卷挂载覆盖）
RUN mkdir -p /srv/data
ENV MIGRATION_DB_PATH=/srv/data/migration.db
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
