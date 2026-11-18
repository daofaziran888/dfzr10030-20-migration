# 自由文本依赖 → 结构化流转节点关系 迁移说明

## 1. 背景与目标

存量流转管道（`flow`）把依赖任务保存为自由文本（任务名 + 中英文逗号分隔片段），
新编排器无法判断哪些文本对应真实节点。本次改造在不破坏存量读取的前提下，把自由文本
迁移为**可校验的流转节点关系**，并产出：

- **迁移关系清单（manifest）**：逐条保留「原依赖文本 + 文本片段 + 节点标识/名称」，可逐条回查；
- **兼容流转拓扑（topology）**：节点使用真实节点标识，未映射片段生成占位节点，存量拓扑仍完整可读；
- **待复核清单**：多候选 / 无映射片段进入人工取舍；
- **审计历史**：每个对象的每次状态变更带版本前后值，历史可查。

链路跨两个既有业务模块：

| 模块 | 职责 |
| --- | --- |
| 数据流转编排 `flow` | 提供存量自由文本；执行单编辑入口守卫；回显迁移状态 |
| 采集转换管道 `transform` | 读取同一份清单+拓扑（指纹一致）后回写持久化、选择节点表达式、生成流转管道记录 |
| 依赖迁移 `migration`（新增） | 迁移编排、冻结/冲突/复核、两种切换、回滚、版本升级、SQLite 持久化 |

## 2. 数据与持久化

- 持久化使用标准库 `sqlite3`，不新增第三方依赖；默认路径 `backend/data/migration.db`，
  可用环境变量 `MIGRATION_DB_PATH` 覆盖（容器内为 `/srv/data/migration.db`，由命名卷挂载）。
- 表：`migration_jobs`、`migration_items`、`relation_snapshots`、`pipeline_records`、
  `audit_logs`、`schema_meta`。
- 所有写操作在进程内互斥锁内完成，并以 `version` 乐观锁保护：
  **同一对象同时操作仅一次成功，余下返回 409**（`ConcurrentUpdateError` / `ObjectLockedError`）。
- 清单与拓扑以快照整体落盘，带内容指纹（SHA-256 截断）。拓扑必须引用当前清单指纹，
  转换管道服务才允许生成流转管道记录，否则拒绝（读取同一份依据）。

## 3. 迁移生命周期与两种成立边界

### 3.1 整批影子切换 `batch_shadow`

```
新建作业 → 解析文本 → 冻结旧文本（关闭旧文本编辑入口）→ 影子读取 →
冲突检测/按冲突重算 → 待复核清空 → 回写清单/拓扑 → 统一切换 → （需要时）回滚
```

冻结后所有管道旧文本入口关闭，只做影子读取，整批统一切换到结构化关系。

### 3.2 按管道粒度切换 `per_pipeline`

按管道逐个切换，**已切换管道立即停用旧文本入口，未切换管道仍以旧文本为唯一可编辑来源**，
因此迁移期间系统内会混合存在两种读取方式，但任一管道始终只有一个可编辑依赖来源。

两种边界都满足：**任一管道在迁移后只能有一个可编辑依赖来源**，旧文本与新关系不会互相覆盖。

### 3.3 冲突与重算

- 冻结后运营侧若修改了旧文本，冲突检测会比对冻结基线与存量当前文本，按**管道边界**标记冲突；
- 带冲突不允许发布产物；
- 「按冲突重算」只重建冲突管道的条目，非冲突管道的复核结论保留，处理范围随冲突边界改变。

### 3.4 回滚

回滚把读取方式与编辑入口恢复到冻结基线（`legacy`），作业置为 `rolled_back`；
清单/拓扑快照与审计日志全部保留，回滚边界清晰、历史可查。

## 4. 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/migration/jobs` | 新建迁移作业（带切换边界） |
| POST | `/api/migration/jobs/{id}/analyze` | 解析自由文本为迁移条目 |
| POST | `/api/migration/jobs/{id}/freeze` | 冻结旧文本 |
| POST | `/api/migration/jobs/{id}/conflicts` | 冲突检测 |
| POST | `/api/migration/jobs/{id}/recalculate` | 按冲突边界重算 |
| GET | `/api/migration/jobs/{id}/pending` | 待复核清单 |
| POST | `/api/migration/items/{item_id}/resolve` | 人工取舍（选节点或确认无映射） |
| POST | `/api/transform/migration/{id}/writeback` | 转换管道回写清单/拓扑 |
| GET | `/api/transform/migration/{id}/artifacts` | 读拓扑并按清单选节点表达式 |
| POST | `/api/transform/migration/{id}/flow-records` | 同指纹生成流转管道记录 |
| POST | `/api/migration/jobs/{id}/cutover` | 切换（整批或单管道） |
| POST | `/api/migration/jobs/{id}/rollback` | 回滚到冻结基线 |
| GET | `/api/migration/jobs/{id}/export` | 导出双依据 CSV（原文本+新关系） |
| GET | `/api/migration/jobs/{id}/audit-logs` | 审计历史 |
| GET | `/api/migration/pipelines/{pipeline_id}/edit-state` | 单编辑入口判定 |
| PUT | `/api/flow/{id}/legacy-text` | 旧文本唯一写入口（守卫 409） |
| POST | `/api/migration/schema/upgrade` | 关系版本升级 |

所有变更类接口都要求携带 `expected_version`，版本过期返回 `409`。

## 5. 版本升级与回滚

- 当前关系版本由 `RELATION_SCHEMA_VERSION`（默认 2）与 `schema_meta` 表共同维护。
- `POST /api/migration/schema/upgrade` 为每个作业按新版本重新生成清单与拓扑快照，
  **旧版本快照原样保留**，因此可随时回查、按旧版本回滚；不能降级或重复升级到相同版本。
- 流水线 `scripts/upgrade_relation_schema.sh` 在部署成功后推进版本；
  `scripts/deploy.sh` 健康检查失败会调用 `scripts/rollback.sh` 回退上一镜像，数据卷不清理。

## 6. 本地开发与验证

```bash
# 后端测试（标准库 unittest）
make backend-test

# 前端类型检查 + 构建
make frontend-build

# 本地起服务
cd backend && ./run.sh
cd frontend && npm run dev
```

前端「依赖迁移」页（导航：依赖迁移）可查看迁移状态、原依赖文本与新节点关系、
待复核取舍、兼容拓扑、审计历史，并导出保留两份依据的 CSV；
「数据流转编排」页节点详情同步展示原依赖文本与迁移状态/可编辑来源。
