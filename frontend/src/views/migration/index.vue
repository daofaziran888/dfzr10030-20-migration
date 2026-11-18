<template>
  <section class="page" data-module="migration">
    <header class="page-head">
      <div>
        <h2>依赖迁移工作台</h2>
        <p class="page-desc">
          把存量流转管道的自由文本依赖迁移为可校验的流转节点关系：冻结旧文本、影子读取、
          冲突按边界重算、复核取舍后统一切换，保留原文本与节点标识两份依据并可逐条回查。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">新建迁移作业</button>
        <button class="btn" data-testid="export" type="button" :disabled="!currentJob" @click="exportEvidence">导出双依据清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in statCards" :key="card.label" class="stat-card" :data-testid="`kpi-${card.key}`">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <div class="migration-layout">
      <!-- 左：作业列表与阶段操作 -->
      <div class="migration-panel">
        <h3>迁移作业</h3>
        <ul class="job-list" data-testid="job-list">
          <li
            v-for="job in jobs"
            :key="job.id"
            :class="['job-item', { active: currentJobId === job.id }]"
            :data-testid="`job-${job.id}`"
            @click="selectJob(job.id)"
          >
            <div class="job-name">{{ job.name }}</div>
            <div class="job-meta">
              <span class="badge" :data-status="job.status">{{ statusLabel(job.status) }}</span>
              <span>{{ strategyLabel(job.cutover_strategy) }}</span>
            </div>
            <div class="job-meta">v{{ job.version }} · {{ job.pipeline_count }} 条管道</div>
          </li>
          <li v-if="!jobs.length" class="empty-state">暂无迁移作业</li>
        </ul>

        <div v-if="currentJob" class="stage-actions" data-testid="stage-actions">
          <button class="btn" data-testid="stage-analyze" :disabled="!canAnalyze" @click="runStage('analyze')">解析自由文本</button>
          <button class="btn" data-testid="stage-freeze" :disabled="!canFreeze" @click="runStage('freeze')">冻结旧文本</button>
          <button class="btn" data-testid="stage-conflict" :disabled="!canDetect" @click="runStage('conflicts')">冲突检测</button>
          <button class="btn" data-testid="stage-recalc" :disabled="!canRecalc" @click="runStage('recalculate')">按冲突重算</button>
          <button class="btn primary" data-testid="stage-publish" :disabled="!canPublish" @click="publishArtifacts">转换管道回写清单/拓扑</button>
          <button class="btn primary" data-testid="stage-cutover" :disabled="!canCutover" @click="cutover">切换到节点关系</button>
          <button class="btn ghost" data-testid="stage-rollback" :disabled="!canRollback" @click="rollback">回滚到冻结基线</button>
          <button class="btn ghost" data-testid="stage-record" :disabled="!canRecord" @click="generateRecord">生成流转管道记录</button>
        </div>
      </div>

      <!-- 右：明细 / 待复核 / 拓扑 / 审计 -->
      <div class="migration-main">
        <div v-if="!currentJob" class="empty-state big">请选择或新建一个迁移作业</div>
        <template v-else>
          <div class="detail-head">
            <h3>{{ currentJob.name }}</h3>
            <div class="job-meta">
              <span class="badge" :data-status="currentJob.status">{{ statusLabel(currentJob.status) }}</span>
              <span>关系版本 v{{ currentJob.schema_version }}</span>
              <span>读取/编辑：{{ readModeSummary }}</span>
            </div>
          </div>

          <div class="tabs">
            <button
              v-for="tab in tabs"
              :key="tab.key"
              :class="['tab', { active: activeTab === tab.key }]"
              :data-testid="`tab-${tab.key}`"
              @click="activeTab = tab.key"
            >
              {{ tab.label }}<i v-if="tab.count">（{{ tab.count }}）</i>
            </button>
          </div>

          <!-- 迁移关系清单：原文本 + 新节点关系 -->
          <div v-show="activeTab === 'items'" class="tab-panel" data-testid="panel-items">
            <table class="data-table">
              <thead>
                <tr>
                  <th>管道</th><th>序号</th><th>原依赖文本</th><th>文本片段</th>
                  <th>匹配</th><th>节点标识</th><th>节点名称</th><th>复核</th><th>冲突</th><th>读取/编辑</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="item in items" :key="item.id" :data-testid="`item-${item.id}`" :data-conflict="item.conflict ? '1' : '0'">
                  <td>{{ item.pipeline_code }}</td>
                  <td>{{ item.ordinal }}</td>
                  <td class="cell-text">{{ item.original_text }}</td>
                  <td>{{ item.fragment }}</td>
                  <td><span class="badge" :data-status="item.match_status">{{ matchLabel(item.match_status) }}</span></td>
                  <td class="mono">{{ item.matched_node_id || '—' }}</td>
                  <td>{{ item.matched_node_name || '—' }}</td>
                  <td><span class="badge" :data-status="item.review_status">{{ reviewLabel(item.review_status) }}</span></td>
                  <td>{{ item.conflict ? '是' : '否' }}</td>
                  <td>{{ modeLabel(item.read_mode) }} / {{ item.edit_source === 'relation' ? '节点关系' : '旧文本' }}</td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- 待复核清单：人工取舍 -->
          <div v-show="activeTab === 'review'" class="tab-panel" data-testid="panel-review">
            <p class="hint">无法自动判定的片段在此取舍：选择真实节点，或确认无映射（拓扑保留占位节点并保留原文本）。</p>
            <div v-for="item in pendingItems" :key="item.id" class="review-card" :data-testid="`review-${item.id}`">
              <div class="review-text">
                <strong>{{ item.pipeline_code }} #{{ item.ordinal }}</strong>
                <span>原文本：{{ item.original_text }}</span>
                <span>片段：「{{ item.fragment }}」（{{ matchLabel(item.match_status) }}）</span>
              </div>
              <div class="review-actions">
                <select v-model="resolution[item.id]" class="node-select" :data-testid="`select-${item.id}`">
                  <option value="">确认无映射（保留原文本）</option>
                  <option v-for="node in nodeCatalog" :key="node.node_id" :value="node.node_id">
                    {{ node.node_id }} · {{ node.node_name }}
                  </option>
                </select>
                <button class="btn primary" data-testid="resolve-btn" @click="resolveItem(item)">提交取舍</button>
              </div>
            </div>
            <div v-if="!pendingItems.length" class="empty-state">待复核清单已清空</div>
          </div>

          <!-- 兼容流转拓扑 -->
          <div v-show="activeTab === 'topology'" class="tab-panel" data-testid="panel-topology">
            <div v-if="topology" class="topo-summary">
              <span class="mono">清单指纹：{{ topology.manifest_fingerprint }}</span>
              <span class="mono">拓扑指纹：{{ topology.fingerprint }}</span>
              <span>关系版本 v{{ topology.schema_version }}</span>
            </div>
            <div v-for="pipe in topologyPipelines" :key="pipe.pipeline_id" class="topo-pipe">
              <h4>{{ pipe.pipeline_code }} · {{ pipe.pipeline_name }}</h4>
              <div class="topo-chain">
                <template v-for="(node, idx) in pipe.nodes" :key="node.id">
                  <span class="topo-node" :class="{ unmapped: node.kind === 'unmapped' }" :title="node.fragment">
                    {{ node.name }}<small v-if="node.kind === 'unmapped'">（占位）</small>
                  </span>
                  <span v-if="idx < pipe.nodes.length - 1" class="topo-arrow">→</span>
                </template>
              </div>
            </div>
            <div v-if="!topology" class="empty-state">尚未持久化兼容流转拓扑，请先回写清单/拓扑</div>
          </div>

          <!-- 审计历史 -->
          <div v-show="activeTab === 'audit'" class="tab-panel" data-testid="panel-audit">
            <table class="data-table">
              <thead><tr><th>时间</th><th>对象</th><th>动作</th><th>版本</th><th>操作人</th></tr></thead>
              <tbody>
                <tr v-for="log in auditLogs" :key="log.id" :data-testid="`audit-${log.id}`">
                  <td class="mono">{{ log.created_at }}</td>
                  <td>{{ log.object_type }}#{{ String(log.object_id).slice(0, 8) }}</td>
                  <td>{{ log.action }}</td>
                  <td>{{ log.from_version ?? '—' }} → {{ log.to_version ?? '—' }}</td>
                  <td>{{ log.actor }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>
      </div>
    </div>

    <!-- 新建作业 -->
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>新建迁移作业</h3>
        <label class="create-field"><span>作业名称</span>
          <input v-model="createForm.name" data-testid="create-name" required placeholder="例如：11月依赖迁移" />
        </label>
        <label class="create-field"><span>切换边界</span>
          <select v-model="createForm.cutover_strategy" data-testid="create-strategy">
            <option value="batch_shadow">先冻结旧文本→整批影子读取→统一切换</option>
            <option value="per_pipeline">按管道逐个切换并停用对应旧文本入口</option>
          </select>
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit">创建</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span v-if="message" :class="['foot-msg', messageKind]" data-testid="foot-message">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'

const API = '/api/migration'
const TRANSFORM_API = '/api/transform/migration'

type Job = Record<string, any>
type Item = Record<string, any>
type Snapshot = Record<string, any>

const jobs = ref<Job[]>([])
const currentJobId = ref('')
const detail = reactive<{ job: Job | null; items: Item[]; manifest: Snapshot | null; topology: Snapshot | null }>({
  job: null, items: [], manifest: null, topology: null,
})
const auditLogs = ref<Record<string, any>[]>([])
const showCreate = ref(false)
const createForm = reactive({ name: '', cutover_strategy: 'batch_shadow' })
const activeTab = ref('items')
const resolution = reactive<Record<string, string>>({})
const message = ref('')
const messageKind = ref('ok')
const nodeCatalog = ref<Record<string, any>[]>([])

const currentJob = computed(() => detail.job)
const items = computed(() => detail.items)
const topology = computed(() => detail.topology?.payload ?? null)
const topologyPipelines = computed(() => topology.value?.pipelines ?? [])
const pendingItems = computed(() => items.value.filter((i) => i.review_status === 'pending'))

const statCards = computed(() => {
  const list = items.value
  return [
    { key: 'total', label: '迁移片段', value: list.length },
    { key: 'exact', label: '精确匹配', value: list.filter((i) => i.match_status === 'exact').length },
    { key: 'review', label: '待复核', value: pendingItems.value.length },
    { key: 'conflict', label: '冲突片段', value: list.filter((i) => i.conflict).length },
  ]
})

const tabs = computed(() => [
  { key: 'items', label: '迁移关系清单', count: 0 },
  { key: 'review', label: '待复核', count: pendingItems.value.length },
  { key: 'topology', label: '兼容流转拓扑', count: 0 },
  { key: 'audit', label: '审计历史', count: 0 },
])

const readModeSummary = computed(() => {
  const modes = new Set(items.value.map((i) => i.read_mode))
  const sources = new Set(items.value.map((i) => i.edit_source))
  return `${[...modes].map(modeLabel).join('/') || '—'} · ${[...sources].map((s) => (s === 'relation' ? '节点关系' : '旧文本')).join('/')}`
})

// 阶段可用性
const canAnalyze = computed(() => ['created', 'analyzed'].includes(currentJob.value?.status))
const canFreeze = computed(() => ['analyzed', 'frozen', 'conflict', 'rolled_back'].includes(currentJob.value?.status))
const canDetect = computed(() => Boolean(currentJob.value?.frozen))
const canRecalc = computed(() => currentJob.value?.status === 'conflict')
const canPublish = computed(() => Boolean(currentJob.value?.frozen) && pendingItems.value.length === 0 &&
  !items.value.some((i) => i.conflict) && items.value.length > 0)
const canCutover = computed(() => ['ready', 'cutover'].includes(currentJob.value?.status))
const canRollback = computed(() => ['ready', 'cutover', 'frozen'].includes(currentJob.value?.status))
const canRecord = computed(() => Boolean(detail.manifest) && Boolean(detail.topology))

function statusLabel(s: string) {
  return ({ created: '已创建', analyzed: '已解析', frozen: '已冻结', conflict: '有冲突', ready: '待切换', cutover: '已切换', rolled_back: '已回滚' } as Record<string, string>)[s] || s
}
function strategyLabel(s: string) {
  return s === 'batch_shadow' ? '整批影子切换' : '按管道粒度切换'
}
function matchLabel(s: string) {
  return ({ exact: '精确', ambiguous: '多候选', unmapped: '无映射' } as Record<string, string>)[s] || s
}
function reviewLabel(s: string) {
  return ({ pending: '待复核', confirmed: '已确认', resolved: '已取舍' } as Record<string, string>)[s] || s
}
function modeLabel(s: string) {
  return ({ legacy: '旧文本', shadow: '影子', relation: '节点关系' } as Record<string, string>)[s] || s
}

function notify(text: string, kind = 'ok') {
  message.value = text
  messageKind.value = kind
}

async function postJson(path: string, body: Record<string, any>) {
  const res = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.detail || `请求失败 ${res.status}`)
  return data
}

async function loadJobs() {
  const res = await request(`${API}/jobs`)
  const data = await res.json()
  jobs.value = data.items ?? []
  if (currentJobId.value && jobs.value.some((j) => j.id === currentJobId.value)) {
    await loadDetail(currentJobId.value)
  }
}

async function loadDetail(jobId: string) {
  const res = await request(`${API}/jobs/${jobId}`)
  if (!res.ok) {
    notify('迁移作业读取失败', 'err')
    return
  }
  const data = await res.json()
  detail.job = data.job
  detail.items = data.items ?? []
  detail.manifest = data.manifest ?? null
  detail.topology = data.topology ?? null
  const auditRes = await request(`${API}/jobs/${jobId}/audit-logs`)
  const auditData = await auditRes.json()
  auditLogs.value = auditData.items ?? []
}

async function selectJob(jobId: string) {
  currentJobId.value = jobId
  await loadDetail(jobId)
}

function openCreate() {
  createForm.name = ''
  createForm.cutover_strategy = 'batch_shadow'
  showCreate.value = true
}

async function submitCreate() {
  try {
    await postJson(`${API}/jobs`, { ...createForm })
    showCreate.value = false
    notify('迁移作业已创建')
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function runStage(stage: 'analyze' | 'freeze' | 'conflicts' | 'recalculate') {
  if (!currentJob.value) return
  try {
    await postJson(`${API}/jobs/${currentJob.value.id}/${stage}`, { expected_version: currentJob.value.version })
    notify(`阶段「${stage}」执行成功`)
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function publishArtifacts() {
  if (!currentJob.value) return
  try {
    await postJson(`${TRANSFORM_API}/${currentJob.value.id}/writeback`, { expected_version: currentJob.value.version })
    notify('转换管道服务已回写迁移关系清单与兼容流转拓扑')
    activeTab.value = 'topology'
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function cutover() {
  if (!currentJob.value) return
  const body: Record<string, any> = { expected_version: currentJob.value.version }
  if (currentJob.value.cutover_strategy === 'per_pipeline') {
    const firstPending = items.value.find((i) => i.edit_source !== 'relation')
    if (!firstPending) return
    body.pipeline_id = firstPending.pipeline_id
  }
  try {
    await postJson(`${API}/jobs/${currentJob.value.id}/cutover`, body)
    notify('切换成功：对应管道旧文本入口已停用，节点关系成为唯一可编辑来源')
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function rollback() {
  if (!currentJob.value) return
  try {
    await postJson(`${API}/jobs/${currentJob.value.id}/rollback`, { expected_version: currentJob.value.version })
    notify('已回滚到冻结基线，旧文本编辑入口恢复，历史快照保留可查')
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function generateRecord() {
  if (!currentJob.value) return
  try {
    await postJson(`${TRANSFORM_API}/${currentJob.value.id}/flow-records`, {})
    notify('转换管道服务已基于同一份清单与拓扑生成流转管道记录')
    await loadDetail(currentJob.value.id)
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

async function resolveItem(item: Item) {
  try {
    await postJson(`${API}/items/${item.id}/resolve`, {
      expected_version: item.version,
      node_id: resolution[item.id] || null,
    })
    notify('取舍已提交')
    await loadJobs()
  } catch (e) {
    notify((e as Error).message, 'err')
  }
}

function exportEvidence() {
  if (!currentJobId.value) return
  window.open(`${API}/jobs/${currentJobId.value}/export`, '_blank')
}

async function loadCatalog() {
  try {
    const res = await request(`${API}/node-catalog`)
    const data = await res.json()
    nodeCatalog.value = data.items ?? []
  } catch {
    nodeCatalog.value = []
  }
}

onMounted(async () => {
  await loadCatalog()
  await loadJobs()
})
</script>

<style scoped>
.migration-layout { display: grid; grid-template-columns: 320px 1fr; gap: 16px; margin-top: 12px; }
.migration-panel, .migration-main { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 14px; }
.job-list { list-style: none; margin: 0 0 12px; padding: 0; max-height: 320px; overflow: auto; }
.job-item { padding: 10px; border: 1px solid #e2e8f0; border-radius: 8px; margin-bottom: 8px; cursor: pointer; }
.job-item.active { border-color: #2563eb; background: #eff6ff; }
.job-name { font-weight: 600; margin-bottom: 4px; }
.job-meta { display: flex; gap: 8px; align-items: center; font-size: 12px; color: #64748b; flex-wrap: wrap; }
.badge { padding: 2px 8px; border-radius: 999px; background: #f1f5f9; font-size: 12px; }
.badge[data-status='exact'], .badge[data-status='confirmed'], .badge[data-status='cutover'], .badge[data-status='ready'] { background: #d1fae5; color: #065f46; }
.badge[data-status='ambiguous'], .badge[data-status='pending'], .badge[data-status='frozen'] { background: #fef3c7; color: #92400e; }
.badge[data-status='unmapped'], .badge[data-status='conflict'], .badge[data-status='rolled_back'] { background: #fee2e2; color: #991b1b; }
.stage-actions { display: flex; flex-direction: column; gap: 8px; }
.stage-actions .btn { width: 100%; }
.detail-head { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
.tabs { display: flex; gap: 6px; margin: 12px 0; border-bottom: 1px solid #e2e8f0; }
.tab { border: none; background: none; padding: 8px 12px; cursor: pointer; color: #475569; }
.tab.active { color: #2563eb; border-bottom: 2px solid #2563eb; font-weight: 600; }
.cell-text { max-width: 260px; white-space: normal; word-break: break-all; }
.mono { font-family: ui-monospace, monospace; font-size: 12px; }
.review-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; margin-bottom: 10px; }
.review-text { display: flex; flex-direction: column; gap: 4px; font-size: 13px; color: #334155; }
.review-actions { display: flex; gap: 8px; margin-top: 8px; }
.node-select { flex: 1; padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 6px; }
.topo-summary { display: flex; gap: 16px; flex-wrap: wrap; font-size: 12px; color: #475569; margin-bottom: 12px; }
.topo-pipe { margin-bottom: 14px; }
.topo-chain { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.topo-node { background: #e0f2fe; border: 1px solid #7dd3fc; border-radius: 8px; padding: 6px 10px; font-size: 13px; }
.topo-node.unmapped { background: #fef3c7; border-color: #fcd34d; }
.topo-arrow { color: #94a3b8; }
.hint { font-size: 13px; color: #64748b; }
.empty-state.big { padding: 48px; text-align: center; color: #94a3b8; }
.foot-msg.err { color: #b91c1c; }
.foot-msg.ok { color: #047857; }
@media (max-width: 960px) { .migration-layout { grid-template-columns: 1fr; } }
</style>
