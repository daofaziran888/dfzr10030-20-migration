import { expect, test } from '@playwright/test'

// 依赖迁移工作台 E2E：覆盖建作业、阶段推进、待复核、回写、切换与导出。
// 后端使用内存态（每个进程独立），用例内自建作业，不依赖固定种子 ID。
test('依赖迁移：建作业→解析→冻结→复核→回写→切换→导出全链路', async ({ page }) => {
  test.setTimeout(180_000)
  const browserErrors: string[] = []
  page.on('pageerror', (e) => browserErrors.push(e.message))
  page.on('response', (r) => {
    if (r.url().includes('/api/') && r.status() >= 400) browserErrors.push(`${r.status()} ${r.url()}`)
  })

  await page.goto('/migration')
  await expect(page.getByTestId('job-list')).toBeVisible()

  // 新建按管道粒度切换的作业
  await page.getByTestId('open-create').click()
  await expect(page.getByTestId('create-modal')).toBeVisible()
  await page.getByTestId('create-name').fill('E2E迁移作业')
  await page.getByTestId('create-strategy').selectOption('per_pipeline')
  await page.getByTestId('submit-create').click()
  await expect(page.getByTestId('create-modal')).toHaveCount(0)

  // 选中刚创建的作业（列表第一项）
  await page.locator('[data-testid^="job-"]').first().click()
  await expect(page.getByTestId('stage-actions')).toBeVisible()

  // 解析 → 冻结 → 冲突检测
  await page.getByTestId('stage-analyze').click()
  await expect(page.getByTestId('tab-items')).toContainText('迁移关系清单')
  await expect.poll(async () => page.locator('[data-testid^="item-"]').count()).toBeGreaterThan(0)
  await page.getByTestId('stage-freeze').click()
  await page.getByTestId('stage-conflict').click()

  // 待复核页：把待处理片段提交取舍（默认“确认无映射”即可清空）
  await page.getByTestId('tab-review').click()
  const reviewCards = page.locator('[data-testid^="review-"]')
  const pendingCount = await reviewCards.count()
  for (let i = 0; i < pendingCount; i++) {
    await reviewCards.first().getByTestId('resolve-btn').click()
    await expect.poll(async () => page.locator('[data-testid^="review-"]').count()).toBeLessThan(pendingCount - i)
  }

  // 回写清单/拓扑并查看拓扑
  await page.getByTestId('stage-publish').click()
  await expect(page.getByTestId('panel-topology')).toBeVisible()
  await expect(page.locator('.topo-node').first()).toBeVisible()

  // 按管道切换
  await page.getByTestId('stage-cutover').click()
  await expect(page.getByTestId('foot-message')).toContainText('切换成功')

  // 导出双依据 CSV，触发下载
  const downloadPromise = page.waitForEvent('download')
  await page.getByTestId('export').click()
  const download = await downloadPromise
  expect(download.suggestedFilename()).toBe('migration_evidence.csv')

  // 审计历史有记录
  await page.getByTestId('tab-audit').click()
  await expect(page.locator('[data-testid^="audit-"]').first()).toBeVisible()

  expect(browserErrors).toEqual([])
})
