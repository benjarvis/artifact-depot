// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { apiHeaders, deleteAllTasksViaApi, resetGcLockoutViaApi, waitForTaskCompletion, waitForTaskRunning } from './helpers'

test.describe.serial('tasks', () => {

test('tasks page shows singleton task rows', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  // Clean up any existing tasks
  await deleteAllTasksViaApi(page)

  await page.locator('nav a', { hasText: 'Tasks' }).click()
  await expect(page).toHaveURL(/\/tasks/)
  // Singleton rows always visible with Start Now buttons
  await expect(page.locator('.singleton-card').nth(0).locator('.singleton-name', { hasText: 'Garbage Collection' })).toBeVisible()
  await expect(page.locator('.singleton-card').nth(1).locator('.singleton-name', { hasText: 'Integrity Check' })).toBeVisible()
  await expect(page.locator('.singleton-card').nth(2).locator('.singleton-name', { hasText: 'Rebuild Directory Entries' })).toBeVisible()

  // All three should have Start Now buttons
  const startButtons = page.locator('.singleton-card button', { hasText: 'Start Now' })
  await expect(startButtons).toHaveCount(3)

  await snapshotA11y('tasks-empty')
  await uiScreenshot('tasks-empty')
})

test('start check opens modal with options', async ({ authedPage: page, uiScreenshot }) => {
  await page.goto('/tasks')

  // Click Start Now on the Check row
  await page.locator('.singleton-card').nth(1).locator('button', { hasText: 'Start Now' }).click()

  // Modal should appear
  const modal = page.locator('.modal-dialog')
  await expect(modal).toBeVisible()
  await expect(modal.locator('h3', { hasText: 'Start Integrity Check' })).toBeVisible()

  // Dry run should be checked by default
  const dryRunCheckbox = modal.locator('label', { hasText: 'Dry run' }).locator('input[type="checkbox"]')
  await expect(dryRunCheckbox).toBeChecked()

  // Verify hashes should be checked by default
  const hashCheckbox = modal.locator('label', { hasText: 'Verify blob hashes' }).locator('input[type="checkbox"]')
  await expect(hashCheckbox).toBeChecked()
  await uiScreenshot('dialog-task-check')

  // Fix warning should NOT be visible (dry run is on)
  await expect(page.locator('.fix-warning')).not.toBeVisible()

  // Uncheck dry run - fix warning should appear
  await dryRunCheckbox.uncheck()
  await expect(page.locator('.fix-warning')).toBeVisible()

  // Cancel
  await modal.locator('button', { hasText: 'Cancel' }).click()
  await expect(modal).not.toBeVisible()
})

test('start check creates task', async ({ authedPage: page, uiScreenshot }) => {
  await deleteAllTasksViaApi(page)
  await page.goto('/tasks')

  // Start check via singleton row
  await page.locator('.singleton-card').nth(1).locator('button', { hasText: 'Start Now' }).click()
  await page.locator('.modal-dialog button', { hasText: 'Start' }).click()

  // Modal should close
  await expect(page.locator('.modal-dialog')).not.toBeVisible()

  // Check row should show running status
  await expect(page.locator('.singleton-card').nth(1).locator('.kind-badge', { hasText: /running/i })).toBeVisible({ timeout: 10000 })
  await uiScreenshot('tasks-running')

  // Wait for completion
  await waitForTaskCompletion(page)

  // Clean up
  await deleteAllTasksViaApi(page)
})

test('start GC opens modal and creates task', async ({ authedPage: page, uiScreenshot }) => {
  // GC has a min-interval cooldown (60s in test config). Allow enough time
  // for the cooldown to expire if a prior test triggered GC.
  test.setTimeout(120_000);
  await deleteAllTasksViaApi(page)
  await resetGcLockoutViaApi(page)
  await page.goto('/tasks')

  // Wait for Start Now to be enabled (may be locked out from a previous retry)
  const gcCard = page.locator('.singleton-card').nth(0)
  const startBtn = gcCard.locator('button', { hasText: 'Start Now' })
  await expect(startBtn).toBeEnabled({ timeout: 90000 })

  // Click Start Now on the GC row
  await startBtn.click()
  const modal = page.locator('.modal-dialog')
  await expect(modal).toBeVisible()
  await expect(modal.locator('h3', { hasText: 'Start Garbage Collection' })).toBeVisible()

  // Dry run checkbox should be present
  await expect(modal.locator('label', { hasText: 'Dry run' })).toBeVisible()
  await uiScreenshot('dialog-task-gc')

  // Cancel closes modal
  await modal.locator('button', { hasText: 'Cancel' }).click()
  await expect(modal).not.toBeVisible()

  // Re-open and start
  await startBtn.click()
  await expect(modal).toBeVisible()
  await modal.locator('button', { hasText: 'Start' }).click()
  await expect(modal).not.toBeVisible()

  // GC row should show running status
  await expect(gcCard.locator('.kind-badge', { hasText: /running/i })).toBeVisible({ timeout: 10000 })

  await waitForTaskCompletion(page)
  await deleteAllTasksViaApi(page)
})

test('rebuild dir entries creates task without modal', async ({ authedPage: page }) => {
  await deleteAllTasksViaApi(page)
  await page.goto('/tasks')

  // Click Start Now on the Rebuild row
  await page.locator('.singleton-card').nth(2).locator('button', { hasText: 'Start Now' }).click()

  // No modal - rebuild row should show running status
  await expect(page.locator('.singleton-card').nth(2).locator('.kind-badge', { hasText: /running/i })).toBeVisible({ timeout: 10000 })

  await waitForTaskCompletion(page)
  await deleteAllTasksViaApi(page)
})

test('expand and collapse last result', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  test.setTimeout(90000)
  // Wait for any running tasks (including from prior tests) to finish.
  await waitForTaskCompletion(page, 60000)
  await deleteAllTasksViaApi(page)
  const headers = await apiHeaders(page)
  const repoResp = await page.request.get('/api/v1/repositories', { headers })
  if (repoResp.ok()) {
    const repos = await repoResp.json()
    for (const r of repos) {
      if (r.name !== 'default') {
        await page.request.delete(`/api/v1/repositories/${encodeURIComponent(r.name)}`, { headers })
      }
    }
  }
  await page.goto('/tasks')
  // Use Rebuild Directory Entries (nth 2) -- it completes fast with no repos
  const rebuildBtn = page.locator('.singleton-card').nth(2).locator('button', { hasText: 'Start Now' })
  await expect(rebuildBtn).toBeEnabled({ timeout: 30000 })

  // Start rebuild -- no modal, starts immediately
  await rebuildBtn.click()
  // Wait for the task to register as running, then wait for it to finish
  await waitForTaskRunning(page)
  await waitForTaskCompletion(page, 60000)
  // Reload to ensure the UI reflects the completed state
  await page.goto('/tasks')
  await expect(page.locator('.last-result')).toBeVisible({ timeout: 10000 })

  // Click to expand
  await page.locator('.last-result').first().click()
  const detail = page.locator('.task-detail')
  await expect(detail).toBeVisible()
  await expect(detail.locator('text=ID:')).toBeVisible()
  await expect(detail.locator('text=Status:')).toBeVisible()
  await snapshotA11y('task-expanded')
  await uiScreenshot('tasks-expanded')

  // Click to collapse
  await page.locator('.last-result').first().click()
  await expect(detail).not.toBeVisible()

  await deleteAllTasksViaApi(page)
})

}) // end test.describe.serial
