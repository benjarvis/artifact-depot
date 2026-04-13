// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { apiHeaders } from './helpers'

test('backup page renders with both sections', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  await page.locator('nav a', { hasText: 'Backup' }).click()
  await expect(page).toHaveURL(/\/backup/)
  // Backup section
  await expect(page.locator('h3', { hasText: 'Backup' })).toBeVisible()
  await expect(page.locator('button', { hasText: 'Download Backup' })).toBeVisible()

  // Restore section
  await expect(page.locator('h3', { hasText: 'Restore' })).toBeVisible()
  await expect(page.locator('input[type="file"]')).toBeVisible()

  await snapshotA11y('backup-page')
  await uiScreenshot('backup-page')
})

test('restore button disabled until file selected', async ({ authedPage: page }) => {
  await page.goto('/backup')

  // Restore button should be disabled (no file selected)
  const restoreBtn = page.locator('button.btn-primary', { hasText: 'Restore' })
  await expect(restoreBtn).toBeDisabled()

  // Select a file
  const backupData = JSON.stringify({ stores: [], repositories: [], roles: [], users: [] })
  await page.locator('input[type="file"]').setInputFiles({
    name: 'test-backup.json',
    mimeType: 'application/json',
    buffer: Buffer.from(backupData),
  })

  // Now restore button should be enabled
  await expect(restoreBtn).toBeEnabled()
})

test('restore confirmation dialog shows and cancel closes it', async ({ authedPage: page, uiScreenshot }) => {
  await page.goto('/backup')

  // Select a file
  await page.locator('input[type="file"]').setInputFiles({
    name: 'test-backup.json',
    mimeType: 'application/json',
    buffer: Buffer.from('{}'),
  })

  // Click restore
  await page.locator('button.btn-primary', { hasText: 'Restore' }).click()

  // Confirmation modal
  const modal = page.locator('.modal')
  await expect(modal).toBeVisible()
  await expect(modal.locator('h3', { hasText: 'Confirm Restore' })).toBeVisible()
  await expect(modal).toContainText('test-backup.json')
  await uiScreenshot('dialog-restore-confirm')

  // Cancel
  await modal.locator('button', { hasText: 'Cancel' }).click()
  await expect(modal).not.toBeVisible()
})

test('restore with valid backup shows success and summary', async ({ authedPage: page }) => {
  // Get current backup via API
  const headers = await apiHeaders(page)
  const backupResp = await page.request.get('/api/v1/backup', { headers })
  expect(backupResp.ok()).toBeTruthy()
  const backupData = await backupResp.text()

  await page.goto('/backup')

  // Upload the backup file
  await page.locator('input[type="file"]').setInputFiles({
    name: 'depot-backup.json',
    mimeType: 'application/json',
    buffer: Buffer.from(backupData),
  })

  // Click restore, confirm
  await page.locator('button.btn-primary', { hasText: 'Restore' }).click()
  await page.locator('.modal .btn-primary', { hasText: 'Restore' }).click()

  // Should show success
  await expect(page.locator('.success', { hasText: 'Restore completed' })).toBeVisible({ timeout: 10000 })

  // Restore summary should appear
  await expect(page.locator('h3', { hasText: 'Restore Summary' })).toBeVisible()
  await expect(page.locator('dt', { hasText: 'Stores' })).toBeVisible()
  await expect(page.locator('dt', { hasText: 'Repositories' })).toBeVisible()
  await expect(page.locator('dt', { hasText: 'Roles' })).toBeVisible()
  await expect(page.locator('dt', { hasText: 'Users' })).toBeVisible()
  await expect(page.locator('dt', { hasText: 'Settings' })).toBeVisible()
})
