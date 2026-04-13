// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('empty repository list shows table or empty state', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  await expect(page.locator('th', { hasText: 'Name' })).toBeVisible()
  await snapshotA11y()
  await uiScreenshot('repository-list-empty')
})

test('create, view, and delete a hosted raw repository', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  const repoName = `test-repo-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Navigate to create form
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await expect(page).toHaveURL(/\/repositories\/new/)

  // Fill form
  await page.locator('#name').fill(repoName)
  await page.locator('#type').selectOption('hosted')
  await page.locator('#format').selectOption('raw')
  await page.locator('#store').selectOption('default')

  // Submit
  await page.locator('button[type="submit"]').click()
  await expect(page).toHaveURL(/\/repositories$/)

  // Verify repo appears in list
  await expect(page.locator('td', { hasText: repoName })).toBeVisible()
  await snapshotA11y('list-with-repo')
  await uiScreenshot('repository-list')

  // Click to view detail
  await page.locator('td', { hasText: repoName }).click()
  await expect(page).toHaveURL(new RegExp(`/repositories/${repoName}`))
  await expect(page.locator('.detail-card')).toBeVisible()
  await snapshotA11y('detail')

  // Delete the repo via the Manage tab on the detail page
  await page.locator('.tab-bar button', { hasText: 'Manage' }).click()
  await page.locator('.danger-zone button', { hasText: 'Delete Repository' }).click()
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()

  // Should redirect back to list with repo gone
  await expect(page).toHaveURL(/\/repositories$/)
  await expect(page.locator('td', { hasText: repoName })).not.toBeVisible()
})

test('stats header shows pie chart and summary cards when repos exist', async ({ authedPage: page, uiScreenshot }) => {
  const repoName = `stats-repo-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
  const token = await page.evaluate(() => localStorage.getItem('depot_token'))

  // Create a repo via API so the list is non-empty.
  await page.request.post('/api/v1/repositories', {
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    data: { name: repoName, repo_type: 'hosted', format: 'raw', store: 'default' },
  })
  try {
    await page.reload()
    await expect(page.locator('.stats-header .pie-card')).toBeVisible({ timeout: 10000 })
    await expect(page.locator('.stat-label', { hasText: 'Total Artifacts' })).toBeVisible()
    await expect(page.locator('.stat-label', { hasText: 'Total Size' })).toBeVisible()
    await expect(page.locator('.stat-label', { hasText: 'Repositories' })).toBeVisible()
    await uiScreenshot('repositories-with-stats-header')
  } finally {
    await page.request.delete(`/api/v1/repositories/${repoName}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  }
})

test('create repository link navigates to form', async ({ authedPage: page, uiScreenshot }) => {
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await expect(page).toHaveURL(/\/repositories\/new/)
  await expect(page.locator('h2', { hasText: 'Create Repository' })).toBeVisible()
  await uiScreenshot('repository-create-form')
})

test('clicking repo row navigates to detail', async ({ authedPage: page }) => {
  const repoName = `nav-test-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Create repo via UI
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await page.locator('#name').fill(repoName)
  await page.locator('#type').selectOption('hosted')
  await page.locator('#format').selectOption('raw')
  await page.locator('#store').selectOption('default')
  await page.locator('button[type="submit"]').click()
  await expect(page).toHaveURL(/\/repositories$/)

  // Click the repo row
  await page.locator('td', { hasText: repoName }).click()
  await expect(page).toHaveURL(new RegExp(`/repositories/${repoName}`))
  await expect(page.locator('.detail-card')).toBeVisible()

  // Clean up
  const token = await page.evaluate(() => localStorage.getItem('depot_token'))
  await page.request.delete(`/api/v1/repositories/${repoName}`, {
    headers: { Authorization: `Bearer ${token}` },
  })
})
