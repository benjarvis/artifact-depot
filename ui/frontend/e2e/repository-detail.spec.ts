// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { createRepoViaApi, deleteRepoViaApi, deleteAllTasksViaApi } from './helpers'

test('detail card shows repo metadata with badges', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  const repoName = await createRepoViaApi(page)

  try {
    await page.goto(`/repositories/${repoName}`)
    await expect(page.locator('.detail-card')).toBeVisible()
    // Type and format badges
    await expect(page.locator('.badge', { hasText: 'hosted' })).toBeVisible()
    await expect(page.locator('.badge', { hasText: 'raw' })).toBeVisible()
    await snapshotA11y('repo-detail-card')
    await uiScreenshot('repository-detail-browse')
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})

test('Browse and Manage tabs switch content', async ({ authedPage: page, uiScreenshot }) => {
  const repoName = await createRepoViaApi(page)

  try {
    await page.goto(`/repositories/${repoName}`)
    // Browse tab active by default - artifact browser visible
    await expect(page.locator('.tab-bar button.active', { hasText: 'Browse' })).toBeVisible()

    // Switch to Manage tab
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()
    await expect(page.locator('.tab-bar button.active', { hasText: 'Manage' })).toBeVisible()
    await expect(page.locator('.manage-section')).toBeVisible()
    await uiScreenshot('repository-detail-manage')
    // Disabled fields on manage tab
    await expect(page.locator('.manage-form input[disabled]').first()).toBeVisible()

    // Switch back to Browse
    await page.locator('.tab-bar button', { hasText: 'Browse' }).click()
    await expect(page.locator('.tab-bar button.active', { hasText: 'Browse' })).toBeVisible()
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})

test('save settings on manage tab shows success', async ({ authedPage: page }) => {
  const repoName = await createRepoViaApi(page, { format: 'raw' })

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Change content disposition
    await page.locator('#content_disposition').selectOption('inline')
    await page.locator('.manage-form button[type="submit"]').click()
    await expect(page.locator('.success', { hasText: 'Settings saved' })).toBeVisible()
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})

test('clone dialog opens with pre-filled name and cancel closes it', async ({ authedPage: page, uiScreenshot }) => {
  const repoName = await createRepoViaApi(page)

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Click Clone Repository button
    await page.locator('button', { hasText: 'Clone Repository' }).click()
    const modal = page.locator('.modal')
    await expect(modal).toBeVisible()
    await expect(modal.locator('h3', { hasText: 'Clone Repository' })).toBeVisible()
    // Pre-filled with name-copy
    await expect(page.locator('#clone_name')).toHaveValue(`${repoName}-copy`)
    await uiScreenshot('dialog-clone-repo')

    // Cancel closes dialog
    await modal.locator('button', { hasText: 'Cancel' }).click()
    await expect(modal).not.toBeVisible()
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})

test('clone repository redirects to tasks', async ({ authedPage: page }) => {
  const repoName = await createRepoViaApi(page)
  const cloneName = `${repoName}-clone`

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    await page.locator('button', { hasText: 'Clone Repository' }).click()
    await page.locator('#clone_name').fill(cloneName)
    await page.locator('.modal .btn-primary', { hasText: 'Clone' }).click()

    // Should redirect to /tasks
    await expect(page).toHaveURL(/\/tasks/)
  } finally {
    await deleteRepoViaApi(page, repoName)
    await deleteRepoViaApi(page, cloneName)
    await deleteAllTasksViaApi(page)
  }
})

test('delete dialog opens and cancel closes it', async ({ authedPage: page, uiScreenshot }) => {
  const repoName = await createRepoViaApi(page)

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Scroll danger zone into view and uiScreenshot before clicking delete
    await page.locator('.danger-zone').scrollIntoViewIfNeeded()
    await uiScreenshot('danger-zone')

    // Click Delete Repository in danger zone
    await page.locator('.danger-zone button', { hasText: 'Delete Repository' }).click()
    const modal = page.locator('.modal')
    await expect(modal).toBeVisible()
    await expect(modal.locator('h3', { hasText: 'Delete Repository' })).toBeVisible()
    await expect(modal).toContainText(repoName)
    await uiScreenshot('dialog-delete-repo')

    // Cancel
    await modal.locator('button', { hasText: 'Cancel' }).click()
    await expect(modal).not.toBeVisible()
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})

test('delete repository via manage tab redirects to list', async ({ authedPage: page }) => {
  const repoName = await createRepoViaApi(page)

  await page.goto(`/repositories/${repoName}`)
  await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

  await page.locator('.danger-zone button', { hasText: 'Delete Repository' }).click()
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()

  await expect(page).toHaveURL(/\/repositories$/)
  await expect(page.locator('td', { hasText: repoName })).not.toBeVisible()
})

test('clean button visible with cleanup policy and redirects to tasks', async ({ authedPage: page }) => {
  const repoName = await createRepoViaApi(page, { cleanup_max_age_days: 30 })

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Clean button should be visible
    const cleanBtn = page.locator('.btn-clean', { hasText: 'Clean' })
    await expect(cleanBtn).toBeVisible()
    await cleanBtn.click()

    await expect(page).toHaveURL(/\/tasks/)
  } finally {
    await deleteRepoViaApi(page, repoName)
    await deleteAllTasksViaApi(page)
  }
})

test('clean button not visible without cleanup policy', async ({ authedPage: page }) => {
  const repoName = await createRepoViaApi(page)

  try {
    await page.goto(`/repositories/${repoName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Clean button should NOT be visible (no cleanup policy, not docker)
    await expect(page.locator('.btn-clean')).not.toBeVisible()
  } finally {
    await deleteRepoViaApi(page, repoName)
  }
})
