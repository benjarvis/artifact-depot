// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { createStoreViaApi, deleteStoreViaApi, createRepoViaApi, deleteRepoViaApi } from './helpers'

// --- CreateStore ---

test('create store: submit disabled when name empty', async ({ authedPage: page }) => {
  await page.goto('/stores/new')
  // Name is empty, root is empty
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()
})

test('create store: submit disabled for file type without root', async ({ authedPage: page }) => {
  await page.goto('/stores/new')
  await page.locator('#name').fill('test-store')
  // File type is default, root is empty
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()

  // Fill root path - should enable
  await page.locator('#root').fill('/tmp/test')
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeEnabled()
})

test('create store: submit disabled for s3 type without bucket', async ({ authedPage: page }) => {
  await page.goto('/stores/new')
  await page.locator('#name').fill('test-store')
  await page.locator('#type').selectOption('s3')
  // Bucket is empty
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()

  // Fill bucket - should enable
  await page.locator('#bucket').fill('my-bucket')
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeEnabled()
})

test('create store: file fields visible, s3 hidden (and vice versa)', async ({ authedPage: page, uiScreenshot }) => {
  await page.goto('/stores/new')

  // File type (default) - file fields visible
  await expect(page.locator('#root')).toBeVisible()
  await expect(page.locator('#io_size')).toBeVisible()
  await expect(page.locator('#bucket')).not.toBeVisible()
  await expect(page.locator('#endpoint')).not.toBeVisible()
  await uiScreenshot('store-create-form-file')

  // Switch to S3
  await page.locator('#type').selectOption('s3')
  await expect(page.locator('#root')).not.toBeVisible()
  await expect(page.locator('#io_size')).not.toBeVisible()
  await expect(page.locator('#bucket')).toBeVisible()
  await expect(page.locator('#endpoint')).toBeVisible()
  await expect(page.locator('#region')).toBeVisible()
  await expect(page.locator('#access_key')).toBeVisible()
  await expect(page.locator('#secret_key')).toBeVisible()
  await uiScreenshot('store-create-form-s3')
})

test('create store: file store happy path', async ({ authedPage: page, snapshotA11y }) => {
  const storeName = `test-store-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  try {
    await page.goto('/stores/new')
    await page.locator('#name').fill(storeName)
    await page.locator('#root').fill(`/tmp/${storeName}`)
    await page.locator('button[type="submit"], .btn-primary').click()

    await expect(page).toHaveURL(/\/stores$/)
    await expect(page.locator('td', { hasText: storeName })).toBeVisible()
    await snapshotA11y('store-created')
  } finally {
    await deleteStoreViaApi(page, storeName)
  }
})

test('create store: cancel returns to list', async ({ authedPage: page }) => {
  await page.goto('/stores/new')
  await page.locator('a.btn', { hasText: 'Cancel' }).click()
  await expect(page).toHaveURL(/\/stores$/)
})

// --- StoreList ---

test('store list: default store visible with table headers', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  await page.goto('/stores')
  // Table headers
  await expect(page.locator('th', { hasText: 'Name' })).toBeVisible()
  await expect(page.locator('th', { hasText: 'Type' })).toBeVisible()
  await expect(page.locator('th', { hasText: 'Blobs' })).toBeVisible()
  await expect(page.locator('th', { hasText: 'Size' })).toBeVisible()
  // Default store should exist
  await expect(page.locator('tbody tr').first()).toBeVisible()
  await snapshotA11y('store-list')
  await uiScreenshot('store-list')
})

test('store list: delete store with confirmation modal', async ({ authedPage: page, uiScreenshot }) => {
  const storeName = await createStoreViaApi(page)

  await page.goto('/stores')
  const row = page.locator('tr', { hasText: storeName })
  await row.locator('.delete-btn').click()

  // Modal should appear
  const modal = page.locator('.modal')
  await expect(modal).toBeVisible()
  await expect(modal).toContainText(storeName)
  await uiScreenshot('dialog-delete-store')

  // Cancel first
  await modal.locator('button', { hasText: 'Cancel' }).click()
  await expect(modal).not.toBeVisible()
  await expect(page.locator('td', { hasText: storeName })).toBeVisible()

  // Now actually delete
  await row.locator('.delete-btn').click()
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()
  await expect(page.locator('td', { hasText: storeName })).not.toBeVisible()
})

// --- StoreDetail ---

test('store detail: Browse and Manage tabs switch content', async ({ authedPage: page, uiScreenshot }) => {
  const storeName = await createStoreViaApi(page)

  try {
    await page.goto(`/stores/${storeName}`)
    await expect(page.locator('.detail-card')).toBeVisible()

    // Browse tab active by default
    await expect(page.locator('.tab-bar button.active', { hasText: 'Browse' })).toBeVisible()
    await uiScreenshot('store-detail-browse')

    // Switch to Manage
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()
    await expect(page.locator('.manage-section')).toBeVisible()
    await expect(page.locator('.manage-form')).toBeVisible()
    await uiScreenshot('store-detail-manage')

    // Switch back to Browse
    await page.locator('.tab-bar button', { hasText: 'Browse' }).click()
    await expect(page.locator('.browse-section')).toBeVisible()
  } finally {
    await deleteStoreViaApi(page, storeName)
  }
})

test('store detail: save settings shows success', async ({ authedPage: page }) => {
  const storeName = await createStoreViaApi(page)

  try {
    await page.goto(`/stores/${storeName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Click save
    await page.locator('.manage-form button[type="submit"]').click()
    await expect(page.locator('.success', { hasText: 'Settings saved' })).toBeVisible()
  } finally {
    await deleteStoreViaApi(page, storeName)
  }
})

test('store detail: delete via danger zone', async ({ authedPage: page }) => {
  const storeName = await createStoreViaApi(page)

  await page.goto(`/stores/${storeName}`)
  await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

  // Click Delete Store
  await page.locator('.danger-zone .btn-danger', { hasText: 'Delete Store' }).click()

  const modal = page.locator('.modal')
  await expect(modal).toBeVisible()
  await expect(modal).toContainText(storeName)

  // Confirm
  await modal.locator('.btn-danger', { hasText: /delete/i }).click()
  await expect(page).toHaveURL(/\/stores$/)
})

test('store detail: delete store cancel keeps store', async ({ authedPage: page }) => {
  const storeName = await createStoreViaApi(page)

  try {
    await page.goto(`/stores/${storeName}`)
    await page.locator('.tab-bar button', { hasText: 'Manage' }).click()

    // Open delete dialog then cancel
    await page.locator('.danger-zone .btn-danger', { hasText: 'Delete Store' }).click()
    const modal = page.locator('.modal')
    await expect(modal).toBeVisible()
    await modal.locator('button', { hasText: 'Cancel' }).click()
    await expect(modal).not.toBeVisible()

    // Still on detail page
    await expect(page.locator('.detail-card')).toBeVisible()
  } finally {
    await deleteStoreViaApi(page, storeName)
  }
})
