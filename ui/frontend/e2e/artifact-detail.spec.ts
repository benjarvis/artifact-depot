// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('click artifact row opens detail modal with metadata', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  const repoName = `detail-test-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Create a hosted raw repo
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await page.locator('#name').fill(repoName)
  await page.locator('#type').selectOption('hosted')
  await page.locator('#format').selectOption('raw')
  await page.locator('#store').selectOption('default')
  await page.locator('button[type="submit"]').click()
  await expect(page).toHaveURL(/\/repositories$/)

  // Navigate into the repo
  await page.locator('td', { hasText: repoName }).click()
  await expect(page).toHaveURL(new RegExp(`/repositories/${repoName}`))

  // Upload a file
  const fileContent = Buffer.from('hello world', 'utf-8')
  const fileInput = page.locator('input[type="file"]')
  await fileInput.setInputFiles({
    name: 'test-file.txt',
    mimeType: 'text/plain',
    buffer: fileContent,
  })

  // Wait for the file to appear in the table
  await expect(page.locator('td', { hasText: 'test-file.txt' })).toBeVisible()

  // Click the artifact row (not the download/delete buttons)
  const fileRow = page.locator('tr', { hasText: 'test-file.txt' })
  await fileRow.locator('td').first().click()

  // Detail modal should appear
  const modal = page.locator('.modal-detail')
  await expect(modal).toBeVisible()

  // Verify modal title shows the filename
  await expect(modal.locator('h3')).toHaveText('test-file.txt')

  // Verify metadata fields are present
  await expect(modal.locator('dt', { hasText: 'Full Path' })).toBeVisible()
  await expect(modal.locator('dd').first()).toContainText('test-file.txt')

  await expect(modal.locator('dt', { hasText: 'UUID' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'BLAKE3 Hash' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'Size' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'Content Type' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'Created' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'Updated' })).toBeVisible()
  await expect(modal.locator('dt', { hasText: 'Last Accessed' })).toBeVisible()

  // UUID, hash, and ETag should be in monospace (have .mono class)
  const monoFields = modal.locator('.mono')
  const count = await monoFields.count()
  expect(count).toBeGreaterThanOrEqual(2)

  // Download link should exist
  await expect(modal.locator('a', { hasText: 'Download' })).toBeVisible()

  // Delete button should exist
  await expect(modal.locator('button', { hasText: 'Delete' })).toBeVisible()

  await snapshotA11y('detail-modal-open')
  await uiScreenshot('dialog-artifact-detail')

  // Close modal via X button
  await modal.locator('.modal-close').click()
  await expect(modal).not.toBeVisible()
})

test('detail modal delete button opens delete confirmation', async ({ authedPage: page }) => {
  const repoName = `detail-del-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Create repo
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await page.locator('#name').fill(repoName)
  await page.locator('#type').selectOption('hosted')
  await page.locator('#format').selectOption('raw')
  await page.locator('#store').selectOption('default')
  await page.locator('button[type="submit"]').click()
  await expect(page).toHaveURL(/\/repositories$/)

  // Navigate into repo
  await page.locator('td', { hasText: repoName }).click()

  // Upload a file
  await page.locator('input[type="file"]').setInputFiles({
    name: 'delete-me.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from('delete this'),
  })
  await expect(page.locator('td', { hasText: 'delete-me.txt' })).toBeVisible()

  // Open detail modal
  const fileRow = page.locator('tr', { hasText: 'delete-me.txt' })
  await fileRow.locator('td').first().click()
  await expect(page.locator('.modal-detail')).toBeVisible()

  // Click delete in detail modal
  await page.locator('.modal-detail button', { hasText: 'Delete' }).click()

  // Detail modal should close, delete confirmation should open
  await expect(page.locator('.modal-detail')).not.toBeVisible()
  await expect(page.locator('.modal h3', { hasText: 'Delete Artifact' })).toBeVisible()
  await expect(page.locator('.modal', { hasText: 'delete-me.txt' })).toBeVisible()

  // Confirm delete
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()

  // File should be gone
  await expect(page.locator('td', { hasText: 'delete-me.txt' })).not.toBeVisible()
})

test('clicking overlay outside detail modal closes it', async ({ authedPage: page }) => {
  const repoName = `overlay-test-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Create repo + upload file
  await page.locator('a', { hasText: 'Create Repository' }).click()
  await page.locator('#name').fill(repoName)
  await page.locator('#type').selectOption('hosted')
  await page.locator('#format').selectOption('raw')
  await page.locator('#store').selectOption('default')
  await page.locator('button[type="submit"]').click()
  await expect(page).toHaveURL(/\/repositories$/)
  await page.locator('td', { hasText: repoName }).click()

  await page.locator('input[type="file"]').setInputFiles({
    name: 'overlay-test.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from('test'),
  })
  await expect(page.locator('td', { hasText: 'overlay-test.txt' })).toBeVisible()

  // Open detail modal
  await page.locator('tr', { hasText: 'overlay-test.txt' }).locator('td').first().click()
  await expect(page.locator('.modal-detail')).toBeVisible()

  // Click the overlay (outside the modal)
  await page.locator('.modal-overlay').click({ position: { x: 5, y: 5 } })
  await expect(page.locator('.modal-detail')).not.toBeVisible()
})
