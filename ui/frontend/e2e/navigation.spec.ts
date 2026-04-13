// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('header nav links navigate correctly', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Users' }).click()
  await expect(page).toHaveURL(/\/users/)

  await page.locator('nav a', { hasText: 'Roles' }).click()
  await expect(page).toHaveURL(/\/roles/)

  await page.locator('nav a', { hasText: 'Repositories' }).click()
  await expect(page).toHaveURL(/\/repositories/)
})

test('logout clears session and redirects to login', async ({ authedPage: page }) => {
  await page.locator('.logout-btn').click()
  await expect(page).toHaveURL(/\/login/)

  // Verify we can't access protected pages
  await page.goto('/repositories')
  await expect(page).toHaveURL(/\/login/)
})

test('version badge is visible', async ({ authedPage: page, uiScreenshot }) => {
  await expect(page.locator('.version-badge')).toBeVisible()

  // Capture license dialog
  await page.locator('.info-btn').click()
  await expect(page.locator('.license-body')).toBeVisible()
  await uiScreenshot('dialog-license')
  await page.locator('.modal-close').click()
})

test('stores nav link navigates correctly', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Stores' }).click()
  await expect(page).toHaveURL(/\/stores/)
})

test('settings nav link navigates correctly', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Settings' }).click()
  await expect(page).toHaveURL(/\/settings/)
})

test('tasks nav link navigates correctly', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Tasks' }).click()
  await expect(page).toHaveURL(/\/tasks/)
})

test('dashboard path is no longer routable', async ({ authedPage: page }) => {
  await page.goto('/dashboard')
  await expect(page.locator('h2', { hasText: 'Page Not Found' })).toBeVisible()
})

test('backup nav link navigates correctly', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Backup' }).click()
  await expect(page).toHaveURL(/\/backup/)
})

test('not found page renders correctly', async ({ authedPage: page, uiScreenshot }) => {
  await page.goto('/this-page-does-not-exist')
  await expect(page.locator('h2', { hasText: 'Page Not Found' })).toBeVisible()
  await uiScreenshot('not-found')
})
