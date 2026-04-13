// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('unauthenticated user is redirected to login', async ({ page }) => {
  await page.goto('/repositories')
  await expect(page).toHaveURL(/\/login/)
  await expect(page.locator('#username')).toBeVisible()
})

test('successful login redirects to repositories', async ({ page, uiScreenshot }) => {
  await page.goto('/login')
  await uiScreenshot('login-page')
  await page.locator('#username').fill('admin')
  await page.locator('#password').fill('admin')
  await page.locator('.login-btn').click()

  // Handle mandatory password change on fresh server bootstrap
  try {
    await page.locator('.change-pw-msg').waitFor({ state: 'visible', timeout: 3000 })
    await page.locator('#new-password').fill('admin')
    await page.locator('#confirm-password').fill('admin')
    await page.locator('.login-btn').click()
  } catch {
    // No password change required
  }

  await expect(page).toHaveURL(/\/repositories/)
  await expect(page.locator('nav')).toBeVisible()
})

test('invalid credentials show error', async ({ page }) => {
  await page.goto('/login')
  await page.locator('#username').fill('admin')
  await page.locator('#password').fill('wrongpassword')
  await page.locator('.login-btn').click()
  await expect(page.locator('.error')).toBeVisible()
})

test('login with empty username shows validation or stays on page', async ({ page }) => {
  await page.goto('/login')
  await page.locator('#password').fill('admin')
  // Leave username empty and click login
  await page.locator('.login-btn').click()
  // Should stay on login page (browser required validation or API error)
  await expect(page).toHaveURL(/\/login/)
})

test('login with empty password shows validation or stays on page', async ({ page }) => {
  await page.goto('/login')
  await page.locator('#username').fill('admin')
  // Leave password empty and click login
  await page.locator('.login-btn').click()
  // Should stay on login page
  await expect(page).toHaveURL(/\/login/)
})
