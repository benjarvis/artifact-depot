// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { createUserViaApi, deleteUserViaApi } from './helpers'

test('create user with role assignment', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  const username = `test-user-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  try {
    await page.locator('nav a', { hasText: 'Users' }).click()
    await page.locator('a', { hasText: 'Create User' }).click()
    await expect(page).toHaveURL(/\/users\/new/)
    await uiScreenshot('user-create-form')

    await page.locator('#username').fill(username)
    await page.locator('#password').fill('testpass123')

    // Check a role
    await page.locator('.checkbox-group', { hasText: 'admin' }).locator('input[type="checkbox"]').check()

    await page.locator('button[type="submit"], .btn-primary').click()
    await expect(page).toHaveURL(/\/users$/)

    // Verify user appears in list
    await expect(page.locator('td', { hasText: username })).toBeVisible()
    await snapshotA11y('user-created')
  } finally {
    await deleteUserViaApi(page, username)
  }
})

test('create user submit disabled when username empty', async ({ authedPage: page }) => {
  await page.goto('/users/new')
  // Fill password but leave username empty
  await page.locator('#password').fill('testpass123')
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()
})

test('create user submit disabled when password empty', async ({ authedPage: page }) => {
  await page.goto('/users/new')
  // Fill username but leave password empty
  await page.locator('#username').fill('someuser')
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()
})

test('edit user shows existing roles checked', async ({ authedPage: page }) => {
  const username = await createUserViaApi(page, { roles: ['read-only'] })

  try {
    await page.goto(`/users/${username}`)
    await expect(page.locator('h2')).toContainText(username)

    // read-only should be checked
    const readOnlyCheckbox = page.locator('.checkbox-group', { hasText: 'read-only' }).locator('input[type="checkbox"]')
    await expect(readOnlyCheckbox).toBeChecked()

    // admin should not be checked
    const adminCheckbox = page.locator('.checkbox-group', { hasText: 'admin' }).locator('input[type="checkbox"]')
    await expect(adminCheckbox).not.toBeChecked()
  } finally {
    await deleteUserViaApi(page, username)
  }
})

test('change password dialog -- passwords must match', async ({ authedPage: page }) => {
  const username = await createUserViaApi(page)

  try {
    await page.goto(`/users/${username}`)
    await page.locator('button', { hasText: 'Change Password' }).click()
    await expect(page.locator('.modal')).toBeVisible()

    // Fill mismatched passwords
    await page.locator('#cp-new').fill('newpass1')
    await page.locator('#cp-confirm').fill('newpass2')
    await page.locator('.modal button[type="submit"]').click()

    // Should show error
    await expect(page.locator('.modal .error', { hasText: 'do not match' })).toBeVisible()
  } finally {
    await deleteUserViaApi(page, username)
  }
})

test('change password dialog -- empty password shows error or stays open', async ({ authedPage: page }) => {
  const username = await createUserViaApi(page)

  try {
    await page.goto(`/users/${username}`)
    await page.locator('button', { hasText: 'Change Password' }).click()
    await expect(page.locator('.modal')).toBeVisible()

    // Leave new password empty, submit — should show JS error or browser validation blocks it
    await page.locator('.modal button[type="submit"]').click()
    // Modal should remain open (either validation error or browser required attribute)
    await expect(page.locator('.modal')).toBeVisible()
  } finally {
    await deleteUserViaApi(page, username)
  }
})

test('change password dialog cancel closes it', async ({ authedPage: page }) => {
  const username = await createUserViaApi(page)

  try {
    await page.goto(`/users/${username}`)
    await page.locator('button', { hasText: 'Change Password' }).click()
    await expect(page.locator('.modal')).toBeVisible()

    await page.locator('.modal button', { hasText: 'Cancel' }).click()
    await expect(page.locator('.modal')).not.toBeVisible()
  } finally {
    await deleteUserViaApi(page, username)
  }
})

test('delete user from user list', async ({ authedPage: page, uiScreenshot }) => {
  const username = await createUserViaApi(page)

  await page.locator('nav a', { hasText: 'Users' }).click()
  await expect(page.locator('td', { hasText: username })).toBeVisible()

  // Click delete on the user row
  const row = page.locator('tr', { hasText: username })
  await row.locator('.delete-btn').click()

  // Confirm deletion
  await expect(page.locator('.modal')).toBeVisible()
  await uiScreenshot('dialog-delete-user')
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()

  // User should be gone
  await expect(page.locator('td', { hasText: username })).not.toBeVisible()
})
