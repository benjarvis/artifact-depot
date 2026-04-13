// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'
import { createRoleViaApi, deleteRoleViaApi } from './helpers'

test('create role with capabilities', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  const roleName = `test-role-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  try {
    await page.locator('nav a', { hasText: 'Roles' }).click()
    await page.locator('a', { hasText: 'Create Role' }).click()
    await expect(page).toHaveURL(/\/roles\/new/)
    await uiScreenshot('role-create-form')

    await page.locator('#name').fill(roleName)
    await page.locator('#description').fill('Test role description')

    // Add a capability
    await page.locator('button', { hasText: '+ Add capability' }).click()
    const capRow = page.locator('.cap-row').first()
    await expect(capRow).toBeVisible()
    await capRow.locator('select').selectOption('write')
    await capRow.locator('.cap-repo').fill('*')
    await uiScreenshot('role-create-form-with-capability')

    await page.locator('button[type="submit"], .btn-primary').click()
    await expect(page).toHaveURL(/\/roles$/)
    await expect(page.locator('td', { hasText: roleName })).toBeVisible()
    await snapshotA11y('role-created')
  } finally {
    await deleteRoleViaApi(page, roleName)
  }
})

test('create role disabled when name empty', async ({ authedPage: page }) => {
  await page.goto('/roles/new')
  // Name is empty by default
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeDisabled()

  // Fill name - should become enabled
  await page.locator('#name').fill('some-role')
  await expect(page.locator('button[type="submit"], .btn-primary')).toBeEnabled()
})

test('add and remove capabilities dynamically', async ({ authedPage: page }) => {
  await page.goto('/roles/new')
  await page.locator('#name').fill('cap-test')

  // Add two capabilities
  await page.locator('button', { hasText: '+ Add capability' }).click()
  await page.locator('button', { hasText: '+ Add capability' }).click()
  await expect(page.locator('.cap-row')).toHaveCount(2)

  // Remove the first one
  await page.locator('.cap-row').first().locator('.remove-btn').click()
  await expect(page.locator('.cap-row')).toHaveCount(1)
})

test('edit role loads existing data', async ({ authedPage: page }) => {
  const roleName = await createRoleViaApi(page, {
    description: 'My test description',
    capabilities: [{ capability: 'read', repo: '*' }],
  })

  try {
    await page.goto(`/roles/${roleName}`)
    await expect(page.locator('h2')).toContainText(roleName)
    await expect(page.locator('#description')).toHaveValue('My test description')

    // Capability row should be populated
    const capRow = page.locator('.cap-row').first()
    await expect(capRow).toBeVisible()
    await expect(capRow.locator('select')).toHaveValue('read')
    await expect(capRow.locator('.cap-repo')).toHaveValue('*')
  } finally {
    await deleteRoleViaApi(page, roleName)
  }
})

test('edit role save navigates to list', async ({ authedPage: page }) => {
  const roleName = await createRoleViaApi(page, { description: 'original' })

  try {
    await page.goto(`/roles/${roleName}`)
    await page.locator('#description').fill('updated description')
    await page.locator('button[type="submit"], .btn-primary').click()
    await expect(page).toHaveURL(/\/roles$/)
  } finally {
    await deleteRoleViaApi(page, roleName)
  }
})

test('delete role from role list', async ({ authedPage: page, uiScreenshot }) => {
  const roleName = await createRoleViaApi(page)

  await page.locator('nav a', { hasText: 'Roles' }).click()
  await expect(page.locator('td', { hasText: roleName })).toBeVisible()

  // Click delete on the role row
  const row = page.locator('tr', { hasText: roleName })
  await row.locator('.delete-btn').click()

  // Confirm deletion
  await expect(page.locator('.modal')).toBeVisible()
  await uiScreenshot('dialog-delete-role')
  await page.locator('.modal .btn-danger', { hasText: /delete/i }).click()

  // Role should be gone
  await expect(page.locator('td', { hasText: roleName })).not.toBeVisible()
})

test('admin role has no delete button', async ({ authedPage: page }) => {
  await page.locator('nav a', { hasText: 'Roles' }).click()
  // Find the row containing the admin role cell
  const adminCell = page.getByRole('cell', { name: 'admin', exact: true })
  await expect(adminCell).toBeVisible()
  const adminRow = page.locator('tr').filter({ has: adminCell })
  await expect(adminRow.locator('.delete-btn')).toHaveCount(0)
})
