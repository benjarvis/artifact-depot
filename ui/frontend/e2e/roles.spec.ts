// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('default roles are visible in role list', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  await page.locator('nav a', { hasText: 'Roles' }).click()
  await expect(page).toHaveURL(/\/roles/)
  await expect(page.getByRole('cell', { name: 'admin', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'read-only', exact: true })).toBeVisible()
  await snapshotA11y()
  await uiScreenshot('role-list')
})
