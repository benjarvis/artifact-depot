// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('admin user is visible in user list', async ({ authedPage: page, snapshotA11y, uiScreenshot }) => {
  await page.locator('nav a', { hasText: 'Users' }).click()
  await expect(page).toHaveURL(/\/users/)
  await expect(page.getByRole('cell', { name: 'admin', exact: true }).first()).toBeVisible()
  await snapshotA11y()
  await uiScreenshot('user-list')
})
