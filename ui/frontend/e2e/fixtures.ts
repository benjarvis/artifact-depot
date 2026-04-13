// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test as base, expect, type Page } from '@playwright/test'
import { writeFileSync, mkdirSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = resolve(__dirname, '../../..')
const SNAPSHOTS_DIR = join(ROOT, 'build/test/ui/snapshots')
const SCREENSHOTS_DIR = join(ROOT, 'build/test/ui/screenshots')
const NYC_OUTPUT = resolve(__dirname, '../.nyc_output')

/** Save Istanbul coverage from a page to the .nyc_output dir. */
async function saveCoverage(page: Page, testInfo: { title: string; testId: string }) {
  try {
    const coverage = await page.evaluate(() => (window as any).__coverage__)
    if (coverage) {
      mkdirSync(NYC_OUTPUT, { recursive: true })
      const safeName = testInfo.title.replace(/[^a-zA-Z0-9]/g, '-')
      const filePath = join(NYC_OUTPUT, `coverage-${safeName}-${testInfo.testId}.json`)
      writeFileSync(filePath, JSON.stringify(coverage))
    }
  } catch {
    // Page may have closed or coverage not available
  }
}

type Fixtures = {
  authedPage: Page
  snapshotA11y: (label?: string) => Promise<void>
  uiScreenshot: (label: string) => Promise<void>
}

export const test = base.extend<Fixtures>({
  authedPage: async ({ page }, use, testInfo) => {
    await page.goto('/login')
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
      // No password change required — already redirecting to /repositories
    }

    await page.waitForURL('**/repositories')
    await use(page)

    // Collect coverage while the page is still alive
    await saveCoverage(page, testInfo)
  },

  snapshotA11y: async ({ page }, use, testInfo) => {
    const fn = async (label?: string) => {
      mkdirSync(SNAPSHOTS_DIR, { recursive: true })
      const snapshot = await page.locator('body').ariaSnapshot()
      const suffix = label ? `-${label}` : ''
      const safeName = testInfo.title.replace(/[^a-zA-Z0-9]/g, '-')
      const filePath = join(SNAPSHOTS_DIR, `${safeName}${suffix}.txt`)
      writeFileSync(filePath, snapshot)
    }
    await use(fn)
  },

  uiScreenshot: async ({ page }, use) => {
    const fn = async (label: string) => {
      mkdirSync(SCREENSHOTS_DIR, { recursive: true })
      await page.screenshot({ path: join(SCREENSHOTS_DIR, `${label}.png`), fullPage: true })
      await page.emulateMedia({ colorScheme: 'dark' })
      await page.screenshot({ path: join(SCREENSHOTS_DIR, `${label}-dark.png`), fullPage: true })
      await page.emulateMedia({ colorScheme: 'light' })
    }
    await use(fn)
  },
})

// Collect Istanbul coverage after each test for non-authedPage tests
test.afterEach(async ({ page }, testInfo) => {
  await saveCoverage(page, testInfo)
})

export { expect }
