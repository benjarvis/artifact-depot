// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// Curated screenshot capture for the docs site.
//
// This spec is NOT a regression test. It assumes the depot under
// `DEPOT_TEST_URL` has been seeded by `depot-bench demo` and is being
// driven by `depot-bench trickle` for live activity. Outputs one PNG per
// view to `SCREENSHOTS_OUT` (or `build/screenshots/` by default), which
// the harness in `scripts/screenshots.sh` then copies into
// `docs/screenshots/`.
//
// Only runs under the `screenshots` Playwright project so a normal
// `npx playwright test` doesn't spend its time here.

import { test } from './fixtures'
import { mkdirSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'
import type { Page } from '@playwright/test'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = resolve(__dirname, '../../..')
const OUT = process.env.SCREENSHOTS_OUT
  ? resolve(process.env.SCREENSHOTS_OUT)
  : join(ROOT, 'build/screenshots')

mkdirSync(OUT, { recursive: true })

// Capture a screenshot. Defaults to viewport-only (fullPage: false) so
// long content -- a populated artifact browser, the full Swagger spec --
// doesn't blow up the docs PNG into something tens of viewports tall.
// Pass `{ fullPage: true }` for short-content pages where seeing the
// whole thing matters (the repository list with the stats header on
// top, settings with all collapsibles, etc).
async function snap(page: Page, name: string, opts: { fullPage?: boolean } = {}) {
  await page.screenshot({
    path: join(OUT, `${name}.png`),
    fullPage: opts.fullPage ?? false,
  })
}

// Match the typical desktop browser used to view the docs.
test.use({ viewport: { width: 1440, height: 900 } })

// `make screenshots` (scripts/screenshots.sh) sets SCREENSHOTS_OUT after
// boot + seed + trickle. Without that env var the depot has no seeded
// content, so every screenshot would be uninteresting (or assertions
// would fail). Skip the whole file rather than produce blank PNGs.
test.skip(
  !process.env.SCREENSHOTS_OUT,
  'Screenshots only run via the screenshots harness (sets SCREENSHOTS_OUT).',
)

test.describe.serial('docs screenshots', () => {
  test('repositories list with stats header', async ({ authedPage: page }) => {
    await page.goto('/repositories')
    // demo should have populated the list; wait for the stats header to
    // render so the pie chart is in the screenshot.
    await page.locator('.stats-header .pie-card').waitFor({ timeout: 15000 })
    await page.locator('table tbody tr').first().waitFor()
    // fullPage so the whole stats header + repo list lands in one image.
    await snap(page, 'repositories', { fullPage: true })
  })

  test('repository detail browse', async ({ authedPage: page }) => {
    await page.goto('/repositories')
    await page.locator('table tbody tr').first().waitFor({ timeout: 15000 })
    // Click the first row -- demo's Docker repos are typically near the top
    // and have the most visually interesting tag/manifest layout, but any
    // populated repo works.
    await page.locator('table tbody tr').first().click()
    await page.locator('.detail-card').waitFor()
    // Default tab is Browse; wait for the artifact tree to render.
    await page.waitForTimeout(500)
    // Viewport-only -- the artifact tree can be very long.
    await snap(page, 'repository-detail')
  })

  test('stores list', async ({ authedPage: page }) => {
    await page.goto('/stores')
    await page.locator('table tbody tr').first().waitFor({ timeout: 15000 })
    await snap(page, 'stores', { fullPage: true })
  })

  test('store detail browse', async ({ authedPage: page }) => {
    await page.goto('/stores')
    // Store rows route via a `<router-link class="store-link">` on the
    // name cell, not by clicking the whole row.
    await page.locator('.store-link').first().waitFor({ timeout: 15000 })
    await page.locator('.store-link').first().click()
    await page.locator('.detail-card').waitFor({ timeout: 15000 })
    await page.waitForTimeout(500)
    // Viewport-only -- the blob list under a populated store is huge.
    await snap(page, 'store-detail')
  })

  test('tasks page with a recent integrity check', async ({ authedPage: page }) => {
    // Trigger an integrity check first so the page has interesting
    // content (running task or recent result) rather than three empty
    // singleton rows.
    await page.goto('/tasks')
    const checkCard = page.locator('.singleton-card').nth(1)
    await checkCard.locator('.singleton-name', { hasText: 'Integrity Check' })
      .waitFor({ timeout: 10000 })
    await checkCard.locator('button', { hasText: 'Start Now' }).click()
    const modal = page.locator('.modal-dialog')
    await modal.waitFor()
    // Default options (dry run + verify hashes) are fine for the screenshot.
    await modal.locator('button', { hasText: 'Start' }).click()
    await modal.waitFor({ state: 'hidden' })
    // Wait for either a running badge OR a completed last-result row to
    // render -- short check on a small dataset finishes quickly.
    await page.locator('.kind-badge, .last-result').first().waitFor({ timeout: 15000 })
    await snap(page, 'tasks', { fullPage: true })
  })

  test('settings page', async ({ authedPage: page }) => {
    await page.goto('/settings')
    // Wait for the first collapsible section to render so the page
    // isn't blank.
    await page.locator('h2, h3, .settings-section').first().waitFor({ timeout: 10000 })
    await snap(page, 'settings', { fullPage: true })
  })

  test('api docs (swagger)', async ({ authedPage: page }) => {
    await page.goto('/api-docs')
    await page.locator('.swagger-ui').waitFor({ timeout: 20000 })
    // Swagger streams in operations; give it a beat to settle.
    await page.waitForTimeout(2000)
    // Viewport-only -- the full OpenAPI spec is hundreds of viewports long.
    await snap(page, 'api-docs')
  })
})
