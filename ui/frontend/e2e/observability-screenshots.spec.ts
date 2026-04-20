// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// Curated Explore screenshot capture for the Observability section of the
// docs site. Only covers Loki logs and Tempo traces — the Grafana
// dashboard screenshot is captured via Grafana's server-side Image
// Renderer (curl to /render/d/{uid}) in the harness script, not here.
//
// Runs against a `docker compose --profile monitoring` stack that has
// been seeded with `depot-bench demo` and is being driven by
// `depot-bench trickle`. Outputs to `OBS_SCREENSHOTS_OUT`.

import { test as base } from '@playwright/test'
import { mkdirSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'
import type { Page } from '@playwright/test'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = resolve(__dirname, '../../..')
const OUT = process.env.OBS_SCREENSHOTS_OUT
  ? resolve(process.env.OBS_SCREENSHOTS_OUT)
  : join(ROOT, 'build/observability-screenshots')

mkdirSync(OUT, { recursive: true })

const test = base
test.use({ viewport: { width: 1600, height: 1000 } })

// Dismiss Grafana's "Explore has moved" promo banner if visible.
async function dismissExploreBanner(page: Page) {
  await page
    .getByRole('button', { name: /^close$/i })
    .first()
    .click({ timeout: 3000 })
    .catch(() => {})
}

test.skip(
  !process.env.OBS_SCREENSHOTS_OUT,
  'Observability screenshots only run via scripts/observability-screenshots.sh.',
)

test.describe.serial('grafana explore screenshots', () => {
  // Each test navigates twice (light + dark) with ~12s of waiting per
  // theme. 90s gives plenty of headroom.
  test.setTimeout(90_000)

  test('loki log stream', async ({ page }) => {
    const left = JSON.stringify({
      datasource: 'loki',
      queries: [
        {
          refId: 'A',
          datasource: { type: 'loki', uid: 'loki' },
          editorMode: 'code',
          expr: '{service_name="depot"}',
          queryType: 'range',
        },
      ],
      range: { from: 'now-5m', to: 'now' },
    })
    for (const theme of ['light', 'dark'] as const) {
      await page.goto(
        `/explore?orgId=1&left=${encodeURIComponent(left)}&theme=${theme}`,
        { waitUntil: 'domcontentloaded', timeout: 30000 },
      )
      await page.waitForTimeout(10000)
      await dismissExploreBanner(page)
      await page.waitForTimeout(2000)
      await page.screenshot({
        path: join(OUT, `loki-logs-${theme}.png`),
        fullPage: false,
      })
    }
  })

  test('tempo trace search', async ({ page }) => {
    const left = JSON.stringify({
      datasource: 'tempo',
      queries: [
        {
          refId: 'A',
          datasource: { type: 'tempo', uid: 'tempo' },
          queryType: 'traceqlSearch',
          query: '{}',
        },
      ],
      range: { from: 'now-5m', to: 'now' },
    })
    for (const theme of ['light', 'dark'] as const) {
      await page.goto(
        `/explore?orgId=1&left=${encodeURIComponent(left)}&theme=${theme}`,
        { waitUntil: 'domcontentloaded', timeout: 30000 },
      )
      await page.waitForTimeout(10000)
      await dismissExploreBanner(page)
      await page.waitForTimeout(2000)
      await page.screenshot({
        path: join(OUT, `tempo-search-${theme}.png`),
        fullPage: false,
      })
    }
  })
})
