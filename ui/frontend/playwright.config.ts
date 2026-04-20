// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from '@playwright/test'

const chromiumOptions = {
  browserName: 'chromium' as const,
  launchOptions: {
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  },
}

export default defineConfig({
  testDir: './e2e',
  outputDir: '../../build/test/ui/results',
  workers: '50%',
  fullyParallel: true,
  retries: 1,
  reporter: [
    ['list'],
    ['html', { outputFolder: '../../build/test/ui/report', open: 'never' }],
    ['json', { outputFile: '../../build/test/ui/results.json' }],
  ],
  use: {
    baseURL: process.env.DEPOT_TEST_URL,
    trace: 'retain-on-failure',
    screenshot: 'on',
    video: 'retain-on-failure',
  },
  globalSetup: './e2e/global-setup.ts',
  globalTeardown: './e2e/global-teardown.ts',
  projects: [
    {
      name: 'parallel',
      testIgnore: /\b(tasks|settings|screenshots|observability-screenshots)\b.*\.spec\.ts$/,
      use: chromiumOptions,
    },
    {
      name: 'serial',
      testMatch: /\b(tasks|settings)\b.*\.spec\.ts$/,
      dependencies: ['parallel'],
      use: chromiumOptions,
    },
    // Curated screenshot capture for the docs site. Not part of `npx
    // playwright test` by default -- run via `make screenshots` (which
    // also seeds the depot with realistic data first).
    {
      name: 'screenshots',
      testMatch: '**/screenshots.spec.ts',
      use: chromiumOptions,
    },
    // Observability screenshots -- drives Chromium at Grafana to capture
    // dashboards / traces / logs from a monitoring-profile compose stack.
    // Run via `make observability-screenshots`.
    {
      name: 'observability',
      testMatch: '**/observability-screenshots.spec.ts',
      use: chromiumOptions,
    },
  ],
})
