// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('api docs page renders in light and dark mode', async ({
  authedPage: page,
  uiScreenshot,
}) => {
  await page.goto('/api-docs')

  // Wait for Swagger UI to finish loading (at least one operation block).
  await page.locator('.swagger-ui .opblock').first().waitFor({ state: 'visible' })

  // Our custom JSON link should be present and precede the Swagger UI block.
  await expect(page.locator('.api-docs-header a')).toHaveText(/OpenAPI spec/)

  await uiScreenshot('api-docs')
})

test('api docs expanded operation renders in light and dark mode', async ({
  authedPage: page,
  uiScreenshot,
}) => {
  await page.goto('/api-docs')

  // Expand the first operation so we can verify the inner detail panels.
  const firstOp = page.locator('.swagger-ui .opblock').first()
  await firstOp.waitFor({ state: 'visible' })
  await firstOp.locator('.opblock-summary').click()
  await firstOp.locator('.opblock-body').waitFor({ state: 'visible' })

  await uiScreenshot('api-docs-expanded')
})

test('api docs schemas section renders in light and dark mode', async ({
  authedPage: page,
  uiScreenshot,
}) => {
  await page.goto('/api-docs')

  // Wait for operations to render so the page is fully built out.
  await page.locator('.swagger-ui .opblock').first().waitFor({ state: 'visible' })

  // Swagger UI renders a Schemas block after all tag groups.
  const schemasSection = page.locator('.swagger-ui section.models')
  await schemasSection.waitFor({ state: 'visible' })
  await schemasSection.scrollIntoViewIfNeeded()

  await uiScreenshot('api-docs-schemas')
})
