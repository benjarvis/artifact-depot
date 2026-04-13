// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { test, expect } from './fixtures'

test('artifact browser columns are wide enough for content', async ({ authedPage: page }) => {
  const repoName = `layout-test-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`

  // Get auth token from the logged-in page
  const token = await page.evaluate(() => localStorage.getItem('depot_token'))
  const headers = { Authorization: `Bearer ${token}` }

  // Create a hosted raw repo via API
  const createResp = await page.request.post('/api/v1/repositories', {
    headers,
    data: { name: repoName, repo_type: 'hosted', format: 'raw', store: 'default' },
  })
  expect(createResp.ok()).toBeTruthy()

  // Upload artifacts with realistic paths and content types
  const artifacts = [
    { path: 'docs/architecture-overview.pdf', type: 'application/pdf' },
    { path: 'releases/v2.1.0/depot-linux-amd64.tar.gz', type: 'application/gzip' },
    { path: 'config/production.yaml', type: 'application/x-yaml' },
    { path: 'images/logo.png', type: 'image/png' },
    { path: 'lib/commons-lang3-3.12.0.jar', type: 'application/java-archive' },
  ]

  for (const art of artifacts) {
    const size = 1024 + Math.floor(Math.random() * 50000)
    const body = Buffer.alloc(size, 'x')
    const resp = await page.request.put(`/repository/${repoName}/${art.path}`, {
      headers: { ...headers, 'Content-Type': art.type },
      data: body,
    })
    expect(resp.ok()).toBeTruthy()
  }

  // Navigate to repo detail and wait for artifact table to load
  await page.goto(`/repositories/${repoName}`)
  await expect(page.locator('.artifact-browser table')).toBeVisible({ timeout: 10000 })

  // Helper: check that no table cell has its text clipped (scrollWidth > clientWidth)
  async function assertNoTruncation() {
    const issues = await page.evaluate(() => {
      const rows = document.querySelectorAll('.artifact-browser tbody tr')
      const result: string[] = []
      rows.forEach((row, rowIdx) => {
        row.querySelectorAll('td').forEach((cell, colIdx) => {
          if (cell.scrollWidth > cell.clientWidth + 2) {
            result.push(
              `Row ${rowIdx} Col ${colIdx} ("${cell.textContent?.trim().slice(0, 40)}") ` +
              `scrollW=${cell.scrollWidth} clientW=${cell.clientWidth}`
            )
          }
        })
      })
      return result
    })
    expect(issues, 'No table cells should have truncated content').toEqual([])
  }

  // Verify directory-level rows (Size with item counts, Folder type, timestamps) fit
  await assertNoTruncation()

  // Verify "Folder" is shown as content type for directories
  const folderCells = page.locator('.artifact-browser tbody td:nth-child(3)')
  await expect(folderCells.first()).toHaveText('Folder')

  // Navigate into a directory to check file-level rows fit too
  await page.locator('.artifact-browser tbody tr').first().click()
  await expect(page.locator('.artifact-browser table')).toBeVisible({ timeout: 10000 })
  await assertNoTruncation()

  // Clean up
  await page.request.delete(`/api/v1/repositories/${repoName}`, { headers })
})
