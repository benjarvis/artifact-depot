// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import type { Page } from '@playwright/test'

/** Short random suffix for parallel-safe unique naming. */
function uid(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
}

/** Extract auth headers from a logged-in page's localStorage token. */
export async function apiHeaders(page: Page): Promise<Record<string, string>> {
  const token = await page.evaluate(() => localStorage.getItem('depot_token'))
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  }
}

/** Create a repository via API. Returns the repo name. */
export async function createRepoViaApi(
  page: Page,
  opts: {
    name?: string
    repo_type?: 'hosted' | 'cache' | 'proxy'
    format?: 'raw' | 'docker' | 'cargo' | 'pypi' | 'apt' | 'golang' | 'helm' | 'yum' | 'npm'
    upstream_url?: string
    cleanup_max_age_days?: number
    cleanup_max_unaccessed_days?: number
    members?: string[]
    write_member?: string
  } = {},
): Promise<string> {
  const name = opts.name ?? `test-repo-${uid()}`
  const headers = await apiHeaders(page)
  const data: Record<string, unknown> = {
    name,
    repo_type: opts.repo_type ?? 'hosted',
    format: opts.format ?? 'raw',
    store: 'default',
  }
  if (opts.upstream_url) data.upstream_url = opts.upstream_url
  if (opts.cleanup_max_age_days) data.cleanup_max_age_days = opts.cleanup_max_age_days
  if (opts.cleanup_max_unaccessed_days) data.cleanup_max_unaccessed_days = opts.cleanup_max_unaccessed_days
  if (opts.members) data.members = opts.members
  if (opts.write_member) data.write_member = opts.write_member

  const resp = await page.request.post('/api/v1/repositories', {
    headers,
    data,
  })
  if (!resp.ok()) throw new Error(`Failed to create repo ${name}: ${resp.status()}`)
  return name
}

/** Delete a repository via API (ignores 404). */
export async function deleteRepoViaApi(page: Page, name: string): Promise<void> {
  const headers = await apiHeaders(page)
  await page.request.delete(`/api/v1/repositories/${encodeURIComponent(name)}`, { headers })
}

/** Create a file store via API. Returns the store name. */
export async function createStoreViaApi(
  page: Page,
  opts: { name?: string; root?: string } = {},
): Promise<string> {
  const name = opts.name ?? `test-store-${uid()}`
  const headers = await apiHeaders(page)
  const resp = await page.request.post('/api/v1/stores', {
    headers,
    data: {
      name,
      store_type: 'file',
      root: opts.root ?? `/tmp/depot-test-${name}`,
    },
  })
  if (!resp.ok()) throw new Error(`Failed to create store ${name}: ${resp.status()}`)
  return name
}

/** Delete a store via API (ignores 404). */
export async function deleteStoreViaApi(page: Page, name: string): Promise<void> {
  const headers = await apiHeaders(page)
  await page.request.delete(`/api/v1/stores/${encodeURIComponent(name)}`, { headers })
}

/** Create a user via API. Returns the username. */
export async function createUserViaApi(
  page: Page,
  opts: { username?: string; password?: string; roles?: string[] } = {},
): Promise<string> {
  const username = opts.username ?? `test-user-${uid()}`
  const headers = await apiHeaders(page)
  const resp = await page.request.post('/api/v1/users', {
    headers,
    data: {
      username,
      password: opts.password ?? 'testpass123',
      roles: opts.roles ?? [],
    },
  })
  if (!resp.ok()) throw new Error(`Failed to create user ${username}: ${resp.status()}`)
  return username
}

/** Delete a user via API (ignores 404). */
export async function deleteUserViaApi(page: Page, username: string): Promise<void> {
  const headers = await apiHeaders(page)
  await page.request.delete(`/api/v1/users/${encodeURIComponent(username)}`, { headers })
}

/** Create a role via API. Returns the role name. */
export async function createRoleViaApi(
  page: Page,
  opts: {
    name?: string
    description?: string
    capabilities?: Array<{ capability: string; repo: string }>
  } = {},
): Promise<string> {
  const name = opts.name ?? `test-role-${uid()}`
  const headers = await apiHeaders(page)
  const resp = await page.request.post('/api/v1/roles', {
    headers,
    data: {
      name,
      description: opts.description ?? '',
      capabilities: opts.capabilities ?? [],
    },
  })
  if (!resp.ok()) throw new Error(`Failed to create role ${name}: ${resp.status()}`)
  return name
}

/** Delete a role via API (ignores 404). */
export async function deleteRoleViaApi(page: Page, name: string): Promise<void> {
  const headers = await apiHeaders(page)
  await page.request.delete(`/api/v1/roles/${encodeURIComponent(name)}`, { headers })
}

/** Set gc_min_interval_secs to a low value so GC can be re-triggered quickly in tests. */
export async function resetGcLockoutViaApi(page: Page): Promise<void> {
  const headers = await apiHeaders(page)
  const resp = await page.request.get('/api/v1/settings', { headers })
  if (!resp.ok()) return
  const settings = await resp.json()
  settings.gc_min_interval_secs = 60
  await page.request.put('/api/v1/settings', { headers, data: settings })
}

/** Delete all tasks via API. */
export async function deleteAllTasksViaApi(page: Page): Promise<void> {
  const headers = await apiHeaders(page)
  const resp = await page.request.get('/api/v1/tasks', { headers })
  if (!resp.ok()) return
  const tasks = await resp.json()
  for (const task of tasks) {
    await page.request.delete(`/api/v1/tasks/${encodeURIComponent(task.id)}`, { headers })
  }
}

/** Wait for at least one task to appear as running/pending in the API. */
export async function waitForTaskRunning(
  page: Page,
  timeoutMs: number = 10000,
): Promise<void> {
  const headers = await apiHeaders(page)
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const resp = await page.request.get('/api/v1/tasks', { headers })
    if (resp.ok()) {
      const tasks = await resp.json()
      const running = tasks.some(
        (t: { status: string }) => t.status === 'running' || t.status === 'pending',
      )
      if (running) return
    }
    await page.waitForTimeout(250)
  }
}

/** Wait for a task to complete by polling the task list. Returns when task with given kind is done. */
export async function waitForTaskCompletion(
  page: Page,
  timeoutMs: number = 30000,
): Promise<void> {
  const headers = await apiHeaders(page)
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const resp = await page.request.get('/api/v1/tasks', { headers })
    if (resp.ok()) {
      const tasks = await resp.json()
      const running = tasks.some(
        (t: { status: string }) => t.status === 'running' || t.status === 'pending',
      )
      if (!running) return
    }
    await page.waitForTimeout(500)
  }
  throw new Error('Timed out waiting for task completion')
}
