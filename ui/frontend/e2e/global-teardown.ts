// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { readFileSync, rmSync, existsSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const BUILD_DIR = join(resolve(__dirname, '../../..'), 'build/test/ui')
const STATE_FILE = join(BUILD_DIR, '.server-state.json')

export default async function globalTeardown() {
  // When launched by scripts/ui-test.sh, the bash script handles cleanup.
  if (process.env.DEPOT_UI_SKIP_SETUP === '1') {
    return
  }

  if (!existsSync(STATE_FILE)) {
    return
  }

  const state = JSON.parse(readFileSync(STATE_FILE, 'utf-8'))

  // Kill server process
  if (state.pid) {
    try {
      process.kill(state.pid, 'SIGTERM')
      console.log(`Stopped depot server (pid ${state.pid})`)
    } catch {
      // Already exited
    }
  }

  // Clean up temp dir
  if (state.tmpDir && existsSync(state.tmpDir)) {
    rmSync(state.tmpDir, { recursive: true })
  }

  // Remove state file
  rmSync(STATE_FILE, { force: true })
}
