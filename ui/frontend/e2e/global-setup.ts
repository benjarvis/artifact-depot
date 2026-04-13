// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { execSync, spawn } from 'child_process'
import { mkdirSync, writeFileSync, rmSync, existsSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'
import http from 'http'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = resolve(__dirname, '../../..')
const BUILD_DIR = join(ROOT, 'build/test/ui')
const STATE_FILE = join(BUILD_DIR, '.server-state.json')

function waitForHealth(url: string, timeoutMs = 30000): Promise<void> {
  const start = Date.now()
  return new Promise((resolve, reject) => {
    const check = () => {
      const req = http.get(`${url}/api/v1/health`, (res) => {
        if (res.statusCode === 200) {
          resolve()
        } else if (Date.now() - start > timeoutMs) {
          reject(new Error(`Server not healthy after ${timeoutMs}ms`))
        } else {
          setTimeout(check, 250)
        }
      })
      req.on('error', () => {
        if (Date.now() - start > timeoutMs) {
          reject(new Error(`Server not healthy after ${timeoutMs}ms`))
        } else {
          setTimeout(check, 250)
        }
      })
    }
    check()
  })
}

export default async function globalSetup() {
  // When launched by scripts/ui-test.sh, the server is already running
  // inside a network namespace and DEPOT_TEST_URL is set.
  if (process.env.DEPOT_UI_SKIP_SETUP === '1') {
    return
  }

  const frontendDir = resolve(__dirname, '..')
  const nycOutput = join(frontendDir, '.nyc_output')

  // Clean prior coverage data
  if (existsSync(nycOutput)) {
    rmSync(nycOutput, { recursive: true })
  }
  mkdirSync(nycOutput, { recursive: true })

  // Build instrumented frontend
  console.log('Building instrumented frontend...')
  execSync('npm run build:test', { cwd: frontendDir, stdio: 'inherit' })

  // Rebuild Rust binary to embed instrumented dist/
  console.log('Rebuilding depot binary...')
  execSync('cargo build -p depot', { cwd: ROOT, stdio: 'inherit' })

  // Create temp dirs
  mkdirSync(BUILD_DIR, { recursive: true })
  const tmpDir = join(BUILD_DIR, 'server-data')
  if (existsSync(tmpDir)) {
    rmSync(tmpDir, { recursive: true })
  }
  mkdirSync(tmpDir, { recursive: true })

  // Write server config
  const configPath = join(tmpDir, 'depotd.toml')
  const dbPath = join(tmpDir, 'depot.redb')
  const blobRoot = join(tmpDir, 'blobs')
  mkdirSync(blobRoot, { recursive: true })

  writeFileSync(
    configPath,
    `listen = "127.0.0.1:0"\ndb_path = "${dbPath}"\nblob_root = "${blobRoot}"\ndefault_admin_password = "admin"\n`,
  )

  // Spawn depot server
  console.log('Starting depot server...')
  const binary = join(ROOT, 'target/debug/depot')
  const server = spawn(binary, ['-c', configPath], {
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false,
  })

  // Parse listening address from stdout+stderr (tracing output may go to either)
  const baseURL = await new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timed out waiting for server to print listening address'))
    }, 30000)

    let outputBuf = ''
    const onData = (chunk: Buffer) => {
      const text = chunk.toString()
      outputBuf += text
      process.stderr.write(text)
      // Strip ANSI escape codes before matching
      const stripped = outputBuf.replace(/\x1b\[[0-9;]*m/g, '')
      const match = stripped.match(/listening on (http:\/\/[^\s]+)/)
      if (match) {
        clearTimeout(timeout)
        resolve(match[1])
      }
    }

    server.stdout!.on('data', onData)
    server.stderr!.on('data', onData)

    server.on('error', (err) => {
      clearTimeout(timeout)
      reject(err)
    })

    server.on('exit', (code) => {
      clearTimeout(timeout)
      reject(new Error(`Server exited with code ${code} before printing address`))
    })
  })

  console.log(`Server listening at ${baseURL}`)

  // Wait for health endpoint
  await waitForHealth(baseURL)
  console.log('Server is healthy')

  // Write state for teardown and tests
  writeFileSync(
    STATE_FILE,
    JSON.stringify({ pid: server.pid, tmpDir, baseURL }),
  )

  // Set env var for Playwright config
  process.env.DEPOT_TEST_URL = baseURL
}
