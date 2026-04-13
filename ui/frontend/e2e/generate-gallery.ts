// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { readdirSync, writeFileSync } from 'fs'
import { join, resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const SCREENSHOTS_DIR = resolve(__dirname, '../../../build/test/ui/screenshots')

interface ScreenshotPair {
  label: string
  light: string
  dark: string | null
}

function categorize(label: string): string {
  if (label.startsWith('dialog-')) return 'Dialogs'
  if (['tasks-running', 'tasks-expanded', 'success-message', 'danger-zone'].includes(label)) return 'States'
  return 'Pages'
}

function main() {
  let files: string[]
  try {
    files = readdirSync(SCREENSHOTS_DIR).filter((f) => f.endsWith('.png'))
  } catch {
    console.error(`No screenshots found at ${SCREENSHOTS_DIR}`)
    process.exit(1)
  }

  // Group light/dark pairs
  const lightFiles = files.filter((f) => !f.endsWith('-dark.png'))
  const pairs: ScreenshotPair[] = lightFiles.map((f) => {
    const label = f.replace('.png', '')
    const darkFile = `${label}-dark.png`
    return {
      label,
      light: f,
      dark: files.includes(darkFile) ? darkFile : null,
    }
  }).sort((a, b) => a.label.localeCompare(b.label))

  // Group by category
  const categories = new Map<string, ScreenshotPair[]>()
  for (const pair of pairs) {
    const cat = categorize(pair.label)
    if (!categories.has(cat)) categories.set(cat, [])
    categories.get(cat)!.push(pair)
  }

  const timestamp = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC'

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>UI Screenshot Gallery</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 2rem; background: #f5f5f5; color: #333; }
  h1 { margin: 0 0 0.25rem; }
  .timestamp { color: #888; font-size: 0.85rem; margin-bottom: 2rem; }
  h2 { border-bottom: 2px solid #ddd; padding-bottom: 0.5rem; margin-top: 2.5rem; }
  .gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(700px, 1fr)); gap: 2rem; }
  .pair { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; }
  .pair h3 { margin: 0 0 0.75rem; font-size: 0.95rem; font-family: monospace; color: #555; }
  .images { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
  .images.single { grid-template-columns: 1fr; }
  .img-wrap { text-align: center; }
  .img-wrap img { max-width: 100%; border: 1px solid #e0e0e0; border-radius: 4px; cursor: pointer; }
  .img-label { font-size: 0.75rem; color: #888; margin-top: 0.25rem; }
  .stats { color: #888; font-size: 0.85rem; margin-bottom: 1rem; }
</style>
</head>
<body>
<h1>UI Screenshot Gallery</h1>
<p class="timestamp">Generated: ${timestamp}</p>
<p class="stats">${pairs.length} screenshots (${pairs.filter((p) => p.dark).length} with dark mode variants) -- ${pairs.length * 2 - pairs.filter((p) => !p.dark).length} total PNGs</p>
${Array.from(categories.entries())
  .map(
    ([cat, items]) => `
<h2>${cat} (${items.length})</h2>
<div class="gallery">
${items
  .map(
    (p) => `  <div class="pair">
    <h3>${p.label}</h3>
    <div class="images${p.dark ? '' : ' single'}">
      <div class="img-wrap">
        <a href="${p.light}" target="_blank"><img src="${p.light}" alt="${p.label} light" loading="lazy"></a>
        <div class="img-label">Light</div>
      </div>
${
  p.dark
    ? `      <div class="img-wrap">
        <a href="${p.dark}" target="_blank"><img src="${p.dark}" alt="${p.label} dark" loading="lazy"></a>
        <div class="img-label">Dark</div>
      </div>`
    : ''
}
    </div>
  </div>`
  )
  .join('\n')}
</div>`
  )
  .join('\n')}
</body>
</html>`

  const outPath = join(SCREENSHOTS_DIR, 'index.html')
  writeFileSync(outPath, html)
  console.log(`Gallery written to ${outPath} (${pairs.length} screenshots)`)
}

main()
