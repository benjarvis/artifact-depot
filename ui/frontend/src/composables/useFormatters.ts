// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  for (let i = 0; i < units.length - 1; i++) {
    const next = bytes / Math.pow(1024, i + 1)
    if (next < 10) return `${Math.round(bytes / Math.pow(1024, i))} ${units[i]}`
  }
  return `${Math.round(bytes / Math.pow(1024, units.length - 1))} ${units[units.length - 1]}`
}

export { formatBytes as formatSize }

export function formatDate(iso: string): string {
  if (!iso) return ''
  return new Date(iso).toLocaleString()
}

export function formatDateShort(iso: string): string {
  if (!iso) return ''
  return new Date(iso).toLocaleDateString()
}

export function formatTime(ts: string | null): string {
  if (!ts) return '-'
  return new Date(ts).toLocaleString()
}
