// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { Repo } from '../api'

export const useRepoStore = defineStore('repos', () => {
  const repos = ref<Repo[]>([])
  const loaded = ref(false)

  const totalArtifacts = computed(() =>
    repos.value.reduce((sum, r) => sum + r.artifact_count, 0)
  )
  const totalBytes = computed(() =>
    repos.value.reduce((sum, r) => sum + r.total_bytes, 0)
  )

  function byName(name: string): Repo | undefined {
    return repos.value.find(r => r.name === name)
  }

  function applySnapshot(data: Repo[]) {
    repos.value = data
    loaded.value = true
  }

  function applyEvent(event: string, payload: any) {
    switch (event) {
      case 'repo_created': {
        const repo = payload.repo ?? payload
        if (!repos.value.some(r => r.name === repo.name)) {
          repos.value.push(repo)
        }
        break
      }
      case 'repo_updated': {
        const repo = payload.repo ?? payload
        const idx = repos.value.findIndex(r => r.name === repo.name)
        if (idx >= 0) repos.value[idx] = repo
        else repos.value.push(repo)
        break
      }
      case 'repo_deleted': {
        const name = payload.name
        repos.value = repos.value.filter(r => r.name !== name)
        break
      }
      case 'repo_stats_updated': {
        const idx = repos.value.findIndex(r => r.name === payload.name)
        if (idx >= 0) {
          repos.value[idx].artifact_count = payload.artifact_count
          repos.value[idx].total_bytes = payload.total_bytes
        }
        break
      }
    }
  }

  return { repos, loaded, totalArtifacts, totalBytes, byName, applySnapshot, applyEvent }
})
