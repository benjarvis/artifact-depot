// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { HealthStatus } from '../api'

export const useHealthStore = defineStore('health', () => {
  const health = ref<HealthStatus | null>(null)

  function applySnapshot(data: HealthStatus) {
    health.value = data
  }

  function applyEvent(_event: string, payload: any) {
    health.value = payload
  }

  return { health, applySnapshot, applyEvent }
})
