// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { Settings } from '../api'

export const useSettingsStore = defineStore('settings', () => {
  const settings = ref<Settings | null>(null)
  const loaded = ref(false)

  function applySnapshot(data: Settings) {
    settings.value = data
    loaded.value = true
  }

  function applyEvent(_event: string, payload: any) {
    settings.value = payload.settings ?? payload
  }

  return { settings, loaded, applySnapshot, applyEvent }
})
