// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { Store } from '../api'

export const useBlobStoreStore = defineStore('blobStores', () => {
  const stores = ref<Store[]>([])
  const loaded = ref(false)

  function byName(name: string): Store | undefined {
    return stores.value.find(s => s.name === name)
  }

  function applySnapshot(data: Store[]) {
    stores.value = data
    loaded.value = true
  }

  function applyEvent(event: string, payload: any) {
    switch (event) {
      case 'store_created':
        stores.value.push(payload.store ?? payload)
        break
      case 'store_updated': {
        const store = payload.store ?? payload
        const idx = stores.value.findIndex(s => s.name === store.name)
        if (idx >= 0) stores.value[idx] = store
        else stores.value.push(store)
        break
      }
      case 'store_deleted': {
        const name = payload.name
        stores.value = stores.value.filter(s => s.name !== name)
        break
      }
      case 'store_stats_updated': {
        const idx = stores.value.findIndex(s => s.name === payload.name)
        if (idx >= 0) {
          stores.value[idx] = { ...stores.value[idx], blob_count: payload.blob_count, total_bytes: payload.total_bytes }
        }
        break
      }
    }
  }

  return { stores, loaded, byName, applySnapshot, applyEvent }
})
