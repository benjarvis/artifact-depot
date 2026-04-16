// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { Role } from '../api'

export const useRoleStore = defineStore('roles', () => {
  const roles = ref<Role[]>([])
  const loaded = ref(false)

  function byName(name: string): Role | undefined {
    return roles.value.find(r => r.name === name)
  }

  function applySnapshot(data: Role[]) {
    roles.value = data
    loaded.value = true
  }

  function applyEvent(event: string, payload: any) {
    switch (event) {
      case 'role_created': {
        const role = payload.role ?? payload
        if (!roles.value.some(r => r.name === role.name)) {
          roles.value.push(role)
        }
        break
      }
      case 'role_updated': {
        const role = payload.role ?? payload
        const idx = roles.value.findIndex(r => r.name === role.name)
        if (idx >= 0) roles.value[idx] = role
        else roles.value.push(role)
        break
      }
      case 'role_deleted': {
        const name = payload.name
        roles.value = roles.value.filter(r => r.name !== name)
        break
      }
    }
  }

  return { roles, loaded, byName, applySnapshot, applyEvent }
})
