// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { User } from '../api'

export const useUserStore = defineStore('users', () => {
  const users = ref<User[]>([])
  const loaded = ref(false)

  function byUsername(username: string): User | undefined {
    return users.value.find(u => u.username === username)
  }

  function applySnapshot(data: User[]) {
    users.value = data
    loaded.value = true
  }

  function applyEvent(event: string, payload: any) {
    switch (event) {
      case 'user_created': {
        const user = payload.user ?? payload
        if (!users.value.some(u => u.username === user.username)) {
          users.value.push(user)
        }
        break
      }
      case 'user_updated': {
        const user = payload.user ?? payload
        const idx = users.value.findIndex(u => u.username === user.username)
        if (idx >= 0) users.value[idx] = user
        else users.value.push(user)
        break
      }
      case 'user_deleted': {
        const username = payload.username
        users.value = users.value.filter(u => u.username !== username)
        break
      }
    }
  }

  return { users, loaded, byUsername, applySnapshot, applyEvent }
})
