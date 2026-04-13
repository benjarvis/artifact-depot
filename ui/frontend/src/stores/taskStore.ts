// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { TaskInfo } from '../api'

export const useTaskStore = defineStore('tasks', () => {
  const tasks = ref<TaskInfo[]>([])
  const loaded = ref(false)

  const activeTasks = computed(() =>
    tasks.value.filter(t => t.status === 'running' || t.status === 'pending')
  )

  function applySnapshot(data: TaskInfo[]) {
    tasks.value = data
    loaded.value = true
  }

  function applyEvent(event: string, payload: any) {
    switch (event) {
      case 'task_created': {
        const task = payload.task ?? payload
        tasks.value.unshift(task)
        break
      }
      case 'task_updated': {
        const task = payload.task ?? payload
        const idx = tasks.value.findIndex(t => t.id === task.id)
        if (idx >= 0) tasks.value[idx] = task
        else tasks.value.unshift(task)
        break
      }
      case 'task_progress': {
        const idx = tasks.value.findIndex(t => t.id === payload.id)
        if (idx >= 0) {
          tasks.value[idx].progress = payload.progress
          // Do NOT update status from progress events -- status transitions
          // come from TaskUpdated events. Progress events can arrive after
          // TaskUpdated(Completed) due to the bridge loop's timing, and would
          // flip the status back to Running if we applied them.
        }
        break
      }
      case 'task_deleted': {
        const id = payload.id
        tasks.value = tasks.value.filter(t => t.id !== id)
        break
      }
    }
  }

  return { tasks, loaded, activeTasks, applySnapshot, applyEvent }
})
