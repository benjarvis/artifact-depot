// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { api, type Role } from '../api'
import { useRoleStore } from '../stores/roleStore'
import ConfirmDialog from '../components/ConfirmDialog.vue'
import PageHeader from '../components/PageHeader.vue'
import ResponsiveTable from '../components/ResponsiveTable.vue'
import { formatDateShort as formatDate } from '../composables/useFormatters'

const router = useRouter()
const roleStore = useRoleStore()
const roles = computed(() => roleStore.roles)
const loading = computed(() => !roleStore.loaded)
const error = ref('')
const deleteTarget = ref('')
const showDeleteDialog = ref(false)
const deleting = ref(false)

function capSummary(role: Role): string {
  if (!role.capabilities.length) return 'none'
  const caps = role.capabilities.map(c => `${c.capability} on ${c.repo}`)
  return caps.join(', ')
}

function confirmDelete(name: string, e: Event) {
  e.stopPropagation()
  deleteTarget.value = name
  showDeleteDialog.value = true
}

async function doDelete() {
  deleting.value = true
  try {
    await api.deleteRole(deleteTarget.value)
    showDeleteDialog.value = false
  } catch (e: any) {
    error.value = e.message
  } finally {
    deleting.value = false
  }
}
</script>

<template>
  <section>
    <PageHeader>
      <template #actions>
        <router-link to="/roles/new" class="btn btn-primary">+ Create Role</router-link>
      </template>
    </PageHeader>

    <p v-if="error" class="error">{{ error }}</p>
    <p v-if="loading"><span class="loading-spinner"></span> Loading...</p>

    <ResponsiveTable v-else-if="roles.length > 0">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Description</th>
            <th>Capabilities</th>
            <th>Created</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="r in roles"
            :key="r.name"
            class="clickable-row"
            tabindex="0"
            @click="router.push({ name: 'role-detail', params: { name: r.name } })"
            @keydown.enter="router.push({ name: 'role-detail', params: { name: r.name } })"
          >
            <td><strong>{{ r.name }}</strong></td>
            <td>{{ r.description || '-' }}</td>
            <td class="text-truncate">{{ capSummary(r) }}</td>
            <td>{{ formatDate(r.created_at) }}</td>
            <td>
              <button v-if="r.name !== 'admin'" class="delete-btn" @click="confirmDelete(r.name, $event)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </ResponsiveTable>
    <p v-else>No roles yet.</p>

    <ConfirmDialog
      v-if="showDeleteDialog"
      title="Delete Role"
      message="Are you sure you want to delete"
      :item-name="deleteTarget"
      :loading="deleting"
      action-label="Delete"
      action-loading-label="Deleting..."
      action-variant="danger"
      @confirm="doDelete"
      @cancel="showDeleteDialog = false"
    />
  </section>
</template>

<style scoped>
.text-truncate {
  max-width: 400px;
}
</style>
