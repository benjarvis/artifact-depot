// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '../api'
import { useUserStore } from '../stores/userStore'
import ConfirmDialog from '../components/ConfirmDialog.vue'
import PageHeader from '../components/PageHeader.vue'
import ResponsiveTable from '../components/ResponsiveTable.vue'
import { formatDateShort as formatDate } from '../composables/useFormatters'

const router = useRouter()
const userStore = useUserStore()
const users = computed(() => userStore.users)
const loading = computed(() => !userStore.loaded)
const error = ref('')
const deleteTarget = ref('')
const showDeleteDialog = ref(false)
const deleting = ref(false)

function confirmDelete(username: string, e: Event) {
  e.stopPropagation()
  deleteTarget.value = username
  showDeleteDialog.value = true
}

async function doDelete() {
  deleting.value = true
  try {
    await api.deleteUser(deleteTarget.value)
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
        <router-link to="/users/new" class="btn btn-primary">+ Create User</router-link>
      </template>
    </PageHeader>

    <p v-if="error" class="error">{{ error }}</p>
    <p v-if="loading"><span class="loading-spinner"></span> Loading...</p>

    <ResponsiveTable v-else-if="users.length > 0">
      <table>
        <thead>
          <tr>
            <th>Username</th>
            <th>Roles</th>
            <th>Created</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="u in users"
            :key="u.username"
            class="clickable-row"
            tabindex="0"
            @click="router.push({ name: 'user-detail', params: { username: u.username } })"
            @keydown.enter="router.push({ name: 'user-detail', params: { username: u.username } })"
          >
            <td><strong>{{ u.username }}</strong></td>
            <td>
              <span v-for="r in u.roles" :key="r" class="badge badge-blue">{{ r }}</span>
              <span v-if="!u.roles.length" class="text-muted">none</span>
            </td>
            <td>{{ formatDate(u.created_at) }}</td>
            <td>
              <button v-if="u.username !== 'admin' && u.username !== 'anonymous'" class="delete-btn" @click="confirmDelete(u.username, $event)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </ResponsiveTable>
    <p v-else>No users yet.</p>

    <ConfirmDialog
      v-if="showDeleteDialog"
      title="Delete User"
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
.badge {
  margin-right: 0.25rem;
}
</style>
