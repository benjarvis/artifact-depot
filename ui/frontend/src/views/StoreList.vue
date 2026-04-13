// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '../api'
import { useBlobStoreStore } from '../stores/blobStoreStore'
import ConfirmDialog from '../components/ConfirmDialog.vue'
import PageHeader from '../components/PageHeader.vue'
import ResponsiveTable from '../components/ResponsiveTable.vue'
import { formatDateShort as formatDate, formatBytes } from '../composables/useFormatters'

const blobStoreStore = useBlobStoreStore()
const stores = computed(() => blobStoreStore.stores)
const loading = computed(() => !blobStoreStore.loaded)
const error = ref('')
const deleteTarget = ref('')
const showDeleteDialog = ref(false)
const deleting = ref(false)
const deleteError = ref('')

function confirmDelete(name: string, e: Event) {
  e.stopPropagation()
  deleteTarget.value = name
  deleteError.value = ''
  showDeleteDialog.value = true
}

async function doDelete() {
  deleting.value = true
  deleteError.value = ''
  try {
    await api.deleteStore(deleteTarget.value)
    showDeleteDialog.value = false
  } catch (e: any) {
    deleteError.value = e.message
  } finally {
    deleting.value = false
  }
}
</script>

<template>
  <section>
    <PageHeader>
      <template #actions>
        <router-link to="/stores/new" class="btn btn-primary">+ Create Store</router-link>
      </template>
    </PageHeader>

    <p v-if="error" class="error">{{ error }}</p>
    <p v-if="loading"><span class="loading-spinner"></span> Loading...</p>

    <ResponsiveTable v-else-if="stores.length > 0">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Blobs</th>
            <th>Size</th>
            <th>Created</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in stores" :key="s.name">
            <td><router-link :to="`/stores/${encodeURIComponent(s.name)}`" class="store-link"><strong>{{ s.name }}</strong></router-link></td>
            <td><span class="badge" :class="s.store_type === 'file' ? 'badge-green' : 'badge-blue'">{{ s.store_type }}</span></td>
            <td>{{ s.blob_count != null ? s.blob_count.toLocaleString() : '\u2014' }}</td>
            <td>{{ s.total_bytes != null ? formatBytes(s.total_bytes) : '\u2014' }}</td>
            <td>{{ formatDate(s.created_at) }}</td>
            <td>
              <button class="delete-btn" @click="confirmDelete(s.name, $event)">Delete</button>
            </td>
          </tr>
        </tbody>
      </table>
    </ResponsiveTable>
    <p v-else>No stores configured.</p>

    <ConfirmDialog
      v-if="showDeleteDialog"
      title="Delete Store"
      message="Are you sure you want to delete"
      :item-name="deleteTarget"
      :error="deleteError"
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
.store-link {
  color: var(--color-text);
  text-decoration: none;
}
.store-link:hover {
  text-decoration: underline;
}
</style>
