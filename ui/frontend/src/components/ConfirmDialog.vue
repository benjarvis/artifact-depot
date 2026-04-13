// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import BaseModal from './BaseModal.vue'

withDefaults(defineProps<{
  title: string
  message?: string
  itemName?: string
  error?: string
  loading?: boolean
  actionLabel?: string
  actionLoadingLabel?: string
  actionVariant?: 'danger' | 'primary'
  cancelLabel?: string
}>(), {
  actionLabel: 'Delete',
  actionLoadingLabel: 'Deleting…',
  actionVariant: 'danger',
  cancelLabel: 'Cancel',
})

const emit = defineEmits<{
  confirm: []
  cancel: []
}>()
</script>

<template>
  <BaseModal :title="title" @close="emit('cancel')">
    <p v-if="message">
      {{ message }} <strong v-if="itemName">{{ itemName }}</strong><span v-if="itemName">?</span>
    </p>
    <slot />
    <slot name="extra" />
    <p v-if="error" class="error">{{ error }}</p>
    <template #footer>
      <button class="btn" @click="emit('cancel')">{{ cancelLabel }}</button>
      <button
        class="btn"
        :class="actionVariant === 'danger' ? 'btn-danger' : 'btn-primary'"
        :disabled="loading"
        @click="emit('confirm')"
      >
        {{ loading ? actionLoadingLabel : actionLabel }}
      </button>
    </template>
  </BaseModal>
</template>
