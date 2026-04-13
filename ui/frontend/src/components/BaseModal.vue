// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useModal } from '../composables/useModal'

const props = withDefaults(defineProps<{
  title?: string
  maxWidth?: string
  contentClass?: string
  closeOnBackdrop?: boolean
  showClose?: boolean
  persistent?: boolean
}>(), {
  maxWidth: '400px',
  closeOnBackdrop: true,
  showClose: false,
  persistent: false,
})

const emit = defineEmits<{
  close: []
}>()

const modalRef = ref<HTMLElement | null>(null)
const titleId = computed(() => props.title ? `modal-title-${Math.random().toString(36).slice(2, 8)}` : undefined)

function close() {
  if (!props.persistent) {
    emit('close')
  }
}

function onBackdropClick() {
  if (props.closeOnBackdrop) {
    close()
  }
}

useModal(modalRef, close, props.persistent)
</script>

<template>
  <Teleport to="body">
    <div class="modal-overlay" @click.self="onBackdropClick">
      <div
        ref="modalRef"
        :class="['modal', contentClass]"
        :style="{ maxWidth }"
        role="dialog"
        aria-modal="true"
        :aria-labelledby="titleId"
      >
        <button v-if="showClose" class="modal-close" @click="close" aria-label="Close">&times;</button>
        <slot name="title">
          <h3 v-if="title" :id="titleId">{{ title }}</h3>
        </slot>
        <slot />
        <div v-if="$slots.footer" class="modal-actions">
          <slot name="footer" />
        </div>
      </div>
    </div>
  </Teleport>
</template>
