// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
defineProps<{
  title?: string
  backTo?: string
}>()
</script>

<template>
  <div class="page-header" :class="{ 'page-header--detail': backTo }">
    <router-link v-if="backTo" :to="backTo" class="back-link">&larr;</router-link>
    <h2 v-if="title">{{ title }}</h2>
    <slot name="badges" />
    <div v-if="$slots.actions" class="spacer"></div>
    <slot name="actions" />
  </div>
</template>

<style scoped>
.page-header--detail {
  justify-content: start;
  gap: 0.75rem;
}
.spacer {
  flex: 1;
}
</style>
