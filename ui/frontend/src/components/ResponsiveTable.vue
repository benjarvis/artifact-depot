// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<template>
  <div class="table-wrapper">
    <slot />
  </div>
</template>

<style scoped>
.table-wrapper {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}
</style>
