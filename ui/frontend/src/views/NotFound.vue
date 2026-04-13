// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<template>
  <div class="not-found">
    <h2>Page Not Found</h2>
    <p>The page you requested does not exist.</p>
    <router-link to="/" class="btn btn-primary">Go to Repositories</router-link>
  </div>
</template>

<style scoped>
.not-found {
  text-align: center;
  padding: 4rem 2rem;
}
.not-found h2 {
  margin-bottom: 0.5rem;
}
.not-found p {
  color: var(--color-text-muted);
  margin-bottom: 1.5rem;
}
</style>
