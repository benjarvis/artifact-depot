// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { useRepoStore } from '../stores/repoStore'
import PageHeader from '../components/PageHeader.vue'
import PieChart from '../components/PieChart.vue'
import ResponsiveTable from '../components/ResponsiveTable.vue'
import { useAnimatedValue } from '../composables/useAnimatedValue'
import { formatBytes } from '../composables/useFormatters'

const router = useRouter()
const repoStore = useRepoStore()
const repos = computed(() => repoStore.repos)
const loading = computed(() => !repoStore.loaded)

const pieRepos = computed(() => repos.value.map(r => ({
  name: r.name,
  artifact_count: r.artifact_count,
  total_bytes: r.total_bytes,
})))
const animatedArtifactCount = useAnimatedValue(computed(() => repoStore.totalArtifacts))
const animatedTotalBytes = useAnimatedValue(computed(() => repoStore.totalBytes))

function typeClass(t: string): string {
  switch (t) {
    case 'hosted': return 'badge-green'
    case 'cache': return 'badge-orange'
    case 'proxy': return 'badge-blue'
    default: return 'badge-grey'
  }
}


</script>

<template>
  <section>
    <PageHeader>
      <template #actions>
        <router-link to="/repositories/new" class="btn btn-primary">+ Create Repository</router-link>
      </template>
    </PageHeader>

    <div v-if="!loading && repos.length > 0" class="stats-header">
      <div class="card pie-card">
        <h3>Storage by Repository</h3>
        <PieChart :repos="pieRepos" />
      </div>
      <div class="summary-cards">
        <div class="card stat-card">
          <div class="stat-label">Total Artifacts</div>
          <div class="stat-value">{{ Math.round(animatedArtifactCount).toLocaleString() }}</div>
        </div>
        <div class="card stat-card">
          <div class="stat-label">Total Size</div>
          <div class="stat-value">{{ formatBytes(animatedTotalBytes) }}</div>
        </div>
        <div class="card stat-card">
          <div class="stat-label">Repositories</div>
          <div class="stat-value">{{ repos.length }}</div>
        </div>
      </div>
    </div>

    <p v-if="loading"><span class="loading-spinner"></span> Loading...</p>

    <ResponsiveTable v-else-if="repos.length > 0">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Format</th>
            <th>Upstream URL</th>
            <th>Artifacts</th>
            <th>Size</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="r in repos"
            :key="r.name"
            class="clickable-row"
            tabindex="0"
            @click="router.push({ name: 'repository-detail', params: { name: r.name } })"
            @keydown.enter="router.push({ name: 'repository-detail', params: { name: r.name } })"
          >
            <td><strong>{{ r.name }}</strong></td>
            <td><span class="badge" :class="typeClass(r.repo_type)">{{ r.repo_type }}</span></td>
            <td><span class="badge badge-outlined">{{ r.format }}</span></td>
            <td class="text-truncate">{{ r.upstream_url || '-' }}</td>
            <td>{{ r.artifact_count.toLocaleString() }}</td>
            <td>{{ formatBytes(r.total_bytes) }}</td>
          </tr>
        </tbody>
      </table>
    </ResponsiveTable>
    <p v-else>No repositories yet.</p>
  </section>
</template>

<style scoped>
.text-truncate {
  max-width: 300px;
}
.stats-header {
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;
}
.pie-card {
  flex: 1;
  min-width: 0;
  margin-bottom: 0;
}
.summary-cards {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  min-width: 200px;
}
.stat-card {
  text-align: center;
  margin-bottom: 0;
}
.stat-label {
  font-size: 0.85rem;
  color: var(--color-text-faint);
  text-transform: uppercase;
  letter-spacing: 0.02em;
  margin-bottom: 0.25rem;
}
.stat-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--color-text-strong);
  font-family: 'SF Mono', 'Fira Code', monospace;
}
@media (max-width: 1024px) {
  .stats-header {
    flex-direction: column;
  }
  .summary-cards {
    flex-direction: row;
    min-width: auto;
  }
}
@media (max-width: 768px) {
  .summary-cards {
    flex-direction: column;
  }
}
</style>
