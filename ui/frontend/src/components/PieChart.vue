// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref, computed } from 'vue'
import { formatBytes } from '../composables/useFormatters'

interface RepoData {
  name: string
  artifact_count: number
  total_bytes: number
}

const props = defineProps<{ repos: RepoData[] }>()

const colors = [
  '#007bff', '#28a745', '#fd7e14', '#dc3545', '#6f42c1',
  '#20c997', '#e83e8c', '#17a2b8', '#ffc107', '#6c757d',
]

const hoveredSlice = ref<number | null>(null)
const tooltipX = ref(0)
const tooltipY = ref(0)

function onSliceEnter(index: number, event: MouseEvent) {
  hoveredSlice.value = index
  updateTooltipPos(event)
}

function onSliceMove(event: MouseEvent) {
  updateTooltipPos(event)
}

function onSliceLeave() {
  hoveredSlice.value = null
}

function updateTooltipPos(event: MouseEvent) {
  const container = (event.currentTarget as Element).closest('.pie-container') as HTMLElement
  if (!container) return
  const rect = container.getBoundingClientRect()
  tooltipX.value = event.clientX - rect.left + 12
  tooltipY.value = event.clientY - rect.top - 10
}

const slices = computed(() => {
  const nonEmpty = [...props.repos.filter(r => r.total_bytes > 0)]
    .sort((a, b) => b.total_bytes - a.total_bytes)
  if (nonEmpty.length === 0) return []

  const total = nonEmpty.reduce((s, r) => s + r.total_bytes, 0)
  const result: { name: string, artifacts: number, bytes: number, color: string, startAngle: number, endAngle: number }[] = []
  let cumAngle = 0

  for (let i = 0; i < nonEmpty.length; i++) {
    const frac = nonEmpty[i].total_bytes / total
    const sweep = frac * 360
    result.push({
      name: nonEmpty[i].name,
      artifacts: nonEmpty[i].artifact_count,
      bytes: nonEmpty[i].total_bytes,
      color: colors[i % colors.length],
      startAngle: cumAngle,
      endAngle: cumAngle + sweep,
    })
    cumAngle += sweep
  }
  return result
})

const legendColumns = computed(() => {
  const left: typeof slices.value = []
  const right: typeof slices.value = []
  for (let i = 0; i < slices.value.length; i++) {
    if (i % 2 === 0) left.push(slices.value[i])
    else right.push(slices.value[i])
  }
  return [left, right]
})

function arcPath(cx: number, cy: number, r: number, startAngle: number, endAngle: number): string {
  const startRad = (startAngle - 90) * Math.PI / 180
  const endRad = (endAngle - 90) * Math.PI / 180
  const x1 = cx + r * Math.cos(startRad)
  const y1 = cy + r * Math.sin(startRad)
  const x2 = cx + r * Math.cos(endRad)
  const y2 = cy + r * Math.sin(endRad)
  const largeArc = endAngle - startAngle > 180 ? 1 : 0
  return `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} Z`
}
</script>

<template>
  <div class="pie-container">
    <svg viewBox="0 0 200 200" class="pie-svg" v-if="slices.length > 0">
      <path v-if="slices.length === 1"
        :d="`M 100 100 m -80 0 a 80 80 0 1 0 160 0 a 80 80 0 1 0 -160 0`"
        :fill="slices[0].color"
        @mouseenter="onSliceEnter(0, $event)"
        @mousemove="onSliceMove"
        @mouseleave="onSliceLeave" />
      <path v-else v-for="(slice, i) in slices" :key="i"
        :d="arcPath(100, 100, 80, slice.startAngle, slice.endAngle)"
        :fill="slice.color"
        :opacity="hoveredSlice !== null && hoveredSlice !== i ? 0.5 : 1"
        @mouseenter="onSliceEnter(i, $event)"
        @mousemove="onSliceMove"
        @mouseleave="onSliceLeave" />
    </svg>
    <div v-else class="empty-pie">No data</div>
    <div class="legend" v-if="slices.length > 0">
      <div v-for="(col, ci) in legendColumns" :key="ci" class="legend-col">
        <div v-for="slice in col" :key="slice.name" class="legend-item">
          <span class="swatch" :style="{ background: slice.color }"></span>
          <span class="legend-name" :title="slice.name">{{ slice.name }}</span>
          <span class="legend-size">{{ formatBytes(slice.bytes) }}</span>
        </div>
      </div>
    </div>
    <div v-if="hoveredSlice !== null" class="pie-tooltip"
      :style="{ left: tooltipX + 'px', top: tooltipY + 'px' }">
      <strong>{{ slices[hoveredSlice].name }}</strong><br>
      {{ slices[hoveredSlice].artifacts.toLocaleString() }} artifacts<br>
      {{ formatBytes(slices[hoveredSlice].bytes) }}
    </div>
  </div>
</template>

<style scoped>
.pie-container {
  display: flex;
  align-items: center;
  gap: 1.5rem;
  position: relative;
}
.pie-svg {
  width: 180px;
  height: 180px;
  flex-shrink: 0;
}
.pie-svg path {
  transition: opacity 0.15s;
  cursor: pointer;
}
.pie-tooltip {
  position: absolute;
  background: rgba(0, 0, 0, 0.85);
  color: #f0f0f0;
  padding: 0.5rem 0.75rem;
  border-radius: 6px;
  font-size: 0.8rem;
  line-height: 1.4;
  pointer-events: none;
  white-space: nowrap;
  z-index: 10;
}
.empty-pie {
  width: 180px;
  height: 180px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--color-text-disabled);
  border: 2px dashed var(--color-border-strong);
  border-radius: 50%;
  font-size: 0.9rem;
}
.legend {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0 1.5rem;
  font-size: 0.85rem;
  max-height: 180px;
  overflow-y: auto;
  flex: 1;
  min-width: 0;
}
.legend-col {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}
.legend-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.swatch {
  width: 12px;
  height: 12px;
  border-radius: 2px;
  flex-shrink: 0;
}
.legend-name {
  color: var(--color-text-strong);
  font-weight: 500;
  font-size: 0.85rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
}
.legend-size {
  color: var(--color-text-muted);
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 0.85rem;
  flex-shrink: 0;
}
</style>
