// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<template>
  <div class="api-docs-header">
    <a href="/api-docs/openapi.json" target="_blank" rel="noopener">OpenAPI spec (JSON)</a>
  </div>
  <div id="swagger-ui-container"></div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted } from 'vue'
import SwaggerUI from 'swagger-ui-dist/swagger-ui-es-bundle.js'
import 'swagger-ui-dist/swagger-ui.css'
import { getToken } from '../api'

// Swagger UI ships a built-in dark theme keyed off `html.dark-mode`. We toggle
// the class on <html> based on the user's color-scheme preference while this
// view is mounted, and remove it on teardown so other pages aren't affected.
const darkModeQuery =
  typeof window !== 'undefined' && window.matchMedia
    ? window.matchMedia('(prefers-color-scheme: dark)')
    : null

function applyDarkMode(match: MediaQueryList | MediaQueryListEvent) {
  document.documentElement.classList.toggle('dark-mode', match.matches)
}

const onSchemeChange = (ev: MediaQueryListEvent) => applyDarkMode(ev)

onMounted(() => {
  if (darkModeQuery) {
    applyDarkMode(darkModeQuery)
    darkModeQuery.addEventListener('change', onSchemeChange)
  }

  SwaggerUI({
    dom_id: '#swagger-ui-container',
    url: '/api-docs/openapi.json',
    deepLinking: true,
    presets: [SwaggerUI.presets.apis],
    layout: 'BaseLayout',
    requestInterceptor: (req: { headers: Record<string, string> }) => {
      const token = getToken()
      if (token) {
        req.headers['Authorization'] = `Bearer ${token}`
      }
      return req
    },
  })
})

onBeforeUnmount(() => {
  if (darkModeQuery) {
    darkModeQuery.removeEventListener('change', onSchemeChange)
  }
  document.documentElement.classList.remove('dark-mode')
})
</script>

<style scoped>
.api-docs-header {
  padding: 0.75rem 1.5rem;
  font-size: 0.95rem;
}
#swagger-ui-container :deep(.swagger-ui .topbar),
#swagger-ui-container :deep(.swagger-ui .info),
#swagger-ui-container :deep(.swagger-ui .scheme-container) {
  display: none;
}

/* ── Dark-mode color overrides for Swagger UI ──
   Swagger UI ships a light-only stylesheet; these rules re-theme it to match
   the app's dark palette when the user's system prefers dark. */
@media (prefers-color-scheme: dark) {
  #swagger-ui-container :deep(.swagger-ui),
  #swagger-ui-container :deep(.swagger-ui .opblock-tag),
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-operation-id),
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-path),
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-path__deprecated),
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-description),
  #swagger-ui-container :deep(.swagger-ui .opblock-description-wrapper),
  #swagger-ui-container :deep(.swagger-ui .opblock-description-wrapper p),
  #swagger-ui-container :deep(.swagger-ui .opblock-external-docs-wrapper),
  #swagger-ui-container :deep(.swagger-ui .opblock-title_normal),
  #swagger-ui-container :deep(.swagger-ui .opblock-title_normal p),
  #swagger-ui-container :deep(.swagger-ui .opblock-section-header),
  #swagger-ui-container :deep(.swagger-ui .opblock-section-header h4),
  #swagger-ui-container :deep(.swagger-ui .opblock-section-header label),
  #swagger-ui-container :deep(.swagger-ui .tab li),
  #swagger-ui-container :deep(.swagger-ui table thead tr td),
  #swagger-ui-container :deep(.swagger-ui table thead tr th),
  #swagger-ui-container :deep(.swagger-ui .parameter__name),
  #swagger-ui-container :deep(.swagger-ui .parameter__type),
  #swagger-ui-container :deep(.swagger-ui .parameter__in),
  #swagger-ui-container :deep(.swagger-ui .parameter__deprecated),
  #swagger-ui-container :deep(.swagger-ui .parameters-col_description p),
  #swagger-ui-container :deep(.swagger-ui .response-col_description p),
  #swagger-ui-container :deep(.swagger-ui .response-col_status),
  #swagger-ui-container :deep(.swagger-ui .response-col_links),
  #swagger-ui-container :deep(.swagger-ui .responses-inner h4),
  #swagger-ui-container :deep(.swagger-ui .responses-inner h5),
  #swagger-ui-container :deep(.swagger-ui .opblock-body pre),
  #swagger-ui-container :deep(.swagger-ui .model),
  #swagger-ui-container :deep(.swagger-ui .model-title),
  #swagger-ui-container :deep(.swagger-ui .model .property),
  #swagger-ui-container :deep(.swagger-ui .model-toggle::after),
  #swagger-ui-container :deep(.swagger-ui section.models h4),
  #swagger-ui-container :deep(.swagger-ui section.models h4 span),
  #swagger-ui-container :deep(.swagger-ui section.models .model-container .model-box),
  #swagger-ui-container :deep(.swagger-ui .prop-type),
  #swagger-ui-container :deep(.swagger-ui .prop-format),
  #swagger-ui-container :deep(.swagger-ui .renderedMarkdown p),
  #swagger-ui-container :deep(.swagger-ui h1),
  #swagger-ui-container :deep(.swagger-ui h2),
  #swagger-ui-container :deep(.swagger-ui h3),
  #swagger-ui-container :deep(.swagger-ui h4),
  #swagger-ui-container :deep(.swagger-ui h5),
  #swagger-ui-container :deep(.swagger-ui label),
  #swagger-ui-container :deep(.swagger-ui .btn) {
    color: var(--color-text);
  }

  #swagger-ui-container :deep(.swagger-ui .opblock-tag) {
    border-bottom-color: var(--color-border);
  }

  /* Operation summary paths: lighten the "small" text that defaults to #3b4151 */
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-description),
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-summary-method) {
    color: var(--color-text);
  }

  /* Expanded operation body and nested panels — replace the hardcoded white
     backgrounds that break dark-mode. */
  #swagger-ui-container :deep(.swagger-ui .opblock .opblock-section-header),
  #swagger-ui-container :deep(.swagger-ui .responses-inner),
  #swagger-ui-container :deep(.swagger-ui .response-col_description),
  #swagger-ui-container :deep(.swagger-ui .opblock-body),
  #swagger-ui-container :deep(.swagger-ui .parameters-container),
  #swagger-ui-container :deep(.swagger-ui .execute-wrapper),
  #swagger-ui-container :deep(.swagger-ui .responses-wrapper),
  #swagger-ui-container :deep(.swagger-ui .response),
  #swagger-ui-container :deep(.swagger-ui .opblock-description-wrapper),
  #swagger-ui-container :deep(.swagger-ui .opblock-external-docs-wrapper),
  #swagger-ui-container :deep(.swagger-ui .opblock-title_normal) {
    background: var(--color-bg-subtle);
    box-shadow: none;
  }

  /* Method-colored opblocks: darken the pastel tints used in light mode so
     the text (kept at --color-text) contrasts properly. */
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-get) {
    background: rgba(97, 175, 254, 0.12);
    border-color: rgba(97, 175, 254, 0.45);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-post) {
    background: rgba(73, 204, 144, 0.12);
    border-color: rgba(73, 204, 144, 0.45);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-put) {
    background: rgba(252, 161, 48, 0.12);
    border-color: rgba(252, 161, 48, 0.45);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-delete) {
    background: rgba(249, 62, 62, 0.12);
    border-color: rgba(249, 62, 62, 0.45);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-patch) {
    background: rgba(80, 227, 194, 0.12);
    border-color: rgba(80, 227, 194, 0.45);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock.opblock-head) {
    background: rgba(144, 18, 254, 0.12);
    border-color: rgba(144, 18, 254, 0.45);
  }

  /* Tables (parameters, responses) — light mode uses white backgrounds. */
  #swagger-ui-container :deep(.swagger-ui table),
  #swagger-ui-container :deep(.swagger-ui table tbody tr td),
  #swagger-ui-container :deep(.swagger-ui table thead tr td),
  #swagger-ui-container :deep(.swagger-ui table thead tr th) {
    background: transparent;
    border-color: var(--color-border);
    color: var(--color-text);
  }

  /* Schemas section — the main offender for "light on light" */
  #swagger-ui-container :deep(.swagger-ui section.models) {
    background: var(--color-bg-subtle);
    border-color: var(--color-border);
  }
  #swagger-ui-container :deep(.swagger-ui section.models.is-open h4) {
    border-bottom-color: var(--color-border);
  }
  #swagger-ui-container :deep(.swagger-ui section.models .model-container) {
    background: var(--color-bg);
  }
  #swagger-ui-container :deep(.swagger-ui section.models .model-box) {
    background: transparent;
  }
  #swagger-ui-container :deep(.swagger-ui .model-toggle::after) {
    filter: invert(0.9);
  }

  /* Code / JSON examples — keep monospace readable. */
  #swagger-ui-container :deep(.swagger-ui .highlight-code),
  #swagger-ui-container :deep(.swagger-ui .microlight),
  #swagger-ui-container :deep(.swagger-ui .opblock-body pre.example) {
    background: var(--color-bg-muted) !important;
    color: var(--color-text);
  }
  #swagger-ui-container :deep(.swagger-ui .opblock-body pre.example *) {
    color: var(--color-text) !important;
  }

  /* Inputs (try-it-out) */
  #swagger-ui-container :deep(.swagger-ui input[type='text']),
  #swagger-ui-container :deep(.swagger-ui input[type='password']),
  #swagger-ui-container :deep(.swagger-ui input[type='search']),
  #swagger-ui-container :deep(.swagger-ui input[type='email']),
  #swagger-ui-container :deep(.swagger-ui textarea),
  #swagger-ui-container :deep(.swagger-ui select) {
    background: var(--color-bg-input);
    color: var(--color-text);
    border-color: var(--color-border);
  }

  /* Tab headers */
  #swagger-ui-container :deep(.swagger-ui .tab li.active) {
    color: var(--color-primary);
  }

  /* Arrow/expand icons: default is dark SVG fills - invert them */
  #swagger-ui-container :deep(.swagger-ui .expand-operation svg),
  #swagger-ui-container :deep(.swagger-ui .expand-methods svg),
  #swagger-ui-container :deep(.swagger-ui .models-control svg),
  #swagger-ui-container :deep(.swagger-ui section.models h4 svg),
  #swagger-ui-container :deep(.swagger-ui .opblock-control-arrow svg) {
    fill: var(--color-text);
  }
}
</style>
