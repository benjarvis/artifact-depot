// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src'),
    },
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/repository': 'http://localhost:8080',
      '/swagger-ui': 'http://localhost:8080',
      '/api-docs': 'http://localhost:8080',
    },
  },
  build: {
    outDir: 'dist',
    chunkSizeWarningLimit: 2500, // swagger-ui-dist is large; frontend is embedded in binary
  },
})
