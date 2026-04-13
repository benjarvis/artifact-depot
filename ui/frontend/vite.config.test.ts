// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { defineConfig, mergeConfig } from 'vite'
import baseConfig from './vite.config'
import IstanbulPlugin from 'vite-plugin-istanbul'

export default mergeConfig(
  baseConfig,
  defineConfig({
    plugins: [
      IstanbulPlugin({
        include: 'src/**/*',
        exclude: ['node_modules', 'e2e'],
        forceBuildInstrument: true,
      }),
    ],
    build: {
      sourcemap: true,
    },
  }),
)
