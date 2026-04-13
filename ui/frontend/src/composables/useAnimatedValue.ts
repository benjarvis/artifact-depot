// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { type Ref } from 'vue'
import { useTransition, TransitionPresets } from '@vueuse/core'

/**
 * Smoothly tween a numeric ref between values.
 *
 * @param source   Reactive number to animate.
 * @param duration Tween duration in ms (default 600).
 * @param linear   Use linear interpolation instead of ease-out. Set this to
 *                 `true` for values that update on a fixed interval (e.g.
 *                 throughput every 2 s) so the animation fills the entire
 *                 gap between samples with no flat spots.
 */
export function useAnimatedValue(source: Ref<number>, duration = 600, linear = false) {
  return useTransition(source, {
    duration,
    transition: linear ? ((n: number) => n) : TransitionPresets.easeOutCubic,
  })
}
