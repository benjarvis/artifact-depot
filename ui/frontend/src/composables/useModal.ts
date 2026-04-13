// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

import { onMounted, onUnmounted, type Ref } from 'vue'

/**
 * Focus-trap and keyboard handling for modal dialogs.
 * Traps Tab/Shift+Tab inside the modal, handles Escape, and restores focus on unmount.
 */
export function useModal(
  contentRef: Ref<HTMLElement | null>,
  onClose: () => void,
  persistent: boolean = false
) {
  let previousFocus: Element | null = null
  let savedOverflow = ''

  function getFocusableElements(): HTMLElement[] {
    if (!contentRef.value) return []
    return Array.from(
      contentRef.value.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )
    )
  }

  function onKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape' && !persistent) {
      e.preventDefault()
      onClose()
      return
    }

    if (e.key === 'Tab') {
      const focusable = getFocusableElements()
      if (focusable.length === 0) return

      const first = focusable[0]
      const last = focusable[focusable.length - 1]

      if (e.shiftKey) {
        if (document.activeElement === first) {
          e.preventDefault()
          last.focus()
        }
      } else {
        if (document.activeElement === last) {
          e.preventDefault()
          first.focus()
        }
      }
    }
  }

  onMounted(() => {
    previousFocus = document.activeElement
    savedOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    document.addEventListener('keydown', onKeydown)

    // Focus the first focusable element after a tick (to let the DOM settle)
    requestAnimationFrame(() => {
      const focusable = getFocusableElements()
      if (focusable.length > 0) {
        focusable[0].focus()
      }
    })
  })

  onUnmounted(() => {
    document.removeEventListener('keydown', onKeydown)
    document.body.style.overflow = savedOverflow
    if (previousFocus instanceof HTMLElement) {
      previousFocus.focus()
    }
  })
}
