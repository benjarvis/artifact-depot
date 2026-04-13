// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api, getToken, setToken } from '../api'

const router = useRouter()
const username = ref('')
const password = ref('')
const error = ref('')
const loading = ref(false)
const logoReady = ref(false)

// Must-change-password flow
const mustChangePassword = ref(false)
const newPassword = ref('')
const confirmNewPassword = ref('')
const changeError = ref('')
const changingPassword = ref(false)

async function doLogin() {
  error.value = ''
  loading.value = true
  try {
    const resp = await api.login(username.value, password.value)
    setToken(resp.token)
    if (resp.must_change_password) {
      mustChangePassword.value = true
    } else {
      router.push('/')
    }
  } catch (e: any) {
    error.value = e.message || 'Login failed'
  } finally {
    loading.value = false
  }
}

async function submitPasswordChange() {
  changeError.value = ''
  if (!newPassword.value) {
    changeError.value = 'New password is required'
    return
  }
  if (newPassword.value !== confirmNewPassword.value) {
    changeError.value = 'Passwords do not match'
    return
  }
  changingPassword.value = true
  try {
    const resp = await fetch('/api/v1/auth/change-password', {
      method: 'POST',
      headers: (() => {
        const h: Record<string, string> = {
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
        }
        const t = getToken()
        if (t) h['Authorization'] = `Bearer ${t}`
        return h
      })(),
      body: JSON.stringify({
        username: username.value,
        current_password: password.value,
        new_password: newPassword.value,
      }),
    })
    if (!resp.ok) {
      const text = await resp.text()
      changeError.value = text || `HTTP ${resp.status}`
      return
    }
    try {
      const body = await resp.json()
      if (body?.token) setToken(body.token)
    } catch {
      // 204 No Content — no body to parse.
    }
    router.push('/')
  } catch (e: any) {
    changeError.value = e.message
  } finally {
    changingPassword.value = false
  }
}
</script>

<template>
  <div class="login-page">
    <div class="login-card" :class="{ ready: logoReady }">
      <div class="login-logo"><img src="../assets/logo.svg" alt="Artifact Depot" @load="logoReady = true" /></div>

      <!-- Password change form (mandatory, no cancel) -->
      <div v-if="mustChangePassword">
        <p class="change-pw-msg">You must change your password before continuing.</p>
        <form @submit.prevent="submitPasswordChange">
          <div class="form-group">
            <label for="new-password">New Password</label>
            <input
              id="new-password"
              v-model="newPassword"
              type="password"
              autofocus
            />
          </div>
          <div class="form-group">
            <label for="confirm-password">Confirm New Password</label>
            <input
              id="confirm-password"
              v-model="confirmNewPassword"
              type="password"
            />
          </div>
          <p v-if="changeError" class="error">{{ changeError }}</p>
          <button type="submit" :disabled="changingPassword" class="login-btn">
            {{ changingPassword ? 'Changing...' : 'Change Password' }}
          </button>
        </form>
      </div>

      <!-- Login form -->
      <form v-else @submit.prevent="doLogin">
        <div class="form-group">
          <label for="username">Username</label>
          <input
            id="username"
            v-model="username"
            type="text"
            :disabled="loading"
            autofocus
          />
        </div>
        <div class="form-group">
          <label for="password">Password</label>
          <input
            id="password"
            v-model="password"
            type="password"
            :disabled="loading"
          />
        </div>
        <p v-if="error" class="error">{{ error }}</p>
        <button type="submit" :disabled="loading" class="login-btn">
          {{ loading ? 'Logging in...' : 'Login' }}
        </button>
      </form>
    </div>
  </div>
</template>

<style scoped>
.login-page {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  background: var(--color-bg-muted);
}
.login-card {
  background: var(--color-bg);
  border-radius: 8px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
  padding: 2.5rem;
  width: 100%;
  max-width: 380px;
  opacity: 0;
  transition: opacity 0.15s;
}
.login-card.ready {
  opacity: 1;
}
.login-logo {
  text-align: center;
  margin: 0 0 1.5rem;
}
.login-logo img {
  height: 56px;
  width: auto;
}
.change-pw-msg {
  font-size: 0.9rem;
  color: var(--color-text-secondary);
  margin-bottom: 1rem;
}
.login-btn {
  width: 100%;
  padding: 0.6rem;
  background: var(--color-primary);
  color: var(--color-primary-text);
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.95rem;
  margin-top: 0.5rem;
  transition: background 0.2s;
}
.login-btn:hover {
  background: var(--color-primary-hover);
}
.login-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
</style>
