import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    rollupOptions: {
      format: 'iife'
    }
  }
})
