import { defineConfig } from 'vitest/config'
import { existsSync } from 'node:fs'
import { loadEnvFile } from 'node:process'

// Like dotenv: existing environment variables take precedence over `.env`.
// Guard the call — loadEnvFile throws if no `.env` exists (e.g. in CI).
if (existsSync('.env')) {
  loadEnvFile()
}

export default defineConfig({
  define: {
    'import.meta.vitest': 'undefined',
  },
  test: {
    globals: true,
    // projects: ['vitest'],
    includeSource: ['src/**/*.{js,ts}'],
    include: ['test/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.test.ts', 'src/**/index.ts'],
    },
    fileParallelism: false,
    env: process.env,
    // reporters: ['dot'],
  },
})
