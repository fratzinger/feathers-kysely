import { defineConfig } from 'vitest/config'
import dotenv from 'dotenv'

dotenv.config()

export default defineConfig(({ mode }) => {
  return {
    define: {
      'import.meta.vitest': 'undefined',
    },
    test: {
      globals: true,
      // projects: ['vitest'],
      includeSource: ['src/**/*.{js,ts}'],
      coverage: {
        provider: 'v8',
        include: ['src/**/*.ts'],
        exclude: ['src/**/*.test.ts', 'src/**/index.ts'],
      },
      fileParallelism: false,
      env: process.env,
    },
  }
})
