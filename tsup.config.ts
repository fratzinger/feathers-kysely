import { defineConfig } from 'tsup'

export default defineConfig({
  treeshake: true,
  define: {
    'import.meta.vitest': 'false',
  },
  dts: true,
  clean: true,
  sourcemap: true,
  format: ['esm', 'cjs'],
  entry: {
    index: 'src/index.ts',
  },
})
