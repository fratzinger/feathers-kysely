import config from '@feathers-community/eslint-config'

export default config(
  {
    tsconfig: { path: './tsconfig.json' },
  },
  {
    // Assertion regexes read best inline; re-compilation cost is irrelevant in tests.
    files: ['test/**'],
    rules: {
      'e18e/prefer-static-regex': 'off',
    },
  },
)
