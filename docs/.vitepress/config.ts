import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'feathers-kysely',
  description:
    'A FeathersJS database adapter for Kysely — the type-safe SQL query builder',
  head: [['link', { rel: 'icon', href: '/logo.svg' }]],
  themeConfig: {
    logo: '/logo.svg',
    nav: [
      { text: 'Guide', link: '/getting-started' },
      { text: 'API', link: '/api/service' },
      { text: 'Relations', link: '/relations/setup' },
    ],

    sidebar: [
      {
        text: 'Guide',
        items: [{ text: 'Getting Started', link: '/getting-started' }],
      },
      {
        text: 'API',
        items: [
          { text: 'Service & Options', link: '/api/service' },
          { text: 'Query Operators', link: '/api/operators' },
          { text: 'Transactions', link: '/api/transactions' },
        ],
      },
      {
        text: 'Relations',
        items: [
          { text: 'Setup', link: '/relations/setup' },
          { text: 'belongsTo', link: '/relations/belongs-to' },
          { text: 'hasMany', link: '/relations/has-many' },
          { text: 'Querying Relations', link: '/relations/querying' },
          { text: 'Sorting', link: '/relations/sorting' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Upsert', link: '/guides/upsert' },
          { text: 'Custom Queries', link: '/guides/custom-queries' },
        ],
      },
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/fratzinger/feathers-kysely' },
    ],
  },
})
