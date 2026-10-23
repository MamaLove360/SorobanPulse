import { defineConfig } from 'orval'

export default defineConfig({
  api: {
    input: '../openapi.json',
    output: {
      mode: 'tags-split',
      target: 'src/api/generated.ts',
      client: 'react-query',
      baseUrl: '/v1',
      override: {
        mutator: {
          path: './src/api/client.ts',
          name: 'customInstance',
        },
      },
    },
  },
})
