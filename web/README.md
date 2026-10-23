# SorobanPulse Web Dashboard

React + TypeScript dashboard for the SorobanPulse API.

## Setup

```bash
npm install
npm run gen-api  # Generate API client from openapi.json
```

## Development

```bash
npm run dev      # Start dev server with hot reload
npm run build    # Production build
npm run test     # Run Vitest tests
npm run lint     # Lint with ESLint
npm run typecheck # TypeScript type checking
```

## Features

- Event Explorer: Filterable, paginated event table
- Live Stream: Real-time SSE event streaming with pause/resume
- TypeScript API client generated from OpenAPI spec
- React Query for data fetching
- Keyboard navigation support
