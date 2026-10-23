# GraphQL API Documentation

Issue #683: Add GraphQL API layer for more flexible event querying

## Overview

Soroban Pulse provides a GraphQL API endpoint for flexible querying of Soroban contract events. GraphQL offers several advantages over REST:

- **Flexible querying**: Request only the fields you need
- **Single request**: Get related data in one query instead of multiple REST calls
- **Real-time subscriptions**: Subscribe to live events via WebSocket
- **Introspection**: Discover the API schema directly
- **Powerful filtering**: Complex filters without URL parameter encoding

## Quick Start

### Accessing the GraphQL Playground

Open your browser and navigate to:
```
http://localhost:8000/graphql
```

This opens the GraphQL Playground IDE where you can:
- Write and test queries
- Explore the schema
- View real-time documentation
- See query results immediately

### Making GraphQL Requests

#### Using curl

```bash
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: your-api-key" \
  -d '{"query":"query { events(first: 10) { id contractId eventType } }"}'
```

#### Using GraphQL Client Libraries

**JavaScript/TypeScript:**
```bash
npm install @apollo/client graphql
```

```typescript
import { ApolloClient, InMemoryCache, HttpLink, gql } from '@apollo/client';

const client = new ApolloClient({
  link: new HttpLink({
    uri: 'http://localhost:8000/graphql',
    headers: {
      'X-Api-Key': 'your-api-key',
    },
  }),
  cache: new InMemoryCache(),
});

const query = gql`
  query GetEvents {
    events(first: 10) {
      id
      contractId
      eventType
      ledger
    }
  }
`;

const result = await client.query({ query });
```

**Python:**
```bash
pip install gql
```

```python
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url="http://localhost:8000/graphql",
    headers={"X-Api-Key": "your-api-key"}
)

client = Client(transport=transport)

query = gql("""
    query GetEvents {
        events(first: 10) {
            id
            contractId
            eventType
            ledger
        }
    }
""")

result = client.execute(query)
```

## Schema Reference

### Query Root

#### `event(id: ID!): Event`

Get a single event by ID.

**Example:**
```graphql
query {
  event(id: "550e8400-e29b-41d4-a716-446655440000") {
    contractId
    eventType
    txHash
    ledger
  }
}
```

#### `events(filter: EventFilter, first: Int, after: String, last: Int, before: String): EventConnection!`

Query events with optional filters and Relay-style cursor pagination.

The query returns a `EventConnection` (see [Relay-Style Pagination](#relay-style-pagination)) rather than a plain list.

**Arguments:**

| Argument | Type | Description |
|----------|------|-------------|
| `filter` | `EventFilter` | Optional filters (see below) |
| `first` | `Int` | Return the first N events after `after` (forward pagination) |
| `after` | `String` | Cursor to start after (forward pagination) |
| `last` | `Int` | Return the last N events before `before` (backward pagination) |
| `before` | `String` | Cursor to end before (backward pagination) |

`first` and `last` are mutually exclusive; supplying both is an error. `first` defaults to 10 and is capped at 100.

**Example:**
```graphql
query {
  events(
    filter: {
      contractId: "CABC123"
      eventType: "contract_invoke"
      ledgerMin: 1000
      ledgerMax: 2000
    }
    first: 50
  ) {
    edges {
      cursor
      node {
        id
        contractId
        eventType
        ledger
        ledgerCloseTime
        value
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
  }
}
```

#### `eventStats: EventStats!`

Get statistics about events in the system.

**Example:**
```graphql
query {
  eventStats {
    totalEvents
    eventsByType {
      eventType
      count
    }
    topContracts {
      contractId
      count
    }
  }
}
```

### Types

#### Event

```graphql
type Event {
  id: ID!
  contractId: String!
  eventType: String!
  txHash: String!
  ledger: UInt64!
  ledgerCloseTime: DateTime!
  topic: [String!]
  value: String
  sourceAccount: String
}
```

#### EventFilter

Input filter for querying events.

```graphql
input EventFilter {
  # Filter by contract ID (exact match)
  contractId: String
  
  # Filter by event type (exact match)
  eventType: String
  
  # Filter by transaction hash (exact match)
  txHash: String
  
  # Filter by ledger sequence (exact match)
  ledger: UInt64
  
  # Filter by minimum ledger sequence
  ledgerMin: UInt64
  
  # Filter by maximum ledger sequence
  ledgerMax: UInt64
  
  # Filter by source account (exact match)
  sourceAccount: String
  
  # Filter by topic (partial match)
  topicContains: String
}
```

#### EventConnection

The Relay connection type returned by `events`.

```graphql
type EventConnection {
  edges: [EventEdge!]!
  pageInfo: PageInfo!
  totalCount: Int!
}

type EventEdge {
  # Opaque cursor identifying this edge; pass as `after`/`before`
  cursor: String!
  node: Event!
}

type PageInfo {
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
  startCursor: String
  endCursor: String
}
```

#### PageInfo semantics

- `hasNextPage` — `true` when more events exist after `endCursor` (i.e. the page was truncated at the end). At the last page this is `false`.
- `hasPreviousPage` — `true` when more events exist before `startCursor`.
- `startCursor` / `endCursor` — cursors of the first and last edge in the page, or `null` for an empty page.

For forward pagination (`first`/`after`), `hasPreviousPage` is `true` whenever `after` was supplied (there is at least one preceding event). For backward pagination (`last`/`before`), `hasNextPage` is `true` whenever `before` was supplied.

#### EventStats

```graphql
type EventStats {
  totalEvents: Int!
  eventsByType: [EventTypeCount!]!
  topContracts: [ContractCount!]!
}

type EventTypeCount {
  eventType: String!
  count: Int!
}

type ContractCount {
  contractId: String!
  count: Int!
}
```

### Subscription Root

#### `events(filter: EventFilter): Event!`

Subscribe to new events matching optional filters (WebSocket).

**Example:**
```graphql
subscription {
  events(filter: { contractId: "CABC123" }) {
    id
    contractId
    eventType
    ledger
    value
  }
}
```

## Relay-Style Pagination

The `events` query implements the [Relay Cursor Connections specification](https://relay.dev/graphql/connections.htm). Results are ordered by `(ledger, id)` ascending, which gives a stable total ordering.

### Cursor Format

Cursors are opaque strings. Internally each cursor is:

```
base64("<ledger>:<uuid>")
```

For example, the event with `ledger = 5000000` and `id = 550e8400-e29b-41d4-a716-446655440000` has the cursor:

```
NTAwMDAwMDo1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDA=
```

Because cursors encode the ordering key, they remain valid across requests and are not tied to an offset. Treat them as opaque: always use the `cursor` field from an edge rather than constructing cursors yourself.

### Forward Pagination (`first` / `after`)

Fetch the first page, then pass `pageInfo.endCursor` as `after` to fetch the next page:

```graphql
query FirstPage {
  events(first: 10) {
    edges {
      cursor
      node { id ledger }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

```graphql
query NextPage($after: String!) {
  events(first: 10, after: $after) {
    edges {
      cursor
      node { id ledger }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      endCursor
    }
  }
}
```

Continue while `pageInfo.hasNextPage` is `true`. When it becomes `false` you have reached the last page.

### Backward Pagination (`last` / `before`)

Fetch the last page, then pass `pageInfo.startCursor` as `before` to walk backwards:

```graphql
query LastPage {
  events(last: 10) {
    edges {
      cursor
      node { id ledger }
    }
    pageInfo {
      hasPreviousPage
      startCursor
    }
  }
}
```

```graphql
query PreviousPage($before: String!) {
  events(last: 10, before: $before) {
    edges {
      cursor
      node { id ledger }
    }
    pageInfo {
      hasPreviousPage
      hasNextPage
      startCursor
    }
  }
}
```

Continue while `pageInfo.hasPreviousPage` is `true`.

### Pagination with Filters

Filters and cursors compose freely — the cursor is applied to the filtered result set:

```graphql
query {
  events(
    filter: { contractId: "CABC123" }
    first: 25
    after: "NTAwMDAwMDo1NTBlODQwMC1lMjliLTQxZDQtYTcxNi00NDY2NTU0NDAwMDA="
  ) {
    edges {
      cursor
      node { id eventType ledger }
    }
    pageInfo { hasNextPage endCursor }
  }
}
```

## Query Examples

### Find Recent Events for a Contract

```graphql
query {
  events(
    filter: {
      contractId: "CABC123"
      ledgerMin: 1000
    }
    first: 20
  ) {
    edges {
      cursor
      node {
        id
        eventType
        ledger
        ledgerCloseTime
        value
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Get Event Statistics

```graphql
query {
  eventStats {
    totalEvents
    eventsByType {
      eventType
      count
    }
    topContracts(limit: 10) {
      contractId
      count
    }
  }
}
```

### Filter Events by Transaction

```graphql
query {
  events(
    filter: {
      txHash: "a1b2c3d4e5f6..."
    }
  ) {
    edges {
      node {
        id
        contractId
        eventType
        topic
        value
      }
    }
  }
}
```

### Get Events in Ledger Range

```graphql
query {
  events(
    filter: {
      ledgerMin: 5000000
      ledgerMax: 5000100
    }
    first: 100
  ) {
    edges {
      node {
        id
        contractId
        ledger
        ledgerCloseTime
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

## Subscriptions

Subscribe to real-time events using WebSocket:

```graphql
subscription OnNewEvents {
  events(filter: { eventType: "contract_invoke" }) {
    id
    contractId
    eventType
    ledger
    ledgerCloseTime
  }
}
```

### JavaScript WebSocket Example

```typescript
import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

const client = createClient({
  url: 'ws://localhost:8000/graphql/ws',
  connectionParams: {
    'X-Api-Key': 'your-api-key',
  },
  webSocketImpl: WebSocket,
});

client.subscribe(
  {
    query: `
      subscription {
        events(filter: { eventType: "contract_invoke" }) {
          id
          contractId
          eventType
          ledger
        }
      }
    `,
  },
  {
    next: (data) => console.log('New event:', data),
    error: (err) => console.error('Subscription error:', err),
    complete: () => console.log('Subscription closed'),
  }
);
```

## Authentication

GraphQL endpoints use the same authentication as REST endpoints:

```bash
# Using X-Api-Key header
curl -H "X-Api-Key: your-api-key" http://localhost:8000/graphql

# Using Authorization header
curl -H "Authorization: Bearer your-api-key" http://localhost:8000/graphql
```

## Error Handling

GraphQL errors are returned in the response with details:

```json
{
  "errors": [
    {
      "message": "Invalid filter parameter",
      "extensions": {
        "code": "INVALID_ARGUMENT"
      }
    }
  ]
}
```

## Performance Tips

1. **Request only needed fields**: GraphQL allows you to specify exactly what you need
   ```graphql
   # Good: Only request needed fields
   query {
     events { id contractId }
   }
   
   # Avoid: Requesting unnecessary fields
   query {
     events { id contractId eventType txHash topic value sourceAccount }
   }
   ```

2. **Use pagination**: Always paginate large result sets with cursors
   ```graphql
   query {
     events(first: 50) {
       edges { node { id } }
       pageInfo { hasNextPage endCursor }
     }
   }
   ```

3. **Filter early**: Apply filters to reduce result set
   ```graphql
   query {
     events(filter: { contractId: "CA..." }) {
       edges { node { id } }
     }
   }
   ```

4. **Cache responses**: Use HTTP caching for read-heavy workloads

## Comparing REST vs GraphQL

| Feature | REST | GraphQL |
|---------|------|---------|
| Multiple fields | Multiple requests | Single request |
| Over-fetching | Yes | No |
| Schema discovery | Manual docs | Automatic introspection |
| Real-time updates | Polling | Subscriptions |
| Caching | Built-in | Client-side |
| Flexibility | Fixed structure | Full flexibility |

## Related Issues

- Issue #683: Add GraphQL API layer
- Issue #685: Handle RPC pagination cursor expiry gracefully
- Issue #1261: Relay-style cursor pagination for the GraphQL API
