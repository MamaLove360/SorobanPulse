import { useState, useMemo, useEffect, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'

interface Event {
  id: string
  contract_id: string
  event_type: string
  ledger: number
  timestamp: string
  topics: string[]
  data: string
}

type SortKey = keyof Event
type SortOrder = 'asc' | 'desc'

function EventExplorer() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [events, setEvents] = useState<Event[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('ledger')
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc')
  const [selectedRow, setSelectedRow] = useState(0)

  const contractId = searchParams.get('contract_id') || ''
  const eventType = searchParams.get('event_type') || ''
  const fromLedger = searchParams.get('from_ledger') || ''
  const toLedger = searchParams.get('to_ledger') || ''

  const setFilter = (key: string, value: string) => {
    const params = new URLSearchParams(searchParams)
    if (value) {
      params.set(key, value)
    } else {
      params.delete(key)
    }
    setSearchParams(params)
  }

  useEffect(() => {
    const fetchEvents = async () => {
      setLoading(true)
      setError(null)

      try {
        const params = new URLSearchParams()
        if (contractId) params.set('contract_id', contractId)
        if (eventType) params.set('event_type', eventType)
        if (fromLedger) params.set('from_ledger', fromLedger)
        if (toLedger) params.set('to_ledger', toLedger)

        const url = `/v1/events?${params.toString()}`
        const response = await fetch(url)

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`)
        }

        const data = await response.json()
        setEvents(data.events || [])
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch events')
      } finally {
        setLoading(false)
      }
    }

    fetchEvents()
  }, [contractId, eventType, fromLedger, toLedger])

  const sortedEvents = useMemo(() => {
    const sorted = [...events]
    sorted.sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]

      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortOrder === 'asc' ? aVal - bVal : bVal - aVal
      }

      const aStr = String(aVal)
      const bStr = String(bVal)
      return sortOrder === 'asc' ? aStr.localeCompare(bStr) : bStr.localeCompare(aStr)
    })
    return sorted
  }, [events, sortKey, sortOrder])

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortOrder('asc')
    }
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
  }

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setSelectedRow((prev) => Math.min(prev + 1, sortedEvents.length - 1))
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        setSelectedRow((prev) => Math.max(prev - 1, 0))
      } else if (e.key === 'Enter' && sortedEvents[selectedRow]) {
        console.log('Selected event:', sortedEvents[selectedRow])
      }
    },
    [sortedEvents, selectedRow]
  )

  return (
    <div style={{ padding: '2rem' }}>
      <h1>Event Explorer</h1>

      <div className="filters" style={{ margin: '1rem 0' }}>
        <input
          type="text"
          placeholder="Contract ID"
          value={contractId}
          onChange={(e) => setFilter('contract_id', e.target.value)}
        />
        <input
          type="text"
          placeholder="Event Type"
          value={eventType}
          onChange={(e) => setFilter('event_type', e.target.value)}
        />
        <input
          type="number"
          placeholder="From Ledger"
          value={fromLedger}
          onChange={(e) => setFilter('from_ledger', e.target.value)}
        />
        <input
          type="number"
          placeholder="To Ledger"
          value={toLedger}
          onChange={(e) => setFilter('to_ledger', e.target.value)}
        />
      </div>

      {loading && <p>Loading...</p>}
      {error && <p style={{ color: '#ef4444' }}>Error: {error}</p>}
      {!loading && !error && events.length === 0 && <p>No events found</p>}

      {sortedEvents.length > 0 && (
        <div onKeyDown={handleKeyDown} tabIndex={0} className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th onClick={() => handleSort('id')}>
                  ID {sortKey === 'id' && (sortOrder === 'asc' ? '↑' : '↓')}
                </th>
                <th onClick={() => handleSort('contract_id')}>
                  Contract ID {sortKey === 'contract_id' && (sortOrder === 'asc' ? '↑' : '↓')}
                </th>
                <th onClick={() => handleSort('event_type')}>
                  Type {sortKey === 'event_type' && (sortOrder === 'asc' ? '↑' : '↓')}
                </th>
                <th onClick={() => handleSort('ledger')}>
                  Ledger {sortKey === 'ledger' && (sortOrder === 'asc' ? '↑' : '↓')}
                </th>
                <th onClick={() => handleSort('timestamp')}>
                  Timestamp {sortKey === 'timestamp' && (sortOrder === 'asc' ? '↑' : '↓')}
                </th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {sortedEvents.map((event, idx) => (
                <tr
                  key={event.id}
                  style={{
                    backgroundColor: idx === selectedRow ? 'var(--table-header-hover)' : 'transparent',
                  }}
                >
                  <td>{event.id.substring(0, 8)}...</td>
                  <td>{event.contract_id.substring(0, 12)}...</td>
                  <td>{event.event_type}</td>
                  <td>{event.ledger}</td>
                  <td>{new Date(event.timestamp).toLocaleString()}</td>
                  <td>
                    <button className="copy-btn" onClick={() => copyToClipboard(event.id)}>
                      Copy ID
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default EventExplorer
