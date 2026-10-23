import { useState, useEffect, useRef, useCallback } from 'react'
import { SSEStream } from '../../../../sdk/typescript/sse'

interface Event {
  id: string
  contract_id: string
  event_type: string
  ledger: number
  timestamp: string
  topics: string[]
  data: string
}

const MAX_EVENTS = 500

function LiveStream() {
  const [events, setEvents] = useState<Event[]>([])
  const [isPaused, setIsPaused] = useState(false)
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [eventsPerSecond, setEventsPerSecond] = useState(0)

  const sseStreamRef = useRef<SSEStream | null>(null)
  const pausedEventsRef = useRef<Event[]>([])
  const eventCountRef = useRef(0)
  const lastCountResetRef = useRef(Date.now())

  const addEvent = useCallback((event: Event) => {
    eventCountRef.current++

    const now = Date.now()
    const elapsed = now - lastCountResetRef.current

    if (elapsed >= 1000) {
      setEventsPerSecond(Math.round((eventCountRef.current / elapsed) * 1000))
      eventCountRef.current = 0
      lastCountResetRef.current = now
    }

    setEvents((prev) => {
      const updated = [event, ...prev]
      return updated.slice(0, MAX_EVENTS)
    })
  }, [])

  useEffect(() => {
    const stream = new SSEStream('/v1/events/stream', {
      onMessage: (sseEvent) => {
        try {
          const event = JSON.parse(sseEvent.data) as Event
          if (isPaused) {
            pausedEventsRef.current.push(event)
          } else {
            addEvent(event)
          }
        } catch (err) {
          console.error('Failed to parse event:', err)
        }
      },
      onError: (err) => {
        setError(err.message)
        setIsConnected(false)
      },
      onClose: () => {
        setIsConnected(false)
      },
      autoReconnect: true,
      maxReconnectAttempts: 10,
    })

    sseStreamRef.current = stream
    stream.connect()
    setIsConnected(true)

    return () => {
      stream.disconnect()
    }
  }, [addEvent, isPaused])

  const togglePause = () => {
    setIsPaused((prev) => {
      if (prev) {
        pausedEventsRef.current.forEach((event) => addEvent(event))
        pausedEventsRef.current = []
      }
      return !prev
    })
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
  }

  return (
    <div style={{ padding: '2rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Live Event Stream</h1>
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
          <span>
            {eventsPerSecond} events/s | {events.length} total
          </span>
          <button onClick={togglePause}>{isPaused ? 'Resume' : 'Pause'}</button>
          <span
            style={{
              width: '10px',
              height: '10px',
              borderRadius: '50%',
              backgroundColor: isConnected ? '#4caf50' : '#f44336',
            }}
          />
        </div>
      </div>

      {error && <p style={{ color: 'red', marginTop: '1rem' }}>Error: {error}</p>}

      {isPaused && pausedEventsRef.current.length > 0 && (
        <p style={{ marginTop: '1rem', color: '#ff9800' }}>
          {pausedEventsRef.current.length} events buffered
        </p>
      )}

      <div style={{ marginTop: '1rem' }}>
        {events.length === 0 ? (
          <p>Waiting for events...</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Contract ID</th>
                <th>Type</th>
                <th>Ledger</th>
                <th>Timestamp</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <tr key={event.id}>
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
        )}
      </div>
    </div>
  )
}

export default LiveStream
