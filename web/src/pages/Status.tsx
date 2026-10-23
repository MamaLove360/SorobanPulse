import { useEffect, useState } from 'react'

interface HealthStatus {
  ready: boolean
  ledger: number
  lag: number
}

interface MetricsData {
  pool_used: number
  pool_max: number
  rpc_error_rate: number
  sse_connections: number
}

function getStatusColor(metric: 'lag' | 'pool' | 'rpc', value: number): string {
  switch (metric) {
    case 'lag':
      if (value < 10) return '#22c55e'
      if (value < 30) return '#eab308'
      return '#ef4444'
    case 'pool':
      if (value < 0.7) return '#22c55e'
      if (value < 0.9) return '#eab308'
      return '#ef4444'
    case 'rpc':
      if (value < 0.01) return '#22c55e'
      if (value < 0.05) return '#eab308'
      return '#ef4444'
  }
}

function StatusIndicator({ color, label, value }: { color: string; label: string; value: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
      <div style={{ width: '12px', height: '12px', borderRadius: '50%', backgroundColor: color }} />
      <span>{label}: {value}</span>
    </div>
  )
}

export default function Status() {
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [metrics, setMetrics] = useState<MetricsData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [healthRes, metricsRes] = await Promise.all([
          fetch('/healthz/ready'),
          fetch('/metrics'),
        ])

        if (healthRes.ok) {
          const data = await healthRes.json()
          setHealth(data)
        }

        if (metricsRes.ok) {
          const text = await metricsRes.text()
          const poolUsed = parseFloat(text.match(/pool_used (\d+\.?\d*)/)?.[1] || '0')
          const poolMax = parseFloat(text.match(/pool_max (\d+\.?\d*)/)?.[1] || '1')
          const rpcErrorRate = parseFloat(text.match(/rpc_error_rate (\d+\.?\d*)/)?.[1] || '0')
          const sseConnections = parseInt(text.match(/sse_connections (\d+)/)?.[1] || '0')

          setMetrics({
            pool_used: poolUsed,
            pool_max: poolMax,
            rpc_error_rate: rpcErrorRate,
            sse_connections: sseConnections,
          })
        }

        setLoading(false)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch status')
        setLoading(false)
      }
    }

    fetchData()
    const interval = setInterval(fetchData, 10000)
    return () => clearInterval(interval)
  }, [])

  if (loading) return <div style={{ padding: '2rem' }}>Loading status...</div>
  if (error) return <div style={{ padding: '2rem', color: '#ef4444' }}>Error: {error}</div>

  const poolUtilization = metrics ? (metrics.pool_used / metrics.pool_max) : 0
  const lagColor = health ? getStatusColor('lag', health.lag) : '#ccc'
  const poolColor = getStatusColor('pool', poolUtilization)
  const rpcColor = metrics ? getStatusColor('rpc', metrics.rpc_error_rate) : '#ccc'

  return (
    <div style={{ padding: '2rem', maxWidth: '800px' }}>
      <h1>System Status</h1>
      <div style={{ marginTop: '2rem' }}>
        <h2>Indexer Health</h2>
        {health && (
          <>
            <StatusIndicator
              color={health.ready ? '#22c55e' : '#ef4444'}
              label="Status"
              value={health.ready ? 'Ready' : 'Not Ready'}
            />
            <StatusIndicator color={lagColor} label="Lag (blocks)" value={health.lag.toString()} />
            <StatusIndicator color={lagColor} label="Current Ledger" value={health.ledger.toString()} />
          </>
        )}
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>Database</h2>
        {metrics && (
          <StatusIndicator
            color={poolColor}
            label="Connection Pool"
            value={`${(poolUtilization * 100).toFixed(1)}% (${metrics.pool_used}/${metrics.pool_max})`}
          />
        )}
      </div>

      <div style={{ marginTop: '2rem' }}>
        <h2>RPC</h2>
        {metrics && (
          <>
            <StatusIndicator
              color={rpcColor}
              label="Error Rate (5min)"
              value={`${(metrics.rpc_error_rate * 100).toFixed(2)}%`}
            />
            <StatusIndicator color="#22c55e" label="SSE Connections" value={metrics.sse_connections.toString()} />
          </>
        )}
      </div>
    </div>
  )
}
