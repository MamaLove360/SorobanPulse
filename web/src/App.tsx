import { Routes, Route, Link } from 'react-router-dom'
import { useState, useEffect } from 'react'
import EventExplorer from './pages/EventExplorer'
import LiveStream from './pages/LiveStream'
import Status from './pages/Status'
import AdminConsole from './pages/AdminConsole'
import { useTheme } from './context/ThemeContext'

function App() {
  const { toggleTheme, theme } = useTheme()
  const [menuOpen, setMenuOpen] = useState(false)
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768)

  useEffect(() => {
    const handleResize = () => {
      setIsMobile(window.innerWidth < 768)
    }
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', minHeight: '100vh' }}>
      <nav
        style={{
          padding: '1rem',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', flex: 1 }}>
          {isMobile ? (
            <>
              <button
                onClick={() => setMenuOpen(!menuOpen)}
                style={{
                  padding: '0.25rem 0.5rem',
                  fontSize: '1.5rem',
                  minWidth: '44px',
                  minHeight: '44px',
                }}
              >
                ☰
              </button>
              {menuOpen && (
                <div
                  style={{
                    position: 'absolute',
                    top: '60px',
                    left: 0,
                    right: 0,
                    backgroundColor: 'var(--bg-color)',
                    borderBottom: '1px solid var(--border-color)',
                    display: 'flex',
                    flexDirection: 'column',
                    padding: '1rem',
                  }}
                >
                  <Link to="/" onClick={() => setMenuOpen(false)}>
                    Event Explorer
                  </Link>
                  <Link to="/live" onClick={() => setMenuOpen(false)}>
                    Live Stream
                  </Link>
                  <Link to="/status" onClick={() => setMenuOpen(false)}>
                    Status
                  </Link>
                  <Link to="/admin" onClick={() => setMenuOpen(false)}>
                    Admin
                  </Link>
                </div>
              )}
            </>
          ) : (
            <>
              <Link to="/">Event Explorer</Link>
              <Link to="/live">Live Stream</Link>
              <Link to="/status">Status</Link>
              <Link to="/admin">Admin</Link>
            </>
          )}
        </div>
        <button
          onClick={toggleTheme}
          style={{
            padding: '0.5rem 0.75rem',
            minWidth: '44px',
            minHeight: '44px',
          }}
          title="Toggle theme"
        >
          {theme === 'light' ? '🌙' : '☀️'}
        </button>
      </nav>
      <Routes>
        <Route path="/" element={<EventExplorer />} />
        <Route path="/live" element={<LiveStream />} />
        <Route path="/status" element={<Status />} />
        <Route path="/admin" element={<AdminConsole />} />
      </Routes>
    </div>
  )
}

export default App
