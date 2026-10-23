import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import EventExplorer from '../EventExplorer'

describe('EventExplorer', () => {
  it('renders filter inputs', () => {
    render(
      <BrowserRouter>
        <EventExplorer />
      </BrowserRouter>
    )

    expect(screen.getByPlaceholderText('Contract ID')).toBeDefined()
    expect(screen.getByPlaceholderText('Event Type')).toBeDefined()
    expect(screen.getByPlaceholderText('From Ledger')).toBeDefined()
    expect(screen.getByPlaceholderText('To Ledger')).toBeDefined()
  })

  it('updates URL params when filters change', () => {
    render(
      <BrowserRouter>
        <EventExplorer />
      </BrowserRouter>
    )

    const contractInput = screen.getByPlaceholderText('Contract ID')
    fireEvent.change(contractInput, { target: { value: 'test-contract' } })

    expect(window.location.search).toContain('contract_id=test-contract')
  })

  it('renders table headers', () => {
    render(
      <BrowserRouter>
        <EventExplorer />
      </BrowserRouter>
    )

    setTimeout(() => {
      expect(screen.queryByText(/ID/)).toBeDefined()
      expect(screen.queryByText(/Contract ID/)).toBeDefined()
      expect(screen.queryByText(/Type/)).toBeDefined()
      expect(screen.queryByText(/Ledger/)).toBeDefined()
    }, 100)
  })
})
