import { test, expect } from '@playwright/test'

test.describe('Event Explorer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('should load Event Explorer page', async ({ page }) => {
    await expect(page).toHaveTitle(/Event Explorer/i)
    await expect(page.getByRole('heading', { name: /Event Explorer/i })).toBeVisible()
  })

  test('should apply contract filter and display events', async ({ page }) => {
    // Fill in contract ID filter
    const contractInput = page.getByPlaceholder(/Contract ID/i)
    await contractInput.fill('CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4')

    // Wait for events to load (with timeout for test stability)
    await page.waitForTimeout(1000)

    // Table should be visible
    const table = page.locator('table')
    await expect(table).toBeVisible()
  })

  test('should open event detail when clicking row', async ({ page }) => {
    // Wait for table to load
    const table = page.locator('table tbody tr').first()
    await expect(table).toBeVisible()

    // Click first row
    await table.click()

    // Verify action buttons are visible (indicating detail view or drawer opened)
    await expect(page.getByRole('button', { name: /Copy/i })).toBeVisible()
  })

  test('should navigate to Status page', async ({ page }) => {
    // Click Status link in navigation
    await page.getByRole('link', { name: /Status/i }).click()

    // Verify we're on status page
    await expect(page).toHaveURL(/\/status/)
    await expect(page.getByRole('heading', { name: /System Status/i })).toBeVisible()
  })
})
