# Accessibility (WCAG 2.2 AA) Guide

SorobanPulse dashboard is committed to WCAG 2.2 AA accessibility standards.

## Standards

- **WCAG 2.2 Level AA**: Minimum standard for all features
- **Target**: 100% automated accessibility violations fixed
- **Testing**: Continuous with axe-core and manual audits

## Automated Testing

### Running accessibility tests locally

```bash
cd web
npm run test:a11y
```

This runs axe-core violations checks in the development environment and Vitest.

### Integration tests with axe

Accessibility checks are integrated into unit and E2E tests. Each page component has an accessibility test:

```typescript
// web/src/pages/EventExplorer.test.tsx
import { render } from '@testing-library/react'
import { axe, toHaveNoViolations } from 'jest-axe'
import EventExplorer from './EventExplorer'

expect.extend(toHaveNoViolations)

test('EventExplorer should have no accessibility violations', async () => {
  const { container } = render(<EventExplorer />)
  const results = await axe(container)
  expect(results).toHaveNoViolations()
})
```

## Key Accessibility Requirements

### 1. Color Contrast (4.5:1 for normal text)

Check contrast ratios:
- Use Chrome DevTools > Accessibility inspector
- Use Contrast Checker: https://webaim.org/resources/contrastchecker/

Dark theme and light theme must both meet 4.5:1 for body text.

### 2. Interactive Element Indicators

All interactive elements must have:
- **Visible focus indicator**: At least 3:1 contrast ratio
- **Keyboard accessible**: Tab/Enter/Space support
- **Touch targets**: Minimum 44×44px

Example CSS for focus indicators:

```css
button:focus,
input:focus,
a:focus {
  outline: 3px solid var(--focus-color);
  outline-offset: 2px;
}
```

### 3. Icon-only Buttons

Icon buttons must have aria-label:

```tsx
<button aria-label="Toggle theme">
  {theme === 'light' ? '🌙' : '☀️'}
</button>
```

### 4. Live Regions (SSE stream, metrics updates)

Dynamic content updates must use `role="status"` or `role="alert"`:

```tsx
<div role="status" aria-live="polite" aria-atomic="true">
  {metrics.lag} blocks
</div>
```

### 5. Semantic HTML

- Use `<button>` not `<div>` for clickable elements
- Use `<h1>`, `<h2>` in order (no skipping levels)
- Use `<label>` with `htmlFor` for form inputs
- Use `<nav>`, `<main>`, `<article>` for landmarks

### 6. ARIA Attributes

Only use ARIA when semantics don't exist:

```tsx
// Avoid—just use <button>
<div role="button" onClick={...}>Click me</div>

// Good
<button onClick={...}>Click me</button>
```

## Development Workflow

1. **Write component** with semantic HTML first
2. **Run accessibility test**: `npm run test:a11y`
3. **Fix violations** reported by axe-core
4. **Test keyboard navigation**: Tab through all interactive elements
5. **Test with screen reader** (optional): NVDA, JAWS, or VoiceOver
6. **Manual review**: Verify color contrast and focus indicators

## Testing with Screen Readers

### macOS (VoiceOver)
```
⌘ + F5  # Enable/disable
⌃ + ⌘  # VoiceOver keys
```

### Windows (NVDA)
- Download free: https://www.nvaccess.org/
- Insert (Left Alt + Shift) = NVDA key

### Browser DevTools
- Chrome: Accessibility Inspector (Ctrl+Shift+I → Accessibility)
- Firefox: Accessibility Checker

## Common Issues and Fixes

| Issue | Fix |
|-------|-----|
| Low color contrast | Adjust text color or background to meet 4.5:1 |
| Missing focus indicator | Add `:focus` CSS with outline/border |
| Icon button without label | Add `aria-label="description"` |
| Keyboard inaccessible | Use native `<button>` or add `onKeyDown` handler |
| Stale live region | Add `role="status"` and `aria-live="polite"` |
| Unlabeled form input | Add `<label htmlFor="id">` |

## Continuous Improvement

- Run accessibility audit monthly: `npm run audit:a11y`
- Review axe DevTools browser extension findings during QA
- Collect accessibility feedback from users
- Update test coverage when new features are added

## Resources

- [WebAIM](https://webaim.org/) - Comprehensive A11y info
- [ARIA Authoring Practices](https://www.w3.org/WAI/ARIA/apg/)
- [MDN Accessibility](https://developer.mozilla.org/en-US/docs/Web/Accessibility)
- [axe DevTools](https://www.deque.com/axe/devtools/)
