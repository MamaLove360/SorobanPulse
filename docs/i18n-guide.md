# Internationalization (i18n) Guide

This guide explains how to add and manage translations in SorobanPulse dashboard.

## Architecture

We use `react-i18next` with static JSON locale files. Translations are:
- Stored in `web/src/locales/` as JSON files
- Loaded at build time (no runtime overhead)
- Namespace-organized by feature (nav, eventExplorer, status, admin)
- Persisted in localStorage for user language preference

## Adding a New Locale

### 1. Create the locale file

Create `web/src/locales/[lang-code].json` with the same structure as `en.json`:

```bash
cp web/src/locales/en.json web/src/locales/fr.json
```

### 2. Translate the strings

Edit the new file and replace English strings with translations:

```json
{
  "nav": {
    "eventExplorer": "Explorateur d'événements",
    "liveStream": "Flux en direct",
    "status": "Statut",
    "admin": "Admin",
    "toggleTheme": "Basculer le thème"
  },
  ...
}
```

### 3. Register in `web/src/i18n.ts`

Import and add the locale to the resources:

```typescript
import frTranslations from './locales/fr.json'

i18n.use(initReactI18next).init({
  resources: {
    en: { translation: enTranslations },
    es: { translation: esTranslations },
    fr: { translation: frTranslations },  // Add this
  },
  ...
})
```

### 4. Update the language picker

Add the language option to the language picker component (typically in `App.tsx` or a LanguageSwitcher component):

```typescript
<select value={i18n.language} onChange={(e) => i18n.changeLanguage(e.target.value)}>
  <option value="en">English</option>
  <option value="es">Español</option>
  <option value="fr">Français</option>  {/* Add this */}
</select>
```

## Using Translations in Components

### Basic usage with hooks

```typescript
import { useTranslation } from 'react-i18next'

function EventExplorer() {
  const { t } = useTranslation()

  return (
    <>
      <h1>{t('eventExplorer.title')}</h1>
      <input placeholder={t('eventExplorer.contractIdPlaceholder')} />
    </>
  )
}
```

### Interpolation (dynamic values)

For strings with dynamic content:

```json
{
  "error": "Error: {{message}}"
}
```

In component:

```typescript
{t('error', { message: error.message })}
```

### Pluralization

Add plural forms to locale files:

```json
{
  "events": {
    "one": "1 event",
    "other": "{{count}} events"
  }
}
```

Use with:

```typescript
{t('events.key', { count: eventCount })}
```

## Language Persistence

- User's language choice is automatically saved to localStorage
- Next visit will restore their preferred language
- Falls back to browser language or English if not set

## Best Practices

1. **Keep strings simple**: Avoid complex markup or components in strings
2. **Use consistent keys**: Follow the namespace pattern (feature.item)
3. **Provide context**: Add comments in JSON for translators if string is ambiguous
4. **Test translations**: Verify text fits UI (some languages are longer)
5. **Update all locales**: When adding a new string, update all locale files
6. **Use type safety**: Consider using `as const` for key types

## CI/CD

The build process validates that:
- All locales have the same key structure
- No untranslated strings exist
- All locale files are valid JSON

Merges are blocked if validation fails.
