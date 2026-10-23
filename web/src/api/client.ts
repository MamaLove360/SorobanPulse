import type { AxiosRequestConfig } from 'axios'

export const customInstance = async <T>(config: AxiosRequestConfig): Promise<T> => {
  const response = await fetch(config.url!, {
    method: config.method,
    headers: config.headers as Record<string, string>,
    body: config.data ? JSON.stringify(config.data) : undefined,
  })

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`)
  }

  return response.json()
}
