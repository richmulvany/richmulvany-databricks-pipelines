/**
 * API layer — fetches static JSON exports from the data directory.
 * Add one function per gold table export.
 */

const BASE_URL = import.meta.env.VITE_DATA_BASE_URL ?? '/data'

export interface ExportEnvelope<T> {
  exported_at: string
  record_count: number
  data: T[]
}

async function fetchExport<T>(tableName: string): Promise<ExportEnvelope<T>> {
  const res = await fetch(`${BASE_URL}/${tableName}.json`)
  if (!res.ok) throw new Error(`Failed to fetch ${tableName}: ${res.status} ${res.statusText}`)
  return res.json() as Promise<ExportEnvelope<T>>
}

export interface EntitySummaryRow {
  category: string
  total_count: number
  unique_count: number
  latest_created_at: string
  earliest_created_at: string
  _gold_generated_at: string
}

export interface ExportManifest {
  exported_at: string
  tables: string[]
  total_records: number
}

export const api = {
  fetchEntitySummary: () => fetchExport<EntitySummaryRow>('entity_summary'),
  fetchManifest: () =>
    fetch(`${BASE_URL}/manifest.json`).then((r) => r.json()) as Promise<ExportManifest>,
  // Add more fetchers here as your project grows:
  // fetchOtherTable: () => fetchExport<OtherRow>('other_table'),
}
