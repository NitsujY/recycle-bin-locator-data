/**
 * SourceAdapter.ts
 *
 * Defines the SourceAdapter interface for the data pipeline, along with the
 * shared domain types needed by adapters. These types are defined locally
 * because the data repo is a separate package from the app repo.
 *
 * Requirements: 5.2
 */

// ---------------------------------------------------------------------------
// Raw record type — flexible container for unprocessed source data
// ---------------------------------------------------------------------------

export type RawRecord = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Domain types (mirrored from the app repo's src/types/index.ts)
// ---------------------------------------------------------------------------

export interface LatLng {
  lat: number;
  lng: number;
}

/** Material category — const object + derived union type (enum-equivalent, erasable at runtime) */
export const MaterialCategory = {
  Paper: "paper",
  Plastic: "plastic",
  Glass: "glass",
  LightBulb: "light_bulb",
  Battery: "battery",
  Other: "other",
} as const;

export type MaterialCategory =
  (typeof MaterialCategory)[keyof typeof MaterialCategory];

export interface DayHours {
  days: string[];  // e.g. ["Mon", "Tue", "Wed", "Thu", "Fri"]
  open: string;    // e.g. "09:00"
  close: string;   // e.g. "18:00"
}

export interface OpeningHours {
  raw?: string;              // Free-text from source (e.g. "Mon–Fri 9am–6pm")
  structured?: DayHours[];   // Parsed structured form if available
}

export interface CollectionPoint {
  id: string;                        // Unique identifier (source-prefixed, e.g. "epd-12345")
  name: string;                      // Display name
  nameZhHK?: string;                 // Traditional Chinese name if available from source
  coordinates: LatLng;
  acceptedCategories: MaterialCategory[];
  openingHours?: OpeningHours;       // Absent if not provided by source
  sourceId: string;                  // e.g. "hk-epd"
  lastUpdated: string;               // ISO 8601 date string from pipeline run
  distanceMetres?: number;           // Computed client-side, not stored in JSON
}

// ---------------------------------------------------------------------------
// SourceAdapter interface
// ---------------------------------------------------------------------------

/**
 * A SourceAdapter encapsulates all logic for a single external data source:
 * fetching raw records and normalising them into the unified CollectionPoint
 * schema. Adding a new data source requires only a new adapter class — the
 * pipeline orchestrator needs no changes.
 *
 * Requirements: 5.2
 */
export interface SourceAdapter {
  /** Stable identifier for this source (e.g. "hk-epd"). Used as a directory
   *  name for caching last-successful raw data. */
  readonly sourceId: string;

  /** Fetch raw records from the upstream data source. May throw on network
   *  or API errors; the pipeline orchestrator handles fallback. */
  fetch(): Promise<RawRecord[]>;

  /** Normalise an array of raw records into the unified CollectionPoint
   *  schema. Must be a pure transformation — no I/O side effects. */
  normalise(raw: RawRecord[]): CollectionPoint[];
}
