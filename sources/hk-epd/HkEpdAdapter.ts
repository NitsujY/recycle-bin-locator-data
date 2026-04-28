/**
 * HkEpdAdapter.ts
 *
 * SourceAdapter implementation for the Hong Kong Environmental Protection
 * Department (EPD) recycling collection points dataset, served via the
 * wastereduction.gov.hk CSV file.
 *
 * Requirements: 5.1, 5.3, 5.7
 */

import {
  type SourceAdapter,
  type RawRecord,
  type CollectionPoint,
  MaterialCategory,
} from "../SourceAdapter";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SOURCE_ID = "hk-epd";

/**
 * Direct CSV download URL for the EPD recycling collection points dataset.
 * Source: https://data.gov.hk/en-data/dataset/hk-epd-recycteam-waste-less-recyclable-collection-points-data
 */
const CSV_URL =
  "https://www.wastereduction.gov.hk/sites/default/files/wasteless250918.csv";

// ---------------------------------------------------------------------------
// CSV parsing helper
// ---------------------------------------------------------------------------

/**
 * Parse a CSV string into an array of objects keyed by the header row.
 * Handles quoted fields (including fields containing commas and newlines).
 */
function parseCsv(text: string): RawRecord[] {
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return [];

  // Parse header row — strip BOM if present
  const headerLine = lines[0].replace(/^\uFEFF/, "");
  const headers = parseRow(headerLine);

  const records: RawRecord[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const values = parseRow(line);
    const record: RawRecord = {};
    headers.forEach((header, idx) => {
      record[header.trim()] = values[idx] ?? "";
    });
    records.push(record);
  }
  return records;
}

/**
 * Parse a single CSV row, respecting quoted fields.
 */
function parseRow(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        // Escaped quote inside quoted field
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

// ---------------------------------------------------------------------------
// Category mapping helpers
// ---------------------------------------------------------------------------

/**
 * Map a single EPD waste_type string (English) to a MaterialCategory value.
 * Returns MaterialCategory.Other for unrecognised strings.
 */
function mapEpdCategory(raw: string): MaterialCategory {
  const lower = raw.trim().toLowerCase();

  if (lower.includes("paper")) return MaterialCategory.Paper;
  if (lower.includes("metal") || lower.includes("aluminium") || lower.includes("aluminum")) {
    return MaterialCategory.Metal;
  }
  if (lower.includes("plastic")) return MaterialCategory.Plastic;
  if (lower.includes("glass")) return MaterialCategory.Glass;
  if (
    lower.includes("fluorescent") ||
    lower.includes("lamp") ||
    lower.includes("bulb") ||
    lower.includes("light")
  ) {
    return MaterialCategory.LightBulb;
  }
  if (lower.includes("battery") || lower.includes("batteries")) {
    return MaterialCategory.Battery;
  }
  // Metal, clothes, electrical equipment, etc. → Other
  return MaterialCategory.Other;
}

/**
 * Extract accepted material categories from the waste_type field.
 * The field contains comma-separated category names, e.g. "Metals,Paper,Plastics".
 */
function extractCategories(record: RawRecord): MaterialCategory[] {
  const categories = new Set<MaterialCategory>();

  const wasteType =
    (record["waste_type"] as string | undefined) ??
    (record["Type"] as string | undefined) ??
    "";

  if (wasteType.trim().length > 0) {
    const parts = wasteType.split(/[,/、，]+/);
    for (const part of parts) {
      if (part.trim().length > 0) {
        categories.add(mapEpdCategory(part));
      }
    }
  }

  if (categories.size === 0) {
    categories.add(MaterialCategory.Other);
  }

  return Array.from(categories);
}

// ---------------------------------------------------------------------------
// HkEpdAdapter
// ---------------------------------------------------------------------------

export class HkEpdAdapter implements SourceAdapter {
  readonly sourceId = SOURCE_ID;

  /**
   * Fetch raw records from the EPD recycling collection points CSV.
   * Uses native fetch (Node 20+).
   *
   * Requirements: 5.1
   */
  async fetch(): Promise<RawRecord[]> {
    const response = await fetch(CSV_URL, {
      headers: {
        Accept: "text/csv,text/plain,*/*",
        "User-Agent": "RecycleBinLocator/1.0 (data pipeline)",
      },
    });

    if (!response.ok) {
      throw new Error(
        `[${SOURCE_ID}] HTTP ${response.status} ${response.statusText} from ${CSV_URL}`
      );
    }

    const text = await response.text();
    const records = parseCsv(text);

    if (records.length === 0) {
      throw new Error(`[${SOURCE_ID}] CSV parsed to 0 records — unexpected empty response`);
    }

    return records;
  }

  /**
   * Normalise an array of raw EPD CSV records into the unified CollectionPoint
   * schema. Records with missing or unparseable coordinates are skipped.
   *
   * CSV columns: cp_id, cp_state, district_id, address_en, address2_en,
   *   address_tc, address2_tc, address_sc, address2_sc, lat, lgt,
   *   waste_type, legend, accessibilty_notes, contact_en, contact_tc,
   *   contact_sc, openhour_en, openhour_tc, openhour_sc
   *
   * Requirements: 5.3, 5.7
   */
  normalise(raw: RawRecord[]): CollectionPoint[] {
    const results: CollectionPoint[] = [];
    const now = new Date().toISOString();

    for (let index = 0; index < raw.length; index++) {
      const record = raw[index];

      // Skip non-accepted records
      const state = (record["cp_state"] as string | undefined) ?? "";
      if (state.trim().toLowerCase() === "rejected") continue;

      // -----------------------------------------------------------------------
      // Coordinates — skip records that lack valid lat/lng
      // -----------------------------------------------------------------------
      const latRaw = record["lat"] ?? record["Latitude"] ?? record["latitude"];
      const lngRaw = record["lgt"] ?? record["lng"] ?? record["Longitude"] ?? record["longitude"];

      if (latRaw == null || lngRaw == null) continue;

      const lat = parseFloat(String(latRaw));
      const lng = parseFloat(String(lngRaw));

      if (!isFinite(lat) || !isFinite(lng)) continue;

      // -----------------------------------------------------------------------
      // Unique identifier
      // -----------------------------------------------------------------------
      const recordId = record["cp_id"] ?? record["RecordID"] ?? record["id"];
      const id =
        recordId != null && String(recordId).trim().length > 0
          ? `epd-${String(recordId).trim()}`
          : `epd-idx-${index}`;

      // -----------------------------------------------------------------------
      // Names
      // -----------------------------------------------------------------------
      const engName =
        ((record["address_en"] as string | undefined) ?? "").trim();
      const engName2 =
        ((record["address2_en"] as string | undefined) ?? "").trim();
      const chiName =
        ((record["address_tc"] as string | undefined) ?? "").trim();

      // Combine address lines for a fuller name
      const fullEngName = engName2.length > 0
        ? `${engName} ${engName2}`.trim()
        : engName;

      const name =
        fullEngName.length > 0
          ? fullEngName
          : chiName.length > 0
          ? chiName
          : `EPD Collection Point ${id}`;

      const nameZhHK = chiName.length > 0 ? chiName : undefined;

      // -----------------------------------------------------------------------
      // Accepted categories
      // -----------------------------------------------------------------------
      const acceptedCategories = extractCategories(record);

      // -----------------------------------------------------------------------
      // Opening hours
      // -----------------------------------------------------------------------
      const hoursRaw =
        ((record["openhour_en"] as string | undefined) ?? "").trim() ||
        ((record["openhour_tc"] as string | undefined) ?? "").trim();

      const openingHours =
        hoursRaw.length > 0 ? { raw: hoursRaw } : undefined;

      // -----------------------------------------------------------------------
      // Assemble CollectionPoint
      // -----------------------------------------------------------------------
      const point: CollectionPoint = {
        id,
        name,
        ...(nameZhHK !== undefined && { nameZhHK }),
        coordinates: { lat, lng },
        acceptedCategories,
        ...(openingHours !== undefined && { openingHours }),
        sourceId: SOURCE_ID,
        lastUpdated: now,
      };

      results.push(point);
    }

    return results;
  }
}
