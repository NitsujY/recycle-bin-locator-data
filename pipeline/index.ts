/**
 * pipeline/index.ts
 *
 * Pipeline orchestrator for the Recycle Bin Locator data repository.
 *
 * Responsibilities:
 *   - Run each SourceAdapter: fetch → normalise → accumulate results
 *   - On fetch failure: log error, fall back to last-successful cached data
 *   - Persist raw records after each successful fetch
 *   - Publish the final normalised dataset as a versioned static JSON file
 *
 * Requirements: 5.4, 5.5, 5.6
 */

import fs from "fs/promises";
import path from "path";

import {
  type SourceAdapter,
  type RawRecord,
  type CollectionPoint,
} from "../sources/SourceAdapter";
import { HkEpdAdapter } from "../sources/hk-epd/HkEpdAdapter";

// ---------------------------------------------------------------------------
// Paths — all relative to the data repo root (the directory containing this
// pipeline folder).
// ---------------------------------------------------------------------------

/** Absolute path to the data repository root. */
const REPO_ROOT = path.resolve(__dirname, "..");

/**
 * Resolve a path relative to the data repo root.
 */
function repoPath(...segments: string[]): string {
  return path.join(REPO_ROOT, ...segments);
}

// ---------------------------------------------------------------------------
// saveLastSuccessful
// ---------------------------------------------------------------------------

/**
 * Persist raw records for a source so they can be used as a fallback on the
 * next pipeline run if the live fetch fails.
 *
 * Writes to `sources/{sourceId}/last_successful.json` relative to the repo
 * root, creating the directory if it does not exist.
 *
 * Requirements: 5.6
 */
export async function saveLastSuccessful(
  sourceId: string,
  raw: RawRecord[]
): Promise<void> {
  const dir = repoPath("sources", sourceId);
  await fs.mkdir(dir, { recursive: true });
  const filePath = path.join(dir, "last_successful.json");
  await fs.writeFile(filePath, JSON.stringify(raw, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// loadLastSuccessful
// ---------------------------------------------------------------------------

/**
 * Load the last-successful raw records for a source from the cache file.
 *
 * Returns the parsed array, or `null` if the file does not exist or cannot
 * be parsed.
 *
 * Requirements: 5.6
 */
export async function loadLastSuccessful(
  sourceId: string
): Promise<RawRecord[] | null> {
  const filePath = repoPath("sources", sourceId, "last_successful.json");
  try {
    const content = await fs.readFile(filePath, "utf-8");
    const parsed: unknown = JSON.parse(content);
    if (Array.isArray(parsed)) {
      return parsed as RawRecord[];
    }
    console.warn(
      `[${sourceId}] last_successful.json did not contain an array — ignoring cache`
    );
    return null;
  } catch (err: unknown) {
    // File not found or JSON parse error — treat as no cache available
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== "ENOENT") {
      console.warn(`[${sourceId}] Could not read last_successful.json:`, err);
    }
    return null;
  }
}

// ---------------------------------------------------------------------------
// publishDataset
// ---------------------------------------------------------------------------

/**
 * Write the final normalised dataset to:
 *   - `data/collection_points.json`  (latest snapshot, overwritten each run)
 *   - `archive/YYYY-MM-DD/collection_points.json`  (dated archive snapshot)
 *
 * The file format follows the `DatasetManifest` schema defined in the design
 * document.
 *
 * Requirements: 5.5
 */
export async function publishDataset(points: CollectionPoint[]): Promise<void> {
  const generatedAt = new Date().toISOString();

  // Derive the unique set of source IDs present in the dataset
  const sourceIds = Array.from(new Set(points.map((p) => p.sourceId)));

  const manifest = {
    generatedAt,
    sourceIds,
    totalPoints: points.length,
    points,
  };

  const json = JSON.stringify(manifest, null, 2);

  // -------------------------------------------------------------------------
  // Write latest dataset
  // -------------------------------------------------------------------------
  const dataDir = repoPath("data");
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(path.join(dataDir, "collection_points.json"), json, "utf-8");
  console.log(
    `[pipeline] Published ${points.length} points to data/collection_points.json`
  );

  // -------------------------------------------------------------------------
  // Write dated archive snapshot
  // -------------------------------------------------------------------------
  const dateStr = generatedAt.slice(0, 10); // "YYYY-MM-DD"
  const archiveDir = repoPath("archive", dateStr);
  await fs.mkdir(archiveDir, { recursive: true });
  await fs.writeFile(
    path.join(archiveDir, "collection_points.json"),
    json,
    "utf-8"
  );
  console.log(
    `[pipeline] Archived snapshot to archive/${dateStr}/collection_points.json`
  );
}

// ---------------------------------------------------------------------------
// runPipeline
// ---------------------------------------------------------------------------

/**
 * Orchestrate the full data pipeline:
 *
 * For each adapter:
 *   1. Attempt `fetch()` + `normalise()` and accumulate results.
 *   2. On success, persist raw records via `saveLastSuccessful()`.
 *   3. On failure, log the error, load the last-successful cache, normalise
 *      the cached data, and accumulate those results instead.
 *
 * After all adapters have been processed, publish the combined dataset via
 * `publishDataset()`.
 *
 * Requirements: 5.4, 5.5, 5.6
 */
export async function runPipeline(adapters: SourceAdapter[]): Promise<void> {
  const results: CollectionPoint[] = [];

  for (const adapter of adapters) {
    try {
      console.log(`[${adapter.sourceId}] Fetching…`);
      const raw = await adapter.fetch();
      console.log(`[${adapter.sourceId}] Fetched ${raw.length} raw records`);

      const normalised = adapter.normalise(raw);
      console.log(
        `[${adapter.sourceId}] Normalised to ${normalised.length} collection points`
      );

      results.push(...normalised);
      await saveLastSuccessful(adapter.sourceId, raw);
    } catch (err) {
      console.error(`[${adapter.sourceId}] fetch failed:`, err);

      const cached = await loadLastSuccessful(adapter.sourceId);
      if (cached !== null) {
        console.log(
          `[${adapter.sourceId}] Using last-successful cache (${cached.length} records)`
        );
        const normalised = adapter.normalise(cached);
        results.push(...normalised);
      } else {
        console.warn(
          `[${adapter.sourceId}] No cache available — skipping this source`
        );
      }
    }
  }

  await publishDataset(results);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

const adapters: SourceAdapter[] = [new HkEpdAdapter()];
runPipeline(adapters).catch(console.error);
