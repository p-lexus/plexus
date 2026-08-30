/**
 * Job state: what is running, what was cancelled, and the recent history the
 * panel renders.
 *
 * Persisted, because the alternative was tried and does not hold. Retained
 * results do replay from the broker on resubscribe — but a retained result
 * carries the answer alone: no service name, no milestones, no requeue. After
 * a restart the panel showed a list of nameless "done" rows, and any job that
 * never reached a terminal result had vanished outright. That reads as "my
 * jobs are gone", and it is close enough to true.
 *
 * The file is written atomically and holds the same bounded ring as memory, so
 * it cannot grow without limit and a crash mid-write cannot empty it.
 */

import fs from "node:fs";
import path from "node:path";
import type { JobEvent, JobRecord } from "../types.js";
import { TERMINAL_STATES } from "../types.js";

export const MAX_HISTORY = 100;
export const MAX_JOB_EVENTS = 24;

export interface JobStore {
  readonly active: Set<string>;
  readonly cancelled: Set<string>;
  history(): JobRecord[];
  /** Newest first — the order the panel wants. */
  recent(): JobRecord[];
  record(
    rec: Partial<JobRecord> & { jobId: string },
    // `at` is the moment the event happened, which is not the moment it
    // arrived: the same event reaches this store twice — once when we record
    // it locally and again when we hear our own publish — and a retained
    // result arrives again on every resubscribe, forever. Carrying the
    // event's own timestamp is what makes those the same event rather than
    // three.
    event?: { type: string; note?: string; at?: number },
  ): JobRecord;
  find(jobId: string): JobRecord | undefined;
}

export interface JobStoreOptions {
  /** Where history survives a restart. Omit for a store that does not persist. */
  file?: string;
  /** Reported at info level: the gateway log keeps nothing quieter than that. */
  log?: (msg: string) => void;
}

export function createJobStore(
  onChange: (rec: JobRecord) => void,
  opts: JobStoreOptions = {},
): JobStore {
  const active = new Set<string>();
  // Cancelled jobs are terminal: the mesh promises no further traffic after
  // cancel_acknowledged, so late executor publishes are suppressed by id.
  const cancelled = new Set<string>();
  const jobs: JobRecord[] = [];   // ring, newest last

  // ── persistence ──────────────────────────────────────────────────────────
  const file = opts.file;
  const say = opts.log ?? (() => {});

  if (file) {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
      const loaded: JobRecord[] = Array.isArray(parsed?.jobs) ? parsed.jobs : [];
      // Trust the file for shape but not for size: an older build, or a hand
      // edit, must not hand this process an unbounded ring.
      // Restored as written. A repair pass over history was tried here and
      // removed, because it collapsed a job's real timeline — accepted,
      // started, requeued, result-ready — down to its ending, and then
      // persisted that. Preventing new duplicates is worth doing; rewriting
      // the record of what already happened, on data that cannot be checked
      // first, is not.
      for (const rec of loaded.slice(-MAX_HISTORY)) {
        if (rec && typeof rec.jobId === "string") jobs.push(rec);
      }
      if (jobs.length) say(`job history restored: ${jobs.length} from ${path.basename(file)}`);
    } catch (e: any) {
      // Absent is the normal first run. Corrupt is worth saying out loud, once,
      // rather than starting empty and letting it look like data loss.
      if (e?.code !== "ENOENT") say(`job history unreadable (${e?.code ?? e?.message}) — starting empty`);
    }
  }

  let pending: NodeJS.Timeout | null = null;
  function persist(): void {
    if (!file || pending) return;
    // Debounced: a busy job publishes milestones faster than a disk write is
    // worth doing, and every one of them would otherwise be a write.
    pending = setTimeout(() => {
      pending = null;
      try {
        const tmp = `${file}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify({ jobs }, null, 2) + "\n", { mode: 0o600 });
        fs.renameSync(tmp, file);   // atomic: a crash cannot leave half a file
      } catch (e: any) {
        say(`writing ${path.basename(file)} failed: ${e?.code ?? e?.message}`);
      }
    }, 400);
    pending.unref?.();   // never hold the process open for a history file
  }

  // Two copies of one event, stamped on their way out of the same function.
  const SAME_EVENT_MS = 1_000;


  function record(
    rec: Partial<JobRecord> & { jobId: string },
    event?: { type: string; note?: string; at?: number },
  ): JobRecord {
    const now = Date.now();
    const existing = jobs.find((j) => j.jobId === rec.jobId);
    let merged: JobRecord;

    if (existing) {
      Object.assign(existing, rec, { updatedAt: now });
      merged = existing;
    } else {
      merged = { state: "accepted", createdAt: now, events: [], updatedAt: now, ...rec } as JobRecord;
      jobs.push(merged);
      if (jobs.length > MAX_HISTORY) jobs.shift();
    }

    merged.createdAt ??= now;
    if (event) {
      const at = event.at ?? now;
      const events = (merged.events ??= []);

      // The same event, twice or seven times, is one event.
      //
      // Two ways it arrives more than once. The bridge records an event AND
      // publishes it, then hears its own publish — so every dispatch wrote
      // "accepted" and "started" twice. And a result is RETAINED, so the
      // broker replays it on every resubscribe: one finished job had "review"
      // seven times, one per gateway restart, stretching a job's story across
      // days it did not run on.
      //
      // Matched on type and WHEN IT HAPPENED, not on the note. The two copies
      // of one dispatch carry different notes — the local record knows the
      // service, the echo off the wire does not — so matching notes would call
      // them different events, which is exactly what they were doing.
      //
      // A window rather than an exact instant, because the local record and
      // the publish are stamped microseconds apart on the way out.
      const twin = events.find(
        (e) => e.type === event.type && Math.abs(e.ts - at) <= SAME_EVENT_MS);
      if (twin) {
        // Keep whichever copy says more. A note is information; losing it to
        // a duplicate would make deduplication cost something.
        if (!twin.note && event.note) twin.note = event.note;
      } else {
        events.push({ type: event.type, note: event.note, ts: at });
        if (events.length > MAX_JOB_EVENTS) events.shift();
      }
    }
    // Stamped once: a late duplicate publish must not restart the clock.
    if (TERMINAL_STATES.has(merged.state)) merged.finishedAt ??= now;

    persist();
    onChange(merged);
    return merged;
  }

  return {
    active,
    cancelled,
    history: () => jobs,
    recent: () => jobs.slice().reverse(),
    find: (jobId) => jobs.find((j) => j.jobId === jobId),
    record,
  };
}
