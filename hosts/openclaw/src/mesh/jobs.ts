/**
 * In-memory job state: what is running, what was cancelled, and the recent
 * history the panel renders.
 *
 * Deliberately not persisted. Retained results replay from the broker on
 * resubscribe, so completed jobs reappear by themselves; jobs in flight across
 * a restart rely on the executor publishing its own retained result.
 */

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
  record(rec: Partial<JobRecord> & { jobId: string }, event?: { type: string; note?: string }): JobRecord;
  find(jobId: string): JobRecord | undefined;
}

export function createJobStore(onChange: (rec: JobRecord) => void): JobStore {
  const active = new Set<string>();
  // Cancelled jobs are terminal: the mesh promises no further traffic after
  // cancel_acknowledged, so late executor publishes are suppressed by id.
  const cancelled = new Set<string>();
  const jobs: JobRecord[] = [];   // ring, newest last

  function record(
    rec: Partial<JobRecord> & { jobId: string },
    event?: { type: string; note?: string },
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
      const e: JobEvent = { type: event.type, note: event.note, ts: now };
      (merged.events ??= []).push(e);
      if (merged.events.length > MAX_JOB_EVENTS) merged.events.shift();
    }
    // Stamped once: a late duplicate publish must not restart the clock.
    if (TERMINAL_STATES.has(merged.state)) merged.finishedAt ??= now;

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
