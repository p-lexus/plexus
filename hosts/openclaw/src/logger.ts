/**
 * Prefixed logger with an `alert` level for operational state changes.
 *
 * Some deployments capture only info-level plugin output, so a warn or error
 * alone can be silently dropped. `alert` emits at error level for correctness
 * AND mirrors to info so it is actually visible — an alert nobody can see is
 * not an alert. Reserved for things an operator must act on; ordinary problems
 * still use warn.
 */

import type { Logger } from "./types.js";

export function createLogger(host: { info(m: string): void; warn(m: string): void; error(m: string): void }, prefix: string): Logger {
  const tag = (m: string) => `${prefix}: ${m}`;
  return {
    info: (m) => host.info(tag(m)),
    warn: (m) => host.warn(tag(m)),
    error: (m) => host.error(tag(m)),
    alert: (m) => { host.error(tag(m)); host.info(tag(`[ALERT] ${m}`)); },
  };
}
