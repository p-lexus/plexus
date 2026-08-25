/**
 * Server-Sent Events fan-out.
 *
 * The panel used to poll every 2.5 seconds. This is the push replacement: job
 * and status changes are written to open streams as they happen, so the panel
 * only falls back to polling if the stream itself dies.
 */

import type { ServerResponse } from "http";

export interface SseHub {
  readonly size: number;
  /** Registers a response as a stream and returns a disposer. */
  attach(res: ServerResponse, initial: Array<[string, unknown]>): () => void;
  broadcast(event: string, data: unknown): void;
  closeAll(): void;
}

const frame = (event: string, data: unknown) =>
  `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

export function createSseHub(): SseHub {
  const clients = new Set<ServerResponse>();

  return {
    get size() { return clients.size; },

    attach(res, initial) {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        // Proxies that buffer will hold every event until the response ends,
        // which for a stream is never.
        "X-Accel-Buffering": "no",
      });
      res.write("retry: 3000\n\n");
      for (const [event, data] of initial) res.write(frame(event, data));
      clients.add(res);

      // Comment ping: keeps idle connections from being reaped by a proxy or
      // an OS-level idle timeout.
      const ka = setInterval(() => { try { res.write(": ka\n\n"); } catch { /* gone */ } }, 25_000);
      ka.unref?.();

      return () => { clearInterval(ka); clients.delete(res); };
    },

    broadcast(event, data) {
      if (!clients.size) return;
      const f = frame(event, data);
      for (const res of [...clients]) {
        try { res.write(f); } catch { clients.delete(res); }
      }
    },

    closeAll() {
      for (const res of [...clients]) { try { res.end(); } catch { /* noop */ } }
      clients.clear();
    },
  };
}
