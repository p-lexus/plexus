/**
 * Panel authentication and CSRF protection.
 *
 * Two distinct levels, because they protect different things:
 *
 *   authorized()  guards the DATA. When web.auth is unset the panel runs open
 *                 on loopback, which is a reasonable default for reading job
 *                 state on your own machine.
 *
 *   elevated()    guards SECRETS, and requires a configured token ALWAYS.
 *                 Reading or writing deployment variables over an
 *                 unauthenticated endpoint is not a trade worth making for
 *                 convenience, so it fails closed.
 *
 * sameOrigin() is separate from both. Binding to 127.0.0.1 keeps other
 * machines out; it does nothing about other tabs. Any page in the operator's
 * browser can POST to localhost, so state-changing routes additionally demand
 * a header that cannot be set cross-origin without a preflight this server
 * never approves.
 */

import type { IncomingMessage } from "http";

export const PANEL_HEADER = "x-mesh-panel";

export interface Auth {
  readonly configured: boolean;
  /** Data access: true when no token is configured, else requires a valid one. */
  authorized(req: IncomingMessage, url: URL): boolean;
  /** Secret access: always requires a configured, valid token. */
  elevated(req: IncomingMessage, url: URL): boolean;
  /** CSRF: same-origin and sent by the panel, not by another page. */
  sameOrigin(req: IncomingMessage): boolean;
}

export function createAuth(token: string): Auth {
  const hasToken = (req: IncomingMessage, url: URL): boolean => {
    if (!token) return false;
    const header = String(req.headers.authorization ?? "");
    if (header === `Bearer ${token}` || header === token) return true;
    // EventSource cannot set headers, so the stream accepts a query token.
    return url.searchParams.get("token") === token;
  };

  return {
    configured: Boolean(token),
    authorized: (req, url) => (token ? hasToken(req, url) : true),
    elevated: (req, url) => Boolean(token) && hasToken(req, url),
    sameOrigin(req) {
      if (String(req.headers[PANEL_HEADER] ?? "") !== "1") return false;
      const origin = req.headers.origin;
      if (!origin) return true;   // non-browser client: curl, scripts, tests
      try {
        return new URL(String(origin)).host === String(req.headers.host ?? "");
      } catch {
        return false;
      }
    },
  };
}
