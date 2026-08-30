/**
 * The control-panel HTTP surface: a small JSON API, an SSE stream, and the
 * static panel.
 *
 * Auth guards the DATA, not the shell. The page itself carries no secrets —
 * every value arrives over /api/* — so it is served unauthenticated, which
 * lets the panel present a sign-in screen instead of the browser showing a raw
 * JSON 401 with no way forward.
 */

import * as fs from "fs";
import * as path from "path";
import { createServer } from "http";
import type { IncomingMessage, ServerResponse, Server } from "http";
import type { Logger } from "../types.js";
import type { ResolvedConfig } from "../config.js";
import type { Auth } from "./auth.js";
import type { SseHub } from "./sse.js";
import type { JobStore } from "../mesh/jobs.js";
import type { VarStore } from "../mesh/vars.js";
import type { Dispatcher } from "../mesh/dispatch.js";
import type { Registry } from "../mesh/registry.js";

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

export interface HttpDeps {
  cfg: ResolvedConfig;
  logger: Logger;
  auth: Auth;
  sse: SseHub;
  jobs: JobStore;
  vars: VarStore;
  dispatcher: Dispatcher;
  registry: Registry;
  snapshot(): Record<string, unknown>;
  profileWithBroker(): Record<string, unknown>;
  peers(): unknown[];
}

const sendJson = (res: ServerResponse, code: number, obj: unknown) => {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
};

async function readBody(req: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  try { return JSON.parse(Buffer.concat(chunks).toString() || "{}"); } catch { return {}; }
}

export function createHttpHandler(deps: HttpDeps) {
  const { cfg, auth, sse, jobs, vars, dispatcher, registry } = deps;
  const base = cfg.web.basePath;
  const webDir = cfg.web.dir;

  const refuseCrossOrigin = (res: ServerResponse) =>
    sendJson(res, 403, { ok: false, error: "cross-origin request refused" });

  return async function handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const url = new URL(req.url ?? "/", "http://local");
    let p = url.pathname;
    // The panel is served twice: mounted under basePath inside the gateway, and
    // bare at the root on the standalone port. Only /api/* was normalised, so a
    // bare asset request kept its root path and then had base.length characters
    // sliced off it below — leaving "/", which the SPA fallback answers with
    // index.html. A stylesheet that arrives as text/html is discarded by the
    // browser, which is an unstyled panel and no error anywhere to say why.
    //
    // It stayed hidden while the panel was a single self-contained file with no
    // asset to request. Splitting the shared theme out of the inline <style> is
    // what asked the question for the first time.
    if (base && !p.startsWith(base)) p = base + p;

    const isApi = p.startsWith(`${base}/api/`);
    if (isApi && !auth.authorized(req, url)) {
      sendJson(res, 401, { ok: false, error: "unauthorized", authRequired: true });
      return true;
    }

    // ── Live stream ──
    if (p === `${base}/api/events`) {
      const detach = sse.attach(res, [
        ["status", deps.snapshot()],
        ["profile", registry.buildProfile()],
        ["snapshot", { active: [...jobs.active], history: jobs.recent() }],
        ["peers", deps.peers()],
      ]);
      req.on("close", detach);
      req.on("error", detach);
      return true;
    }

    // ── Read ──
    if (p === `${base}/api/profile`) { sendJson(res, 200, deps.profileWithBroker()); return true; }
    if (p === `${base}/api/status`) { sendJson(res, 200, deps.snapshot()); return true; }
    if (p === `${base}/api/peers`) { sendJson(res, 200, { peers: deps.peers() }); return true; }
    if (p === `${base}/api/jobs`) {
      sendJson(res, 200, { active: [...jobs.active], history: jobs.recent() });
      return true;
    }

    // ── Deployment variables ──
    // There is deliberately no path that reads a value back: the panel only
    // needs to know what is set, never what it is.
    if (p === `${base}/api/secrets`) {
      if (!auth.elevated(req, url)) {
        sendJson(res, 403, {
          ok: false,
          authRequired: true,
          error: auth.configured
            ? "A valid token is required to manage deployment variables."
            : "Set web.auth in the plugin config to manage deployment variables from the panel.",
        });
        return true;
      }
      if (req.method === "GET") {
        sendJson(res, 200, { ok: true, file: vars.fileName, vars: vars.describe() });
        return true;
      }
      if (req.method === "POST") {
        if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
        const body = await readBody(req);
        const name = String(body.name ?? "").trim();
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
          sendJson(res, 400, { ok: false, error: "Name must look like AN_ENV_VAR: letters, digits and underscores, not starting with a digit." });
          return true;
        }
        if (vars.isPinned(name)) {
          sendJson(res, 409, { ok: false, error: `${name} is pinned in openclaw.json (mesh.promptVars) and takes precedence. Remove it there to manage it here.` });
          return true;
        }
        const removing = body.delete === true;
        if (!removing && !String(body.value ?? "")) {
          sendJson(res, 400, { ok: false, error: "Value cannot be empty. Use delete to remove it." });
          return true;
        }
        const err = removing ? vars.remove(name) : vars.set(name, String(body.value));
        if (err) { sendJson(res, 500, { ok: false, error: err }); return true; }
        sse.broadcast("status", deps.snapshot());
        sendJson(res, 200, { ok: true, name, removed: removing });
        return true;
      }
    }

    // ── Write ──
    if (p === `${base}/api/invoke` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const r = dispatcher.dispatch(
        { jobId: body.jobId, service: body.service, args: body.args, requestedBy: body.requestedBy },
        // The panel is an authenticated local operator surface and supplies its
        // own identity rather than relying on the required-owner check.
        { defaultOwner: "web-ui", clientUsername: cfg.mesh.verifyOwner ? "web-ui" : undefined },
      );
      sendJson(res, r.ok ? 200 : 400, r);
      return true;
    }

    if (p === `${base}/api/cancel` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const ok = dispatcher.cancel(String(body.jobId ?? ""), body.requestedBy ?? "web-ui");
      sendJson(res, ok ? 200 : 404, { ok, jobId: body.jobId });
      return true;
    }

    if (p === `${base}/api/config` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const r = registry.runConfigAction(body);
      sendJson(res, r.ok ? 200 : 400, r);
      return true;
    }

    // ── Static panel (single-page app) ──
    let rel = p.slice(base.length) || "/";
    if (!/\.[a-zA-Z0-9]+$/.test(rel)) rel = "/index.html";
    const file = path.normalize(path.join(webDir, rel));
    // Containment check: a normalised path that escapes webDir is a traversal.
    if (!file.startsWith(webDir)) { res.writeHead(403); res.end("forbidden"); return true; }
    try {
      const data = fs.readFileSync(file);
      res.writeHead(200, { "Content-Type": MIME[path.extname(file)] ?? "application/octet-stream" });
      res.end(data);
    } catch {
      res.writeHead(404);
      res.end("not found");
    }
    return true;
  };
}

export function startHttpServer(deps: HttpDeps): { server: Server | null; handle: ReturnType<typeof createHttpHandler> } {
  const handle = createHttpHandler(deps);
  if (!deps.cfg.web.enabled) {
    deps.logger.info("web panel disabled (web.enabled=false)");
    return { server: null, handle };
  }
  const server = createServer((req, res) => { void handle(req, res); });
  server.on("error", (e: any) => deps.logger.warn(`standalone UI port ${deps.cfg.web.port} failed: ${e.message}`));
  server.listen(deps.cfg.web.port, "127.0.0.1", () => {
    deps.logger.info(
      `standalone UI listening on http://127.0.0.1:${deps.cfg.web.port}` +
      `${deps.auth.configured ? " (token required)" : ""}`,
    );
  });
  return { server, handle };
}
