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
import type { Said } from "../mesh/feedback.js";
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

export type { MeshView } from "../mesh/view.js";
import type { MeshView } from "../mesh/view.js";

export interface HttpDeps {
  /** Shared configuration: the panel's own settings. Never a mesh's. */
  cfg: ResolvedConfig;
  logger: Logger;
  auth: Auth;
  sse: SseHub;
  vars: VarStore;
  meshes: {
    names(): string[];
    /**
     * The mesh a request is about. With one mesh the parameter is optional and
     * everything behaves as it always did; with several, omitting it is
     * ambiguous and answered as such rather than guessed.
     */
    pick(name?: string | null): MeshView | undefined;
  };
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
  const { cfg, auth, sse, vars, meshes } = deps;
  const base = cfg.web.basePath;
  const webDir = cfg.web.dir;

  const refuseCrossOrigin = (res: ServerResponse) =>
    sendJson(res, 403, { ok: false, error: "cross-origin request refused" });

  return async function handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    try {
      return await route(req, res);
    } catch (e: any) {
      // Answered here as well as at the server, because a route that throws
      // must not depend on who called it to stay contained.
      deps.logger.error(`panel request ${req.method} ${req.url} failed: ${e?.message ?? e}`);
      // Both branches guarded, and symmetrically: a socket that died mid-
      // response makes the reply itself throw, and a throw from inside the
      // catch re-rejects — which is the failure this whole guard exists to
      // stop, arrived at one layer further in.
      try {
        if (res.headersSent) res.end();
        else sendJson(res, 500, { ok: false, error: "the panel failed to answer that request" });
      } catch { /* the socket is gone; nothing left to say */ }
      return true;
    }
  };

  async function route(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
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

    // Which meshes there are. Answered before a mesh is resolved, because it is
    // the question a panel asks in order to resolve one.
    if (p === `${base}/api/meshes`) {
      sendJson(res, 200, { meshes: meshes.names() });
      return true;
    }

    // Every route below acts on one mesh. With a single mesh `?mesh=` is
    // optional and nothing about these routes has changed; with several,
    // leaving it out is ambiguous and is answered rather than guessed at.
    const mesh = isApi ? meshes.pick(url.searchParams.get("mesh")) : undefined;
    if (isApi && !mesh) {
      sendJson(res, 400, {
        ok: false,
        error: `name a mesh with ?mesh= — this agent is on ${meshes.names().join(", ")}`,
        meshes: meshes.names(),
      });
      return true;
    }

    // ── Live stream ──
    if (p === `${base}/api/events`) {
      // Tagged exactly as a broadcast is, so one event has one shape however it
      // was produced — and `peers` is a named field rather than a bare list,
      // because the tag is added by spreading and an array does not survive it.
      const tag = (payload: object) => ({ ...payload, mesh: mesh!.name });
      const detach = sse.attach(res, [
        ["status", tag(mesh!.snapshot())],
        ["profile", tag(mesh!.registry.buildProfile())],
        ["snapshot", tag({ active: [...mesh!.jobs.active], history: mesh!.jobs.recent() })],
        ["peers", tag({ peers: mesh!.peers.list() })],
      ]);
      req.on("close", detach);
      req.on("error", detach);
      return true;
    }

    // ── Read ──
    if (p === `${base}/api/profile`) { sendJson(res, 200, mesh!.profileWithBroker()); return true; }
    if (p === `${base}/api/status`) { sendJson(res, 200, mesh!.snapshot()); return true; }
    if (p === `${base}/api/peers`) { sendJson(res, 200, { peers: mesh!.peers.list() }); return true; }
    if (p === `${base}/api/jobs`) {
      sendJson(res, 200, { active: [...mesh!.jobs.active], history: mesh!.jobs.recent() });
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
        sse.broadcast("status", { ...mesh!.snapshot(), mesh: mesh!.name });
        sendJson(res, 200, { ok: true, name, removed: removing });
        return true;
      }
    }

    // ── Write ──
    if (p === `${base}/api/invoke` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const r = mesh!.dispatcher.dispatch(
        { jobId: body.jobId, service: body.service, args: body.args, requestedBy: body.requestedBy },
        // The panel is an authenticated local operator surface and supplies its
        // own identity rather than relying on the required-owner check.
        { defaultOwner: "web-ui", clientUsername: mesh!.conf.mesh.verifyOwner ? "web-ui" : undefined },
      );
      sendJson(res, r.ok ? 200 : 400, r);
      return true;
    }

    if (p === `${base}/api/cancel` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const ok = mesh!.dispatcher.cancel(String(body.jobId ?? ""), body.requestedBy ?? "web-ui");
      sendJson(res, ok ? 200 : 404, { ok, jobId: body.jobId });
      return true;
    }

    if (p === `${base}/api/feedback` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const refused = mesh!.fileVerdict(
        String(body.agent ?? ""), String(body.jobId ?? ""), String(body.verdict ?? ""),
        { reason: body.reason, details: body.details, lesson: body.lesson });
      sendJson(res, refused ? 400 : 200, refused ? { ok: false, error: refused } : { ok: true, jobId: body.jobId });
      return true;
    }

    if (p === `${base}/api/config` && req.method === "POST") {
      if (!auth.sameOrigin(req)) { refuseCrossOrigin(res); return true; }
      const body = await readBody(req);
      const r = mesh!.registry.runConfigAction(body);
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
  // A throwing route must not be able to end the process.
  //
  // `void handle(...)` with nothing catching it makes any throw inside a route
  // an unhandled rejection, and Node ends the process on those — so one bad
  // panel request took the whole gateway down, and with it every other plugin
  // the gateway was running. That is far too much blast radius for a console
  // on loopback: the panel is a window onto the mesh, and a window that breaks
  // should not burn the house down.
  //
  // Answered as a 500 where the response has not started, and simply closed
  // where it has — an SSE stream has already sent its headers and cannot be
  // turned back into an error.
  const server = createServer((req, res) => {
    void handle(req, res).catch((e: any) => {
      deps.logger.error(`panel request ${req.method} ${req.url} failed: ${e?.message ?? e}`);
      try {
        if (res.headersSent) res.end();
        else sendJson(res, 500, { ok: false, error: "the panel failed to answer that request" });
      } catch { /* the socket is gone; nothing left to say */ }
    });
  });
  server.on("error", (e: any) => deps.logger.warn(`standalone UI port ${deps.cfg.web.port} failed: ${e.message}`));
  server.listen(deps.cfg.web.port, "127.0.0.1", () => {
    deps.logger.info(
      `standalone UI listening on http://127.0.0.1:${deps.cfg.web.port}` +
      `${deps.auth.configured ? " (token required)" : ""}`,
    );
  });
  return { server, handle };
}
