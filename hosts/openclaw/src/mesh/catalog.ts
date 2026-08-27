/**
 * The capability catalog: services.json, plus the file watch that republishes
 * the retained profile when it changes.
 *
 * The catalog is deployment-local and gitignored, so upstream updates never
 * collide with an operator's own services.
 */

import * as fs from "fs";
import * as path from "path";
import type { Logger, ServicesFile } from "../types.js";

export interface Catalog {
  read(): ServicesFile;
  write(svc: ServicesFile): boolean;
  /** Records the file's mtime so the reconciler knows the change was ours. */
  markPublished(): void;
  /** Has the file changed since the last markPublished()? */
  isStale(): boolean;
  /** Starts push-based change detection. Returns a disposer. */
  watch(onChange: () => void): () => void;
}

export function createCatalog(servicesFile: string, logger: Logger, exampleFile?: string): Catalog {
  let lastMtime = 0;

  const read = (): ServicesFile => {
    try {
      return JSON.parse(fs.readFileSync(servicesFile, "utf8")) as ServicesFile;
    } catch (e: any) {
      // On a fresh clone the catalog does not exist yet, because it is
      // gitignored. Fall back to the shipped example rather than starting with
      // an empty, silently useless agent.
      // The example ships WITH THE PLUGIN, while the catalog belongs to the
      // deployment — so it is not simply this path with another extension.
      const example = exampleFile ?? servicesFile.replace(/\.json$/, ".example.json");
      try {
        const svc = JSON.parse(fs.readFileSync(example, "utf8")) as ServicesFile;
        // logger.info, not warn: the gateway keeps info from plugins and drops
        // warn, so this said nothing where it mattered. And it matters — an
        // agent serving the example's prompts under its own name looks right
        // in every listing and answers with somebody else's instructions.
        logger.info(
          `CATALOG NOT FOUND at ${servicesFile} — serving ${path.basename(example)} instead. ` +
          `These are EXAMPLE capabilities, not this deployment's: same names, different prompts. ` +
          `Put this deployment's catalog at ${servicesFile}.`,
        );
        return svc;
      } catch { /* no example either — report the original failure */ }
      logger.error(`read ${servicesFile} failed: ${e.message}`);
      return { capabilities: [] };
    }
  };

  const write = (svc: ServicesFile): boolean => {
    try {
      // The panel creates this file, it does not merely edit one: a deployment
      // that has never written a catalog has no directory for it either, and a
      // failed write here is a capability the operator believes they added.
      fs.mkdirSync(path.dirname(servicesFile), { recursive: true });
      fs.writeFileSync(servicesFile, JSON.stringify(svc, null, 2) + "\n");
      return true;
    } catch (e: any) {
      logger.error(`write services failed: ${e.message}`);
      return false;
    }
  };

  const markPublished = () => {
    try { lastMtime = fs.statSync(servicesFile).mtimeMs; } catch { /* noop */ }
  };

  const isStale = (): boolean => {
    try { return fs.statSync(servicesFile).mtimeMs !== lastMtime; } catch { return false; }
  };

  const watch = (onChange: () => void): (() => void) => {
    let debounce: NodeJS.Timeout | null = null;
    const schedule = () => {
      if (debounce) clearTimeout(debounce);
      debounce = setTimeout(() => { if (isStale()) onChange(); }, 250);
      debounce.unref?.();
    };

    let watcher: fs.FSWatcher | null = null;
    try {
      // Watch the DIRECTORY, not the file: editors save by rename, which
      // silently detaches a file watch.
      const dir = path.dirname(servicesFile);
      // And the directory has to exist to be watched. On a deployment whose
      // catalog has not been written yet, creating it here is the difference
      // between edits being noticed and the reconciler picking them up five
      // minutes later.
      fs.mkdirSync(dir, { recursive: true });
      const baseName = path.basename(servicesFile);
      watcher = fs.watch(dir, { persistent: false }, (_ev, fname) => {
        if (!fname || fname === baseName) schedule();
      });
      logger.info("services catalog watched via fs.watch (push)");
    } catch (e: any) {
      logger.warn(`fs.watch unavailable (${e.message}) — falling back to mtime polling`);
    }

    // Slow reconciler: a cheap safety net for filesystems where watch is
    // unreliable (network mounts, some containers). Rare when watch works.
    const timer = setInterval(() => { if (isStale()) onChange(); }, watcher ? 300_000 : 30_000);
    timer.unref?.();

    return () => {
      clearInterval(timer);
      if (debounce) clearTimeout(debounce);
      try { watcher?.close(); } catch { /* noop */ }
    };
  };

  return { read, write, markPublished, isStale, watch };
}
