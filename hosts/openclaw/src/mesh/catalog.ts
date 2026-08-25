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

export function createCatalog(servicesFile: string, logger: Logger): Catalog {
  let lastMtime = 0;

  const read = (): ServicesFile => {
    try {
      return JSON.parse(fs.readFileSync(servicesFile, "utf8")) as ServicesFile;
    } catch (e: any) {
      // On a fresh clone the catalog does not exist yet, because it is
      // gitignored. Fall back to the shipped example rather than starting with
      // an empty, silently useless agent.
      const example = servicesFile.replace(/\.json$/, ".example.json");
      try {
        const svc = JSON.parse(fs.readFileSync(example, "utf8")) as ServicesFile;
        logger.warn(
          `${path.basename(servicesFile)} not readable — falling back to ` +
          `${path.basename(example)}. Copy it to ${path.basename(servicesFile)} to customise.`,
        );
        return svc;
      } catch { /* no example either — report the original failure */ }
      logger.error(`read ${servicesFile} failed: ${e.message}`);
      return { capabilities: [] };
    }
  };

  const write = (svc: ServicesFile): boolean => {
    try {
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
