/**
 * Route matching and message rendering.
 *
 * Pure: no I/O, no clock, no state. Everything Hermes decides about *whether*
 * and *what* to deliver is decided here, so the interesting logic can be tested
 * without a broker, a network, or a Slack workspace.
 */

/** Read a dotted path. Returns undefined rather than throwing on a missing branch. */
export function get(obj, path) {
  return String(path).split(".").reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

/**
 * Does one condition hold?
 *
 * A bare value means equality. Objects express the rest:
 *
 *   "code.review"                  equals
 *   ["a", "b"]                     any of
 *   { $ne: "APPROVE" }             not equal
 *   { $exists: true }              present (and not null)
 *   { $re: "^REQUEST" }            regular expression, case-insensitive
 *   { $in: ["high", "critical"] }  any of, explicitly
 *   { $gt: 5 } / { $lt: 5 }        numeric comparison
 */
export function testCondition(expected, actual) {
  if (Array.isArray(expected)) return expected.some((e) => testCondition(e, actual));

  if (expected && typeof expected === "object") {
    return Object.entries(expected).every(([op, operand]) => {
      switch (op) {
        case "$eq": return actual === operand;
        case "$ne": return actual !== operand;
        case "$exists": return (actual !== undefined && actual !== null) === Boolean(operand);
        case "$in": return Array.isArray(operand) && operand.includes(actual);
        case "$nin": return Array.isArray(operand) && !operand.includes(actual);
        case "$re": return actual != null && new RegExp(operand, "i").test(String(actual));
        case "$gt": return Number(actual) > Number(operand);
        case "$lt": return Number(actual) < Number(operand);
        case "$contains": return String(actual ?? "").toLowerCase().includes(String(operand).toLowerCase());
        default: return false;                      // an unknown operator matches nothing
      }
    });
  }

  return actual === expected;
}

/**
 * Does a message match a route's `when` block?
 *
 * All keys must hold — the block is an AND. An empty or absent `when` matches
 * everything, which is what a catch-all audit route wants.
 */
export function matches(when, message) {
  if (!when || Object.keys(when).length === 0) return true;
  return Object.entries(when).every(([path, expected]) => testCondition(expected, get(message, path)));
}

/**
 * Fill `{{dotted.path}}` placeholders from the message.
 *
 * Missing paths render as empty rather than as the literal placeholder: a
 * notification with a gap in it is readable, one with `{{result.verdict}}` in
 * the middle looks broken. Objects are JSON-stringified so a template can drop
 * in a whole payload without special-casing.
 */
export function render(template, message) {
  if (typeof template !== "string") return "";
  return template.replace(/\{\{\s*([\w.$-]+)\s*\}\}/g, (_m, path) => {
    const value = get(message, path);
    if (value === undefined || value === null) return "";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  });
}

/**
 * Flatten a mesh result into the shape routes and templates address.
 *
 * Result fields are lifted to the top level so `{{verdict}}` works, while
 * `result.*` keeps the unambiguous path for a field whose name collides with an
 * envelope field. Envelope fields win the collision — `jobId` always means the
 * job, never something a handler happened to return under that name.
 */
export function deliveryContext({ jobId, owner, kind, service, ...rest }) {
  const result = { ...rest };
  return {
    ...result,
    jobId, owner, kind,
    service: service ?? result.service,
    type: result.type,
    ts: result.ts,
    result,
  };
}

/**
 * Every route that wants this message, paired with the rendered payload.
 *
 * Routes are independent: two matching routes both fire. `stop: true` on a
 * route ends evaluation after it, for the "page someone, and don't also post
 * the cheerful version" case.
 */
export function plan(routes, message) {
  const ctx = deliveryContext(message);
  const out = [];
  for (const route of routes ?? []) {
    if (route.enabled === false) continue;
    if (!matches(route.when, ctx)) continue;
    out.push({
      route,
      channels: Array.isArray(route.to) ? route.to : [route.to].filter(Boolean),
      payload: {
        title: render(route.title ?? "{{service}} — {{type}}", ctx),
        body: render(route.body ?? "{{result}}", ctx),
        level: route.level ?? "info",
        jobId: ctx.jobId,
        owner: ctx.owner,
        service: ctx.service,
        type: ctx.type,
        context: ctx,
      },
    });
    if (route.stop) break;
  }
  return out;
}
