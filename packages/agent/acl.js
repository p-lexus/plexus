/**
 * Broker ACLs, derived from the topic map.
 *
 * Owner scoping is a convention until a broker enforces it. On a shared broker
 * with no rules, any authenticated client can publish a retained profile
 * claiming to be any agent, subscribe to every requester's results, or
 * overwrite a finished job's retained answer. None of that is a protocol flaw —
 * each one is an ACL nobody wrote, because writing them by hand, per agent and
 * per owner, is tedious and easy to get subtly wrong. `jobs/ci/#` against
 * `jobs/+/#` is one character and the difference between isolation and none.
 *
 * So they are generated from the same address space `topics` builds, and they
 * are generated here rather than in a broker's config format: any broker can
 * consume these, and any implementation of the protocol gets the same answer.
 * Rendering them into a particular broker's syntax is that broker's problem.
 *
 *   import { aclFor } from "plexus-agent/acl";
 *   aclFor({ root: "acme/agents", role: "requester", id: "ci" });
 *   // → { publish: [...], subscribe: ["acme/agents/jobs/ci/#", ...] }
 */

/** Roles a mesh identity can hold. */
export const ROLES = /** @type {const} */ (["agent", "requester", "console"]);

/**
 * An id becomes part of a topic filter, so a `+` or `#` in one would silently
 * widen every rule built from it — the exact failure this module exists to
 * prevent. Reject rather than sanitise: a caller who passes "ci/+" wanted
 * something, and quietly granting them something else is worse than an error.
 */
const VALID_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/** A root is a topic prefix, so it may contain `/` but never a wildcard. */
const VALID_ROOT = /^[A-Za-z0-9][A-Za-z0-9._/-]*$/;

function check(what, value, pattern) {
  if (typeof value !== "string" || !value.length) {
    throw new TypeError(`${what} is required`);
  }
  if (!pattern.test(value)) {
    throw new TypeError(
      `${what} ${JSON.stringify(value)} is not usable in a topic filter — ` +
      `letters, digits, dot, dash and underscore only${pattern === VALID_ROOT ? ", plus /" : ""}`,
    );
  }
  return value;
}

/**
 * The rules for one identity.
 *
 * @param {object} opts
 * @param {string} opts.root           mesh root, e.g. "acme/agents"
 * @param {"agent"|"requester"|"console"} opts.role
 * @param {string} opts.id             agentId for an agent, owner for a requester
 * @param {boolean} [opts.ownerInTopic] v1.4 proposal: invokes carry the owner in
 *   the topic, so the broker can enforce who a requester claims to be. Off by
 *   default — with it off, `requestedBy` is a payload field and no ACL can
 *   police it.
 * @returns {{ publish: string[], subscribe: string[] }}
 */
export function aclFor({ root, role, id, ownerInTopic = false } = {}) {
  check("root", root, VALID_ROOT);
  check("id", id, VALID_ID);
  if (!ROLES.includes(role)) {
    throw new TypeError(`role must be one of ${ROLES.join(", ")} — got ${JSON.stringify(role)}`);
  }

  // Publishing an invoke is publishing as an owner. With the owner in the
  // topic that is enforceable; without it, the rule can only say "some invoke".
  const invoke = ownerInTopic
    ? `${root}/commands/+/invoke/${id}`
    : `${root}/commands/+/invoke`;

  // Filing a verdict on work somebody did for you (v1.5).
  //
  // The FILING leg only. Nobody but the console — the mesh's recorder — may
  // publish on `commands/+/feedback/+`, the leg that actually reaches an agent.
  // That asymmetry is the enforcement: a requester can say what it thought, and
  // cannot make an agent hear it. A rule granting the second leg to requesters
  // would hand back exactly the thing this design removes.
  //
  // Not gated on ownerInTopic, because feedback has only ever had the one form:
  // it was specified after the v1.4 lesson, so there is no unscoped legacy
  // shape to keep working. A deployment still publishing invokes the old way
  // therefore has unforgeable verdicts on forgeable requests — one rule
  // reaching further than the other, and the narrower one is worth having.
  const feedback = `${root}/feedback/${id}/+/+`;

  if (role === "agent") {
    return {
      publish: [
        // Its own identity, and nobody else's — this is what stops one agent
        // publishing a retained profile claiming another's capabilities.
        `${root}/registry/${id}/profile`,
        `${root}/registry/${id}/status`,
        // It serves many owners, so job traffic cannot be narrowed by id here.
        // The narrowing that matters is on the subscribe side.
        `${root}/jobs/+/+/events`,
        `${root}/jobs/+/+/result`,
        // Delegation: an agent asks its peers, as itself.
        invoke,
        `${root}/commands/+/cancel`,
        // And files a verdict on what they gave back. An agent that delegates
        // is a requester and owes the same verdict a person does — as itself,
        // so it cannot file one under another identity's name, and to the
        // recorder, so it cannot deliver one at all.
        feedback,
      ],
      subscribe: [
        `${root}/commands/${id}/#`,      // only its own commands
        `${root}/registry/+/profile`,    // peer discovery
        `${root}/registry/+/status`,
        `${root}/jobs/${id}/#`,          // answers to what it delegated
      ],
    };
  }

  if (role === "requester") {
    return {
      publish: [invoke, `${root}/commands/+/cancel`, feedback],
      subscribe: [
        `${root}/jobs/${id}/#`,          // its own scope, and nothing else
        `${root}/registry/+/profile`,    // so it can see what is on offer
        `${root}/registry/+/status`,
      ],
    };
  }

  // The console reads everything, which is precisely the privilege the rules
  // above exist to withhold. It is a separate identity for that reason, and the
  // UI in front of it is where per-user scoping belongs.
  return {
    publish: [`${root}/commands/+/#`],
    subscribe: [`${root}/#`],
  };
}

/**
 * Does an MQTT topic filter match a topic?
 *
 * Here so the rules above can be tested against real topics rather than against
 * strings that look right. A test that asserts the filter text passes whether
 * or not the filter does what anyone intended.
 */
export function topicMatches(filter, topic) {
  const f = String(filter).split("/");
  const t = String(topic).split("/");

  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") {
      // `#` matches the rest, but never the $SYS tree from a bare wildcard.
      return !(i === 0 && t[0]?.startsWith("$"));
    }
    if (i >= t.length) return false;
    if (f[i] === "+") {
      if (i === 0 && t[0]?.startsWith("$")) return false;
      continue;
    }
    if (f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}

/** Is this topic permitted by any of these filters? */
export function permits(filters, topic) {
  return filters.some((f) => topicMatches(f, topic));
}

export default { aclFor, permits, topicMatches, ROLES };
