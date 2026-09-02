/**
 * Regenerate test/fixtures/topics.json from the reference implementation.
 *
 * The address space exists three times — plexus-agent in JavaScript, the
 * OpenClaw bridge in TypeScript, the Hermes host in Python — and three
 * implementations drift. Three times now one of them has quietly not known
 * about a topic the others were already using: postmortems were published for
 * a day that plexus-notify could never have delivered, because the client
 * library it watches through still stopped at events and result.
 *
 * A version number is already held in common by a test. This is the same idea
 * for the addresses, and it is generated rather than written down: a fixture
 * somebody typed records what they believed the layout was.
 *
 *   node tools/topic-fixture.mjs            # print
 *   node tools/topic-fixture.mjs --write    # update the committed fixture
 */

import { writeFileSync } from "node:fs";
import { topics, ownerScope, PROTOCOL_VERSION } from "../packages/agent/index.js";

const ROOT = "acme/agents";
const AGENT = "reviewer";
const OWNER = "ci";
const JOB = "j1";
const SERVICE = "code.review";

/** Every address, by the name the reference implementation gives it. */
const built = {
  profile: topics.profile(ROOT, AGENT),
  status: topics.status(ROOT, AGENT),
  invoke: topics.invoke(ROOT, AGENT),
  invokeAs: topics.invokeAs(ROOT, AGENT, OWNER),
  invokeFilter: topics.invokeFilter(ROOT, AGENT),
  cancel: topics.cancel(ROOT, AGENT),
  query: topics.query(ROOT, AGENT),
  config: topics.config(ROOT, AGENT),
  events: topics.events(ROOT, OWNER, JOB),
  result: topics.result(ROOT, OWNER, JOB),
  postmortem: topics.postmortem(ROOT, OWNER, JOB),
  feedback: topics.feedback(ROOT, AGENT, OWNER),
  feedbackFilter: topics.feedbackFilter(ROOT, AGENT),
  feedbackFile: topics.feedbackFile(ROOT, OWNER, AGENT, JOB),
  feedbackFileFilter: topics.feedbackFileFilter(ROOT),
  memory: topics.memory(ROOT, SERVICE),
  memoryFilter: topics.memoryFilter(ROOT),
  memoryAsk: topics.memoryAsk(ROOT, AGENT, SERVICE),
  memoryAskFilter: topics.memoryAskFilter(ROOT),
  memoryReply: topics.memoryReply(ROOT, AGENT, SERVICE),
  memoryReplyFilter: topics.memoryReplyFilter(ROOT, AGENT),
  alert: topics.alert(ROOT, SERVICE),
  alertFilter: topics.alertFilter(ROOT),
};

/**
 * Topics a job parser must accept or refuse, and what it must read off them.
 *
 * Every entry here is a mistake something has actually made: the unscoped form
 * predates v1.1 and must never match again, and `postmortem` is the kind two
 * implementations did not know about.
 */
const jobTopics = [
  { topic: built.events, owner: OWNER, jobId: JOB, kind: "events" },
  { topic: built.result, owner: OWNER, jobId: JOB, kind: "result" },
  { topic: built.postmortem, owner: OWNER, jobId: JOB, kind: "postmortem" },
  { topic: `${ROOT}/jobs/${JOB}/result`, match: false },
  { topic: `${ROOT}/jobs/${OWNER}/${JOB}/other`, match: false },
  { topic: built.memory, match: false },
];

/** What an invoke topic's owner segment is, or that it is not one. */
const invokeOwners = [
  { topic: built.invokeAs, owner: OWNER },
  { topic: built.invoke, owner: null },
  { topic: `${ROOT}/commands/${AGENT}/invoke/${OWNER}/extra`, owner: null },
  { topic: built.feedback, owner: null },
];

const ownerScopes = [
  { from: "Mohanad.Q!", scope: "mohanad-q" },
  { from: "--ci--", scope: "ci" },
  { from: "", scope: "public" },
];

export function fixture() {
  return {
    fixtureVersion: 1,
    protocolVersion: PROTOCOL_VERSION,
    root: ROOT,
    agentId: AGENT,
    owner: OWNER,
    jobId: JOB,
    service: SERVICE,
    built,
    jobTopics,
    invokeOwners,
    ownerScopes: ownerScopes.map((o) => ({ ...o, actual: ownerScope(o.from) })),
  };
}

const out = JSON.stringify(fixture(), null, 2) + "\n";
if (process.argv.includes("--write")) {
  const path = new URL("../test/fixtures/topics.json", import.meta.url);
  writeFileSync(path, out);
  console.log(`wrote ${path.pathname}`);
} else {
  console.log(out);
}
