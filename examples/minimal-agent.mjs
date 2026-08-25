/**
 * A complete Agent Mesh agent, in about 40 lines.
 *
 * No framework, no plugin — just an MQTT client speaking the protocol. This is
 * the whole contract: advertise what you can do, accept directed work, publish
 * progress and one retained terminal result.
 *
 *   npm i mqtt
 *   MESH_BROKER=mqtt://localhost:1883 node examples/minimal-agent.mjs
 *
 * Then, from anywhere that can reach the same broker:
 *
 *   mosquitto_sub -t 'agents/jobs/alice/#' &
 *   mosquitto_pub -t 'agents/commands/echo-agent/invoke' -m \
 *     '{"service":"echo.say","requestedBy":"alice","args":{"phrase":"hello"}}'
 */

import mqtt from "mqtt";

const BROKER = process.env.MESH_BROKER ?? "mqtt://localhost:1883";
const ROOT = process.env.MESH_ROOT ?? "agents";
const ID = process.env.MESH_AGENT_ID ?? "echo-agent";

// Owner scope: lowercase, [a-z0-9_-], empty becomes "public". This is what
// keeps each requester's traffic on its own topic branch.
const owner = (s) => (String(s ?? "").toLowerCase().replace(/[^a-z0-9_-]+/g, "-")
  .replace(/^-+|-+$/g, "") || "public");

const profile = {
  agentId: ID,
  displayName: "Echo Agent",
  protocolVersion: "1.3",
  status: "online",
  capabilities: [{
    service: "echo.say",
    description: "Echoes a phrase back.",
    requestSchema: { phrase: "string (what to echo)" },
  }],
};

const client = mqtt.connect(BROKER, {
  clientId: `mesh-${ID}`,            // STABLE: this is what makes the session durable
  clean: false,                       // queue jobs published while we are offline
  will: {                             // the broker announces our death for us
    topic: `${ROOT}/registry/${ID}/status`,
    payload: JSON.stringify({ status: "offline" }), qos: 1, retain: true,
  },
});

// A broker URL often carries credentials. Never log it raw — this is example
// code, and the pattern gets copied.
const safeUrl = (u) => u.replace(/\/\/[^@/]*@/, "//***@");

client.on("connect", () => {
  // Retained, so agents connecting later discover us without us re-announcing.
  client.publish(`${ROOT}/registry/${ID}/profile`, JSON.stringify(profile), { qos: 1, retain: true });
  client.publish(`${ROOT}/registry/${ID}/status`, JSON.stringify({ status: "online" }), { qos: 1, retain: true });
  client.subscribe(`${ROOT}/commands/${ID}/invoke`, { qos: 1 });
  console.log(`${ID} online at ${safeUrl(BROKER)}, offering echo.say`);
});

client.on("message", async (_topic, payload) => {
  const job = JSON.parse(payload.toString());
  const base = `${ROOT}/jobs/${owner(job.requestedBy)}/${job.jobId ?? `job-${Date.now().toString(36)}`}`;

  // Progress: QoS 1, never retained — a late subscriber gets the outcome, not stale steps.
  client.publish(`${base}/events`, JSON.stringify({ type: "started", ts: new Date().toISOString() }), { qos: 1 });

  const result = job.service === "echo.say"
    ? { type: "echo", echoed: job.args?.phrase ?? null }
    : { type: "error", error: `unknown service "${job.service}"` };

  // Terminal result: RETAINED, so it survives for whoever asks later.
  client.publish(`${base}/result`, JSON.stringify({ ...result, ts: new Date().toISOString() }), { qos: 1, retain: true });
  console.log(`answered ${job.service} -> ${base}/result`);
});

for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => {
  // Empty retained payload deletes a retained message: this is how an agent
  // leaves the mesh cleanly instead of lingering as a ghost in the registry.
  client.publish(`${ROOT}/registry/${ID}/profile`, "", { qos: 1, retain: true });
  client.publish(`${ROOT}/registry/${ID}/status`, "", { qos: 1, retain: true });
  setTimeout(() => process.exit(0), 300);
});
