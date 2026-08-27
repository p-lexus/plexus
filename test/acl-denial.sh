#!/usr/bin/env bash
# Does the agent notice when the broker refuses a subscription?
#
# Stands up mosquitto with the dynamic security plugin, gives "conan" the ACL
# that `plexus-agent/acl` generates for an agent, and asks for the four filters
# the plugin subscribes to on connect. Three are allowed; jobs/# is not.
#
#   ./test-acl-denial.sh
#
# Needs: mosquitto, and a built plexus checkout (npm run build).
set -u
PLEXUS="${PLEXUS_REPO:-$HOME/Developer/p-lexus/plexus}"
PORT="${PORT:-21899}"
DIR="$(mktemp -d)"
trap 'kill %1 2>/dev/null; rm -rf "$DIR"' EXIT

mosquitto_ctrl dynsec init "$DIR/dynsec.json" plexus-admin adminpw >/dev/null 2>&1
cat > "$DIR/broker.conf" <<EOF
listener $PORT 127.0.0.1
allow_anonymous false
persistence false
plugin $(brew --prefix)/lib/mosquitto_dynamic_security.so
plugin_opt_config_file $DIR/dynsec.json
EOF
mosquitto -c "$DIR/broker.conf" >"$DIR/mosq.log" 2>&1 &
sleep 1.2

ctrl() { mosquitto_ctrl -h 127.0.0.1 -p "$PORT" -u plexus-admin -P adminpw dynsec "$@" >/dev/null 2>&1; }
ctrl createClient conan -p conan-secret
ctrl createRole role-conan
# Exactly the subscribe side of aclFor({ role: "agent", id: "conan" }).
for f in "acme/agents/commands/conan/#" "acme/agents/registry/+/profile" \
         "acme/agents/registry/+/status" "acme/agents/jobs/conan/#"; do
  ctrl addRoleACL role-conan subscribePattern "$f" allow
  ctrl addRoleACL role-conan publishClientReceive "$f" allow
done
ctrl addClientRole conan role-conan

cat > "$PLEXUS/.acl-probe.mjs" <<'EOF'
import mqtt from "mqtt";
import { deniedFilters } from "./dist/mesh/transport.js";

const port = process.env.PORT ?? "21899";
const root = "acme/agents";
const asked = [
  `${root}/commands/conan/invoke`,
  `${root}/registry/+/profile`,
  `${root}/jobs/#`,                 // the panel's firehose — an agent ACL refuses it
  `${root}/jobs/conan/#`,           // what an agent ACL grants
];
const c = mqtt.connect(`mqtt://127.0.0.1:${port}`,
  { username: "conan", password: "conan-secret", clean: true });

c.on("connect", () => {
  c.subscribe(Object.fromEntries(asked.map((t) => [t, { qos: 1 }])), (err, granted) => {
    const denied = deniedFilters(asked, granted, err);
    console.log(`  broker answered   : ${JSON.stringify(err?.packet?.granted ?? "no refusal")}`);
    console.log(`  err argument      : ${err ? "an Error (a refusal looks like a failure)" : "null"}`);
    console.log(`  granted[] says    : ${JSON.stringify((granted ?? []).map((g) => g.qos))}  <- the QoS asked for, not the answer`);
    console.log(`  deniedFilters()   : ${JSON.stringify(denied)}`);
    const ok = denied.length === 1 && denied[0] === `${root}/jobs/#`;
    console.log(ok
      ? "\n  ✅ the refusal is detected, and only the refused filter is named"
      : "\n  ❌ the refusal was NOT detected — this is the silent-degradation bug");
    c.end(true);
    process.exit(ok ? 0 : 1);
  });
});
c.on("error", (e) => { console.log(`  connect failed: ${e.message}`); process.exit(1); });
EOF

echo "Asking for four filters against a broker that allows three:"
( cd "$PLEXUS" && PORT="$PORT" node ./.acl-probe.mjs )
rc=$?
rm -f "$PLEXUS/.acl-probe.mjs"
exit $rc
