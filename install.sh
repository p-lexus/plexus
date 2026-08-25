#!/usr/bin/env bash
#
# Plexus installer — puts your agent on the mesh.
#
#   ./install.sh                      detect what you have, install for it
#   ./install.sh --target hermes      force one
#   ./install.sh --broker mqtt://host:1883 --agent-id dba
#   ./install.sh --dry-run            print every action, change nothing
#
# Deliberately conservative: it never overwrites an existing config, never
# touches your capability catalog, and prints exactly what it is about to do
# before doing it. An installer that silently replaces a working setup is worse
# than no installer.

set -euo pipefail

REPO_URL="https://github.com/MoGhali/plexus.git"
BROKER=""; AGENT_ID=""; TARGET=""; DRY_RUN=0; ASSUME_YES=0

# ── output ───────────────────────────────────────────────────────────────────
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  B=$'\033[1m'; DIM=$'\033[2m'; TEAL=$'\033[36m'; RED=$'\033[31m'; AMBER=$'\033[33m'; R=$'\033[0m'
else
  B=""; DIM=""; TEAL=""; RED=""; AMBER=""; R=""
fi
say()  { printf '%s\n' "$*"; }
step() { printf '%s▸%s %s\n' "$TEAL" "$R" "$*"; }
ok()   { printf '  %s✓%s %s\n' "$TEAL" "$R" "$*"; }
warn() { printf '  %s!%s %s\n' "$AMBER" "$R" "$*"; }
die()  { printf '%serror:%s %s\n' "$RED" "$R" "$*" >&2; exit 1; }

run() {
  if [ "$DRY_RUN" -eq 1 ]; then printf '  %s$ %s%s\n' "$DIM" "$*" "$R"; return 0; fi
  "$@"
}

confirm() {
  [ "$ASSUME_YES" -eq 1 ] && return 0
  [ "$DRY_RUN" -eq 1 ] && return 0
  printf '  %s? %s [y/N] ' "$AMBER$R" "$1"
  read -r reply </dev/tty || return 1
  case "$reply" in [yY]*) return 0 ;; *) return 1 ;; esac
}

usage() {
  cat <<EOF
${B}Plexus installer${R}

  ./install.sh [options]

  --target <openclaw|hermes|both>   which host plugin to install
  --broker <url>                    e.g. mqtt://localhost:1883
  --agent-id <name>                 this agent's name on the mesh
  --yes                             don't ask, just do it
  --dry-run                         print actions, change nothing
  --help

With no options it detects what you have installed and asks.
EOF
  exit 0
}

while [ $# -gt 0 ]; do
  case "$1" in
    --target)   TARGET="${2:-}"; shift 2 ;;
    --broker)   BROKER="${2:-}"; shift 2 ;;
    --agent-id) AGENT_ID="${2:-}"; shift 2 ;;
    --yes|-y)   ASSUME_YES=1; shift ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --help|-h)  usage ;;
    *) die "unknown option: $1 (try --help)" ;;
  esac
done

say ""
say "  ${B}Plexus${R} ${DIM}— agent mesh protocol${R}"
[ "$DRY_RUN" -eq 1 ] && say "  ${AMBER}dry run: nothing will be changed${R}"
say ""

# ── where are we ─────────────────────────────────────────────────────────────
# Works from a clone, or standalone (curl | bash), in which case we fetch one.

SOURCE_DIR=""
if [ -f "$(dirname "$0")/PROTOCOL.md" ] && [ -d "$(dirname "$0")/hosts" ]; then
  SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"
  ok "using this checkout: $SOURCE_DIR"
else
  command -v git >/dev/null 2>&1 || die "git is required to fetch Plexus"
  SOURCE_DIR="$(mktemp -d)/plexus"
  step "fetching Plexus"
  run git clone --depth 1 --quiet "$REPO_URL" "$SOURCE_DIR" || die \
    "could not clone $REPO_URL.
    If it is private, authenticate first (gh auth login, or use an SSH remote),
    or clone it yourself and run ./install.sh from inside the checkout."
  ok "fetched to $SOURCE_DIR"
fi

# ── detect ───────────────────────────────────────────────────────────────────
HAS_OPENCLAW=0; HAS_HERMES=0
[ -d "$HOME/.openclaw" ] && HAS_OPENCLAW=1
[ -d "$HOME/.hermes" ] && HAS_HERMES=1
command -v openclaw >/dev/null 2>&1 && HAS_OPENCLAW=1
command -v hermes   >/dev/null 2>&1 && HAS_HERMES=1

if [ -z "$TARGET" ]; then
  step "looking for an agent platform"
  [ "$HAS_OPENCLAW" -eq 1 ] && ok "found OpenClaw  ${DIM}(~/.openclaw)${R}"
  [ "$HAS_HERMES"   -eq 1 ] && ok "found Hermes    ${DIM}(~/.hermes)${R}"

  if [ "$HAS_OPENCLAW" -eq 1 ] && [ "$HAS_HERMES" -eq 1 ]; then
    TARGET="both"
    warn "both are installed — installing for both"
  elif [ "$HAS_OPENCLAW" -eq 1 ]; then TARGET="openclaw"
  elif [ "$HAS_HERMES"   -eq 1 ]; then TARGET="hermes"
  else
    warn "neither OpenClaw nor Hermes found"
    say ""
    say "  Plexus is a protocol, not an app — it needs an agent platform to plug into,"
    say "  or you can write an agent directly against it:"
    say ""
    say "    ${B}OpenClaw${R}   https://openclaw.ai"
    say "    ${B}Hermes${R}     https://hermes-agent.nousresearch.com"
    say "    ${B}Node${R}       npm install plexus-agent      ${DIM}— no platform needed${R}"
    say ""
    say "  Or just watch two agents talk, with nothing installed at all:"
    say "    ${DIM}cd $SOURCE_DIR && npm install && npm run demo${R}"
    say ""
    exit 0
  fi
fi
say ""

# ── broker ───────────────────────────────────────────────────────────────────
if [ -z "$BROKER" ]; then
  BROKER="mqtt://localhost:1883"
fi

broker_reachable() {
  host="$(printf '%s' "$1" | sed -E 's#^[a-z]+://##; s#/.*##; s#:.*##')"
  port="$(printf '%s' "$1" | sed -E 's#^[a-z]+://##; s#/.*##' | awk -F: 'NF>1{print $NF}')"
  [ -z "$port" ] && port=1883
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null && return 0 || return 1
}

step "checking the broker"
if broker_reachable "$BROKER"; then
  ok "$BROKER is reachable"
else
  warn "$BROKER is NOT reachable"
  say ""
  say "  Plexus needs an MQTT broker. Everything below will install fine without"
  say "  one, but the agent will sit offline until it can connect. Start one with:"
  say ""
  say "    ${DIM}docker run -d -p 1883:1883 eclipse-mosquitto:2 \\"
  say "      sh -c 'printf \"listener 1883\\nallow_anonymous true\\n\" > /m.conf && mosquitto -c /m.conf'${R}"
  say ""
  say "  or  ${DIM}brew install mosquitto && mosquitto -p 1883${R}"
  say ""
  confirm "carry on anyway?" || exit 1
fi
say ""

# ── OpenClaw ─────────────────────────────────────────────────────────────────
install_openclaw() {
  DEST="$HOME/.openclaw/extensions/mqtt-bridge"
  step "installing the OpenClaw host plugin"

  command -v node >/dev/null 2>&1 || die "node is required (18 or newer)"
  node_major="$(node -p 'process.versions.node.split(".")[0]')"
  [ "$node_major" -ge 18 ] || die "node 18+ required, found $(node -v)"
  ok "node $(node -v)"

  if [ -d "$DEST/.git" ]; then
    ok "already installed at $DEST"
    if confirm "update it to the latest commit?"; then
      run git -C "$DEST" pull --ff-only origin main
    fi
  elif [ -d "$DEST" ]; then
    die "$DEST exists but is not a git clone. Move it aside and re-run."
  else
    # A clone rather than a copy: the repository *is* the deployment, so updates
    # arrive by git pull and nothing drifts between the two.
    #
    # Cloned from the local checkout rather than from GitHub — it needs no
    # network, no credentials, and works while the repository is still private.
    # origin is then repointed at the canonical URL so `git pull` behaves
    # normally afterwards.
    if [ -d "$SOURCE_DIR/.git" ]; then
      run git clone --quiet "$SOURCE_DIR" "$DEST" \
        || die "could not clone $SOURCE_DIR into $DEST"
      run git -C "$DEST" remote set-url origin "$REPO_URL"
      ok "cloned to $DEST  ${DIM}(from this checkout; origin → $REPO_URL)${R}"
    else
      run git clone --quiet "$REPO_URL" "$DEST" || die \
        "could not clone $REPO_URL.
    If the repository is private, authenticate first (gh auth login, or an SSH
    remote), or run this script from inside a checkout you already have."
      ok "cloned to $DEST"
    fi
  fi

  run sh -c "cd '$DEST' && npm install --silent"
  ok "dependencies installed"

  if [ ! -f "$DEST/services.json" ]; then
    run cp "$DEST/services.example.json" "$DEST/services.json"
    ok "created services.json  ${DIM}(your capability catalog — edit it)${R}"
  else
    ok "services.json kept  ${DIM}(yours, untouched)${R}"
  fi

  run sh -c "cd '$DEST' && npm run build --silent >/dev/null"
  ok "built"

  AGENT="${AGENT_ID:-$(hostname -s 2>/dev/null || echo agent)}"
  OC_CONFIG="$HOME/.openclaw/openclaw.json"

  # If the plugin is already configured, printing a config block to paste is
  # actively harmful: it carries a freshly generated web.auth, so following the
  # instruction would rotate a working token or produce duplicate JSON keys.
  if [ -f "$OC_CONFIG" ] && grep -q '"mqtt-bridge"' "$OC_CONFIG" 2>/dev/null; then
    ok "already configured in openclaw.json  ${DIM}(left alone)${R}"
    if grep -q 'mqtt_publish' "$OC_CONFIG" 2>/dev/null; then
      ok "mesh tools are allowed"
    else
      warn "${B}tools.alsoAllow${R} does not mention mqtt_publish"
      say "    Without it the agent silently has no mesh tools and jobs finish"
      say "    without publishing a result. Add to $OC_CONFIG:"
      say "      ${DIM}\"tools\": { \"alsoAllow\": [\"mqtt_publish\", \"mesh_ask\", \"mesh_peers\"] }${R}"
    fi
    say ""
    say "  Nothing else to do. Restart only if the plugin's code changed:"
    say "    ${DIM}launchctl kickstart -k gui/\$(id -u)/ai.openclaw.gateway${R}   ${DIM}# macOS${R}"
    say ""
    return 0
  fi

  say ""
  say "  ${B}One manual step left.${R} Add this to ${B}$OC_CONFIG${R}:"
  say ""
  cat <<EOF
${DIM}  {
    "plugins": {
      "allow": ["mqtt-bridge"],
      "entries": {
        "mqtt-bridge": {
          "enabled": true,
          "config": {
            "broker": { "url": "$BROKER" },
            "mesh":   { "root": "agents", "agentId": "$AGENT" },
            "web":    { "auth": "$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)" }
          }
        }
      }
    },
    "tools": {
      "alsoAllow": ["mqtt_publish", "mesh_ask", "mesh_peers"]
    }
  }${R}
EOF
  say ""
  warn "the ${B}tools.alsoAllow${R} block is not optional — without it the agent"
  say "    silently has no mesh tools, and jobs finish without publishing a result"
  say ""
  say "  Then:"
  say "    ${DIM}openclaw config validate${R}   ${DIM}# an invalid config stops the gateway starting${R}"
  say "    ${DIM}launchctl kickstart -k gui/\$(id -u)/ai.openclaw.gateway${R}   ${DIM}# macOS${R}"
  say "    ${DIM}systemctl --user restart openclaw-gateway${R}                  ${DIM}# Linux${R}"
  say ""
}

# ── Hermes ───────────────────────────────────────────────────────────────────
install_hermes() {
  DEST="$HOME/.hermes/plugins/plexus"
  CONFIG="$HOME/.hermes/plexus.json"
  step "installing the Hermes host plugin"

  command -v python3 >/dev/null 2>&1 || die "python3 is required"
  ok "$(python3 --version)"

  run mkdir -p "$HOME/.hermes/plugins"
  if [ -d "$DEST" ]; then
    ok "already installed at $DEST"
    confirm "replace it with this version?" && { run rm -rf "$DEST"; run cp -R "$SOURCE_DIR/hosts/hermes" "$DEST"; ok "updated"; }
  else
    run cp -R "$SOURCE_DIR/hosts/hermes" "$DEST"
    ok "installed to $DEST"
  fi

  if python3 -c "import paho.mqtt" 2>/dev/null; then
    ok "paho-mqtt already present"
  else
    step "installing paho-mqtt"
    run python3 -m pip install --quiet "paho-mqtt>=2.1" || {
      warn "pip install failed — you may be on an externally managed python"
      say "    try: ${DIM}python3 -m pip install --user --break-system-packages 'paho-mqtt>=2.1'${R}"
      say "    or:  ${DIM}pipx inject hermes paho-mqtt${R}"
    }
  fi

  AGENT="${AGENT_ID:-hermes}"
  if [ -f "$CONFIG" ]; then
    ok "config kept  ${DIM}($CONFIG — yours, untouched)${R}"
  elif [ "$DRY_RUN" -eq 1 ]; then
    printf '  %s$ write %s%s\n' "$DIM" "$CONFIG" "$R"
  else
    cat > "$CONFIG" <<EOF
{
  "broker": "$BROKER",
  "root": "agents",
  "agentId": "$AGENT",
  "displayName": "$AGENT",

  "capabilities": [
    {
      "service": "research.summarise",
      "description": "Researches a topic and returns a sourced summary.",
      "requestSchema": { "topic": "string" },
      "prompt": "Research {{topic}}. Return JSON with keys: summary, sources, confidence."
    }
  ]
}
EOF
    ok "wrote $CONFIG  ${DIM}(one example capability — edit it)${R}"
  fi

  say ""
  say "  ${B}Recommended.${R} If your Hermes has its API server enabled, add:"
  say "    ${DIM}\"executor\": \"api\", \"apiUrl\": \"http://127.0.0.1:8000/v1\"${R}"
  say ""
  say "  Without it the plugin pushes jobs into a session and depends on the agent"
  say "  choosing to call mesh_publish to report. The API route returns results directly."
  say ""
  say "  Then restart Hermes and look for:"
  say "    ${DIM}plexus: $AGENT online on $BROKER ...${R}"
  say ""
}

case "$TARGET" in
  openclaw) install_openclaw ;;
  hermes)   install_hermes ;;
  both)     install_openclaw; install_hermes ;;
  *)        die "unknown target: $TARGET (openclaw, hermes or both)" ;;
esac

say "  ${B}Done.${R} Watch the mesh with:"
say "    ${DIM}mosquitto_sub -h ${BROKER#*://} -t 'agents/registry/+/profile' -v${R}"
say ""
say "  Docs: ${DIM}$SOURCE_DIR/docs/INSTALL.md${R}"
say ""
