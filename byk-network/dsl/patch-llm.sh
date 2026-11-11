#!/bin/sh
# Bürokrati võrgustiku demo: chats/messages/llm.yml muudatus (POSIX sh + awk, töötab ka busyboxiga).
#   sh patch-llm.sh llm.orig.yml > llm.yml
# - "check_value" ette tuleb võrgustiku kontroll: loeb Bürokrati seadistusest network_config
#   (demo.sh install kirjutab) ja kui LLM-i vastus on "ei tea" tekst või sisaldab $backoffice'i,
#   käivitab pärast salvestamist network/ask. Kui network_config puudub, käitub nagu originaal.
# - Originaalne check_value nimetatakse ümber check_value_orig.
# - Juba muudetud faili ei muudeta teist korda.
set -eu
src=$1
if grep -q "Bürokrati võrgustik (byk-demo)" "$src"; then cat "$src"; exit 0; fi
tmp=${TMPDIR:-/tmp}/patch-llm.$$
mkdir -p "$tmp"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/check.yml" <<'EOF'
# --- Bürokrati võrgustik (byk-demo) ---
check_value:
  call: http.post
  args:
    url: "[#CHATBOT_RESQL]/get-configuration"
    body:
      key: "network_config"
  error: check_value_orig
  result: network_cfg_res

network_cfg_assign:
  assign:
    networkCfg: "$=JSON.parse(network_cfg_res.response.body[0]?.value ?? '{}')="
  next: network_trigger

network_trigger:
  switch:
    - condition: ${(networkCfg.triggerPrefixes ?? []).some(p => (message.content ?? '').trim().startsWith(p)) || (networkCfg.triggerContains ?? []).some(p => (message.content ?? '').includes(p))}
      next: assign_network_forward
  next: check_value_orig

assign_network_forward:
  assign:
    networkForward: true
  next: network_route

# Suunamise vastust ($backoffice jms) ei salvestata: kohe võrgustikku
network_route:
  switch:
    - condition: ${(networkCfg.triggerContains ?? []).some(p => (message.content ?? '').includes(p))}
      next: network_forward
  next: format_messages
# --- /Bürokrati võrgustik ---

check_value_orig:
EOF

cat > "$tmp/return.yml" <<'EOF'
# --- Bürokrati võrgustik (byk-demo) ---
network_check:
  switch:
    - condition: ${networkForward === true}
      next: network_forward
  next: return_ok

network_forward:
  call: http.post
  args:
    url: "[#CHATBOT_RUUTER_PUBLIC_INTERNAL]/network/ask"
    body:
      chatId: ${sender}
  timeout: 20000
  error: return_ok
  result: network_forward_res
  next: return_ok
# --- /Bürokrati võrgustik ---

EOF

awk -v check="$tmp/check.yml" -v ret="$tmp/return.yml" '
  function emit(f,   l) { while ((getline l < f) > 0) print l; close(f) }
  BEGIN { a = 0; c = 0 }
  /^check_value:[[:space:]]*$/ && !a { emit(check); a = 1; next }
  /^return_ok:[[:space:]]*$/ && !c { emit(ret); c = 1 }
  { print }
  END { if (!(a && c)) { printf "patch-llm: ankrud puudu: check_value=%d return_ok=%d\n", a, c > "/dev/stderr"; exit 3 } }
' "$src"
