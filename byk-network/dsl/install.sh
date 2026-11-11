#!/bin/sh
# Bürokrati võrgustiku demo: Ruuteri DSL-ide lisamine (Ruuteri initContainer byk-network-dsl).
# Jookseb pärast NoOpsi init-copy-files'i: kopeerib network/* ja internal/* DSL-i kausta ning
# muudab chats/messages/llm.yml-i (patch-llm.sh). Ruuter laeb DSL-id käivitumisel.
#   sh install.sh /DSL
set -eu
here=$(cd "$(dirname "$0")" && pwd)
dsl=${1:-/DSL}
post="$dsl/backoffice/POST"
llm="$post/chats/messages/llm.yml"
if [ ! -f "$llm" ]; then
  echo "byk-network-dsl: $llm puudub (DSL-i kaust on mujal?)"; ls -la "$dsl" "$dsl/backoffice" 2>&1 | head -30; exit 1
fi
mkdir -p "$post/network" "$post/internal"
cp "$here"/network/*.yml "$post/network/"
cp "$here"/internal/*.yml "$post/internal/"
sh "$here/patch-llm.sh" "$llm" > "$llm.byk"
mv "$llm.byk" "$llm"
echo "byk-network-dsl: paigaldatud $(ls "$post/network" | tr '\n' ' ')+ internal/$(ls "$post/internal" | grep network | tr "\n" " "), llm.yml muudetud"