#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

tree=$(cargo tree -p hermes-sdk-embedded --edges normal --depth 1 --locked --offline)
if [[ $(printf '%s\n' "$tree" | wc -l | tr -d ' ') != 3 ]] \
  || ! printf '%s\n' "$tree" | rg -q 'hermes-ledger' \
  || ! printf '%s\n' "$tree" | rg -q 'hermes-host' \
  || printf '%s\n' "$tree" | rg -q 'hermes-local'; then
  printf '%s\n' "$tree" >&2
  echo 'Hermes embedded SDK escaped its engine-and-host closure' >&2
  exit 1
fi

echo 'Hermes embedded SDK dependency closure verified'

adapter=$(cargo tree -p hermes-aegis --edges normal --depth 1 --locked --offline)
if [[ $(printf '%s\n' "$adapter" | wc -l | tr -d ' ') != 5 ]] \
  || ! printf '%s\n' "$adapter" | rg -q 'aegis-client' \
  || ! printf '%s\n' "$adapter" | rg -q 'aegis-protocol' \
  || printf '%s\n' "$adapter" | rg -q 'aegis-(authority|host|service)'; then
  printf '%s\n' "$adapter" >&2
  echo 'Hermes Aegis adapter escaped its public-client production closure' >&2
  exit 1
fi

echo 'Hermes Aegis adapter public-client dependency closure verified'

conformance=$(cargo tree -p hermes-conformance --edges normal --depth 1 --locked --offline)
if [[ $(printf '%s\n' "$conformance" | wc -l | tr -d ' ') != 2 ]] \
  || ! printf '%s\n' "$conformance" | rg -q 'hermes-protocol' \
  || printf '%s\n' "$conformance" | rg -q 'hermes-(ledger|host|service|wire|local)'; then
  printf '%s\n' "$conformance" >&2
  echo 'Hermes conformance suite escaped its public-protocol closure' >&2
  exit 1
fi

echo 'Hermes conformance suite public-protocol dependency closure verified'

for command in hermes-consumer-command hermes-disclosure-command hermes-operator-command; do
  command_tree=$(cargo tree -p "$command" --edges normal --depth 1 --locked --offline)
  if [[ $(printf '%s\n' "$command_tree" | wc -l | tr -d ' ') != 3 ]] \
    || ! printf '%s\n' "$command_tree" | rg -q 'hermes-client' \
    || ! printf '%s\n' "$command_tree" | rg -q 'hermes-protocol' \
    || printf '%s\n' "$command_tree" | rg -q 'hermes-(ledger|host|service|wire|local)|keystone|ratatui|crossterm'; then
    printf '%s\n' "$command_tree" >&2
    echo "$command escaped its public-client closure" >&2
    exit 1
  fi
done

echo 'Hermes command guards public-client closure verified'

local_client=$(cargo tree -p hermes-local-client --edges normal --depth 1 --locked --offline)
if [[ $(printf '%s\n' "$local_client" | wc -l | tr -d ' ') != 4 ]] \
  || ! printf '%s\n' "$local_client" | rg -q 'hermes-client' \
  || ! printf '%s\n' "$local_client" | rg -q 'hermes-protocol' \
  || ! printf '%s\n' "$local_client" | rg -q 'hermes-wire' \
  || printf '%s\n' "$local_client" | rg -q 'hermes-(ledger|host|service|local-daemon)|keystone|ratatui|crossterm'; then
  printf '%s\n' "$local_client" >&2
  echo 'Hermes local client escaped its public transport closure' >&2
  exit 1
fi

echo 'Hermes local client public transport closure verified'

peer=$(cargo tree -p hermes-unix-peer --edges normal --depth 1 --locked --offline)
if [[ $(printf '%s\n' "$peer" | wc -l | tr -d ' ') != 4 ]] \
  || ! printf '%s\n' "$peer" | rg -q 'hermes-local-daemon' \
  || ! printf '%s\n' "$peer" | rg -q 'hermes-service' \
  || ! printf '%s\n' "$peer" | rg -q 'nix' \
  || printf '%s\n' "$peer" | rg -q 'hermes-(ledger|host|protocol|wire|client)|keystone'; then
  printf '%s\n' "$peer" >&2
  echo 'Hermes Unix peer authenticator escaped its credential-mapping closure' >&2
  exit 1
fi
echo 'Hermes Unix peer authenticator dependency closure verified'
