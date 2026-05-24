#!/bin/zsh
set -euo pipefail

SERVER_HOST="${YOUBI_SERVER_HOST:-120.53.92.66}"
SERVER_USER="${YOUBI_SERVER_USER:-root}"
LOCAL_VNC_PORT="${YOUBI_LOCAL_VNC_PORT:-5901}"
REMOTE_VNC_PORT="${YOUBI_REMOTE_VNC_PORT:-5901}"
CDP_PORTS_RAW="${YOUBI_DOUYIN_CDP_PORTS:-9333 9334}"
VNC_PASSWORD="${YOUBI_VNC_PASSWORD:-Hoshuuch0815@}"

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
CONTROL_PATH="/tmp/youbi-douyin-vnc-%r@%h:%p"

forward_args=(
  -L "${LOCAL_VNC_PORT}:127.0.0.1:${REMOTE_VNC_PORT}"
)

for port in ${(z)CDP_PORTS_RAW}; do
  [[ -n "${port}" ]] || continue
  forward_args+=(-L "${port}:127.0.0.1:${port}")
done

if ! ssh -S "${CONTROL_PATH}" -O check "${SSH_TARGET}" >/dev/null 2>&1; then
  ssh -fN \
    -M \
    -S "${CONTROL_PATH}" \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    "${forward_args[@]}" \
    "${SSH_TARGET}"
fi

printf '%s' "${VNC_PASSWORD}" | pbcopy
open "vnc://127.0.0.1:${LOCAL_VNC_PORT}"
printf 'VNC password copied to clipboard. Connect to vnc://127.0.0.1:%s\n' "${LOCAL_VNC_PORT}"
