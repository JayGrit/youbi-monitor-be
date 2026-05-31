#!/bin/zsh
set -euo pipefail

SERVER_HOST="${YOUBI_SERVER_HOST:-120.53.92.66}"
SERVER_USER="${YOUBI_SERVER_USER:-root}"
LOCAL_VNC_PORT="${YOUBI_LOCAL_VNC_PORT:-5901}"
REMOTE_VNC_PORT="${YOUBI_REMOTE_VNC_PORT:-5901}"
VNC_PASSWORD="${YOUBI_VNC_PASSWORD:-Hoshuuch0815@}"

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
CONTROL_PATH="/tmp/youbi-douyin-vnc-%r@%h:%p"

if ! ssh -S "${CONTROL_PATH}" -O check "${SSH_TARGET}" >/dev/null 2>&1; then
  ssh -fN \
    -M \
    -S "${CONTROL_PATH}" \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -L "${LOCAL_VNC_PORT}:127.0.0.1:${REMOTE_VNC_PORT}" \
    "${SSH_TARGET}"
fi

printf '%s' "${VNC_PASSWORD}" | pbcopy
open "vnc://127.0.0.1:${LOCAL_VNC_PORT}"
printf 'VNC password copied to clipboard. Connect to vnc://127.0.0.1:%s\n' "${LOCAL_VNC_PORT}"
