#!/bin/zsh
set -euo pipefail

SERVER_HOST="${YOUBI_SERVER_HOST:-120.53.92.66}"
SERVER_USER="${YOUBI_SERVER_USER:-root}"
LOCAL_VNC_PORT="${YOUBI_LOCAL_VNC_PORT:-5901}"
REMOTE_VNC_PORT="${YOUBI_REMOTE_VNC_PORT:-5901}"
VNC_PASSWORD="${YOUBI_VNC_PASSWORD:-Hoshuuch0815@}"
DISPLAY_ID="${YOUBI_DOUYIN_DISPLAY:-:99}"
SCREEN_SIZE="${YOUBI_DOUYIN_SCREEN_SIZE:-1400x900x24}"
PROFILE_ROOT="${YOUBI_DOUYIN_PROFILE_ROOT:-/hoshuuch/YouBi/workfolder/douyin-chrome-profiles}"
LOG_DIR="${YOUBI_DOUYIN_LOG_DIR:-/hoshuuch/YouBi/logs}"
PROFILE_KEY="${1:-${YOUBI_DOUYIN_PROFILE:-animal}}"

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
CONTROL_PATH="/tmp/youbi-douyin-vnc-%r@%h:%p"
SAFE_PROFILE_KEY="${PROFILE_KEY//[^A-Za-z0-9._-]/_}"

if [[ -z "${SAFE_PROFILE_KEY}" ]]; then
  echo "Profile key 不能为空。用法：$0 <accountKey>，例如：$0 animal"
  exit 1
fi

ssh "${SSH_TARGET}" \
  "YOUBI_DOUYIN_DISPLAY='${DISPLAY_ID}' YOUBI_DOUYIN_SCREEN_SIZE='${SCREEN_SIZE}' YOUBI_DOUYIN_PROFILE_ROOT='${PROFILE_ROOT}' YOUBI_DOUYIN_LOG_DIR='${LOG_DIR}' YOUBI_DOUYIN_PROFILE='${SAFE_PROFILE_KEY}' YOUBI_REMOTE_VNC_PORT='${REMOTE_VNC_PORT}' YOUBI_VNC_PASSWORD='${VNC_PASSWORD}' bash -s" <<'REMOTE'
set -euo pipefail

DISPLAY_ID="${YOUBI_DOUYIN_DISPLAY}"
SCREEN_SIZE="${YOUBI_DOUYIN_SCREEN_SIZE}"
PROFILE_ROOT="${YOUBI_DOUYIN_PROFILE_ROOT}"
LOG_DIR="${YOUBI_DOUYIN_LOG_DIR}"
PROFILE_KEY="${YOUBI_DOUYIN_PROFILE}"
REMOTE_VNC_PORT="${YOUBI_REMOTE_VNC_PORT}"
VNC_PASSWORD="${YOUBI_VNC_PASSWORD}"
PROFILE_DIR="${PROFILE_ROOT}/${PROFILE_KEY}"
CHROME_BIN="$(command -v google-chrome || command -v google-chrome-stable || true)"

mkdir -p "${PROFILE_DIR}" "${LOG_DIR}" /root/.vnc

if [[ -z "${CHROME_BIN}" ]]; then
  echo "Cannot find google-chrome or google-chrome-stable on server"
  exit 1
fi

if [[ ! -f /root/.vnc/passwd ]]; then
  x11vnc -storepasswd "${VNC_PASSWORD}" /root/.vnc/passwd >/dev/null
  chmod 600 /root/.vnc/passwd
fi

if ! pgrep -f "Xvfb ${DISPLAY_ID}" >/dev/null; then
  nohup Xvfb "${DISPLAY_ID}" -screen 0 "${SCREEN_SIZE}" > "${LOG_DIR}/douyin-xvfb.log" 2>&1 &
  sleep 1
fi

if ! DISPLAY="${DISPLAY_ID}" pgrep -f "fluxbox" >/dev/null; then
  DISPLAY="${DISPLAY_ID}" nohup fluxbox > "${LOG_DIR}/douyin-fluxbox.log" 2>&1 &
  sleep 1
fi

if ! pgrep -f "x11vnc .*${DISPLAY_ID}.*${REMOTE_VNC_PORT}" >/dev/null; then
  nohup x11vnc \
    -display "${DISPLAY_ID}" \
    -localhost \
    -forever \
    -shared \
    -rfbport "${REMOTE_VNC_PORT}" \
    -rfbauth /root/.vnc/passwd \
    > "${LOG_DIR}/douyin-x11vnc.log" 2>&1 &
  sleep 1
fi

if pgrep -f "google-chrome.*--user-data-dir=${PROFILE_DIR}" >/dev/null; then
  echo "Chrome already running for profile ${PROFILE_KEY}: ${PROFILE_DIR}"
else
  DISPLAY="${DISPLAY_ID}" nohup "${CHROME_BIN}" \
    --user-data-dir="${PROFILE_DIR}" \
    --no-first-run \
    --no-default-browser-check \
    --disable-dev-shm-usage \
    --no-sandbox \
    https://creator.douyin.com/creator-micro/content/upload \
    > "${LOG_DIR}/douyin-${PROFILE_KEY}-chrome.log" 2>&1 &
  echo "Started Chrome for profile ${PROFILE_KEY}: ${PROFILE_DIR}"
fi
REMOTE

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
printf 'Opened Douyin profile %s over VNC. Password copied to clipboard. Connect to vnc://127.0.0.1:%s\n' "${SAFE_PROFILE_KEY}" "${LOCAL_VNC_PORT}"
