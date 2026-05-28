#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/update_shipinhao_storage_state_from_chrome.py"
REPO_DIR="/Users/hoshuuch/Money/YouBi/monitor/monitor-be"
PYTHON="${REPO_DIR}/.venv/bin/python"
PIP="${REPO_DIR}/.venv/bin/pip"
CDP_URL="${YDBI_CHROME_CDP_URL:-http://127.0.0.1:9222}"
CHROME_APP="/Applications/Google Chrome.app"
CHROME_BIN="${CHROME_APP}/Contents/MacOS/Google Chrome"
CHROME_PROFILE_DIR="${YDBI_CHROME_PROFILE_DIR:-${HOME}/Library/Application Support/Google/Chrome}"

if [[ ! -f "${PY_SCRIPT}" ]]; then
  echo "找不到脚本：${PY_SCRIPT}"
  read "?按回车关闭窗口..."
  exit 1
fi

if [[ ! -x "${PYTHON}" ]]; then
  echo "首次运行：创建 Python 虚拟环境..."
  python3 -m venv "${REPO_DIR}/.venv"
fi

if ! "${PYTHON}" - <<'PY' >/dev/null 2>&1
import mysql.connector
import playwright.sync_api
PY
then
  echo "安装所需 Python 依赖：playwright、mysql-connector-python..."
  "${PIP}" install -U pip
  "${PIP}" install playwright mysql-connector-python
fi

echo "准备同步视频号 Chrome Default Profile 登录态到数据库。"
echo

wait_for_cdp() {
  local attempts="${1:-30}"
  local index
  for index in $(seq 1 "${attempts}"); do
    if curl --noproxy "*" -fsS "${CDP_URL}/json/version" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

if ! curl --noproxy "*" -fsS "${CDP_URL}/json/version" >/dev/null 2>&1; then
  echo "当前没有检测到 Chrome remote debugging：${CDP_URL}"
  echo "脚本会尝试用你的 Chrome Default Profile 启动调试窗口。"
  echo "如果 Chrome 已经打开且没有调试端口，请先完全退出 Chrome 再继续。"
  read "?按回车启动 Default Profile 调试 Chrome..."

  "${CHROME_BIN}" \
    --remote-debugging-port=9222 \
    --user-data-dir="${CHROME_PROFILE_DIR}" \
    --profile-directory=Default \
    --no-first-run \
    --no-default-browser-check \
    "https://channels.weixin.qq.com/platform" >/tmp/youbi-shipinhao-chrome.log 2>&1 &

  if ! wait_for_cdp 30; then
    echo "Chrome 已启动，但 9222 调试端口仍不可用。"
    echo "最近的 Chrome 日志："
    tail -n 40 /tmp/youbi-shipinhao-chrome.log 2>/dev/null || true
    read "?按回车关闭窗口..."
    exit 1
  fi
fi

if ! env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  NO_PROXY="127.0.0.1,localhost,::1" \
  no_proxy="127.0.0.1,localhost,::1" \
  "${PYTHON}" "${PY_SCRIPT}" "$@"; then
  echo
  echo "同步失败。上面是具体错误。"
  read "?按回车关闭窗口..."
  exit 1
fi

echo
echo "完成。"
read "?按回车关闭窗口..."
