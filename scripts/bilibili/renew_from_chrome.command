#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/renew_from_chrome.py"
REPO_DIR="/Users/hoshuuch/Money/YouBi/monitor/monitor-be"
PYTHON="${REPO_DIR}/.venv/bin/python"
PIP="${REPO_DIR}/.venv/bin/pip"
CDP_URL="${YDBI_CHROME_CDP_URL:-http://127.0.0.1:9222}"
CHROME_APP="/Applications/Google Chrome.app"
CHROME_BIN="${CHROME_APP}/Contents/MacOS/Google Chrome"
CHROME_DEBUG_PROFILE_DIR=""
CHROME_STARTED_BY_SCRIPT=0

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

echo "准备同步 Bilibili Chrome 登录态到数据库。"
echo "脚本会根据 B 站 myinfo 返回的 mid 自动匹配 uploader_account_bilibili.account_key。"
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
  echo "脚本会启动一个全新的临时 Chrome 调试用户目录；如果要同步你日常 Chrome 里的多个 profile，请先手动用 remote debugging 启动那个 Chrome。"
  CHROME_DEBUG_PROFILE_DIR="$(mktemp -d /private/tmp/youbi-bilibili-debug.XXXXXX)"
  echo "专用目录：${CHROME_DEBUG_PROFILE_DIR}"
  read "?按回车启动专用调试 Chrome..."

  echo "正在用 remote debugging 启动 Chrome..."
  "${CHROME_BIN}" \
    --remote-debugging-port=9222 \
    --user-data-dir="${CHROME_DEBUG_PROFILE_DIR}" \
    --no-first-run \
    --no-default-browser-check \
    "https://member.bilibili.com/platform/upload/video/frame?page_from=creative_home_top_upload" >/tmp/youbi-bilibili-chrome.log 2>&1 &
  CHROME_STARTED_BY_SCRIPT=1

  if ! wait_for_cdp 30; then
    echo "Chrome 已启动，但 9222 调试端口仍不可用。"
    echo "最近的 Chrome 日志："
    tail -n 40 /tmp/youbi-bilibili-chrome.log 2>/dev/null || true
    echo
    echo "请确认 9222 端口没有被其它程序占用，然后重新双击脚本。"
    read "?按回车关闭窗口..."
    exit 1
  fi

  echo "请在打开的 Chrome 里确认 Bilibili 已登录。"
  echo "登录完成后回到这个窗口按回车。"
  read "?按回车开始同步..."
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
if [[ "${CHROME_STARTED_BY_SCRIPT}" == "1" ]]; then
  echo "这个 Chrome 是脚本以调试模式启动的；同步完成后可以正常关闭它。"
fi
read "?按回车关闭窗口..."
