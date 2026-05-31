#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/update_shipinhao_storage_state_from_chrome.py"
REPO_DIR="/Users/hoshuuch/Money/YouBi/monitor/monitor-be"
PYTHON="${REPO_DIR}/.venv/bin/python"
PIP="${REPO_DIR}/.venv/bin/pip"

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

echo "准备新增视频号登录态到数据库。"
echo "脚本会打开一个全新的临时 Chrome 窗口；如 default 已存在，会自动写入 default-1/default-2。"
echo

if ! "${PYTHON}" "${PY_SCRIPT}" "$@"; then
  echo
  echo "同步失败。上面是具体错误。"
  read "?按回车关闭窗口..."
  exit 1
fi

echo
echo "完成。"
read "?按回车关闭窗口..."
