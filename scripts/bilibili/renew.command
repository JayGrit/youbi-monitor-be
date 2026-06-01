#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/renew.py"
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

echo "准备逐个扫码同步 Bilibili Playwright 登录态。"
echo "每个窗口都会提示要登录的 key/name，账号 mid 匹配后才写入数据库。"
echo

if ! "${PYTHON}" "${PY_SCRIPT}" "$@"; then
  echo
  echo "同步未完全成功。上面是具体结果。"
  read "?按回车关闭窗口..."
  exit 1
fi

echo
echo "完成。"
read "?按回车关闭窗口..."
