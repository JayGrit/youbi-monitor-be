#!/usr/bin/env zsh
set -euo pipefail

ACCOUNT_KEY="${YDBI_ALIDRIVE_ACCOUNT_KEY:-default}"
MYSQL_HOST="${YDBI_MYSQL_HOST:-120.53.92.66}"
MYSQL_PORT="${YDBI_MYSQL_PORT:-3306}"
MYSQL_DATABASE="${YDBI_MYSQL_DATABASE:-youbi}"
MYSQL_USER="${YDBI_MYSQL_USER:-hoshuuch}"
MYSQL_PASSWORD="${YDBI_MYSQL_PASSWORD:-490229}"

CHROME_DIR="${CHROME_DIR:-${HOME}/Library/Application Support/Google/Chrome}"
TOKEN="$(
  find "${CHROME_DIR}" -path "*/Local Storage/leveldb/*" -type f \( -name "*.log" -o -name "*.ldb" \) -print0 2>/dev/null \
    | xargs -0 strings -a -n 8 2>/dev/null \
    | awk '
        /default_sbox_drive_id/ || /default_drive_id/ {
          in_www_token = 80
        }
        in_www_token > 0 && previous ~ /refresh_/ && $0 ~ /^[0-9a-f]{32}"/ {
          sub(/".*/, "", $0)
          print
          exit
        }
        {
          previous = $0
          if (in_www_token > 0) {
            in_www_token--
          }
        }
      ' || true
)"

if [[ -z "${TOKEN}" ]]; then
  echo "No AliDrive refresh token found under: ${CHROME_DIR}" >&2
  echo "Open https://www.aliyundrive.com/drive/ in Chrome, make sure you are logged in, then run this script again." >&2
  exit 1
fi

esc() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\'/\\\'}"
  print -r -- "${value}"
}

ACCOUNT_KEY_SQL="$(esc "${ACCOUNT_KEY}")"
TOKEN_SQL="$(esc "${TOKEN}")"

mysql \
  -h "${MYSQL_HOST}" \
  -P "${MYSQL_PORT}" \
  -u "${MYSQL_USER}" \
  "-p${MYSQL_PASSWORD}" \
  "${MYSQL_DATABASE}" <<SQL
INSERT INTO uploader_account_alidrive (account_key, refresh_token)
VALUES ('${ACCOUNT_KEY_SQL}', '${TOKEN_SQL}')
ON DUPLICATE KEY UPDATE
  refresh_token = VALUES(refresh_token),
  updated_at = CURRENT_TIMESTAMP;
SQL

echo "Updated AliDrive refresh token in uploader_account_alidrive.account_key=${ACCOUNT_KEY}"
