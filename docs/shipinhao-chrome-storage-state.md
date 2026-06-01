# 视频号 Chrome 登录态导入经验

本文记录 2026-05-28 从本机 Chrome Default Profile 读取视频号登录态并写入 MySQL 的实操过程，供后续 agent 继续实现视频号上传链路时复用。

## 目标表

表名：`uploader_account_shipinhao`

建表 SQL：

```sql
CREATE TABLE IF NOT EXISTS uploader_account_shipinhao (
  account_key VARCHAR(64) NOT NULL PRIMARY KEY,
  user_id VARCHAR(128) NULL,
  nickname VARCHAR(128) NULL,
  storage_state_json MEDIUMTEXT NOT NULL,
  is_enabled TINYINT(1) NOT NULL DEFAULT 1,
  upload_cooldown_min_seconds INT NOT NULL DEFAULT 3600,
  upload_cooldown_max_seconds INT NOT NULL DEFAULT 7200,
  last_upload_at DATETIME NULL,
  next_upload_allowed_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

验证 SQL：

```bash
mysql -h 120.53.92.66 -P 3306 -u hoshuuch -p490229 youbi \
  -e "SELECT account_key, user_id, nickname, CHAR_LENGTH(storage_state_json) AS json_chars, is_enabled, updated_at FROM uploader_account_shipinhao;"
```

本次验证结果：

```text
account_key  user_id     nickname  json_chars  is_enabled  updated_at
default      1396070809  NULL      12606       1           2026-05-28 14:17:21
```

## 已落地脚本

- `scripts/shipinhao/new.py`
- `scripts/shipinhao/new.command`

Python 脚本参考了 Bilibili 的 `bilibili/renew_from_chrome.py`：

- 连接本机 Chrome CDP：默认 `http://127.0.0.1:9222`
- 打开 `https://channels.weixin.qq.com/platform`
- 从 Playwright `context.storage_state()` 读取 cookies 和 localStorage
- 默认只保留视频号相关域：`channels.weixin.qq.com`、`weixin.qq.com`、`wx.qq.com`、`qq.com`
- 写入 `uploader_account_shipinhao.storage_state_json`
- 尝试从 cookie/localStorage 里提取 `user_id`，当前可识别到 `1396070809`

直接运行：

```bash
/Users/hoshuuch/Money/YouBi/monitor/monitor-be/.venv/bin/python \
  /Users/hoshuuch/Money/YouBi/monitor/monitor-be/scripts/shipinhao/new.py \
  --account-key default
```

成功输出示例：

```text
Updated uploader_account_shipinhao account_key=default cookies=33 origins=1 bytes=12606 url=https://channels.weixin.qq.com/platform
Detected user_id=1396070809
```

## Chrome 142 踩坑

本机 Chrome 版本：`Chrome/142.0.7444.162`。

直接对真实 Default Profile 开启 CDP 会失败：

```text
DevTools remote debugging requires a non-default data directory. Specify this using --user-data-dir.
```

也就是说，这种命令现在不可用：

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222 \
  --user-data-dir="$HOME/Library/Application Support/Google/Chrome" \
  --profile-directory=Default \
  https://channels.weixin.qq.com/platform
```

成功方案是复制 Default Profile 到临时目录，然后用临时副本启动 CDP Chrome：

```bash
mkdir -p /private/tmp/youbi-shipinhao-chrome-profile

rsync -a \
  --exclude Cache \
  --exclude Code\ Cache \
  --exclude GPUCache \
  --exclude GrShaderCache \
  --exclude ShaderCache \
  "$HOME/Library/Application Support/Google/Chrome/Local State" \
  /private/tmp/youbi-shipinhao-chrome-profile/

rsync -a \
  --exclude Cache \
  --exclude Code\ Cache \
  --exclude GPUCache \
  --exclude GrShaderCache \
  --exclude ShaderCache \
  "$HOME/Library/Application Support/Google/Chrome/Default" \
  /private/tmp/youbi-shipinhao-chrome-profile/

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222 \
  --user-data-dir=/private/tmp/youbi-shipinhao-chrome-profile \
  --profile-directory=Default \
  --no-first-run \
  --no-default-browser-check \
  https://channels.weixin.qq.com/platform
```

CDP 可用时：

```bash
curl --noproxy '*' -fsS http://127.0.0.1:9222/json/version
```

会返回 `webSocketDebuggerUrl`。

## Codex 沙箱注意点

在 sandbox 内直接用 Playwright 连接 `127.0.0.1:9222` 可能失败：

```text
BrowserType.connect_over_cdp: connect EPERM 127.0.0.1:9222 - Local (0.0.0.0:0)
```

解决：对运行 Python 导入脚本的命令申请 `require_escalated`。本次提权后导入成功。

另外，`py_compile` 会尝试在 `scripts/__pycache__` 写 `.pyc`，在当前沙箱下可能失败：

```text
Operation not permitted: '.../__pycache__/update_shipinhao_storage_state_from_chrome...pyc'
```

只做语法检查可以用只读 AST：

```bash
/Users/hoshuuch/Money/YouBi/monitor/monitor-be/.venv/bin/python -c \
  "import ast, pathlib; ast.parse(pathlib.Path('/Users/hoshuuch/Money/YouBi/monitor/monitor-be/scripts/shipinhao/new.py').read_text())"
```

## 清理

导入后关闭临时调试 Chrome，释放 9222：

```bash
curl --noproxy '*' -fsS http://127.0.0.1:9222/json/version
kill <chrome-main-pid>
```

本次关闭后再次 curl 9222 返回：

```text
curl: (7) Failed to connect to 127.0.0.1 port 9222
```

最后用普通方式恢复 Chrome：

```bash
open -a "Google Chrome"
```

## 后续实现提示

视频号上传链路可以先从 `uploader_account_shipinhao.storage_state_json` 创建 Playwright context，目标页仍是：

```text
https://channels.weixin.qq.com/platform
```

建议在后续上传实现里复用现有 `DiagnosticArtifactService`，关键节点至少保存：

- 打开平台页后
- 进入发表/发布页后
- 选择视频文件后
- 表单填写后
- 点击发布前后
- 任意超时或风控提示时
