# Douyin CDP Upload Runbook

本文记录一次已验证成功的抖音上传方案：使用一个干净的 Chrome 调试实例手动登录抖音，然后 monitor-be 通过 Chrome DevTools Protocol 复用该浏览器上下文完成自动上传。

## 背景

抖音创作者中心登录态不能稳定依赖单纯导出 cookie/storage state：

- 从普通 Chrome 导出 `storage_state` 写入 `yd_douyin_account` 后，后端新 Playwright 浏览器仍可能被判定未登录。
- 普通 Chrome 即使页面已登录，也不一定能通过现有 `9222` CDP 端口暴露当前页面 target，表现为 `/json/list` 为空。
- 成功路径是启动一个独立、干净、带 remote debugging 的 Chrome Profile，在该窗口内手动登录，再让后端通过同一个 CDP browser websocket 操作。

## 成功环境

- monitor-be 默认端口：`8200`
- Chrome 调试端口：`9333`
- Chrome Profile 目录：`/private/tmp/youbi-douyin-debug-chrome`
- 抖音上传页：`https://creator.douyin.com/creator-micro/content/upload`
- 本次验证任务：`pj05eb_2wKU`

## 1. 启动干净 Chrome 调试实例

```bash
open -na "Google Chrome" --args \
  --remote-debugging-port=9333 \
  --user-data-dir=/private/tmp/youbi-douyin-debug-chrome \
  --no-first-run \
  --no-default-browser-check \
  https://creator.douyin.com/creator-micro/content/upload
```

注意：

- 不要复用普通 Chrome 主窗口。
- 不要复用小红书调试 Profile。
- 这个窗口打开后，在该窗口内手动完成抖音登录、短信验证、滑块验证等流程，直到页面能看到“上传视频”入口。

## 2. 验证 CDP target 可见

```bash
curl -sS http://127.0.0.1:9333/json/version
curl -sS http://127.0.0.1:9333/json/list
```

`/json/version` 应返回类似：

```json
{
  "webSocketDebuggerUrl": "ws://127.0.0.1:9333/devtools/browser/..."
}
```

如果 `/json/list` 暂时为空，可以用 Playwright 连接 browser websocket 新开页面验证登录态：

```bash
.venv/bin/python -c 'from playwright.sync_api import sync_playwright; ws="ws://127.0.0.1:9333/devtools/browser/<id>"; p=sync_playwright().start(); b=p.chromium.connect_over_cdp(ws); c=b.contexts[0]; pg=c.new_page(); pg.goto("https://creator.douyin.com/creator-micro/content/upload", wait_until="domcontentloaded", timeout=60000); pg.wait_for_timeout(3000); print("url", pg.url); print("file_inputs", pg.locator("input[type=\"file\"]").count()); print("login_text", pg.get_by_text("手机号登录").count(), pg.get_by_text("扫码登录").count()); p.stop()'
```

成功判定：

- `file_inputs 1`
- `login_text 0 0`

这说明该调试 Profile 的登录态可被 CDP 新页面复用。

## 3. 使用 CDP 模式启动 monitor-be

从 `/Users/hoshuuch/Money/YouBi/monitor/monitor-be` 启动：

```bash
mvn spring-boot:run \
  -Dspring-boot.run.arguments=--youbi.douyin.cdp-url=ws://127.0.0.1:9333/devtools/browser/<id>
```

其中 `<id>` 来自 `/json/version` 的 `webSocketDebuggerUrl`。

服务启动后确认端口：

```bash
lsof -nP -iTCP:8200 -sTCP:LISTEN
```

### 服务器 Docker 接入方式

服务器上 Chrome 使用宿主机网络监听 `127.0.0.1:9333`；`monitor-be` 容器使用 `--network host`，所以容器内访问 `127.0.0.1:9333` 就是访问同一个宿主机 Chrome。

服务器上可视化 Chrome 环境：

```bash
Xvfb :99 -screen 0 1400x900x24
DISPLAY=:99 fluxbox
x11vnc -display :99 -localhost -forever -shared -rfbport 5901 -rfbauth /root/.vnc/passwd

DISPLAY=:99 google-chrome \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9333 \
  --user-data-dir=/hoshuuch/YouBi/douyin-chrome-profile-animal \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  https://creator.douyin.com/creator-micro/content/upload
```

本机通过 SSH 隧道访问 VNC/CDP，不需要开放公网端口：

```bash
ssh -N \
  -L 5901:127.0.0.1:5901 \
  -L 9333:127.0.0.1:9333 \
  root@120.53.92.66
```

登录完成后，在服务器上获取 browser websocket：

```bash
DOUYIN_CDP_URL="$(curl -fsS http://127.0.0.1:9333/json/version \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["webSocketDebuggerUrl"])')"
printf '%s\n' "$DOUYIN_CDP_URL"
```

重启 `monitor-be` 时传入 `YDBI_DOUYIN_CDP_URL`：

```bash
docker rm -f monitor-be
docker run -d \
  --name monitor-be \
  --network host \
  --restart unless-stopped \
  -v /hoshuuch/YouBi/workfolder:/work \
  -e YDBI_DOUYIN_CDP_URL="$DOUYIN_CDP_URL" \
  monitor-be:latest
```

验证容器能看到宿主机 Chrome：

```bash
docker exec monitor-be curl -sS http://127.0.0.1:9333/json/version
```

### 多账号 CDP 路由

如果需要按视频 `type` 使用不同的已登录抖音 Chrome，会话可以用多个独立 Chrome Profile 和多个 CDP 端口。后端按上传请求里的 `accountKey` 选择 CDP endpoint；当前 uploader 会把 `yd_video_info.type` 作为各平台上传账号路由键，所以无需额外传 `type` 字段。

同一个 `DISPLAY=:99` 下可以启动两个 Chrome：

```bash
DISPLAY=:99 nohup google-chrome \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9333 \
  --user-data-dir=/hoshuuch/YouBi/douyin-chrome-profile-animal \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  https://creator.douyin.com/creator-micro/content/upload \
  > /hoshuuch/YouBi/logs/douyin-animal-chrome.log 2>&1 &

DISPLAY=:99 nohup google-chrome \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9334 \
  --user-data-dir=/hoshuuch/YouBi/douyin-chrome-profile-knowledge \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  https://creator.douyin.com/creator-micro/content/upload \
  > /hoshuuch/YouBi/logs/douyin-knowledge-chrome.log 2>&1 &
```

CDP 端口维护在 `yd_douyin_cdp_session`，不依赖账号是否已经有登录态：

```sql
CREATE TABLE IF NOT EXISTS yd_douyin_cdp_session (
  account_key VARCHAR(64) NOT NULL PRIMARY KEY,
  cdp_port INT NULL,
  cdp_endpoint VARCHAR(255) NULL,
  note VARCHAR(255) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO yd_douyin_cdp_session (account_key, cdp_port, cdp_endpoint, note)
VALUES
  ('animal', 9333, NULL, 'animal chrome profile on DISPLAY=:99'),
  ('knowledge', 9334, NULL, 'knowledge chrome profile on DISPLAY=:99')
ON DUPLICATE KEY UPDATE
  cdp_port = VALUES(cdp_port),
  cdp_endpoint = VALUES(cdp_endpoint),
  note = VALUES(note),
  updated_at = NOW();
```

如果需要跨主机或非本机端口，也可以直接写 `cdp_endpoint`，例如 `http://127.0.0.1:9334` 或 `ws://127.0.0.1:9334/devtools/browser/<id>`。推荐只维护 `cdp_port` 或 HTTP endpoint，不固定 `ws://.../devtools/browser/<id>`。Chrome 重启后 browser websocket id 会变化，后端会在每次上传前从 `/json/version` 读取最新 `webSocketDebuggerUrl`。

兼容规则：

- `yd_douyin_cdp_session` 命中 `accountKey` 的 `cdp_endpoint` 或 `cdp_port` 时优先使用对应会话。
- `yd_douyin_account` 上的 `cdp_endpoint` / `cdp_port` 字段保留为兼容 fallback。
- `YDBI_DOUYIN_CDP_ENDPOINTS` 仍保留为环境变量兜底，格式为 `animal=http://127.0.0.1:9333;knowledge=http://127.0.0.1:9334`。
- 可配置 `default=http://127.0.0.1:9333` 作为映射默认值。
- 没有命中映射时，仍会回退到旧的 `YDBI_DOUYIN_CDP_URL`。
- 同一个 endpoint 内部加了上传锁，避免并发请求同时操作同一个 Chrome Profile。

本地打开服务器 VNC 可以直接运行：

```bash
scripts/open_douyin_server_vnc.command
```

脚本会建立 SSH 隧道，转发 `5901`、`9333`、`9334`，然后用 macOS `open vnc://127.0.0.1:5901` 打开屏幕共享。
VNC 密码会自动复制到本机剪贴板，连接弹窗出现后直接粘贴即可。

## 4. 触发抖音上传

本次验证使用 `pj05eb_2wKU`：

```bash
curl -sS -X POST http://127.0.0.1:8200/api/douyin/upload \
  -H 'Content-Type: application/json' \
  -d '{
    "taskId":"pj05eb_2wKU",
    "accountKey":"douyin_main",
    "videoUrl":"https://120.53.92.66/minio/ydbi/pj05eb_2wKU/combiner/video_final.mp4",
    "coverUrl":"https://120.53.92.66/minio/ydbi/pj05eb_2wKU/downloader/thumbnail.jpg",
    "title":"Tropical Royal Flycatcher This Bird is Insane",
    "description":"Tropical Royal Flycatcher This Bird is Insane",
    "tags":"Tropical Royal Flycatcher,Royal Flycatcher,Tropical Flycatcher"
  }'
```

成功日志关键点：

```text
Douyin upload connected existing Chrome over CDP
Douyin upload set video input
Douyin upload entered publish page v2
Douyin upload video uploaded
Douyin upload publish success page reached
url=https://creator.douyin.com/creator-micro/content/manage?enter_from=publish
```

## 5. 弹窗处理

本次发布中出现过两个遮挡发布按钮的抖音引导弹窗：

- “视频预览功能”提示，按钮：“我知道了”
- 封面设置弹窗：“设置竖封面获得更多流量”，可点“暂不设置”或完成封面设置

如果自动流程卡住，会在诊断目录生成截图：

```text
/var/folders/.../T/ydbi/monitor-be/uploads/diagnostics/<taskId>/
```

本次实际卡点截图：

```text
douyin-publish-wait-1.png
```

可通过 CDP 临时点掉弹窗：

```bash
.venv/bin/python -c 'from playwright.sync_api import sync_playwright; ws="ws://127.0.0.1:9333/devtools/browser/<id>"; p=sync_playwright().start(); b=p.chromium.connect_over_cdp(ws); page=b.contexts[0].pages[0]; 
for text in ["我知道了", "暂不设置", "完成"]:
    try:
        loc=page.get_by_text(text).last
        if loc.count():
            print("click", text, loc.count())
            loc.click(timeout=3000)
            page.wait_for_timeout(800)
    except Exception as e:
        print("skip", text, str(e)[:120])
p.stop()'
```

点掉遮挡后，后端上传请求继续执行，并成功进入管理页。

## 6. 已修复问题

本次成功发布后，接口曾误报失败：

```text
Data truncation: Data too long for column 'user_id'
```

原因是发布成功后保存 CDP context 的 `storageState`，解析出的 `user_id` 超过 `yd_douyin_account.user_id VARCHAR(128)`。

已修复：

- `DouyinAccountService.saveStorageState` 保存 `user_id` / `nickname` 前截断到 128 字符。
- `mvn -q -DskipTests compile` 已通过。

## 7. 结论

可靠上传路径：

1. 启动独立 Chrome 调试实例，使用固定端口 `9333` 和固定 profile 目录。
2. 在该窗口手动完成抖音登录和风控验证。
3. 用 `/json/version` 获取 browser websocket。
4. 用 `--youbi.douyin.cdp-url=<webSocketDebuggerUrl>` 启动 monitor-be。
5. 调用 `/api/douyin/upload`。

不要把“普通 Chrome 已登录”直接等同于“后端 Playwright 可用”。后端必须能通过 CDP 连接到同一个已登录调试 Profile，并且新开的 CDP 页面能检测到上传 file input。
