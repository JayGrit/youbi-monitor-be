# Bilibili Playwright Upload

## 背景

Monitor 后端原有 B 站上传路径保留为主路径，继续通过 `BilibiliUploadService` 调用 Bilibili 投稿接口上传。新增 Playwright 上传路径用于处理接口上传遇到频控、风控时的降级场景。

本次目标不是替换原实现，而是在同一个监控后端中增加一套可调试、可复用数据库登录态的浏览器上传方案。

## 最终方案

### 账号存储

Playwright 登录态复用现有 B 站账号表 `yd_bilibili_account`，不单独维护新账号表。新增字段：

```sql
ALTER TABLE yd_bilibili_account ADD COLUMN playwright_mid BIGINT NULL;
ALTER TABLE yd_bilibili_account ADD COLUMN playwright_uname VARCHAR(128) NULL;
ALTER TABLE yd_bilibili_account ADD COLUMN playwright_storage_state_json MEDIUMTEXT NULL;
ALTER TABLE yd_bilibili_account ADD COLUMN playwright_updated_at DATETIME NULL;
```

这些字段只保存 Playwright 浏览器方案需要的状态，不覆盖原有 `mid`、`uname`、`login_info_json`。这样原 Bilibili API 上传路径和账号续期逻辑不受影响。

当前手动登录得到的 storage state 已写入 `account_key = 'knowledge'` 行：

- `playwright_storage_state_json`: Playwright `BrowserContext.storageState()`
- `playwright_mid`: cookie 校验得到的 B 站 UID
- `playwright_uname`: cookie 校验得到的昵称
- `playwright_updated_at`: 保存时间

### 后端服务

新增 `BilibiliPlaywrightAccountService`：

- 从 `yd_bilibili_account.playwright_storage_state_json` 读取 Playwright storage state。
- 打开手动登录窗口并轮询登录结果。
- 登录成功后将 `BrowserContext.storageState()` 写回 `yd_bilibili_account`。
- 调用 `https://api.bilibili.com/x/space/myinfo` 校验 cookie 是否仍有效。
- 启动时自动补齐 `playwright_*` 字段。

新增 `BilibiliPlaywrightUploadService`：

- 使用数据库 storage state 创建 Playwright context。
- 打开创作中心投稿页。
- 设置隐藏的文件选择 input。
- 填写标题、简介、标签等投稿信息。
- 选择自制内容声明。
- 点击立即投稿。
- 每个关键步骤保存截图和 HTML 到诊断目录。

新增 `BilibiliPlaywrightController`：

- `GET /api/bilibili/playwright/accounts`
- `GET /api/bilibili/playwright/status?accountKey=knowledge`
- `POST /api/bilibili/playwright/login/open`
- `POST /api/bilibili/playwright/login/poll`
- `POST /api/bilibili/playwright/upload`
- `POST /api/bilibili/playwright/inspect-upload-page`
- `POST /api/bilibili/playwright/inspect-upload-selection`

### 统一上传接口降级

`POST /api/bilibili/upload` 仍然优先调用原来的 `BilibiliUploadService`。只有原路径返回失败或抛异常，并且错误文本命中以下条件时，才降级到 Playwright：

- `投稿频繁`
- `操作频繁`
- `频繁`
- `风控`
- `rate limit`
- `too frequent`

降级成功时，返回体的 `raw` 中会包含：

- `fallbackFrom = bilibili-api`
- `fallbackReason = 原上传错误信息`

## 调试结论

### Cookie 复用能力

B 站 Playwright storage state 更接近小红书：把 cookie/storage state 存到数据库后，在新的 Playwright context 中可以直接打开投稿页，不要求像抖音那样在新机器上保持已有 CDP 浏览器连接。

验证方式：

1. 本地手动登录 B 站创作中心。
2. 调用登录轮询接口保存 storage state。
3. 将 storage state 写入 `yd_bilibili_account.account_key = 'knowledge'`。
4. 重启后端并调用 `inspect-upload-page`。
5. Playwright 使用数据库中的 storage state 成功打开上传页。

验证截图示例：

```text
/var/folders/sz/nbbt64s14f70dncmsxdjhbjc0000gn/T/ydbi/monitor-be/uploads/diagnostics/bilibili-playwright/inspect-5fa7296b-bd4e-4f32-9d5c-481cbaff215b/upload-page.png
```

### 实际上传

本地使用测试视频实际走过一次 Playwright 投稿流程，页面进入“稿件处理进度”，证明上传、表单填写、提交链路可用。

## 遇到的问题

### 账号表选择错误

早期实现曾临时创建 `yd_bilibili_playwright_account` 存 Playwright 登录态。后来确认 B 站已有统一账号表 `yd_bilibili_account`，并且已有 `animal`、`game`、`knowledge` 三个账号，所以最终改为在现有表上增加 `playwright_*` 字段。

旧临时表不再被代码读取。后续可以在确认无其他依赖后手动清理。

### 数据库名称混淆

全局记忆里的 MySQL 示例连接使用 `in-book`，但 monitor 后端实际 `application.yml` 连接的是 `youbi`。执行 DDL 前需要以应用配置为准：

```yaml
spring:
  datasource:
    url: jdbc:mysql://120.53.92.66:3306/youbi
```

### Playwright 账号身份与账号 key 的语义

这次手动登录并保存到 `knowledge` 的 Playwright cookie，接口校验出的 B 站账号是 `尖errrrrr / 678811807`。为了避免覆盖原账号表的旧字段，保存到了 `playwright_mid` 和 `playwright_uname`，原 `mid`、`uname`、`login_info_json` 保持不变。

如果后续要求 `knowledge` 的原 API 登录态和 Playwright 登录态必须是同一个 B 站号，需要重新用目标账号手动登录一次 Playwright。

### 上传页稳定性

B 站创作中心页面是动态前端，上传控件和投稿按钮可能随页面版本变化。当前实现用 Playwright 截图和 HTML 诊断文件保留每一步状态，便于后续排查选择器或风控变化。

## 运行配置

```yaml
youbi:
  bilibili:
    playwright:
      headless: ${YDBI_BILIBILI_PLAYWRIGHT_HEADLESS:false}
      browser-channel: ${YDBI_BILIBILI_PLAYWRIGHT_BROWSER_CHANNEL:}
```

本地调试建议 `headless=false`，服务器运行可根据环境调整为 headless。
