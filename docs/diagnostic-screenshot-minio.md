# Diagnostic Artifact MinIO Archive

## 目标

将 monitor 后端产生的诊断截图和 HTML 页面快照全部上传到 MinIO，并把对应 URL 按产生顺序写入 MySQL，便于前端、任务详情页和线上排障直接查看诊断材料，不再依赖进入服务器查文件。

本方案覆盖现有诊断来源：

- `BilibiliPlaywrightUploadService`
- `SocialPlaywrightInspectService`
- `XiaohongshuUploadService`
- `DouyinUploadService`

首期同时归档两类文件：

- 截图：`screenshot_url`
- HTML 页面快照：`html_url`

## 表设计

新建表：`uploader_diagonostic`

```sql
CREATE TABLE IF NOT EXISTS uploader_diagonostic (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    platform VARCHAR(32) NOT NULL,
    source VARCHAR(64) NOT NULL,
    account_key VARCHAR(128) NULL,
    step_index INT NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    screenshot_url TEXT NOT NULL,
    html_url TEXT NULL,
    screenshot_size_bytes BIGINT NULL,
    html_size_bytes BIGINT NULL,
    screenshot_width INT NULL,
    screenshot_height INT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'uploaded',
    error_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_uploader_diag_task_order (task_id, run_id, step_index, id),
    KEY idx_uploader_diag_platform_time (platform, created_at),
    KEY idx_uploader_diag_source_time (source, created_at)
);
```

字段说明：

- `id`: 自增主键，也是全局插入顺序兜底字段。
- `task_id`: 业务任务 ID。手动 inspect 没有真实任务时使用当前代码里的 `inspect-*` / `manual`。
- `run_id`: 一次诊断运行的唯一 ID。上传流程可复用 `taskId`；inspect 场景使用生成的 UUID。
- `platform`: `bilibili`、`douyin`、`xiaohongshu` 等。
- `source`: 产生诊断材料的服务或场景，例如 `bilibili-playwright-upload`、`social-playwright-inspect`。
- `account_key`: 平台账号 key，无法确定时为空。
- `step_index`: 单次运行内的步骤序号，从 1 递增。
- `step_name`: 当前代码里的诊断 label，例如 `01-open-upload-page`、`publish-validation-error`。
- `screenshot_url`: 截图 MinIO URL。
- `html_url`: HTML 页面快照 MinIO URL。
- `screenshot_size_bytes`、`html_size_bytes`: 排查空文件或异常文件时使用。
- `screenshot_width`、`screenshot_height`: 排查空图、异常图时使用。
- `status`: `uploaded`、`upload_failed`。首期推荐只在上传成功后插入 `uploaded`。
- `error_message`: 上传失败时的错误信息。

表里不存 bucket、object key 和本地路径。对调用方来说 URL 是唯一需要关心的定位信息。

查询时必须按以下顺序展示：

```sql
SELECT *
FROM uploader_diagonostic
WHERE task_id = ? AND run_id = ?
ORDER BY step_index ASC, id ASC;
```

`step_index` 保证单次运行内的业务步骤顺序，`id` 兜底保证同一步骤内的插入顺序稳定。

## MinIO 路径规范

虽然数据库只保存 URL，服务内部仍需要生成稳定的 MinIO object key。截图和 HTML 使用同一目录、不同扩展名：

```text
diagnostics/{platform}/{source}/{yyyyMMdd}/{taskId}/{runId}/{stepIndex}-{safeStepName}.png
diagnostics/{platform}/{source}/{yyyyMMdd}/{taskId}/{runId}/{stepIndex}-{safeStepName}.html
```

示例：

```text
diagnostics/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/01-01-open-upload-page.png
diagnostics/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/01-01-open-upload-page.html
diagnostics/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/02-02-video-selected.png
diagnostics/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/02-02-video-selected.html
```

规范要求：

- `taskId`、`runId`、`stepName` 必须经过 `TextSupport.safeSegment`。
- `stepIndex` 使用 2 位左补零，超过 99 时按实际数字输出，例如 `100-publish-wait`。
- URL 使用当前 MinIO endpoint 拼接：

```text
{youbi.minio.endpoint}/{bucket}/{objectKey}
```

如果未来 bucket 改为私有访问，接口层再返回后端代理 URL 或预签名 URL，表结构不变。

## 服务设计

新增 `DiagnosticArtifactService`，职责如下：

1. 接收诊断上下文。
2. 直接从 Playwright 页面生成截图字节和 HTML 字符串。
3. 上传 PNG 和 HTML 到 MinIO。
4. 插入 `uploader_diagonostic`。
5. 返回 `DiagnosticArtifactRecord`，供 inspect 接口直接返回 URL。

建议方法：

```java
DiagnosticArtifactRecord archive(DiagnosticArtifactRequest request);
```

`DiagnosticArtifactRequest` 建议字段：

```java
record DiagnosticArtifactRequest(
        Page page,
        String taskId,
        String runId,
        String platform,
        String source,
        String accountKey,
        int stepIndex,
        String stepName
) {}
```

归档流程：

```text
Playwright page -> screenshot bytes + html bytes -> upload both to MinIO -> insert row -> return URLs
```

不再把截图和 HTML 写入服务器本地路径。这样容器重启、宿主机目录挂载差异、临时目录清理都不会影响诊断材料保留。

上传 MinIO 或插表失败不应阻断上传主流程。推荐行为：

- 业务上传流程：记录 warning，继续执行。
- inspect 接口：在响应中带 `archiveError`。

## 有序插入方案

每次诊断运行维护一个 `DiagnosticRunContext`：

```java
final class DiagnosticRunContext {
    private final String taskId;
    private final String runId;
    private final AtomicInteger sequence = new AtomicInteger(0);

    int nextStepIndex() {
        return sequence.incrementAndGet();
    }

    String objectStepPrefix(int stepIndex) {
        return stepIndex < 100 ? "%02d".formatted(stepIndex) : String.valueOf(stepIndex);
    }
}
```

接入规则：

- 单次上传或 inspect 开始时创建一个 `runId`。
- 每次归档前调用 `nextStepIndex()`。
- 插表使用该 `stepIndex`。
- 返回和查询统一按 `step_index, id` 排序。

现有部分 label 已带 `01-`、`02-` 前缀，但不能依赖字符串排序，因为等待、重试、失败截图可能是动态 label。`step_index` 是唯一可信顺序。

## 接入点

### Bilibili Playwright 上传

当前入口：

- `BilibiliPlaywrightUploadService.dumpDiagnostics`
- `inspectUploadPage`
- `inspectUploadSelection`

改造：

- 构造 `DiagnosticRunContext`。
- 用 `DiagnosticArtifactService.archive(...)` 替换本地 dump。
- `inspectUploadPage` 响应增加 `screenshotUrl`、`htmlUrl`。
- `inspectUploadSelection` 响应增加 `beforeScreenshotUrl`、`beforeHtmlUrl`、`selectedScreenshotUrl`、`selectedHtmlUrl`、`formScreenshotUrl`、`formHtmlUrl`、`filledScreenshotUrl`、`filledHtmlUrl`。

### 统一 inspect 服务

当前入口：

- `SocialPlaywrightInspectService.inspectUploadPage`

改造：

- 使用 `source = social-playwright-inspect`。
- 返回 `screenshotUrl`、`htmlUrl`。

### 小红书上传

当前入口：

- `XiaohongshuUploadService.dumpDiagnostics`

改造：

- 用 `DiagnosticArtifactService.archive(...)` 替换 `PlaywrightDiagnostics.dump(...)`。
- 使用 `platform = xiaohongshu`，`source = xiaohongshu-upload`。

### 抖音上传

当前入口：

- `DouyinUploadService.dumpDiagnostics`

改造：

- 用 `DiagnosticArtifactService.archive(...)` 替换抖音自有本地 dump 逻辑。
- 使用 `platform = douyin`，`source = douyin-upload`。

## API 设计

新增查询接口：

```http
GET /api/diagnostics/screenshots?taskId={taskId}
GET /api/diagnostics/screenshots?taskId={taskId}&runId={runId}
```

虽然接口路径沿用 `screenshots`，返回体同时包含截图和 HTML URL：

```json
{
  "taskId": "abc123",
  "runId": "abc123",
  "items": [
    {
      "id": 101,
      "platform": "bilibili",
      "source": "bilibili-playwright-upload",
      "stepIndex": 1,
      "stepName": "01-open-upload-page",
      "screenshotUrl": "http://120.53.92.66:9000/ydbi/diagnostics/bilibili/...",
      "htmlUrl": "http://120.53.92.66:9000/ydbi/diagnostics/bilibili/...",
      "createdAt": "2026-05-27T12:30:01"
    }
  ]
}
```

任务详情页后续可通过 `taskId` 拉取最近一次或全部诊断运行的材料。若同一个 `taskId` 有多次运行，后端默认按 `created_at DESC` 分组展示。

## 数据库初始化

建议在 monitor-be 启动时自动补齐表结构，保持当前项目风格：

- 新增 `DiagnosticArtifactService.ensureSchema()`，构造后执行。
- 或在 `MonitorService` 的 schema 初始化区域新增 `ensureDiagnosticArtifactSchema()`。

首选放在独立服务里，原因是该表属于诊断归档能力，不属于任务监控 SQL 聚合主体。

## 错误处理

归档失败不能影响上传主链路：

- 截图生成失败：记录 warning，不插入成功记录。
- HTML 生成失败：截图仍可上传，`html_url` 为空，并记录 warning。
- MinIO 上传失败：记录 warning，不插入成功记录。
- 插表失败：记录 warning；业务流程继续执行。

如果需要完整审计失败，可插入 `status = upload_failed`，`screenshot_url = ''`，`error_message = ?`。首期不建议这样做，避免诊断表堆积无图记录。

## 实施步骤

1. 新增 `uploader_diagonostic` schema 初始化。
2. 新增 `DiagnosticArtifactService` 和请求/响应 record。
3. 修改或替换 `PlaywrightDiagnostics.dump`，从“本地落盘”改为“直接上传 MinIO 并返回 URL”。
4. 改造 Bilibili Playwright 上传和 inspect 接口。
5. 改造统一 `SocialPlaywrightInspectService`。
6. 改造小红书和抖音上传诊断。
7. 增加查询接口 `/api/diagnostics/screenshots`。
8. 用一次 `inspect-upload-page` 和一次 B 站上传失败/成功链路验证：
   - MinIO 有 PNG 和 HTML。
   - 表内有 `screenshot_url` 和 `html_url`。
   - `ORDER BY step_index, id` 顺序正确。
   - 服务器上不依赖本地诊断文件。

## 验收标准

- 每次诊断都会尽力将截图和 HTML 上传到 MinIO。
- `uploader_diagonostic` 至少包含任务、时间、截图 URL、HTML URL、步骤名，并能按 `step_index, id` 稳定排序。
- B 站 Playwright inspect 响应直接返回 `screenshotUrl` 和 `htmlUrl`。
- 业务上传不会因为诊断材料上传失败而失败。
- 线上只通过数据库和 MinIO 就能定位某次任务的诊断材料。
