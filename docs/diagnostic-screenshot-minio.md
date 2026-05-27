# Diagnostic Screenshot MinIO Archive

## 目标

将 monitor 后端产生的所有诊断截图上传到 MinIO，并把截图的 MinIO 地址按产生顺序写入 MySQL，便于前端、任务详情页和线上排障直接查看诊断图，不再依赖进入服务器查本地文件。

本方案覆盖现有诊断来源：

- `BilibiliPlaywrightUploadService`
- `SocialPlaywrightInspectService`
- `XiaohongshuUploadService`
- `DouyinUploadService`

首期只要求截图归档。HTML 诊断文件继续本地保存；表结构预留 `html_minio_url`，后续可按同一机制上传。

## 表设计

新建表：`yd_diagnostic_screenshot`

```sql
CREATE TABLE IF NOT EXISTS yd_diagnostic_screenshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    platform VARCHAR(32) NOT NULL,
    source VARCHAR(64) NOT NULL,
    account_key VARCHAR(128) NULL,
    step_index INT NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    minio_bucket VARCHAR(128) NOT NULL,
    minio_object_key VARCHAR(512) NOT NULL,
    minio_url TEXT NOT NULL,
    html_minio_url TEXT NULL,
    local_path TEXT NULL,
    content_type VARCHAR(64) NOT NULL DEFAULT 'image/png',
    file_size_bytes BIGINT NULL,
    width INT NULL,
    height INT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'uploaded',
    error_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_diag_object (minio_bucket, minio_object_key),
    KEY idx_diag_task_order (task_id, run_id, step_index, id),
    KEY idx_diag_platform_time (platform, created_at),
    KEY idx_diag_source_time (source, created_at)
);
```

字段说明：

- `id`: 自增主键，也是全局插入顺序兜底字段。
- `task_id`: 业务任务 ID。手动 inspect 没有真实任务时使用当前代码里的 `inspect-*` / `manual`。
- `run_id`: 一次诊断运行的唯一 ID。上传流程可复用 `taskId`；inspect 场景使用生成的 UUID。
- `platform`: `bilibili`、`douyin`、`xiaohongshu`、`inspect` 等。
- `source`: 产生截图的服务或场景，例如 `bilibili-playwright-upload`、`social-playwright-inspect`。
- `account_key`: 平台账号 key，无法确定时为空。
- `step_index`: 单次运行内的步骤序号，从 1 递增。
- `step_name`: 当前代码里的诊断 label，例如 `01-open-upload-page`、`publish-validation-error`。
- `minio_bucket`: MinIO bucket，默认 `ydbi`。
- `minio_object_key`: MinIO object key。
- `minio_url`: 可供后端或前端直接访问的 MinIO 地址。
- `html_minio_url`: 预留给 HTML 诊断文件。
- `local_path`: 本地落盘路径，便于迁移期反查。
- `content_type`: 截图固定为 `image/png`。
- `file_size_bytes`、`width`、`height`: 排查空图、异常图时使用。
- `status`: `uploaded`、`upload_failed`。首期只在上传成功后插入 `uploaded`；如果要记录失败，可插入 `upload_failed`。
- `error_message`: 上传失败时的错误信息。

查询时必须按以下顺序展示：

```sql
SELECT *
FROM yd_diagnostic_screenshot
WHERE task_id = ? AND run_id = ?
ORDER BY step_index ASC, id ASC;
```

`step_index` 保证单次运行内的业务步骤顺序，`id` 兜底保证同一毫秒或同一步骤内的插入顺序稳定。

## MinIO 路径规范

截图 object key 使用固定前缀：

```text
diagnostics/screenshots/{platform}/{source}/{yyyyMMdd}/{taskId}/{runId}/{stepIndex}-{safeStepName}.png
```

示例：

```text
diagnostics/screenshots/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/0001-01-open-upload-page.png
diagnostics/screenshots/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/0002-02-video-selected.png
diagnostics/screenshots/bilibili/bilibili-playwright-upload/20260527/abc123/abc123/0003-03-metadata-filled.png
```

规范要求：

- `taskId`、`runId`、`stepName` 必须经过 `TextSupport.safeSegment`。
- `stepIndex` 使用 4 位左补零，便于 MinIO 控制台按名称排序。
- object key 中包含日期，便于后续按时间清理。
- 表里的 `minio_url` 使用当前 MinIO endpoint 拼接：

```text
{youbi.minio.endpoint}/{bucket}/{objectKey}
```

如果未来 bucket 改为私有访问，接口层再返回后端代理 URL 或预签名 URL，表结构不变。

## 服务设计

新增 `DiagnosticScreenshotService`，职责如下：

1. 接收截图上下文和本地截图路径。
2. 读取图片元信息和文件大小。
3. 上传 PNG 到 MinIO。
4. 插入 `yd_diagnostic_screenshot`。
5. 返回 `DiagnosticScreenshotRecord`，供 inspect 接口直接返回 MinIO URL。

建议方法：

```java
DiagnosticScreenshotRecord archive(DiagnosticScreenshotRequest request);
```

`DiagnosticScreenshotRequest` 建议字段：

```java
record DiagnosticScreenshotRequest(
        String taskId,
        String runId,
        String platform,
        String source,
        String accountKey,
        int stepIndex,
        String stepName,
        Path screenshotPath,
        Path htmlPath
) {}
```

`PlaywrightDiagnostics.dump` 仍负责截图和 HTML 本地落盘，避免截图失败时连本地诊断也丢失。归档服务在 dump 成功后调用：

```text
page screenshot -> local png/html -> upload png to MinIO -> insert row -> return MinIO URL
```

上传 MinIO 或插表失败不应阻断上传主流程。推荐行为：

- 业务上传流程：记录 warning，继续执行。
- inspect 接口：返回本地路径，同时在响应中带 `archiveError`。

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
}
```

接入规则：

- 单次上传或 inspect 开始时创建一个 `runId`。
- 每次 dump 前调用 `nextStepIndex()`。
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
- `dumpDiagnostics` 返回值中增加归档结果。
- `inspectUploadPage` 响应增加 `screenshotMinioUrl`。
- `inspectUploadSelection` 响应增加 `beforeScreenshotMinioUrl`、`selectedScreenshotMinioUrl`、`formScreenshotMinioUrl`、`filledScreenshotMinioUrl`。

### 统一 inspect 服务

当前入口：

- `SocialPlaywrightInspectService.inspectUploadPage`

改造：

- 使用 `source = social-playwright-inspect`。
- 返回 `screenshotMinioUrl`。

### 小红书上传

当前入口：

- `XiaohongshuUploadService.dumpDiagnostics`

改造：

- 将 `PlaywrightDiagnostics.dump(..., false)` 的结果传给归档服务。
- 使用 `platform = xiaohongshu`，`source = xiaohongshu-upload`。

### 抖音上传

当前入口：

- `DouyinUploadService.dumpDiagnostics`

改造：

- 先将抖音自有 dump 逻辑收敛到 `PlaywrightDiagnostics.dump`，保持截图文件名兼容可不强求。
- 使用 `platform = douyin`，`source = douyin-upload`。

## API 设计

新增查询接口：

```http
GET /api/diagnostics/screenshots?taskId={taskId}
GET /api/diagnostics/screenshots?taskId={taskId}&runId={runId}
```

返回示例：

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
      "minioUrl": "http://120.53.92.66:9000/ydbi/diagnostics/screenshots/bilibili/...",
      "createdAt": "2026-05-27T12:30:01"
    }
  ]
}
```

任务详情页后续可通过 `taskId` 拉取最近一次或全部诊断运行的截图。若同一个 `taskId` 有多次运行，后端默认按 `created_at DESC` 分组展示。

## 数据库初始化

建议在 monitor-be 启动时自动补齐表结构，保持当前项目风格：

- 新增 `DiagnosticScreenshotService.ensureSchema()`，构造后执行。
- 或在 `MonitorService` 的 schema 初始化区域新增 `ensureDiagnosticScreenshotSchema()`。

首选放在独立服务里，原因是该表属于诊断归档能力，不属于任务监控 SQL 聚合主体。

## 错误处理

归档失败不能影响上传主链路：

- MinIO 上传失败：记录日志，返回本地截图路径。
- 插表失败：截图已经在 MinIO 时记录日志；下一轮清理可通过 object key 前缀排查孤儿对象。
- 图片为空或不存在：不上传，不插入成功记录。

如果需要完整审计失败，可插入 `status = upload_failed`，`minio_url = ''`，`error_message = ?`。首期不建议这样做，避免诊断表堆积无图记录。

## 清理策略

诊断图是排障资产，不应永久保存。建议默认保留 30 天：

```sql
DELETE FROM yd_diagnostic_screenshot
WHERE created_at < NOW() - INTERVAL 30 DAY;
```

MinIO 清理使用同样日期前缀：

```text
diagnostics/screenshots/{platform}/{source}/{yyyyMMdd}/
```

清理任务可以后续放到 monitor-be 管理接口或独立脚本中，不作为首期阻塞项。

## 实施步骤

1. 新增 `yd_diagnostic_screenshot` schema 初始化。
2. 新增 `DiagnosticScreenshotService` 和请求/响应 record。
3. 修改 `PlaywrightDiagnostics.DiagnosticSnapshot`，允许携带 `screenshotMinioUrl` 和 `htmlMinioUrl`，或新增包装返回对象避免影响旧调用。
4. 改造 Bilibili Playwright 上传和 inspect 接口。
5. 改造统一 `SocialPlaywrightInspectService`。
6. 改造小红书和抖音上传诊断。
7. 增加查询接口 `/api/diagnostics/screenshots`。
8. 用一次 `inspect-upload-page` 和一次 B 站上传失败/成功链路验证：
   - 本地仍有 PNG。
   - MinIO 有对应 object。
   - 表内有记录。
   - `ORDER BY step_index, id` 顺序正确。

## 验收标准

- 每次诊断截图本地落盘后，都会尽力上传到 MinIO。
- `yd_diagnostic_screenshot` 至少包含任务、时间、MinIO 地址、步骤名，并能按 `step_index, id` 稳定排序。
- B 站 Playwright inspect 响应直接返回 MinIO URL。
- 业务上传不会因为诊断图上传失败而失败。
- 线上只通过数据库和 MinIO 就能定位某次任务的诊断截图。
