# Douyin Persistent Profile Upload Runbook

本文记录当前抖音上传方案：不再维护常驻 Chrome/CDP 端口。monitor-be 每次上传时按 `accountKey` 启动对应的 Chrome Profile，上传完成后关闭浏览器上下文。

## 当前结论

抖音创作者中心登录态不能稳定依赖单纯导出的 cookie/storage state。可靠做法仍然是使用一个独立、干净、已人工登录过的 Chrome Profile，但这个 Profile 不需要对应一个长期运行的 Chrome 进程。

当前生命周期：

1. 人工登录时临时启动可视化 Chrome，使用固定 Profile 目录。
2. 登录完成后关闭人工 Chrome。
3. 上传任务到来时，monitor-be 使用同一个 Profile 目录启动 Chrome persistent context。
4. 上传完成后关闭 context，释放 Chrome 进程。

## Profile 目录

monitor-be 容器内默认根目录：

```text
/work/douyin-chrome-profiles
```

服务器宿主机对应目录：

```text
/hoshuuch/YouBi/workfolder/douyin-chrome-profiles
```

账号目录按 `accountKey` 区分：

```text
/work/douyin-chrome-profiles/animal
/work/douyin-chrome-profiles/knowledge
```

可通过环境变量覆盖：

```bash
YDBI_DOUYIN_PROFILE_ROOT_DIR=/work/douyin-chrome-profiles
```

## 人工登录/续期

先确保服务器上 VNC 基础环境可用：

```bash
nohup Xvfb :99 -screen 0 1400x900x24 > /hoshuuch/YouBi/logs/douyin-xvfb.log 2>&1 &
DISPLAY=:99 nohup fluxbox > /hoshuuch/YouBi/logs/douyin-fluxbox.log 2>&1 &
nohup x11vnc -display :99 -localhost -forever -shared -rfbport 5901 -rfbauth /root/.vnc/passwd > /hoshuuch/YouBi/logs/douyin-x11vnc.log 2>&1 &
```

人工登录某个账号时，临时启动 Chrome。示例 `animal`：

```bash
mkdir -p /hoshuuch/YouBi/workfolder/douyin-chrome-profiles/animal /hoshuuch/YouBi/logs

DISPLAY=:99 google-chrome \
  --user-data-dir=/hoshuuch/YouBi/workfolder/douyin-chrome-profiles/animal \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  https://creator.douyin.com/creator-micro/content/upload
```

在 VNC 中完成登录、短信验证、滑块验证等流程，直到能进入上传页。确认后关闭这个 Chrome。不要在上传任务运行时同时打开同一个 Profile。

本地打开服务器 VNC：

```bash
scripts/open_douyin_server_vnc.command animal
scripts/open_douyin_server_vnc.command knowledge
```

该脚本会在服务器上启动 VNC 图形环境和指定 `accountKey` 的 Chrome Profile，然后在本机打开屏幕共享。脚本只转发 VNC 端口，不再转发 `9333/9334`。

## 后端上传行为

`/api/douyin/upload` 只依赖 `accountKey`：

- 后端取 `YDBI_DOUYIN_PROFILE_ROOT_DIR/<accountKey>`。
- 使用 Playwright `launchPersistentContext` 启动 Chrome。
- 使用本次任务下载到容器内的文件路径直接设置上传 input。
- 上传后保存一份 `storageState` 到 `uploader_account_douyin`，用于状态展示和辅助诊断。
- 关闭 context，Chrome 进程随之退出。

同一 `accountKey` 在进程内有锁，避免同一个 Profile 被并发上传任务同时打开。

## 触发上传

```bash
curl -sS -X POST http://127.0.0.1:8200/api/douyin/upload \
  -H 'Content-Type: application/json' \
  -d '{
    "taskId":"pj05eb_2wKU",
    "accountKey":"animal",
    "videoUrl":"https://120.53.92.66/minio/ydbi/pj05eb_2wKU/combiner/video_final.mp4",
    "coverUrl":"https://120.53.92.66/minio/ydbi/pj05eb_2wKU/downloader/thumbnail.jpg",
    "title":"Tropical Royal Flycatcher This Bird is Insane",
    "description":"Tropical Royal Flycatcher This Bird is Insane",
    "tags":"Tropical Royal Flycatcher,Royal Flycatcher,Tropical Flycatcher"
  }'
```

成功日志关键点：

```text
Douyin upload launch persistent Chrome
Douyin upload set video input
Douyin upload entered publish page
Douyin upload video uploaded
Douyin upload publish success page reached
```

## 常见问题

### 上传提示登录态失效

说明该 `accountKey` 对应的 Profile 被抖音要求重新验证。用 VNC 临时打开对应 Profile，人工完成验证后关闭 Chrome，再重试上传。

### Profile 被占用

不要同时运行人工 Chrome 和上传任务使用同一个 Profile。若异常退出后 Chrome 锁文件残留，先确认没有对应 Chrome 进程，再清理该 Profile 下的 `SingletonLock` / `SingletonSocket` / `SingletonCookie`。

### 旧 CDP 字段

`uploader_account_douyin.cdp_port`、`cdp_endpoint` 是历史字段，当前上传链路不再读取。后续可以在前端和表结构清理时一并删除。
