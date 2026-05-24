# Server VNC Chrome Runbook

本文记录如何在服务器 `120.53.92.66` 上启动一个可视化 Chrome，并在本机 macOS 通过屏幕共享连接使用。当前用途是手动登录抖音创作者中心，再让后端通过 Chrome DevTools Protocol 复用同一个 Chrome Profile。

## 端口与路径

| 项目 | 值 |
|------|----|
| 服务器 | `root@120.53.92.66` |
| Xvfb display | `:99` |
| VNC 端口 | `127.0.0.1:5901` |
| Chrome CDP 端口 | `127.0.0.1:9333` |
| Chrome Profile | `/hoshuuch/YouBi/douyin-chrome-profile-animal` |
| 日志目录 | `/hoshuuch/YouBi/logs` |
| VNC 密码文件 | `/root/.vnc/passwd` |

VNC 和 CDP 都只监听服务器本机 `127.0.0.1`，本机通过 SSH 隧道访问，不需要开放公网防火墙端口。

## 1. 首次安装依赖

服务器系统是 OpenCloudOS 9.2，使用 `dnf`：

```bash
ssh root@120.53.92.66

dnf install -y xorg-x11-server-Xvfb x11vnc fluxbox
```

如果服务器没有 Chrome，添加 Google Chrome repo 并安装：

```bash
cat > /etc/yum.repos.d/google-chrome.repo <<'EOF'
[google-chrome]
name=google-chrome
baseurl=https://dl.google.com/linux/chrome/rpm/stable/x86_64
enabled=1
gpgcheck=1
gpgkey=https://dl.google.com/linux/linux_signing_key.pub
EOF

dnf install -y google-chrome-stable
google-chrome --version
```

## 2. 设置 VNC 密码

macOS 屏幕共享客户端通常不接受空密码，所以需要给 `x11vnc` 设置密码：

```bash
mkdir -p /root/.vnc
x11vnc -storepasswd 'Hoshuuch0815@' /root/.vnc/passwd
chmod 600 /root/.vnc/passwd
```

## 3. 启动可视化 Chrome

```bash
mkdir -p /hoshuuch/YouBi/douyin-chrome-profile-animal /hoshuuch/YouBi/logs

nohup Xvfb :99 -screen 0 1400x900x24 \
  > /hoshuuch/YouBi/logs/douyin-xvfb.log 2>&1 &

DISPLAY=:99 nohup fluxbox \
  > /hoshuuch/YouBi/logs/douyin-fluxbox.log 2>&1 &

nohup x11vnc \
  -display :99 \
  -localhost \
  -forever \
  -shared \
  -rfbport 5901 \
  -rfbauth /root/.vnc/passwd \
  > /hoshuuch/YouBi/logs/douyin-x11vnc.log 2>&1 &

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
```

## 4. 本机连接屏幕共享

本机新开一个终端，保持 SSH 隧道运行：

```bash
ssh -N \
  -L 5901:127.0.0.1:5901 \
  -L 9333:127.0.0.1:9333 \
  root@120.53.92.66
```

这个命令正常情况下没有输出，会一直占用当前终端。不要关闭它。

然后在 macOS 打开“屏幕共享”，连接：

```text
vnc://127.0.0.1:5901
```

密码：

```text
Hoshuuch0815@
```

连接后看到的是服务器上的 Chrome。手动完成抖音登录、短信验证、滑块验证等流程，直到能进入创作者中心上传页。

## 5. 验证服务状态

服务器上检查进程和端口：

```bash
ps -ef | grep -E 'Xvfb :99|x11vnc|fluxbox|google-chrome.*douyin-chrome-profile-animal' | grep -v grep
ss -lntp | grep -E ':(5901|9333) '
curl -sS http://127.0.0.1:9333/json/version
curl -sS http://127.0.0.1:9333/json/list
```

本机在 SSH 隧道打开时检查 CDP 是否可用：

```bash
curl -sS http://127.0.0.1:9333/json/version
```

`/json/version` 应返回：

```json
{
  "webSocketDebuggerUrl": "ws://127.0.0.1:9333/devtools/browser/..."
}
```

## 6. 重启可视化环境

如果 Chrome 或 VNC 卡住，可以在服务器上重启这一套环境：

```bash
pkill -f 'google-chrome.*douyin-chrome-profile-animal' || true
pkill -f 'x11vnc.*:99' || true
pkill -f 'fluxbox' || true
pkill -f 'Xvfb :99' || true
```

然后重新执行“启动可视化 Chrome”一节的命令。

注意：Chrome 重启后，`/json/version` 里的 `webSocketDebuggerUrl` 会变化。后端如果要通过 CDP 连接，需要重新读取这个 URL。

## 7. 常见问题

### SSH 隧道命令没有反应

正常。`ssh -N -L ...` 的作用是保持端口转发，不会进入 shell，也不会输出提示。保持这个终端打开，然后另开屏幕共享连接 `vnc://127.0.0.1:5901`。

### 屏幕共享要求输入密码

输入 VNC 密码：

```text
Hoshuuch0815@
```

如果仍然失败，检查 `x11vnc` 是否用 `-rfbauth /root/.vnc/passwd` 启动。

### 本机连不上 VNC

先确认 SSH 隧道还在运行，再在服务器上检查：

```bash
ss -lntp | grep ':5901 '
tail -n 80 /hoshuuch/YouBi/logs/douyin-x11vnc.log
```

VNC 应监听：

```text
127.0.0.1:5901
```

### Chrome 日志里有 GLX/GPU 报错

Xvfb 环境下常见，例如 `GLX is not present`。只要 VNC 中 Chrome 能正常显示、`9333/json/version` 能返回数据，就可以忽略。

### 不要开放公网 VNC 端口

`5901` 不应该直接暴露公网。保持 `x11vnc -localhost`，通过 SSH 隧道访问即可。
