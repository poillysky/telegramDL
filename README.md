# Telegram 群组下载器

Docker 部署的 Telegram 群组媒体下载工具：Web 管理界面选择群组，按消息顺序依次下载，并根据媒体文案自动创建嵌套目录归类文件。

## 功能

- Docker / docker-compose 一键部署
- Web 管理：密码登录控制台；设置内完成 Telegram 登录；选择群组、创建/暂停/继续任务
- **同任务可配置并发下载**（创建任务时设 1–8；扫描仍按消息顺序，下载按批并发）
- 实时测速：任务总速度 + 各文件进度
- **按文案 `#标签` + 日期建嵌套目录**（边下边建；暂停/完成后同类合并）
- 断点续传：记录已处理 `message_id`，中断后可继续
- 失败自动判定（空文件 / 大小不符 / 异常）；点「继续」优先重试失败项
- FloodWait 自动等待后继续（超过 `MAX_FLOOD_WAIT` 则暂停）
- 媒体类型 / 扩展名 / `#标签` / 文案关键词 / 消息 ID / 日期范围过滤；可限制最多下载条数
- 可配置下载延迟（固定或 `delay_min`–`delay_max` 随机），减轻限流
- 目录模式：按文案 `#标签`、按媒体类型、或扁平目录
- 多选群组批量建任务；下载历史可搜索翻页
- Web 多账号：设置里可新增账号、修改密码、删除账号（首次由 `.env` 的 `WEB_USERNAME` / `WEB_PASSWORD` 自动创建）
- 重启自动重连 Telegram：已登录会话 + 设置里保存的 API/代理会在启动时后台恢复
- 可选 SOCKS5/HTTP 代理

### 目录规则示例

文案：`#风流狗尾巴 7.18自录 @csdkl333`（文件夹按文案；文件名用 Telegram 原始名）

```
downloads/某某群/#风流狗尾巴/7.18/原文件名.jpg
```

流程（边下边合并，不预扫）：

1. `#1` → 建 `#1`；`#2` → 建 `#2`
2. 下到 `#1#2`（或一文案同时带 `#1` `#2`）→ 立刻把前面的 `#1`、`#2` 并进 `#1 #2`
3. 之后再出现单独 `#1` / `#2` 也进同一个 `#1 #2`

```
downloads/某某群/#1 #2/7.18/xxx.jpg
```

规则：

1. 标签一律 `#xxx` 前缀写法
2. `7.18` / `7月18日` → 二级目录（合并时保留）
3. 相册无文案成员继承同组已见过的文案
4. 解析不到标签时进入 `_未分类`

## 快速开始

### 1. 获取 API 凭证

打开 [https://my.telegram.org](https://my.telegram.org) → API development tools，创建应用并记下 `api_id` / `api_hash`。

### 2. 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env`：

```env
API_ID=你的api_id
API_HASH=你的api_hash
WEB_USERNAME=admin
WEB_PASSWORD=自定义管理密码
# PROXY=socks5://127.0.0.1:1080   # 可选
```

### 3. 启动

本地构建：

```bash
docker compose up -d --build
```

或直接使用已发布镜像（[Docker Hub](https://hub.docker.com/r/poillysky/telegramdl)）：

```bash
docker pull poillysky/telegramdl:1.0.7
docker run -d --name telegramdl -p 9345:9345 \
  --env-file .env \
  -v "$(pwd)/downloads:/app/downloads" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/sessions:/app/sessions" \
  poillysky/telegramdl:1.0.7
```

浏览器打开 [http://localhost:9345](http://localhost:9345)：

1. 输入 Web 账号和密码进入控制台
2. 在右上角「设置」中完成 Telegram 登录（API / 手机号 / 验证码）
3. 选择群组 → 勾选媒体类型 → 设置并发数 → 开始下载

### 数据目录

| 宿主机目录 | 说明 |
|-----------|------|
| `./downloads` | 下载的文件 |
| `./data` | SQLite 任务数据库 |
| `./sessions` | Telethon 登录会话（勿泄露） |

## 本地开发（不用 Docker）

需要 Python 3.10+。改 `app/` 下代码会**自动重启**（`--reload`）。

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
# source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
```

启动（推荐）：

```bash
# Windows
start.bat
# 或
.\start.ps1

# 手动
uvicorn app.main:app --host 0.0.0.0 --port 9345 --reload --reload-dir app
```

Docker 开发（挂载源码 + 热重载）：

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

## API 摘要

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/auth/status` | Telegram 登录状态 |
| POST | `/api/auth/send-code` | 发送验证码 |
| POST | `/api/auth/sign-in` | 验证码登录 |
| GET | `/api/chats` | 群组列表 |
| GET/POST | `/api/tasks` | 任务列表 / 创建 |
| POST | `/api/tasks/batch` | 批量多群创建任务 |
| POST | `/api/tasks/{id}/start` | 继续 |
| POST | `/api/tasks/{id}/pause` | 暂停 |
| DELETE | `/api/tasks/{id}` | 删除任务记录 |
| GET | `/api/history` | 下载历史（`q` 搜索） |

若设置了 `WEB_PASSWORD`，需先用 `WEB_USERNAME` / `WEB_PASSWORD` 登录；请求需带 Cookie（Web 页登录后自动写入）。

## 注意事项

- 只能下载你账号已加入、有权访问的群组/频道内容
- 请合理设置 `DOWNLOAD_DELAY`，避免触发 Telegram 限流
- `sessions` 目录等同于账号登录态，请妥善保管
- 删除 Web 上的任务不会删除已下载文件

## 许可证

仅供个人备份与学习使用，请遵守当地法律与 Telegram 服务条款。
