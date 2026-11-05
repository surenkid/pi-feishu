# Pi-Feishu

[![npm version](https://img.shields.io/npm/v/pi-feishu.svg)](https://www.npmjs.com/package/pi-feishu)
[![license](https://img.shields.io/npm/l/pi-feishu.svg)](https://www.npmjs.com/package/pi-feishu)

通过飞书官方 Bot API（WebSocket 长连接），将飞书作为聊天渠道远程控制 Pi Agent。

在飞书中给 Bot 发消息，Pi 自动接收并处理，响应实时回传到飞书。

## 功能

- 使用飞书官方 Node.js SDK（@larksuiteoapi/node-sdk）WebSocket 长连接，稳定可靠
- 工具执行进度实时推送：每次工具调用在飞书中显示可编辑的进度卡片
- 中间文本推送：assistant 思考过程中的每一步文本都推送到飞书，不只是最终回复
- Reaction 输入指示：处理中显示 Typing 表情，完成后自动移除
- 媒体收发：图片、文件自动下载/上传，支持双向传递
- Pi LLM 可主动发送文本、图片、文件到飞书（通过注册的工具）
- 消息去重（12h TTL FIFO）+ 过期消息过滤（30min）
- 支持多用户同时与 Bot 对话
- 支持飞书（国内）和 Lark（海外）两种域名

## 安装

需要 Pi **0.80.5 或更高版本**（队列在 `agent_settled` 最终空闲事件后继续处理）。

```bash
pi install npm:pi-feishu
```

## 配置

### 获取凭证

在[飞书开放平台](https://open.feishu.cn/)创建应用，获取 App ID 和 App Secret。确保应用已开启机器人能力。

### 配置方式

支持三种配置方式，优先级从高到低：

**1. CLI 标志**

```bash
pi --feishu-app-id=cli_xxx --feishu-app-secret=xxx
```

**2. 环境变量**

```bash
export FEISHU_APP_ID="cli_xxx"
export FEISHU_APP_SECRET="xxx"
```

**3. Pi settings.json（推荐）**

在 `.pi/settings.json`（项目级）或 `~/.pi/agent/settings.json`（全局）中添加：

```json
{
  "feishu": {
    "appId": "cli_xxx",
    "appSecret": "xxx",
    "domain": "feishu",
    "encryptKey": "",
    "verificationToken": ""
  }
}
```

字段名支持 camelCase（`appId`）和 snake_case（`app_id`）两种写法。项目级配置覆盖全局配置。

### 配置项说明

| 字段 | 环境变量 | CLI 标志 | 必需 |
|------|----------|----------|------|
| App ID | `FEISHU_APP_ID` | `--feishu-app-id` | 是 |
| App Secret | `FEISHU_APP_SECRET` | `--feishu-app-secret` | 是 |
| 域名 | `FEISHU_DOMAIN` | `--feishu-domain` | 否，默认 feishu |
| 加密密钥 | `FEISHU_ENCRYPT_KEY` | `--feishu-encrypt-key` | 否 |
| 验证令牌 | `FEISHU_VERIFICATION_TOKEN` | `--feishu-verification-token` | 否 |

`domain` 可选 `feishu`（国内）或 `lark`（海外），默认 `feishu`。

### 启动

```bash
pi
```

Pi 启动时会自动加载 pi-feishu 扩展并通过 WebSocket 连接飞书。在飞书中给 Bot 发消息即可开始使用。

## 管理命令

在 Pi 中使用 `/feishu` 命令管理连接：

| 命令 | 说明 |
|------|------|
| `/feishu start` | 启动飞书 Bot 连接 |
| `/feishu stop` | 断开连接 |
| `/feishu status` | 查看连接状态 |
| `/feishu config` | 查看当前配置 |
| `/feishu help` | 显示帮助 |

## 消息流程

一次典型的工具调用对话，飞书端看到的消息如下：

```
用户: 帮我查看当前目录的文件

Bot:                                    [进度卡片，原地更新]
  执行中 (1)
  Shell ...

Bot:                                    [进度卡片更新]
  工具调用
  Shell

Bot:                                    [最终回复]
  当前目录下有以下文件：
  - src/index.ts
  - src/feishu-client.ts
  ...
```

### 事件映射

| Pi 事件 | 飞书动作 |
|---------|----------|
| 入站消息 | 添加 Typing Reaction + 转发给 Pi |
| `tool_execution_start` | 创建/更新进度卡片 |
| `tool_execution_end` | 更新进度卡片（标记完成/失败） |
| `turn_end`（含工具调用） | 发送中间文本（回复模式） |
| `turn_end`（纯文本） | 发送最终回复（新消息） |
| `agent_settled` | 最终空闲后移除 Typing Reaction、清理回复路由并处理下一条排队消息 |

### 注册的 Pi 工具

Pi LLM 可以主动调用以下工具向飞书发送内容：

| 工具名 | 说明 |
|--------|------|
| `send_to_feishu` | 发送文本消息到飞书 |
| `send_image_to_feishu` | 上传并发送图片到飞书 |
| `send_file_to_feishu` | 上传并发送文件到飞书 |

## 消息类型支持

| 入站类型 | 说明 | 资源下载 |
|----------|------|----------|
| text | 纯文本 | -- |
| post | 富文本（Markdown） | -- |
| image | 图片 | 自动下载到本地 |
| file | 文件 | 自动下载到本地 |
| audio | 语音 | 自动下载到本地 |
| video | 视频 | 自动下载到本地 |
| sticker | 表情 | -- |
| interactive | 卡片消息 | -- |

| 出站类型 | 说明 |
|----------|------|
| post | 富文本（Markdown，支持分块） |
| interactive | 进度卡片（可编辑更新） |
| image | 图片消息（通过 image_key） |
| file | 文件消息（通过 file_key） |

## 架构

```
+---------------------+   WebSocket (SDK)   +--------------------+
|   飞书 Bot 网关      | <-----------------> |  Pi Extension      |
|   open.feishu.cn    |   EventDispatcher   |  (feishu-client)   |
+---------------------+                     +---------+----------+
                                                      |
                                                      | sendUserMessage()
                                                      | on("turn_end")
                                                      | on("tool_execution_*")
                                                      v
                                             +--------------------+
                                             |  Pi Agent Core     |
                                             |  (LLM + Tools)     |
                                             +--------------------+
```

## 项目结构

```
pi-feishu/
+-- src/
|   +-- index.ts           # Pi 扩展主入口，事件处理，工具注册
|   +-- feishu-client.ts   # 飞书 WebSocket 客户端封装
|   +-- types.ts           # 类型定义
+-- package.json
+-- README.md
```

## 开发

```bash
npm install
npm run typecheck
npm test
pi -e ./src/index.ts --feishu-app-id=YOUR_ID --feishu-app-secret=YOUR_SECRET
```

## 注意事项

1. App Secret 请妥善保管，不要提交到版本控制
2. 应用需要在飞书开放平台开启「机器人」能力
3. 单条消息最大 4000 字符（飞书限制），长消息自动分块
4. 进度卡片内容限制约 3500 字符，超出部分自动截断
5. 媒体文件下载到系统临时目录（/tmp/feishu-media/）

## License

MIT
