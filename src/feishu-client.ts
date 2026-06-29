/**
 * feishu-client.ts — 飞书 WebSocket 客户端
 *
 * 基于 @larksuiteoapi/node-sdk 的 Client + WSClient + EventDispatcher 封装。
 * 负责：WebSocket 长连接管理、事件接收、消息发送、媒体收发、
 *        消息去重、Reaction 输入指示、交互卡片。
 *
 * 参考：openclaw-lark 项目的 lark-client.ts 和 monitor.ts
 */

import * as Lark from "@larksuiteoapi/node-sdk";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { FeishuConfig, BridgeStatus } from "./types.js";

// ─── 日志 ─────────────────────────────────────────────

const DEBUG = false;
function _log(...args: unknown[]): void {
  if (DEBUG) console.log("[FeishuClient]", ...args);
}
function _warn(...args: unknown[]): void {
  console.warn("[FeishuClient]", ...args);
}

// ─── 常量 ─────────────────────────────────────────────

/** 消息去重 TTL（12 小时） */
const DEDUP_TTL_MS = 12 * 60 * 60 * 1000;
/** 去重最大条目 */
const DEDUP_MAX_ENTRIES = 5000;
/** 去重定期清理间隔（5 分钟） */
const DEDUP_SWEEP_INTERVAL = 5 * 60 * 1000;
/** 消息过期判定（30 分钟） */
const MESSAGE_EXPIRY_MS = 30 * 60 * 60 * 1000;
/** 资源下载超时（30 秒） */
const RESOURCE_DOWNLOAD_TIMEOUT_MS = 30 * 1000;
/** 媒体文件临时目录 */
const MEDIA_TEMP_DIR = join(tmpdir(), "feishu-media");
/** 飞书 Reaction emoji 类型 */
const REACTION_TYPING = "Typing";
const REACTION_CROSS_MARK = "CrossMark";

// ─── 飞书事件类型 ───────────────────────────────────────

/** SDK 传入的 im.message.receive_v1 事件数据结构 */
interface FeishuMessageEvent {
  sender: {
    sender_id: {
      open_id?: string;
      user_id?: string;
      union_id?: string;
    };
    sender_type?: string;
    tenant_key?: string;
  };
  message: {
    message_id: string;
    root_id?: string;
    parent_id?: string;
    create_time?: string;
    update_time?: string;
    chat_id: string;
    thread_id?: string;
    chat_type: "p2p" | "group";
    message_type: string;
    content: string; // JSON 字符串
    mentions?: Array<{
      key: string;
      id: { open_id?: string; user_id?: string; union_id?: string };
      name: string;
      tenant_key?: string;
    }>;
    user_agent?: string;
  };
}

// ─── 导出类型 ──────────────────────────────────────────

/** 入站消息中可能携带的资源描述 */
export interface InboundResource {
  type: "image" | "file" | "audio" | "video";
  fileKey: string;
  fileName?: string;
}

// ─── FeishuClient 类 ───────────────────────────────────

export class FeishuClient {
  private client: Lark.Client;
  private wsClient: Lark.WSClient | null = null;
  private abortController: AbortController | null = null;
  private status: BridgeStatus = "disconnected";

  // 消息去重
  private dedupMap: Map<string, number> = new Map();
  private dedupSweepTimer: ReturnType<typeof setInterval> | null = null;

  // Bot 身份（连接后探测）
  private botOpenId: string = "";

  // 回调 — 扩展为包含资源列表
  private onMessageCallback:
    | ((
        chatId: string,
        msgId: string,
        text: string,
        chatType: "p2p" | "group",
        resources: InboundResource[],
      ) => void)
    | null = null;
  private onStatusChangeCallback: ((status: BridgeStatus) => void) | null = null;

  // Reaction 跟踪：chatId → { msgId, reactionId }
  private typingMessages: Map<string, { msgId: string; reactionId: string }> = new Map();

  constructor(private config: FeishuConfig) {
    const domain = config.domain === "lark" ? Lark.Domain.Lark : Lark.Domain.Feishu;

    this.client = new Lark.Client({
      appId: config.appId,
      appSecret: config.appSecret,
      appType: Lark.AppType.SelfBuild,
      domain,
    });

    // 确保临时目录存在
    if (!existsSync(MEDIA_TEMP_DIR)) {
      mkdirSync(MEDIA_TEMP_DIR, { recursive: true });
    }
  }

  // ─── 公开 API ───────────────────────────────────────

  /** 连接飞书 WebSocket 长连接 */
  async connect(): Promise<void> {
    if (this.status === "connected" || this.status === "connecting") {
      _log("Already connected or connecting, skip");
      return;
    }

    this.setStatus("connecting");
    _log("Connecting to Feishu WebSocket...");

    try {
      this.abortController = new AbortController();

      const dispatcher = new Lark.EventDispatcher({
        encryptKey: this.config.encryptKey ?? "",
        verificationToken: this.config.verificationToken ?? "",
      });

      dispatcher.register({
        "im.message.receive_v1": (data: any) => {
          this.handleInboundMessage(data);
        },
        "im.message.message_read_v1": async () => {},
        "im.message.reaction.created_v1": async () => {},
        "im.message.reaction.deleted_v1": async () => {},
        "im.chat.member.bot.added_v1": async () => {},
        "im.chat.member.bot.deleted_v1": async () => {},
        "im.chat.access_event.bot_p2p_chat_entered_v1": async () => {},
      });

      if (this.wsClient) {
        try {
          (this.wsClient as any).close({ force: true });
        } catch { /* ignore */ }
      }

      const domain = this.config.domain === "lark" ? Lark.Domain.Lark : Lark.Domain.Feishu;
      this.wsClient = new Lark.WSClient({
        appId: this.config.appId,
        appSecret: this.config.appSecret,
        domain,
        loggerLevel: Lark.LoggerLevel.warn, // 过滤 SDK 内部 info（如 '[ws]', 'ws client ready'）
        autoReconnect: true,
        handshakeTimeoutMs: 15000,
        wsConfig: {
          pingTimeout: 30,
        },
        onReady: () => {
          _log("WSClient: first connection established");
        },
        onReconnecting: () => {
          _warn("WSClient: connection lost, reconnecting...");
          this.setStatus("connecting");
        },
        onReconnected: () => {
          _log("WSClient: reconnected successfully");
          this.setStatus("connected");
        },
        onError: (err: Error) => {
          _warn("WSClient: terminal error:", err.message);
          this.setStatus("error");
        },
      });

      this.patchCardEvents();
      this.startDedupSweep();

      await this.wsClient.start({ eventDispatcher: dispatcher });

      this.setStatus("connected");
      _log("Feishu WebSocket connected");
    } catch (err) {
      _warn("Connect failed:", err);
      this.setStatus("error");
      throw err;
    }
  }

  /** 断开连接 */
  disconnect(): void {
    _log("Disconnecting...");

    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }

    if (this.wsClient) {
      try {
        (this.wsClient as any).close({ force: true });
      } catch { /* ignore */ }
      this.wsClient = null;
    }

    if (this.dedupSweepTimer) {
      clearInterval(this.dedupSweepTimer);
      this.dedupSweepTimer = null;
    }

    // 清除所有 typing reactions
    for (const [chatId, entry] of this.typingMessages) {
      this.removeReactionById(entry.msgId, entry.reactionId).catch(() => {});
    }
    this.typingMessages.clear();

    this.setStatus("disconnected");
  }

  getStatus(): BridgeStatus {
    return this.status;
  }

  // ─── 消息发送 ──────────────────────────────────────────

  /** 发送文本消息到飞书（回复模式优先，使用 interactive 卡片格式） */
  async sendMessage(chatId: string, text: string, replyToMsgId?: string): Promise<void> {
    const card = FeishuClient.buildTextCard(text);
    const content = JSON.stringify(card);

    try {
      if (replyToMsgId) {
        await this.client.im.message.reply({
          path: { message_id: replyToMsgId },
          data: { content, msg_type: "interactive" },
        });
      } else {
        await this.client.im.message.create({
          params: { receive_id_type: "chat_id" },
          data: { receive_id: chatId, content, msg_type: "interactive" },
        });
      }
      _log(`Message sent to ${chatId}${replyToMsgId ? ` (reply to ${replyToMsgId})` : ""}`);
    } catch (err: any) {
      if (replyToMsgId && (err?.code === 230011 || err?.code === 231003)) {
        _warn("Reply failed (message withdrawn), falling back to create");
        await this.client.im.message.create({
          params: { receive_id_type: "chat_id" },
          data: { receive_id: chatId, content, msg_type: "interactive" },
        });
      } else {
        throw err;
      }
    }
  }

  /** 发送交互卡片消息 */
  async sendCard(
    chatId: string,
    card: Record<string, unknown>,
    replyToMsgId?: string,
  ): Promise<string | null> {
    const content = JSON.stringify(card);

    try {
      if (replyToMsgId) {
        const resp = await this.client.im.message.reply({
          path: { message_id: replyToMsgId },
          data: { content, msg_type: "interactive" },
        });
        return resp?.data?.message_id ?? null;
      } else {
        const resp = await this.client.im.message.create({
          params: { receive_id_type: "chat_id" },
          data: { receive_id: chatId, content, msg_type: "interactive" },
        });
        return resp?.data?.message_id ?? null;
      }
    } catch (err: any) {
      _warn("Send card failed:", err?.message ?? err);
      return null;
    }
  }

  /** 更新（PATCH）已有的卡片消息 */
  async updateCard(messageId: string, card: Record<string, unknown>): Promise<void> {
    await this.client.im.message.patch({
      path: { message_id: messageId },
      data: { content: JSON.stringify(card) },
    });
  }

  // ─── 媒体收发 ──────────────────────────────────────────

  /** 下载消息中的资源（图片/文件）到本地临时目录 */
  async downloadResource(
    messageId: string,
    fileKey: string,
    resourceType: string,
    fileName?: string,
  ): Promise<string | null> {
    try {
      _log(`Downloading resource: ${fileKey} from message ${messageId}`);

      // 超时保护：防止 API 无响应时队列永久卡死
      const downloadPromise = this.client.im.messageResource.get({
        path: { message_id: messageId, file_key: fileKey },
        params: { type: resourceType as any },
      });
      const timeoutPromise = new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`Download timeout after ${RESOURCE_DOWNLOAD_TIMEOUT_MS}ms`)), RESOURCE_DOWNLOAD_TIMEOUT_MS)
      );
      const resp = await Promise.race([downloadPromise, timeoutPromise]);

      if (!resp) return null;

      // SDK 返回 { writeFile, getReadableStream, headers }
      // 生成文件名
      const ext = resourceType === "image" ? ".png" : resourceType === "audio" ? ".ogg" : "";
      const safeName = (fileName && fileName.length > 0)
        ? fileName.replace(/[^a-zA-Z0-9._-]/g, "_")
        : `${fileKey}${ext}`;
      const localPath = join(MEDIA_TEMP_DIR, `${Date.now()}-${safeName}`);

      // 优先使用 writeFile()（SDK 原生写入磁盘）
      if (typeof resp.writeFile === "function") {
        await resp.writeFile(localPath);
        _log(`Resource downloaded via writeFile to ${localPath}`);
        return localPath;
      }

      // 回退：使用 getReadableStream() 手动收集
      if (typeof resp.getReadableStream === "function") {
        const stream = resp.getReadableStream();
        const chunks: Buffer[] = [];
        for await (const chunk of stream as AsyncIterable<Buffer>) {
          chunks.push(Buffer.from(chunk));
        }
        const buffer = Buffer.concat(chunks);
        writeFileSync(localPath, buffer);
        _log(`Resource downloaded via stream to ${localPath} (${buffer.length} bytes)`);
        return localPath;
      }

      _warn("No writeFile or getReadableStream on response");
      return null;
    } catch (err) {
      _warn("Download resource failed:", err);
      return null;
    }
  }

  /** 上传图片到飞书，返回 image_key */
  async uploadImage(filePath: string): Promise<string | null> {
    try {
      const { createReadStream } = await import("node:fs");
      const stream = createReadStream(filePath);

      const resp = await this.client.im.image.create({
        data: {
          image_type: "message" as any,
          image: stream as any,
        },
      });

      // SDK 返回 { image_key } | null，image_key 在顶层
      const imageKey = resp?.image_key ?? null;
      _log(`Image uploaded: ${imageKey}`);
      return imageKey;
    } catch (err) {
      _warn("Upload image failed:", err);
      return null;
    }
  }

  /** 上传文件到飞书，返回 file_key */
  async uploadFile(filePath: string, fileName: string, fileType: string = "stream"): Promise<string | null> {
    try {
      const { createReadStream } = await import("node:fs");
      const stream = createReadStream(filePath);

      const resp = await this.client.im.file.create({
        data: {
          file_type: fileType as any,
          file_name: fileName,
          file: stream as any,
        },
      });

      // SDK 返回 { file_key } | null，file_key 在顶层
      const fileKey = resp?.file_key ?? null;
      _log(`File uploaded: ${fileKey}`);
      return fileKey;
    } catch (err) {
      _warn("Upload file failed:", err);
      return null;
    }
  }

  /** 发送图片消息（通过 image_key） */
  async sendImage(chatId: string, imageKey: string, replyToMsgId?: string): Promise<void> {
    const content = JSON.stringify({ image_key: imageKey });

    if (replyToMsgId) {
      await this.client.im.message.reply({
        path: { message_id: replyToMsgId },
        data: { content, msg_type: "image" },
      });
    } else {
      await this.client.im.message.create({
        params: { receive_id_type: "chat_id" },
        data: { receive_id: chatId, content, msg_type: "image" },
      });
    }
  }

  /** 发送文件消息（通过 file_key） */
  async sendFile(chatId: string, fileKey: string, replyToMsgId?: string): Promise<void> {
    const content = JSON.stringify({ file_key: fileKey });

    if (replyToMsgId) {
      await this.client.im.message.reply({
        path: { message_id: replyToMsgId },
        data: { content, msg_type: "file" },
      });
    } else {
      await this.client.im.message.create({
        params: { receive_id_type: "chat_id" },
        data: { receive_id: chatId, content, msg_type: "file" },
      });
    }
  }

  // ─── Reaction 输入指示 ─────────────────────────────────

  /** 添加 typing 指示（开始处理消息时调用） */
  async startTyping(chatId: string, msgId: string): Promise<void> {
    const reactionId = await this.addReaction(msgId, REACTION_TYPING);
    if (reactionId) {
      this.typingMessages.set(chatId, { msgId, reactionId });
    }
  }

  /** 停止 typing 指示（处理完成时调用） */
  async stopTyping(chatId: string, success: boolean = true): Promise<void> {
    const entry = this.typingMessages.get(chatId);
    if (!entry) return;

    this.typingMessages.delete(chatId);

    // 移除 Typing reaction（用真实的 reaction_id）
    await this.removeReactionById(entry.msgId, entry.reactionId).catch(() => {});

    // 失败时添加 CrossMark
    if (!success) {
      await this.addReaction(entry.msgId, REACTION_CROSS_MARK).catch(() => {});
    }
  }

  // ─── 回调注册 ──────────────────────────────────────────

  setOnMessage(
    cb: (
      chatId: string,
      msgId: string,
      text: string,
      chatType: "p2p" | "group",
      resources: InboundResource[],
    ) => void,
  ): void {
    this.onMessageCallback = cb;
  }

  setOnStatusChange(cb: (status: BridgeStatus) => void): void {
    this.onStatusChangeCallback = cb;
  }

  // ─── 内部方法 ───────────────────────────────────────

  private setStatus(status: BridgeStatus): void {
    this.status = status;
    this.onStatusChangeCallback?.(status);
  }

  /** 处理入站消息事件 */
  private handleInboundMessage(data: FeishuMessageEvent): void {
    try {
      const msg = data.message;
      const sender = data.sender;

      if (msg.create_time && this.isMessageExpired(msg.create_time)) return;
      if (!this.tryRecordDedup(msg.message_id)) return;

      const senderType = sender.sender_type;
      if (senderType === "bot" || senderType === "app") return;

      const chatId = msg.chat_id;
      const chatType = msg.chat_type;
      const messageId = msg.message_id;

      // 解析消息内容和资源
      const { text, resources } = this.parseContentWithResources(msg.content, msg.message_type, msg.mentions);

      if (!text && resources.length === 0) return;

      _log(
        `Inbound: chatId=${chatId}, type=${chatType}, msgId=${messageId}, ` +
        `text=${(text ?? "").substring(0, 50)}..., resources=${resources.length}`,
      );

      this.onMessageCallback?.(chatId, messageId, text ?? "", chatType, resources);
    } catch (err) {
      _warn("Error handling inbound message:", err);
    }
  }

  // ─── 内容解析 ─────────────────────────────────────────

  /**
   * 解析消息内容和资源列表。
   * 媒体类型的消息会返回占位文本 + 资源描述，由 index.ts 决定是否下载。
   */
  private parseContentWithResources(
    rawContent: string,
    messageType: string,
    mentions?: FeishuMessageEvent["message"]["mentions"],
  ): { text: string; resources: InboundResource[] } {
    let parsed: any;
    try {
      parsed = JSON.parse(rawContent);
    } catch {
      return { text: rawContent, resources: [] };
    }

    let text = "";
    const resources: InboundResource[] = [];

    switch (messageType) {
      case "text":
        text = parsed?.text ?? "";
        break;

      case "post": {
        const parts: string[] = [];
        // 尝试所有可能的 locale key
        const KNOWN_LOCALES = ["zh_cn", "en_us", "ja_jp", "ko_kr", "de_de", "fr_fr", "ru_ru", "vi_vn", "id_id", "ms_my", "th_th", "es_es"];
        const matchedLocaleKey = KNOWN_LOCALES.find(k => parsed?.[k]) ?? Object.keys(parsed).find(k => parsed[k]?.content);
        // Fallback: 若 parsed 自身带 content（无 locale 嵌套），直接当作 locale 对象
        const locale = matchedLocaleKey ? parsed[matchedLocaleKey] : (parsed?.content ? parsed : undefined);

        if (locale?.title) parts.push(locale.title);
        if (Array.isArray(locale?.content)) {
          for (const row of locale.content) {
            if (Array.isArray(row)) {
              for (const elem of row) {
                if (elem?.tag === "text" && elem.text) parts.push(elem.text);
                else if (elem?.tag === "a" && elem.text) parts.push(elem.text);
                else if (elem?.tag === "md" && elem.text) parts.push(elem.text);
                else if (elem?.tag === "at") parts.push(elem.user_id ?? "");
                else if (elem?.tag === "emotion") parts.push(elem.emoji ?? "");
                else if (elem?.tag === "quote") parts.push("[引用]");
                else if (elem?.tag === "img") {
                  parts.push(`[图片]`);
                  // 兼容多种 key 名称
                  const imgKey = elem.image_key || elem.img_key || elem.file_key;
                  if (imgKey) {
                    resources.push({ type: "image", fileKey: imgKey });
                  } else {
                    _warn(`Post img element has no image_key! elem keys: [${Object.keys(elem).join(",")}]`);
                  }
                }
                else {
                  _log(`Post: unhandled tag="${elem?.tag}", elem keys=[${Object.keys(elem ?? {}).join(",")}]`);
                }
              }
            }
          }
        }
        text = parts.join("");
        break;
      }

      case "image":
        text = "[图片]";
        if (parsed?.image_key) {
          resources.push({ type: "image", fileKey: parsed.image_key });
        } else {
          _warn(`Image message has no image_key! keys: [${Object.keys(parsed).join(",")}]`);
        }
        break;

      case "file":
        text = `[文件: ${parsed?.file_name ?? "unknown"}]`;
        if (parsed?.file_key) {
          resources.push({
            type: "file",
            fileKey: parsed.file_key,
            fileName: parsed?.file_name,
          });
        }
        break;

      case "audio":
        text = "[语音消息]";
        if (parsed?.file_key) {
          resources.push({ type: "audio", fileKey: parsed.file_key });
        }
        break;

      case "video":
        text = "[视频]";
        if (parsed?.file_key) {
          resources.push({ type: "video", fileKey: parsed.file_key, fileName: parsed?.file_name });
        }
        break;

      case "sticker":
        text = "[表情]";
        break;

      case "interactive":
        text = "[卡片消息]";
        break;

      case "share_chat":
        text = "[群分享]";
        break;

      case "merge_forward":
        text = "[合并转发消息]";
        break;

      default:
        text = typeof parsed === "string" ? parsed : JSON.stringify(parsed);
    }

    // 移除 @Bot mention 占位符
    if (mentions && text) {
      text = this.stripMentionPlaceholders(text, mentions);
    }

    return { text: text.trim(), resources };
  }

  /** 移除消息中的 mention 占位符 */
  private stripMentionPlaceholders(
    text: string,
    mentions: FeishuMessageEvent["message"]["mentions"],
  ): string {
    let result = text;
    for (const m of mentions ?? []) {
      if (this.botOpenId && m.id.open_id === this.botOpenId) {
        result = result.replace(new RegExp(escapeRegExp(m.key) + "\\s*", "g"), "");
      }
    }
    return result;
  }

  // ─── 交互卡片构建 ──────────────────────────────────────

  /** v2 卡片外壳 */
  private static v2Card(elements: Record<string, unknown>[]): Record<string, unknown> {
    return {
      schema: "2.0",
      body: { elements },
    };
  }

  /** 截断过长文本 */
  private static safeText(text: string, limit = 3500): string {
    return text.length > limit ? text.substring(0, limit) + "\n..." : text;
  }

  /** 构建纯文本卡片（用于普通消息回复，支持完整 Markdown） */
  static buildTextCard(text: string): Record<string, unknown> {
    return FeishuClient.v2Card([
      { tag: "markdown", content: FeishuClient.safeText(text) },
    ]);
  }

  /** 构建流式更新卡片（状态行 + 内容，支持完整 Markdown） */
  static buildStreamingCard(text: string, status?: string): Record<string, unknown> {
    const elements: Record<string, unknown>[] = [];

    if (status) {
      elements.push({ tag: "markdown", content: `**${status}**` });
    }

    elements.push({ tag: "markdown", content: FeishuClient.safeText(text) });

    return FeishuClient.v2Card(elements);
  }

  /** 构建完成态卡片（支持完整 Markdown） */
  static buildFinalCard(text: string): Record<string, unknown> {
    return FeishuClient.v2Card([
      { tag: "markdown", content: FeishuClient.safeText(text) },
    ]);
  }

  // ─── Reaction ──────────────────────────────────────────

  /** 添加 Reaction，返回 reaction_id（用于后续删除） */
  private async addReaction(msgId: string, emojiType: string): Promise<string | null> {
    try {
      const resp = await this.client.im.messageReaction.create({
        path: { message_id: msgId },
        data: { reaction_type: { emoji_type: emojiType } },
      });
      const reactionId = resp?.data?.reaction_id ?? null;
      _log(`Reaction added: ${emojiType} → ${reactionId}`);
      return reactionId;
    } catch (err) {
      _log("Add reaction failed:", emojiType, err);
      return null;
    }
  }

  /** 通过真实的 reaction_id 删除 Reaction */
  private async removeReactionById(msgId: string, reactionId: string): Promise<void> {
    try {
      await this.client.im.messageReaction.delete({
        path: { message_id: msgId, reaction_id: reactionId },
      });
    } catch (err) {
      _log("Remove reaction failed:", reactionId, err);
    }
  }

  // ─── Card Event Patch ─────────────────────────────────

  private patchCardEvents(): void {
    if (!this.wsClient) return;
    const wsClientAny = this.wsClient as any;
    const origHandleEventData = wsClientAny.handleEventData?.bind(wsClientAny);
    if (!origHandleEventData) return;

    wsClientAny.handleEventData = (data: any) => {
      const msgType = data?.headers?.find?.((h: any) => h?.key === "type")?.value;
      if (msgType === "card") {
        const patchedData = {
          ...data,
          headers: data.headers.map((h: any) =>
            h.key === "type" ? { ...h, value: "event" } : h,
          ),
        };
        return origHandleEventData(patchedData);
      }
      return origHandleEventData(data);
    };
  }

  // ─── 去重 ───────────────────────────────────────────

  private tryRecordDedup(msgId: string): boolean {
    const now = Date.now();
    const existing = this.dedupMap.get(msgId);
    if (existing !== undefined) {
      if (now - existing < DEDUP_TTL_MS) return false;
      this.dedupMap.delete(msgId);
    }

    if (this.dedupMap.size >= DEDUP_MAX_ENTRIES) {
      const firstKey = this.dedupMap.keys().next().value;
      if (firstKey !== undefined) this.dedupMap.delete(firstKey);
    }

    this.dedupMap.set(msgId, now);
    return true;
  }

  private startDedupSweep(): void {
    if (this.dedupSweepTimer) clearInterval(this.dedupSweepTimer);
    this.dedupSweepTimer = setInterval(() => {
      const now = Date.now();
      for (const [key, ts] of this.dedupMap) {
        if (now - ts >= DEDUP_TTL_MS) {
          this.dedupMap.delete(key);
        } else {
          break;
        }
      }
    }, DEDUP_SWEEP_INTERVAL);
    if (this.dedupSweepTimer.unref) this.dedupSweepTimer.unref();
  }

  private isMessageExpired(createTimeStr: string): boolean {
    const createTime = parseInt(createTimeStr, 10);
    if (isNaN(createTime)) return false;
    return Date.now() - createTime > MESSAGE_EXPIRY_MS;
  }
}

// ─── 工具函数 ─────────────────────────────────────────

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
