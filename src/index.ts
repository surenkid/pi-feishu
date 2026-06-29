/**
 * Pi-Feishu 扩展主入口
 *
 * 使用飞书官方 Bot API（WebSocket 长连接）将飞书作为聊天渠道控制 Pi。
 *
 * 功能：
 * 1. 通过飞书官方 Node.js SDK 连接飞书 WebSocket 长连接
 * 2. 接收飞书消息 → 转发为 Pi 用户消息
 * 3. 监听 Pi 响应 → 回传给飞书（回复/新消息/交互卡片）
 * 4. 媒体收发：下载图片/文件 → 上传到 Pi，Pi 生成的图片/文件 → 上传到飞书
 * 5. Reaction 输入指示：处理中显示 Typing，失败显示 CrossMark
 * 6. 工具进度：每个工具调用都实时推送到飞书（可编辑卡片）
 * 7. 中间文本：assistant 思考过程中的文本也推送到飞书
 * 8. 注册 /feishu 命令管理连接状态
 *
 * 消息流程（参考 hermes-agent）：
 *
 *   用户消息 →
 *     [Typing Reaction] →
 *     tool_execution_start → [进度卡片: ⏳ Shell...] →
 *     tool_execution_end   → [进度卡片: ✅ Shell] →
 *     turn_end (text+toolCalls) → [中间文本: "让我查找..."] →
 *     ... 下一轮工具 ... →
 *     turn_end (text only) → [最终回复] →
 *     [移除 Typing Reaction]
 *
 * 配置优先级（从高到低）：
 *   1. CLI 标志: --feishu-app-id, --feishu-app-secret 等
 *   2. 环境变量: FEISHU_APP_ID, FEISHU_APP_SECRET 等
 *   3. Pi settings.json 中的 feishu 字段
 */

import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionCommandContext,
  TurnEndEvent,
  AgentEndEvent,
} from "@earendil-works/pi-coding-agent";
import type { Static } from "typebox";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { FeishuClient } from "./feishu-client.js";
import type { InboundResource } from "./feishu-client.js";
import type { FeishuConfig } from "./types.js";

// ─── 常量 ─────────────────────────────────────────────

/** 飞书 post 消息单条最大字符数（约 4000） */
const MAX_TEXT_CHUNK = 4000;

/** 工具名到友好名称的映射 */
const TOOL_DISPLAY_NAMES: Record<string, string> = {
  bash: "Shell",
  read: "读取文件",
  edit: "编辑文件",
  write: "写入文件",
  grep: "搜索",
  find: "查找文件",
  ls: "列出目录",
  glob: "匹配文件",
  agent: "子代理",
  send_to_feishu: "发送消息",
  send_image_to_feishu: "发送图片",
  send_file_to_feishu: "发送文件",
};

/** 友好化工具名 */
function toolDisplayName(name: string): string {
  return TOOL_DISPLAY_NAMES[name] ?? name;
}

// ─── 从 Pi settings.json 读取 feishu 配置段 ──────────────

function readFeishuFromSettingsFile(filePath: string): Record<string, string> {
  try {
    if (!existsSync(filePath)) return {};
    const raw = readFileSync(filePath, "utf-8");
    const json = JSON.parse(raw);
    const fs = json?.feishu;
    if (!fs || typeof fs !== "object") return {};
    return {
      appId: fs.appId ?? fs.app_id ?? "",
      appSecret: fs.appSecret ?? fs.app_secret ?? "",
      domain: fs.domain ?? "",
      encryptKey: fs.encryptKey ?? fs.encrypt_key ?? "",
      verificationToken: fs.verificationToken ?? fs.verification_token ?? "",
    };
  } catch {
    return {};
  }
}

function loadConfig(): FeishuConfig {
  const globalSettings = readFeishuFromSettingsFile(
    join(homedir(), ".pi", "agent", "settings.json"),
  );
  const projectSettings = readFeishuFromSettingsFile(
    join(process.cwd(), ".pi", "settings.json"),
  );
  const s: Record<string, string> = { ...globalSettings, ...projectSettings };

  const domain = (process.env.FEISHU_DOMAIN || s.domain || "feishu") as "feishu" | "lark";

  return {
    appId: process.env.FEISHU_APP_ID || s.appId || "",
    appSecret: process.env.FEISHU_APP_SECRET || s.appSecret || "",
    domain,
    encryptKey: process.env.FEISHU_ENCRYPT_KEY || s.encryptKey || undefined,
    verificationToken: process.env.FEISHU_VERIFICATION_TOKEN || s.verificationToken || undefined,
  };
}

// ─── Chat 状态 ──────────────────────────────────────────

interface ChatState {
  chatId: string;
  /** 用户原始消息 ID，用于 reply threading */
  userMsgId: string;

  // ── 工具进度追踪 ──
  /** 进度卡片消息 ID（所有工具共用一个可编辑卡片） */
  progressMsgId: string | null;
  /** 卡片是否正在创建中（防止竞态重复创建） */
  progressCreating: boolean;
  /** 工具执行记录 */
  toolEntries: Array<{ name: string; status: "running" | "done" | "error" }>;
}

// ─── 扩展入口 ───────────────────────────────────────────

export default function (pi: ExtensionAPI) {
  let client: FeishuClient | null = null;
  let config: FeishuConfig = loadConfig();
  let ctxRef: ExtensionContext | null = null;

  /** 每个聊天独立的状态 */
  const chatStates: Map<string, ChatState> = new Map();

  // ─── 消息队列 ──────────────────────────────────────────

  interface QueuedMessage {
    msgId: string;
    text: string;
    resources: InboundResource[];
    chatType: "p2p" | "group";
  }

  interface ChatQueue {
    processing: boolean;
    queue: QueuedMessage[];
  }

  /** 每个聊天的消息队列 */
  const chatQueues: Map<string, ChatQueue> = new Map();

  // ─── 注册 CLI 标志 ────────────────────────────────────

  pi.registerFlag("feishu-app-id", {
    description: "飞书 App ID",
    type: "string",
    default: "",
  });
  pi.registerFlag("feishu-app-secret", {
    description: "飞书 App Secret",
    type: "string",
    default: "",
  });
  pi.registerFlag("feishu-domain", {
    description: "飞书域名 (feishu 或 lark)",
    type: "string",
    default: "",
  });
  pi.registerFlag("feishu-encrypt-key", {
    description: "飞书事件加密密钥（可选）",
    type: "string",
    default: "",
  });
  pi.registerFlag("feishu-verification-token", {
    description: "飞书事件验证令牌（可选）",
    type: "string",
    default: "",
  });

  // ─── 启动飞书客户端 ──────────────────────────────────

  async function startFeishuClient(): Promise<void> {
    if (client) {
      client.disconnect();
      client = null;
    }

    const flagMap: Record<string, string> = {
      appId: "feishu-app-id",
      appSecret: "feishu-app-secret",
      domain: "feishu-domain",
      encryptKey: "feishu-encrypt-key",
      verificationToken: "feishu-verification-token",
    };
    const overrides: Partial<FeishuConfig> = {};
    for (const [key, flag] of Object.entries(flagMap)) {
      const val = pi.getFlag(flag);
      if (val) (overrides as any)[key] = String(val);
    }
    config = { ...config, ...overrides };

    if (!config.appId || !config.appSecret) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify("飞书连接失败：缺少 appId/appSecret", "error");
      }
      return;
    }

    client = new FeishuClient(config);

    client.setOnMessage((chatId, msgId, text, chatType, resources) => {
      handleFeishuMessage(chatId, msgId, text, chatType, resources);
    });
    client.setOnStatusChange((status) => {
      updateStatus(ctxRef, status);
    });

    try {
      await client.connect();
    } catch (err) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify(`飞书连接错误: ${err}`, "error");
      }
    }
  }

  // ─── 处理飞书入站消息 → 排队或直接处理 ────────────────

  async function handleFeishuMessage(
    chatId: string,
    msgId: string,
    text: string,
    chatType: "p2p" | "group",
    resources: InboundResource[],
  ): Promise<void> {
    const content = text.trim();
    if (!content && resources.length === 0) return;

    // ── 拦截斜杠命令 ──
    if (content.startsWith("/")) {
      await handleSlashCommand(chatId, msgId, content);
      return;
    }

    // ── 入队 ──
    const queue = chatQueues.get(chatId) ?? { processing: false, queue: [] };
    chatQueues.set(chatId, queue);

    queue.queue.push({ msgId, text: content, resources, chatType });

    if (queue.processing) {
      // 当前正在处理 → 通知排队
      const pos = queue.queue.length;
      await client?.sendMessage(
        chatId,
        `已排队 (前面还有 ${pos - 1} 条)`,
        msgId,
      );
      flashStatus(`飞书: 📥 排队中 (${pos})`);
      return;
    }

    // 当前空闲 → 开始处理
    await dequeueAndProcess(chatId);
  }

  /** 从队列取出下一条消息并开始处理 */
  async function dequeueAndProcess(chatId: string): Promise<void> {
    const queue = chatQueues.get(chatId);
    if (!queue || queue.queue.length === 0) {
      // 队列空，标记空闲
      if (queue) queue.processing = false;
      return;
    }

    // Pi 正忙（压缩中/流式中）→ 不出队，保持 processing=false 等空闲时再触发
    if (ctxRef && !ctxRef.isIdle()) {
      queue.processing = false;
      return;
    }

    queue.processing = true;
    const item = queue.queue.shift()!;

    try {
      flashStatus(`飞书: 📩 ${item.text.substring(0, 20)}${item.text.length > 20 ? "..." : ""}`);

      // 下载入站媒体，将图片转为 base64 ImageContent 供多模态使用
      let resourceDescription = "";
      const imageParts: Array<{ type: "image"; data: string; mimeType: string }> = [];

      for (const res of item.resources) {
        const localPath = await client!.downloadResource(
          item.msgId,
          res.fileKey,
          res.type,
          res.fileName,
        );

        const typeLabel =
          res.type === "image" ? "图片" :
          res.type === "audio" ? "语音" :
          res.type === "video" ? "视频" : "文件";

        if (localPath) {
          if (res.type === "image") {
            // 图片：读取为 base64，直接作为多模态内容发送给 LLM
            try {
              const imageBuffer = readFileSync(localPath);
              const base64Data = imageBuffer.toString("base64");
              const mimeType = detectImageMimeType(localPath);
              imageParts.push({ type: "image", data: base64Data, mimeType });
            } catch (err) {
              console.warn(`[pi-feishu] 读取图片失败 ${localPath}:`, err);
              resourceDescription += `\n[收到${typeLabel}但读取失败: ${localPath}]`;
            }
          } else {
            resourceDescription += `\n[收到${typeLabel}: ${localPath}]`;
          }
        } else {
          resourceDescription += `\n[收到${typeLabel}但下载失败(key=${res.fileKey})，请告知用户重新发送]`;
        }
      }

      // 初始化聊天状态
      chatStates.set(chatId, {
        chatId,
        userMsgId: item.msgId,
        progressMsgId: null,
        progressCreating: false,
        toolEntries: [],
      });

      // 添加 Typing Reaction
      await client!.startTyping(chatId, item.msgId);

      // 发送给 Pi：有图片时使用多模态格式，否则纯文本
      const textContent = item.text + (resourceDescription ? "\n" + resourceDescription : "");
      if (imageParts.length > 0) {
        const contentParts: Array<{ type: "text"; text: string } | { type: "image"; data: string; mimeType: string }> = [
          { type: "text", text: textContent },
          ...imageParts,
        ];
        pi.sendUserMessage(contentParts);
      } else {
        pi.sendUserMessage(textContent);
      }
    } catch (err) {
      // 出错时 agent_end 不会触发，需主动复位队列，防止永久卡死
      console.warn(`[pi-feishu] dequeueAndProcess error:`, err);
      client?.sendMessage(chatId, `处理消息时出错: ${err}`, item.msgId).catch(() => {});
      const q = chatQueues.get(chatId);
      if (q) q.processing = false;
    }
  }

  // ─── 斜杠命令处理 ──────────────────────────────────────

  /**
   * 处理从飞书发来的斜杠命令。
   * 这些命令不会发给 LLM，而是直接在扩展层执行或回复提示。
   */
  async function handleSlashCommand(
    chatId: string,
    msgId: string,
    text: string,
  ): Promise<void> {
    const parts = text.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const args = parts.slice(1).join(" ");

    switch (cmd) {
      case "/new": {
        // 清空飞书侧状态
        const state = chatStates.get(chatId);
        const queue = chatQueues.get(chatId);

        if (state) {
          client?.stopTyping(chatId, false).catch(() => {});
          chatStates.delete(chatId);
        }
        if (queue) {
          queue.queue = [];
          queue.processing = false;
        }

        // 中断当前处理
        if (ctxRef && !ctxRef.isIdle()) {
          ctxRef.abort();
        }

        // 压缩上下文清除历史
        if (ctxRef) {
          ctxRef.compact();
          await client?.sendMessage(chatId, "会话已重置，上下文已清空。", msgId);
        } else {
          await client?.sendMessage(chatId, "无法重置：会话上下文不可用。", msgId);
        }
        break;
      }

      case "/stop": {
        // 中断当前处理 + 清空队列
        const state = chatStates.get(chatId);
        const queue = chatQueues.get(chatId);
        const clearedCount = queue?.queue.length ?? 0;

        if (state) {
          client?.stopTyping(chatId, false).catch(() => {});
          chatStates.delete(chatId);
        }
        if (queue) {
          queue.queue = [];
          queue.processing = false;
        }

        if (ctxRef && !ctxRef.isIdle()) {
          ctxRef.abort();
          await client?.sendMessage(chatId, "已中断当前处理，队列已清空。", msgId);
        } else if (clearedCount > 0) {
          await client?.sendMessage(chatId, `已清空 ${clearedCount} 条排队消息。`, msgId);
        } else {
          await client?.sendMessage(chatId, "当前没有正在处理的任务。", msgId);
        }
        break;
      }

      case "/queue": {
        const queue = chatQueues.get(chatId);
        const state = chatStates.get(chatId);
        const count = queue?.queue.length ?? 0;
        const idle = ctxRef?.isIdle() ?? true;

        if (!state && count === 0) {
          await client?.sendMessage(chatId, "队列为空，当前空闲。", msgId);
        } else {
          let reply = idle ? "状态: 空闲" : "状态: 处理中";
          if (count > 0) {
            reply += `\n排队中: ${count} 条消息`;
          }
          await client?.sendMessage(chatId, reply, msgId);
        }
        break;
      }

      case "/compact": {
        if (ctxRef) {
          ctxRef.compact();
          await client?.sendMessage(chatId, "已触发上下文压缩。", msgId);
        } else {
          await client?.sendMessage(chatId, "无法执行：会话上下文不可用。", msgId);
        }
        break;
      }

      case "/status": {
        const status = client?.getStatus() ?? "未启动";
        const ctxUsage = ctxRef?.getContextUsage();
        const queue = chatQueues.get(chatId);
        let reply = `Pi 状态:\n- 飞书连接: ${status}\n- App ID: ${config.appId ? "****" + config.appId.slice(-4) : "未设置"}`;
        if (ctxUsage && ctxUsage.tokens !== null) {
          reply += `\n- 上下文: ${ctxUsage.tokens}/${ctxUsage.contextWindow} tokens (${ctxUsage.percent ?? "?"}%)`;
        }
        if (queue && queue.queue.length > 0) {
          reply += `\n- 排队: ${queue.queue.length} 条`;
        }
        await client?.sendMessage(chatId, reply, msgId);
        break;
      }

      case "/help": {
        const helpText = [
          "可用命令:",
          "  /new       - 新建会话（重置上下文）",
          "  /stop      - 中断当前处理，清空排队",
          "  /queue     - 查看排队状态",
          "  /compact   - 压缩上下文",
          "  /status    - 查看 Pi 状态",
          "  /help      - 显示帮助",
          "",
          "以下命令请在 Pi 终端中执行:",
          "  /model     - 切换模型",
          "  /tools     - 管理工具",
        ].join("\n");
        await client?.sendMessage(chatId, helpText, msgId);
        break;
      }

      default: {
        await client?.sendMessage(
          chatId,
          `命令 ${cmd} 不支持通过飞书执行。请在 Pi 终端中使用。`,
          msgId,
        );
        break;
      }
    }
  }

  // ═══════════════════════════════════════════════════════
  //  Pi 事件处理 — 工具进度 + 文本回复
  // ═══════════════════════════════════════════════════════

  // ─── tool_execution_start → 更新进度卡片 ──────────────

  pi.on("tool_execution_start", (event: any) => {
    if (!client || client.getStatus() !== "connected") return;
    const state = findActiveState();
    if (!state) return;

    const toolName = event.toolName as string;
    state.toolEntries.push({ name: toolName, status: "running" });

    updateProgressCard(state);
    flashStatus(`飞书: 🔧 ${toolDisplayName(toolName)}...`);
  });

  // ─── tool_execution_end → 更新进度卡片 ────────────────

  pi.on("tool_execution_end", (event: any) => {
    if (!client || client.getStatus() !== "connected") return;
    const state = findActiveState();
    if (!state) return;

    const toolName = event.toolName as string;
    const isError = event.isError as boolean;

    // 找到对应的 running 条目并更新状态
    for (let i = state.toolEntries.length - 1; i >= 0; i--) {
      if (state.toolEntries[i].name === toolName && state.toolEntries[i].status === "running") {
        state.toolEntries[i].status = isError ? "error" : "done";
        break;
      }
    }

    updateProgressCard(state);
  });

  // ─── turn_end → 发送中间/最终文本 ─────────────────────

  pi.on("turn_end", (event: TurnEndEvent) => {
    if (!client || client.getStatus() !== "connected") return;
    const state = findActiveState();
    if (!state) return;

    const message = event.message;
    if (!message || message.role !== "assistant") return;

    // ── 检测 LLM 错误 ──
    if (message.stopReason === "error") {
      const errMsg = message.errorMessage ?? "LLM 返回了未知错误";
      client.sendMessage(state.chatId, `LLM 错误: ${errMsg}`, state.userMsgId).catch(() => {});
      client.stopTyping(state.chatId, false).catch(() => {});
      flashStatus("飞书: ⚠️ LLM 错误");
      return;
    }

    const textContent = extractTextFromMessage(message);
    if (!textContent) return;

    // 标题降级
    const processed = downgradeHeadings(textContent);

    // 检查这一轮是否包含工具调用
    const hasToolCalls = message.content?.some((block: any) => block.type === "toolCall");

    if (hasToolCalls) {
      // 中间轮：assistant 有文本 + 工具调用 → 发送中间文本（回复到用户消息）
      const chunks = chunkText(processed, MAX_TEXT_CHUNK);
      for (const chunk of chunks) {
        client.sendMessage(state.chatId, chunk, state.userMsgId);
      }
    } else {
      // 最终轮（或无工具调用的单轮）→ 发送文本（新消息）
      const chunks = chunkText(processed, MAX_TEXT_CHUNK);
      for (const chunk of chunks) {
        client.sendMessage(state.chatId, chunk);
      }
    }

    flashStatus(`飞书: 📤 推送中 (${textContent.length}字)`);
  });

  // ─── agent_end → 清理 + 处理下一条排队消息 ──────────

  pi.on("agent_end", (_event: AgentEndEvent) => {
    if (!client || client.getStatus() !== "connected") return;
    const state = findActiveState();
    if (!state) return;

    const chatId = state.chatId;

    // 移除 Typing Reaction
    client.stopTyping(chatId, true).catch(() => {});

    // 清理当前状态
    chatStates.delete(chatId);

    // 刷新所有队列（包括当前聊天）
    const queue = chatQueues.get(chatId);
    if (queue) queue.processing = false;
    flushAllQueues();
    flashStatus("飞书: ✅ 完成");
  });

  // ─── Pi 空闲时刷新所有队列 ─────────────────────────────

  /**
   * 检查所有聊天的队列，Pi 空闲时尝试处理下一条。
   * 在 agent_end、session_compact、session_start 后调用。
   */
  function flushAllQueues(): void {
    if (!client || client.getStatus() !== "connected") return;
    if (ctxRef && !ctxRef.isIdle()) return;

    for (const [chatId, queue] of chatQueues) {
      if (!queue.processing && queue.queue.length > 0) {
        dequeueAndProcess(chatId).catch(() => {
          queue.processing = false;
        });
      }
    }
  }

  // 压缩完成后，刷新队列（可能有积压消息）
  pi.on("session_compact", async () => {
    // 延迟一小段，等 Pi 完全恢复空闲
    setTimeout(() => flushAllQueues(), 500);
  });

  // ═══════════════════════════════════════════════════════
  //  进度卡片
  // ═══════════════════════════════════════════════════════

  /**
   * 构建工具进度卡片内容。
   * 所有工具共用一条可编辑消息，只滚动保留最近 10 次操作。
   */
  function buildProgressCardContent(entries: ChatState["toolEntries"]): string {
    const MAX_DISPLAY = 10;
    const total = entries.length;
    const display = total > MAX_DISPLAY ? entries.slice(-MAX_DISPLAY) : entries;

    const lines: string[] = [];

    // 如果有截断，显示省略提示
    if (total > MAX_DISPLAY) {
      lines.push(`... 前 ${total - MAX_DISPLAY} 次操作已折叠\n`);
    }

    for (const entry of display) {
      const displayName = toolDisplayName(entry.name);
      switch (entry.status) {
        case "running":
          lines.push(`⏳ **${displayName}** ...`);
          break;
        case "done":
          lines.push(`✅ ~~${displayName}~~`);
          break;
        case "error":
          lines.push(`❌ **${displayName}**`);
          break;
      }
    }
    return lines.join("\n");
  }

  /** 创建或更新进度卡片（防竞态：全生命周期只创建一条消息） */
  function updateProgressCard(state: ChatState): void {
    if (!client) return;

    const content = buildProgressCardContent(state.toolEntries);
    const runningCount = state.toolEntries.filter((e) => e.status === "running").length;
    const status = runningCount > 0 ? `执行中 (${runningCount})` : "工具调用";

    const card = FeishuClient.buildStreamingCard(content, status);

    // 情况 1: 卡片已创建 → 直接更新
    if (state.progressMsgId) {
      client.updateCard(state.progressMsgId, card).catch(() => {
        state.progressMsgId = null;
        state.progressCreating = false;
      });
      return;
    }

    // 情况 2: 卡片正在创建中 → 跳过，等创建完成后会自动刷新
    if (state.progressCreating) {
      return;
    }

    // 情况 3: 首次创建
    state.progressCreating = true;
    client.sendCard(state.chatId, card, state.userMsgId).then((cardMsgId) => {
      state.progressCreating = false;
      if (cardMsgId) {
        state.progressMsgId = cardMsgId;
        // 创建后立即用最新状态刷新（可能有新事件在创建期间发生）
        const latestContent = buildProgressCardContent(state.toolEntries);
        const latestRunning = state.toolEntries.filter((e) => e.status === "running").length;
        const latestStatus = latestRunning > 0 ? `执行中 (${latestRunning})` : "工具调用";
        const latestCard = FeishuClient.buildStreamingCard(latestContent, latestStatus);
        client?.updateCard(cardMsgId, latestCard).catch(() => {});
      }
    }).catch(() => {
      state.progressCreating = false;
    });
  }

  // ═══════════════════════════════════════════════════════
  //  辅助
  // ═══════════════════════════════════════════════════════

  function findActiveState(): ChatState | null {
    let lastKey: string | null = null;
    for (const key of chatStates.keys()) {
      lastKey = key;
    }
    if (!lastKey) return null;
    return chatStates.get(lastKey) ?? null;
  }

  // ─── 注册 /feishu 命令 ────────────────────────────────

  pi.registerCommand("feishu", {
    description: "管理飞书 Bot 连接 (start/stop/status/config/help)",
    handler: async (args: string, ctx: ExtensionCommandContext) => {
      const action = args.trim().toLowerCase() || "status";

      switch (action) {
        case "start":
          await startFeishuClient();
          ctx.ui.notify("飞书客户端已启动", "info");
          break;

        case "stop":
          if (client) {
            client.disconnect();
            client = null;
          }
          ctx.ui.notify("飞书客户端已停止", "info");
          break;

        case "status": {
          const status = client?.getStatus() ?? "未启动";
          ctx.ui.notify(
            `飞书 Bot 状态: ${status}\n` +
              `App ID: ${config.appId ? "****" + config.appId.slice(-4) : "未设置"}\n` +
              `Domain: ${config.domain || "feishu"}`,
            "info",
          );
          break;
        }

        case "config":
          ctx.ui.notify(
            `当前配置:\n` +
              `App ID: ${config.appId ? "****" + config.appId.slice(-4) : "未设置"}\n` +
              `App Secret: ${config.appSecret ? "****" : "未设置"}\n` +
              `Domain: ${config.domain || "feishu"}\n` +
              `Encrypt Key: ${config.encryptKey ? "已设置" : "未设置"}\n` +
              `Verification Token: ${config.verificationToken ? "已设置" : "未设置"}`,
            "info",
          );
          break;

        case "help":
          ctx.ui.notify(
            `/feishu 命令用法:\n` +
              `  /feishu start   - 启动飞书 Bot 连接\n` +
              `  /feishu stop    - 断开飞书 Bot 连接\n` +
              `  /feishu status  - 查看连接状态\n` +
              `  /feishu config  - 查看当前配置\n` +
              `  /feishu help    - 显示帮助\n\n` +
              `配置优先级（从高到低）:\n` +
              `  1. CLI 标志: --feishu-app-id, --feishu-app-secret\n` +
              `  2. 环境变量: FEISHU_APP_ID, FEISHU_APP_SECRET\n` +
              `  3. settings.json 中的 feishu 字段`,
            "info",
          );
          break;

        default:
          ctx.ui.notify(`未知命令: ${action}，使用 /feishu help 查看帮助`, "warning");
      }
    },
  });

  // ─── 注册自定义工具 ──────────────────────────────────

  // 发送文本消息
  const SendToFeishuParams = {
    type: "object" as const,
    properties: {
      message: { type: "string" as const, description: "要发送的消息内容" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID（飞书 chat_id），留空则发送到最近活跃的聊天",
      },
    },
    required: ["message"],
  };

  pi.registerTool({
    name: "send_to_feishu",
    label: "发送到飞书",
    description: "发送消息到飞书聊天界面。当用户要求通过飞书发送消息时使用。",
    parameters: SendToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const message = params.message as string;
      const chatId = (params.chat_id as string) || findActiveState()?.chatId;

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [
            { type: "text" as const, text: "错误: 飞书 Bot 未连接。请先运行 /feishu start 启动连接。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [
            { type: "text" as const, text: "错误: 没有活跃的飞书聊天。请先在飞书中发送一条消息。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendMessage(chatId, downgradeHeadings(message));
      return {
        content: [{ type: "text" as const, text: `已发送到飞书 [${chatId}]: ${message}` }],
        details: { sent: true, chatId, message } as Record<string, unknown>,
      };
    },
  });

  // 发送图片
  const SendImageToFeishuParams = {
    type: "object" as const,
    properties: {
      file_path: { type: "string" as const, description: "本地图片文件路径" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID，留空则发送到最近活跃的聊天",
      },
    },
    required: ["file_path"],
  };

  pi.registerTool({
    name: "send_image_to_feishu",
    label: "发送图片到飞书",
    description: "将本地图片文件上传到飞书并发送。当需要发送图片到飞书聊天时使用。",
    parameters: SendImageToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendImageToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const filePath = params.file_path as string;
      const chatId = (params.chat_id as string) || findActiveState()?.chatId;

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [{ type: "text" as const, text: "错误: 飞书 Bot 未连接。" }],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [{ type: "text" as const, text: "错误: 没有活跃的飞书聊天。" }],
          details: {} as Record<string, unknown>,
        };
      }

      const imageKey = await client.uploadImage(filePath);
      if (!imageKey) {
        return {
          content: [{ type: "text" as const, text: "错误: 图片上传失败。" }],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendImage(chatId, imageKey);
      return {
        content: [{ type: "text" as const, text: `图片已发送到飞书 [${chatId}]: ${filePath}` }],
        details: { sent: true, chatId, filePath, imageKey } as Record<string, unknown>,
      };
    },
  });

  // 发送文件
  const SendFileToFeishuParams = {
    type: "object" as const,
    properties: {
      file_path: { type: "string" as const, description: "本地文件路径" },
      file_name: { type: "string" as const, description: "文件名" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID，留空则发送到最近活跃的聊天",
      },
    },
    required: ["file_path", "file_name"],
  };

  pi.registerTool({
    name: "send_file_to_feishu",
    label: "发送文件到飞书",
    description: "将本地文件上传到飞书并发送。当需要发送文件到飞书聊天时使用。",
    parameters: SendFileToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendFileToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const filePath = params.file_path as string;
      const fileName = params.file_name as string;
      const chatId = (params.chat_id as string) || findActiveState()?.chatId;

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [{ type: "text" as const, text: "错误: 飞书 Bot 未连接。" }],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [{ type: "text" as const, text: "错误: 没有活跃的飞书聊天。" }],
          details: {} as Record<string, unknown>,
        };
      }

      const fileKey = await client.uploadFile(filePath, fileName);
      if (!fileKey) {
        return {
          content: [{ type: "text" as const, text: "错误: 文件上传失败。" }],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendFile(chatId, fileKey);
      return {
        content: [{ type: "text" as const, text: `文件已发送到飞书 [${chatId}]: ${fileName}` }],
        details: { sent: true, chatId, filePath, fileName, fileKey } as Record<string, unknown>,
      };
    },
  });

  // ─── 会话生命周期 ─────────────────────────────────────

  pi.on("session_start", async (_event, ctx) => {
    ctxRef = ctx;
    updateStatus(ctx, "disconnected");

    try {
      await startFeishuClient();
    } catch (err) {
      if (ctx.hasUI) {
        ctx.ui.notify(`飞书连接失败: ${err}`, "error");
      }
    }

    // 启动后刷新积压的队列
    flushAllQueues();
  });

  pi.on("session_shutdown", async () => {
    if (client) {
      client.disconnect();
      client = null;
    }
    chatStates.clear();
  });

  // ─── 工具函数 ────────────────────────────────────────

  /** 根据文件扩展名检测图片 MIME 类型 */
  function detectImageMimeType(filePath: string): string {
    const ext = filePath.toLowerCase().split(".").pop() ?? "";
    switch (ext) {
      case "png":
        return "image/png";
      case "jpg":
      case "jpeg":
        return "image/jpeg";
      case "gif":
        return "image/gif";
      case "webp":
        return "image/webp";
      case "svg":
        return "image/svg+xml";
      default:
        return "image/png"; // 飞书图片默认为 PNG 格式
    }
  }

  /**
   * Markdown 标题降级：所有出站文本的标题层级 +2，最小 H6。
   * 规则：只处理行首 # 开头、不在代码块内的标题行。
   *   H1 → H3, H2 → H4, H3 → H5, H4 → H6, H5/H6 → H6
   */
  function downgradeHeadings(text: string): string {
    const lines = text.split("\n");
    const result: string[] = [];
    let inCodeBlock = false;

    for (const line of lines) {
      // 追踪代码块状态
      if (line.startsWith("```")) {
        inCodeBlock = !inCodeBlock;
        result.push(line);
        continue;
      }

      if (inCodeBlock) {
        result.push(line);
        continue;
      }

      // 匹配行首标题：1-6 个 # 后跟空格或行尾
      const match = line.match(/^(#{1,6})\s/);
      if (match) {
        const level = match[1].length;
        const newLevel = Math.min(level + 2, 6);
        result.push("#".repeat(newLevel) + line.slice(level));
      } else {
        result.push(line);
      }
    }

    return result.join("\n");
  }

  /** 从 Pi 消息中提取文本内容 */
  function extractTextFromMessage(message: any): string | null {
    if (!message?.content) return null;
    const parts: string[] = [];
    for (const block of message.content) {
      if (block.type === "text" && block.text) {
        parts.push(block.text);
      }
    }
    return parts.length > 0 ? parts.join("\n") : null;
  }

  /** 状态栏瞬态消息定时器 */
  let statusTimer: ReturnType<typeof setTimeout> | null = null;
  let currentStatusText: string = "";

  function updateStatus(ctx: ExtensionContext | null, status: string): void {
    if (!ctx?.hasUI) return;

    if (statusTimer) {
      clearTimeout(statusTimer);
      statusTimer = null;
    }

    const statusMap: Record<string, string> = {
      connecting: "飞书: 连接中",
      connected: "飞书: 已连接",
      disconnected: "飞书: 未连接",
      error: "飞书: 错误",
    };

    const text = statusMap[status] ?? `飞书: ${status}`;
    if (currentStatusText === text) return;
    currentStatusText = text;
    ctx.ui.setStatus("feishu", text);
  }

  function flashStatus(message: string): void {
    if (!ctxRef?.hasUI) return;
    if (statusTimer) clearTimeout(statusTimer);

    if (currentStatusText === message) return;
    currentStatusText = message;
    ctxRef.ui.setStatus("feishu", message);

    statusTimer = setTimeout(() => {
      statusTimer = null;
      if (client && client.getStatus() === "connected") {
        const text = "飞书: 已连接";
        if (currentStatusText !== text) {
          currentStatusText = text;
          ctxRef?.ui.setStatus("feishu", text);
        }
      }
    }, 3000);
  }

  function chunkText(text: string, maxLen: number): string[] {
    if (text.length <= maxLen) return [text];

    const chunks: string[] = [];
    let remaining = text;

    while (remaining.length > 0) {
      if (remaining.length <= maxLen) {
        chunks.push(remaining);
        break;
      }

      let splitPos = remaining.lastIndexOf("\n", maxLen);
      if (splitPos <= 0) {
        splitPos = remaining.lastIndexOf(" ", maxLen);
      }
      if (splitPos <= 0) {
        chunks.push(remaining.substring(0, maxLen));
        remaining = remaining.substring(maxLen);
      } else {
        chunks.push(remaining.substring(0, splitPos + 1));
        remaining = remaining.substring(splitPos + 1);
      }
    }

    return chunks;
  }
}
