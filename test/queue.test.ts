import { setImmediate } from "node:timers/promises";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { beforeEach, describe, expect, it, vi } from "vitest";

type InboundHandler = (
  chatId: string,
  msgId: string,
  text: string,
  chatType: "p2p" | "group",
  resources: [],
) => void;

const { bridge } = vi.hoisted(() => ({
  bridge: {
    inbound: undefined as InboundHandler | undefined,
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn(),
    getStatus: vi.fn(() => "connected"),
    setOnMessage: vi.fn(function (handler: InboundHandler) {
      bridge.inbound = handler;
    }),
    setOnStatusChange: vi.fn(),
    startTyping: vi.fn().mockResolvedValue(undefined),
    stopTyping: vi.fn().mockResolvedValue(undefined),
    sendMessage: vi.fn().mockResolvedValue(undefined),
  },
}));

// Exercise the real extension without reading local credentials or connecting to Feishu.
vi.mock("node:fs", () => ({
  existsSync: () => false,
  readFileSync: () => "{}",
}));
vi.mock("../src/feishu-client.js", () => ({
  FeishuClient: class {
    constructor() {
      return bridge;
    }
  },
}));

import registerExtension from "../src/index.js";

async function createHarness(initiallyIdle = true) {
  let idle = initiallyIdle;
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const sendUserMessage = vi.fn((_text: string) => { idle = false; });
  const ctx = {
    hasUI: false,
    isIdle: () => idle,
    abort: vi.fn(),
  } as unknown as ExtensionContext;
  const pi = {
    on: (name: string, handler: (event: any, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    },
    getFlag: (name: string) => ({
      "feishu-app-id": "test-app",
      "feishu-app-secret": "test-secret",
    })[name],
    registerFlag: vi.fn(),
    registerCommand: vi.fn(),
    registerTool: vi.fn(),
    sendUserMessage,
  } as unknown as ExtensionAPI;

  registerExtension(pi);

  async function emit(name: string, event: any = {}) {
    for (const handler of handlers.get(name) ?? []) await handler(event, ctx);
    // Inbound callbacks and queue draining intentionally launch async work.
    await setImmediate();
  }
  await emit("session_start");

  return {
    sendUserMessage,
    emit,
    async receive(text: string, msgId = text, chatId = "chat-a") {
      bridge.inbound!(chatId, msgId, text, "p2p", []);
      await setImmediate();
    },
    async turn(stopReason: "stop" | "error", text = "reply") {
      await emit("turn_end", {
        message: {
          role: "assistant",
          stopReason,
          content: stopReason === "stop" ? [{ type: "text", text }] : [],
          errorMessage: stopReason === "error" ? "Request timed out." : undefined,
        },
      });
    },
    async settle() {
      idle = true;
      await emit("agent_settled");
    },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  bridge.inbound = undefined;
});

describe("Feishu queue lifecycle", () => {
  it("drains queued messages in FIFO order only after the run settles", async () => {
    const h = await createHarness();
    await h.receive("first");
    await h.receive("second");
    await h.receive("third");

    await h.turn("stop");
    await h.emit("agent_end");
    expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);

    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"]]);
    expect(bridge.stopTyping).toHaveBeenCalledWith("chat-a", true);

    await h.turn("stop");
    await h.emit("agent_end");
    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"], ["third"]]);
  });

  it("keeps reply routing and queued messages through a successful retry", async () => {
    const h = await createHarness();
    await h.receive("first", "original-message");
    await h.receive("second");

    await h.turn("error");
    await h.emit("agent_end");
    bridge.sendMessage.mockClear();
    await h.turn("stop", "recovered reply");
    expect(bridge.sendMessage).toHaveBeenCalledWith("chat-a", "recovered reply");
    expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);

    await h.emit("agent_end");
    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"]]);
  });

  it("retains the original message through exhausted retries, then advances", async () => {
    const h = await createHarness();
    await h.receive("first", "original-message");
    await h.receive("second");
    bridge.sendMessage.mockClear();

    for (let attempt = 0; attempt < 4; attempt++) {
      await h.turn("error");
      await h.emit("agent_end");
      expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);
    }
    expect(bridge.sendMessage.mock.calls).toEqual(Array.from({ length: 4 }, () => [
      "chat-a", "LLM 错误: Request timed out.", "original-message",
    ]));

    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"]]);
  });

  it("wakes messages queued behind a run started outside Feishu", async () => {
    const h = await createHarness(false);
    await h.receive("waiting");
    await h.emit("agent_end");
    expect(h.sendUserMessage).not.toHaveBeenCalled();

    await h.settle();
    expect(h.sendUserMessage).toHaveBeenCalledExactlyOnceWith("waiting");
  });

  it("dispatches one chat at a time and preserves each chat's reply routing", async () => {
    const h = await createHarness();
    await h.receive("first", "message-a", "chat-a");
    await h.receive("second", "message-b", "chat-b");
    await h.receive("third", "message-c", "chat-c");
    await h.emit("agent_end");
    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"]]);

    bridge.sendMessage.mockClear();
    await h.turn("stop", "reply to second chat");
    expect(bridge.sendMessage).toHaveBeenCalledExactlyOnceWith("chat-b", "reply to second chat");
    await h.emit("agent_end");
    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"], ["third"]]);
  });

  it("reserves Pi while awaiting the typing reaction before submitting a message", async () => {
    const h = await createHarness();
    let finishTyping!: () => void;
    bridge.startTyping.mockImplementationOnce(() => new Promise<void>((resolve) => {
      finishTyping = resolve;
    }));
    bridge.inbound!("chat-a", "message-a", "first", "p2p", []);
    await h.receive("second", "message-b", "chat-b");
    expect(h.sendUserMessage).not.toHaveBeenCalled();

    finishTyping();
    await setImmediate();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);
    await h.emit("agent_end");
    await h.settle();
    expect(h.sendUserMessage.mock.calls).toEqual([["first"], ["second"]]);
  });

  it("does not replay a queue cleared by /stop when the aborted run settles", async () => {
    const h = await createHarness();
    await h.receive("first");
    await h.receive("second");
    await h.receive("/stop");
    await h.settle();

    expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);
  });

  it("does not dispatch queued messages after session shutdown", async () => {
    const h = await createHarness();
    await h.receive("first");
    await h.receive("second");
    await h.emit("session_shutdown");
    await h.settle();

    expect(h.sendUserMessage.mock.calls).toEqual([["first"]]);
    expect(bridge.disconnect).toHaveBeenCalledOnce();
  });
});
