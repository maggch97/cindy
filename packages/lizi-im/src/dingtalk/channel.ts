/**
 * DingTalkChannelIM —— 钉钉渠道的连接方式路由。
 *
 * 钉钉渠道在编排层只有一个名字（'dingtalk'），底下有两种传输二选一：
 *   - robot：机器人应用（AppKey/AppSecret + Stream），见 ./index.ts；
 *   - dws：钉钉账号（官方 dws CLI），见 ../dingtalk-dws。
 * 当前方式持久化在 `dingtalk-transport-mode`，缺省 robot —— 老用户升级后行为
 * 不变。任一时刻只有当前方式在运行，收发都委托给它；切换时先停旧的再起新的。
 */

import { BaseIM } from '../BaseIM.js';
import type { ChannelIM, ImFinalOutput } from '../channelIM.js';
import {
  DingTalkDwsIM,
  type DingTalkDwsPublicState,
  type GroupHistoryMessage,
} from '../dingtalk-dws/index.js';
import type {
  IMCardActionEvent,
  IMHost,
  IMMessageEvent,
  IMStatus,
  InteractiveCardSpec,
  SendFileResult,
  StreamingTextHandle,
} from '../types.js';
import { DingTalkIM } from './index.js';
import type { SharedReplyDecision } from './pendingReplies.js';

export type DingTalkTransportMode = 'robot' | 'dws';

const MODE_SECRET = 'dingtalk-transport-mode';

export class DingTalkChannelIM extends BaseIM implements ChannelIM {
  private readonly messageHandlers = new Set<(event: IMMessageEvent) => void>();
  private readonly statusHandlers = new Set<(status: IMStatus) => void>();
  private mode: DingTalkTransportMode;
  private switching: Promise<void> = Promise.resolve();

  constructor(
    host: IMHost,
    readonly robot: DingTalkIM,
    readonly dws: DingTalkDwsIM,
  ) {
    super('dingtalk', host);
    this.mode = this.readMode();
    for (const [mode, transport] of [
      ['robot', robot],
      ['dws', dws],
    ] as const) {
      transport.onMessage((event) => {
        if (this.mode !== mode) return;
        for (const handler of this.messageHandlers) handler(event);
      });
      transport.onStatusChange((status) => {
        if (this.mode !== mode) return;
        for (const handler of this.statusHandlers) handler(status);
      });
    }
    dws.onStateChange(() => {
      this.host.ipc.broadcast('dingtalkBot:dws-state-change', { state: dws.getPublicState() });
    });
  }

  getMode(): DingTalkTransportMode {
    return this.mode;
  }

  /** 当前是否为 dws 方式（群上下文只在这种方式下可取）。 */
  supportsGroupHistory(): boolean {
    return this.mode === 'dws';
  }

  /** 当前连接的会话隔离键：robot = AppKey，dws = `<corpId>:<userId>`。 */
  get contextId(): string {
    return this.mode === 'dws' ? this.dws.contextId : '';
  }

  async init(): Promise<void> {
    this.mode = this.readMode();
    await this.active().init();
  }

  async dispose(): Promise<void> {
    await Promise.allSettled([this.robot.dispose(), this.dws.dispose()]);
  }

  registerIpc(): void {
    // 机器人方式的原有通道（dingtalkBot:get-state / save / reconnect / clear）。
    this.robot.registerIpc();
    this.host.ipc.handle('dingtalkBot:get-mode', () => ({ mode: this.mode }));
    this.host.ipc.handle('dingtalkBot:set-mode', async (payload) => {
      const mode = isRecord(payload) ? payload.mode : undefined;
      if (mode !== 'robot' && mode !== 'dws') {
        this.host.ipc.throwIpcError('INVALID_PARAMS', 'mode must be robot or dws');
      }
      await this.runInAccountScope(() => this.setMode(mode));
      return { mode: this.mode };
    });
    this.host.ipc.handle('dingtalkBot:dws-get-state', () =>
      this.runInAccountScope(() => this.dws.probe()),
    );
    this.host.ipc.handle('dingtalkBot:dws-connect', () =>
      this.runInAccountScope(async (): Promise<DingTalkDwsPublicState> => {
        if (this.mode !== 'dws') await this.setMode('dws');
        return this.dws.enable();
      }),
    );
    this.host.ipc.handle('dingtalkBot:dws-disconnect', () =>
      this.runInAccountScope(() => this.dws.disable()),
    );
    this.host.ipc.handle('dingtalkBot:dws-clear-owner', () =>
      this.runInAccountScope(async () => {
        this.dws.clearOwner();
        return this.dws.getPublicState();
      }),
    );
  }

  private async setMode(mode: DingTalkTransportMode): Promise<void> {
    const run = this.switching.then(async () => {
      if (mode === this.mode) return;
      await this.active().dispose();
      if (!this.host.secrets.write(MODE_SECRET, mode)) {
        throw new Error('[DINGTALK_DWS_STREAM_FAILED] secure storage unavailable');
      }
      this.mode = mode;
      await this.active().init();
    });
    this.switching = run.catch(() => undefined);
    await run;
  }

  // ── 收发委托 ────────────────────────────────────────────────────────────

  onMessage(handler: (event: IMMessageEvent) => void): () => void {
    this.messageHandlers.add(handler);
    return () => this.messageHandlers.delete(handler);
  }

  onStatusChange(handler: (status: IMStatus) => void): () => void {
    this.statusHandlers.add(handler);
    return () => this.statusHandlers.delete(handler);
  }

  onCardAction(handler: (event: IMCardActionEvent) => void): () => void {
    return this.robot.onCardAction(handler);
  }

  getStatus(): IMStatus {
    return this.active().getStatus();
  }

  sendText(userId: string, text: string): Promise<{ messageId: string }> {
    return this.active().sendText(userId, text);
  }

  sendMarkdownText(userId: string, markdown: string): Promise<{ messageId: string }> {
    return this.active().sendMarkdownText(userId, markdown);
  }

  sendFile(userId: string, absPath: string, displayName?: string): Promise<SendFileResult> {
    return this.mode === 'dws'
      ? this.dws.sendFile(userId, absPath)
      : this.robot.sendFile(userId, absPath, displayName);
  }

  commitFinal(output: ImFinalOutput): Promise<void> {
    return this.active().commitFinal(output);
  }

  requestTextReply<T>(
    userId: string,
    prompt: string,
    parse: (text: string) => T | null,
    timeoutMs?: number,
    shared?: SharedReplyDecision<T>,
  ): Promise<T> {
    return this.active().requestTextReply(userId, prompt, parse, timeoutMs, shared);
  }

  fetchRecentGroupMessages(conversationId: string, limit: number): Promise<GroupHistoryMessage[]> {
    if (this.mode !== 'dws') return Promise.resolve([]);
    return this.dws.fetchRecentGroupMessages(conversationId, limit);
  }

  // 两种方式都没有卡片 / 流式能力；编排层按 chunked-text 输出，不会调用这些。
  sendInteractiveCard(userId: string, spec: InteractiveCardSpec): Promise<{ messageId: string }> {
    return this.robot.sendInteractiveCard(userId, spec);
  }

  updateInteractiveCard(messageId: string, spec: InteractiveCardSpec): Promise<void> {
    return this.robot.updateInteractiveCard(messageId, spec);
  }

  patchMarkdownCard(messageId: string, markdown: string): Promise<void> {
    return this.robot.patchMarkdownCard(messageId, markdown);
  }

  startStreamingText(userId: string, initial?: string): Promise<StreamingTextHandle> {
    return this.robot.startStreamingText(userId, initial);
  }

  // ── internals ───────────────────────────────────────────────────────────

  private active(): DingTalkIM | DingTalkDwsIM {
    return this.mode === 'dws' ? this.dws : this.robot;
  }

  private readMode(): DingTalkTransportMode {
    return this.host.secrets.read(MODE_SECRET) === 'dws' ? 'dws' : 'robot';
  }

  private async runInAccountScope<T>(operation: () => Promise<T>): Promise<T> {
    const scope = this.host.accountScope;
    if (!scope) return operation();
    const token = scope.capture();
    if (token === null) throw new Error('[IM_NOT_READY] IM account is not active');
    return scope.run(token, operation);
  }
}

export function createDingTalkChannelIM(
  host: IMHost,
  robot: DingTalkIM,
  dws: DingTalkDwsIM,
): DingTalkChannelIM {
  return new DingTalkChannelIM(host, robot, dws);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
