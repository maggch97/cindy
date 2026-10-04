/**
 * DingTalkDwsIM —— 通过官方 dws CLI 以「钉钉账号」身份收发的钉钉传输。
 *
 * 与机器人传输（../dingtalk）对外能力一致（文字收发、文件、文字交互），由
 * DingTalkChannelIM 二选一启用：
 *   - 入站：长驻 `dws event consume <o2o_all> <at> --flatten -f ndjson`，
 *     stderr 出现 `[event] ready` 才算连上；进程退出按指数退避重连。
 *   - 出站：`dws chat +messages-send`（单聊 open-dingtalk-id / 群 group）。
 *   - 身份：dws 当前登录账号（`dws auth status`）；Cindy 不读取、不保存其凭证。
 *   - 主人：私聊发送设置页一次性配对码的人；只有主人能驱动任务（单聊与群 @）。
 *     dws 登录的是真实账号，不能沿用机器人「第一个私聊者即主人」的规则。
 */

import { randomInt } from 'node:crypto';
import path from 'node:path';

import { BaseIM } from '../BaseIM.js';
import type { ImFinalOutput } from '../channelIM.js';
import { decodeLaneUserId, encodeLaneUserId } from '../dingtalk/codec.js';
import { PendingReplies, type SharedReplyDecision } from '../dingtalk/pendingReplies.js';
import type { IMHost, IMMessageEvent, IMStatus, SendFileResult } from '../types.js';
import {
  DWS_EVENT_DIRECT,
  DWS_EVENT_MENTION,
  isDwsReadyLine,
  parseDwsEventLine,
  parseDwsTransportState,
  stripSelfMention,
  type DwsInboundMessage,
} from './events.js';
import { DWS_NOT_INSTALLED, type DwsRunner, type DwsStreamProcess } from './runner.js';

export { DwsCommandError, DWS_NOT_INSTALLED, parseDwsJsonOutput } from './runner.js';
export type { DwsRunner, DwsStreamProcess } from './runner.js';

const ENABLED_SECRET = 'dingtalk-dws-enabled';
const OWNER_SECRET = 'dingtalk-dws-owner';
const DEDUP_TTL_MS = 5 * 60 * 1_000;
const DEDUP_CAPACITY = 2_048;
const READY_TIMEOUT_MS = 45_000;
const STOP_GRACE_MS = 5_000;
const RECONNECT_BASE_MS = 2_000;
const RECONNECT_MAX_MS = 60_000;
const INTERACTION_TIMEOUT_MS = 30 * 60 * 1_000;
const OUTBOUND_CHUNK_SIZE = 3_500;
const MAX_OUTBOUND_FILES = 4;
const MAX_LINE_CHARS = 1_000_000;
const SEND_TIMEOUT_MS = 60_000;

export const DINGTALK_DWS_ERROR = {
  notInstalled: 'DINGTALK_DWS_NOT_INSTALLED',
  notLoggedIn: 'DINGTALK_DWS_NOT_LOGGED_IN',
  streamFailed: 'DINGTALK_DWS_STREAM_FAILED',
} as const;

export interface DingTalkDwsIdentity {
  corpId: string;
  corpName: string;
  userId: string;
  userName: string;
}

export interface DingTalkDwsPublicState {
  status: IMStatus;
  enabled: boolean;
  installed: boolean;
  identity: Omit<DingTalkDwsIdentity, 'corpId' | 'userId'> | null;
  ownerName: string | null;
  /** 尚未绑定主人且已开启时的一次性配对码；主人私聊发送它完成绑定。 */
  pairingCode: string | null;
}

export interface GroupHistoryMessage {
  messageId: string;
  senderName: string;
  senderId: string;
  text: string;
  createTime: string;
}

interface OwnerRecord {
  contextId: string;
  openId: string;
  name: string;
}

type Target = { kind: 'direct'; openId: string } | { kind: 'group'; conversationId: string };

export class DingTalkDwsIM extends BaseIM {
  private readonly messageHandlers = new Set<(event: IMMessageEvent) => void>();
  private readonly statusHandlers = new Set<(status: IMStatus) => void>();
  private readonly stateHandlers = new Set<() => void>();
  private readonly seen = new Map<string, number>();
  private readonly laneQueues = new Map<string, Promise<void>>();
  private readonly pendingReplies = new PendingReplies({
    alreadyPending: 'DINGTALK_INTERACTION_ALREADY_PENDING',
    timeout: 'DINGTALK_INTERACTION_TIMEOUT',
  });

  private status: IMStatus = { kind: 'idle' };
  private identity: DingTalkDwsIdentity | null = null;
  private installed = false;
  private pairingCode: string | null = null;
  private proc: DwsStreamProcess | null = null;
  private generation = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;

  constructor(
    host: IMHost,
    private readonly runner: DwsRunner,
  ) {
    super('dingtalk', host);
  }

  /** 连接标识（会话隔离键）：`<corpId>:<userId>`。未连接时为空串。 */
  get contextId(): string {
    return this.identity ? `${this.identity.corpId}:${this.identity.userId}` : '';
  }

  isEnabled(): boolean {
    return this.host.secrets.read(ENABLED_SECRET) === '1';
  }

  async init(): Promise<void> {
    if (!this.isEnabled()) {
      this.setStatus({ kind: 'idle' });
      return;
    }
    try {
      await this.connect();
    } catch (error) {
      this.setStatus({ kind: 'error', reason: safeErrorCode(error) });
    }
  }

  async dispose(): Promise<void> {
    await this.stop();
    this.setStatus({ kind: 'idle' });
  }

  /** 由 DingTalkChannelIM 统一注册 IPC；本类不直接持有通道名。 */
  registerIpc(): void {}

  // ── 控制面（供 DingTalkChannelIM 的 IPC 调用）────────────────────────────

  /** 探测本机 dws 安装与登录状态；不改变连接。 */
  async probe(): Promise<DingTalkDwsPublicState> {
    this.installed = await this.runner.isAvailable().catch(() => false);
    if (this.installed && this.status.kind !== 'connected') {
      this.identity = await this.readIdentity().catch(() => null);
    }
    return this.getPublicState();
  }

  getPublicState(): DingTalkDwsPublicState {
    const owner = this.readOwner();
    return {
      // 渲染层只需要连接态；不外送 corpId / userId 组成的内部会话键。
      status:
        this.status.kind === 'connected' ? { kind: 'connected', appId: 'dws' } : this.status,
      enabled: this.isEnabled(),
      installed: this.installed,
      identity: this.identity
        ? { corpName: this.identity.corpName, userName: this.identity.userName }
        : null,
      ownerName: owner ? owner.name : null,
      pairingCode: !owner && this.isEnabled() ? this.ensurePairingCode() : null,
    };
  }

  /** 开启并连接；失败时抛出带 `[CODE]` 前缀的错误，并保持未启用。 */
  async enable(): Promise<DingTalkDwsPublicState> {
    if (!this.host.secrets.write(ENABLED_SECRET, '1')) {
      throw new Error('[DINGTALK_DWS_STREAM_FAILED] secure storage unavailable');
    }
    try {
      await this.connect();
    } catch (error) {
      this.host.secrets.remove(ENABLED_SECRET);
      await this.stop();
      this.setStatus({ kind: 'idle' });
      throw toCodedError(error);
    }
    return this.getPublicState();
  }

  async disable(): Promise<DingTalkDwsPublicState> {
    this.host.secrets.remove(ENABLED_SECRET);
    await this.stop();
    this.setStatus({ kind: 'idle' });
    return this.getPublicState();
  }

  /** 解除主人绑定：之后需用新的配对码重新绑定。 */
  clearOwner(): void {
    this.host.secrets.remove(OWNER_SECRET);
    this.pairingCode = null;
    this.emitStateChange();
  }

  onStateChange(handler: () => void): () => void {
    this.stateHandlers.add(handler);
    return () => this.stateHandlers.delete(handler);
  }

  // ── ChannelIM 收发面 ────────────────────────────────────────────────────

  onMessage(handler: (event: IMMessageEvent) => void): () => void {
    this.messageHandlers.add(handler);
    return () => this.messageHandlers.delete(handler);
  }

  onStatusChange(handler: (status: IMStatus) => void): () => void {
    this.statusHandlers.add(handler);
    return () => this.statusHandlers.delete(handler);
  }

  getStatus(): IMStatus {
    return this.status;
  }

  async sendText(userId: string, text: string): Promise<{ messageId: string }> {
    return this.sendBody(userId, text, '--text');
  }

  async sendMarkdownText(userId: string, markdown: string): Promise<{ messageId: string }> {
    return this.sendBody(userId, markdown, '--markdown');
  }

  async sendFile(userId: string, absPath: string): Promise<SendFileResult> {
    if (!path.isAbsolute(absPath)) return { ok: false, reason: 'NOT_FOUND' };
    try {
      // dws 只接受工作目录内的相对路径：以文件所在目录为 cwd，只传文件名。
      await this.runner.runJson(
        [
          'chat',
          '+messages-send',
          ...targetArgs(this.resolveTarget(userId)),
          '--file',
          path.basename(absPath),
          '--yes',
          '-f',
          'json',
        ],
        { cwd: path.dirname(absPath), timeoutMs: SEND_TIMEOUT_MS },
      );
      return { ok: true };
    } catch {
      this.log.warn('dingtalk dws file send failed');
      return { ok: false, reason: 'SEND_FAIL' };
    }
  }

  async commitFinal(output: ImFinalOutput): Promise<void> {
    await this.sendMarkdownText(output.userId, normalizeFinalText(output.text));
    const files = Array.from(new Set(output.mediaAbsPaths ?? [])).slice(0, MAX_OUTBOUND_FILES);
    for (const absPath of files) {
      await this.sendFile(output.userId, absPath);
    }
  }

  requestTextReply<T>(
    userId: string,
    prompt: string,
    parse: (text: string) => T | null,
    timeoutMs = INTERACTION_TIMEOUT_MS,
    shared?: SharedReplyDecision<T>,
  ): Promise<T> {
    return this.pendingReplies.request(userId, prompt, parse, timeoutMs, shared, (text) =>
      this.sendText(userId, text),
    );
  }

  /** 以当前账号身份读取群最近消息（时间正序），供群上下文注入。 */
  async fetchRecentGroupMessages(
    conversationId: string,
    limit: number,
  ): Promise<GroupHistoryMessage[]> {
    const result = await this.runner.runJson(
      [
        'chat',
        '+chat-messages',
        '--open-conversation-id',
        conversationId,
        '--limit',
        String(Math.max(1, Math.min(100, Math.floor(limit)))),
        '--no-reactions',
        '-f',
        'json',
      ],
      { timeoutMs: 30_000 },
    );
    const messages = isRecord(result) && Array.isArray(result.messages) ? result.messages : [];
    return messages
      .filter(isRecord)
      .map((m) => ({
        messageId: str(m.messageId),
        senderName: str(m.sender) || '钉钉用户',
        senderId: str(m.senderId),
        text: str(m.text),
        createTime: str(m.createTime),
      }))
      .filter((m) => m.messageId && m.text)
      .sort((a, b) => a.createTime.localeCompare(b.createTime));
  }

  // ── 连接 ────────────────────────────────────────────────────────────────

  private async connect(): Promise<void> {
    await this.stop();
    const generation = this.generation;
    this.setStatus({ kind: 'connecting' });
    this.installed = await this.runner.isAvailable().catch(() => false);
    if (!this.installed) throw codedError(DINGTALK_DWS_ERROR.notInstalled);
    const identity = await this.readIdentity();
    if (generation !== this.generation) throw new Error('DINGTALK_DWS_CONNECTION_REPLACED');
    this.identity = identity;
    const owner = this.readOwner();
    if (owner && owner.contextId !== this.contextId) {
      // dws 换了登录账号：旧主人绑定不再适用。
      this.host.secrets.remove(OWNER_SECRET);
    }
    await this.startStream(generation);
  }

  private async readIdentity(): Promise<DingTalkDwsIdentity> {
    let result: unknown;
    try {
      result = await this.runner.runJson(['auth', 'status', '-f', 'json'], { timeoutMs: 30_000 });
    } catch (error) {
      if (error instanceof Error && error.message === DWS_NOT_INSTALLED) {
        throw codedError(DINGTALK_DWS_ERROR.notInstalled);
      }
      throw codedError(DINGTALK_DWS_ERROR.notLoggedIn);
    }
    if (
      !isRecord(result) ||
      result.authenticated !== true ||
      (result.token_valid !== true && result.refresh_token_valid !== true)
    ) {
      throw codedError(DINGTALK_DWS_ERROR.notLoggedIn);
    }
    const corpId = str(result.corp_id);
    const userId = str(result.user_id);
    if (!corpId || !userId) throw codedError(DINGTALK_DWS_ERROR.notLoggedIn);
    return {
      corpId,
      userId,
      corpName: str(result.corp_name),
      userName: str(result.user_name),
    };
  }

  private startStream(generation: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      let proc: DwsStreamProcess;
      try {
        proc = this.runner.spawnStream([
          'event',
          'consume',
          DWS_EVENT_DIRECT,
          DWS_EVENT_MENTION,
          '--flatten',
          '-f',
          'ndjson',
        ]);
      } catch {
        reject(codedError(DINGTALK_DWS_ERROR.notInstalled));
        return;
      }
      this.proc = proc;
      const readyTimer = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(codedError(DINGTALK_DWS_ERROR.streamFailed));
        // 只结束这一次未就绪的进程；首次连接由调用方收口，重连路径由退避继续接管。
        try {
          proc.forceKill();
        } catch {
          // 进程可能已经退出。
        }
      }, READY_TIMEOUT_MS);

      readLines(proc.stdout, (line) => {
        if (generation !== this.generation) return;
        const message = parseDwsEventLine(line);
        if (message) this.accept(message, generation);
      });
      readLines(proc.stderr, (line) => {
        if (generation !== this.generation) return;
        if (isDwsReadyLine(line)) {
          this.reconnectAttempt = 0;
          this.setStatus({ kind: 'connected', appId: this.contextId });
          if (!settled) {
            settled = true;
            clearTimeout(readyTimer);
            resolve();
          }
          return;
        }
        const state = parseDwsTransportState(line);
        if (state === 'connected' && this.status.kind !== 'connected' && settled) {
          this.setStatus({ kind: 'connected', appId: this.contextId });
        } else if (state === 'reconnecting' || state === 'disconnected') {
          if (this.status.kind === 'connected') this.setStatus({ kind: 'connecting' });
        }
      });
      proc.onError(() => {
        // 'exit' 一般随后到达；就绪前的 spawn 失败直接收口。
        if (!settled) {
          settled = true;
          clearTimeout(readyTimer);
          reject(codedError(DINGTALK_DWS_ERROR.notInstalled));
        }
      });
      proc.onExit(() => {
        clearTimeout(readyTimer);
        if (this.proc === proc) this.proc = null;
        if (!settled) {
          settled = true;
          reject(codedError(DINGTALK_DWS_ERROR.streamFailed));
          return;
        }
        if (generation !== this.generation) return;
        this.scheduleReconnect(generation);
      });
    });
  }

  private scheduleReconnect(generation: number): void {
    if (this.reconnectTimer) return;
    const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** this.reconnectAttempt);
    this.reconnectAttempt += 1;
    this.setStatus({ kind: 'connecting' });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (generation !== this.generation) return;
      void this.startStream(generation).catch((error) => {
        if (generation !== this.generation) return;
        this.log.warn(`dingtalk dws stream restart failed: ${safeErrorCode(error)}`);
        this.scheduleReconnect(generation);
      });
    }, delay);
  }

  private async stop(): Promise<void> {
    this.generation += 1;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.laneQueues.clear();
    this.pendingReplies.rejectAll('DINGTALK_DISCONNECTED');
    const proc = this.proc;
    this.proc = null;
    if (!proc) return;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        // 优雅停机超时才强杀；强杀会跳过服务端退订，所以只作兜底。
        try {
          proc.forceKill();
        } catch {
          // 进程可能已经退出。
        }
        resolve();
      }, STOP_GRACE_MS);
      proc.onExit(() => {
        clearTimeout(timer);
        resolve();
      });
      try {
        proc.closeStdin();
      } catch {
        clearTimeout(timer);
        try {
          proc.forceKill();
        } catch {
          // ignore
        }
        resolve();
      }
    });
  }

  // ── 入站 ────────────────────────────────────────────────────────────────

  private accept(message: DwsInboundMessage, generation: number): void {
    if (this.isDuplicate(message.dedupeKey)) return;
    const userId =
      message.kind === 'mention' ? encodeLaneUserId(message.conversationId) : message.senderOpenId;
    const accountToken = this.host.accountScope?.capture();
    if (this.host.accountScope && accountToken === null) return;
    const prior = this.laneQueues.get(userId) ?? Promise.resolve();
    const next = prior
      .catch(() => undefined)
      .then(() => this.runInCapturedAccountScope(accountToken, () => this.process(userId, message, generation)))
      .catch((error) => {
        this.log.warn(`dingtalk dws inbound processing failed: ${safeErrorCode(error)}`);
      })
      .finally(() => {
        if (this.laneQueues.get(userId) === next) this.laneQueues.delete(userId);
      });
    this.laneQueues.set(userId, next);
  }

  private async process(userId: string, message: DwsInboundMessage, generation: number): Promise<void> {
    if (generation !== this.generation) return;
    const isGroup = message.kind === 'mention';
    const owner = this.readOwner();
    if (!owner) {
      // dws 登录的是真实账号，同事随时可能私聊它，不能像新建机器人那样
      // 「第一个私聊者即主人」：只有私聊发送设置页配对码的人才会被绑定。
      // 群里绝不认主；绑定前的其他消息一律忽略，也不回复。
      if (!isGroup && this.pairingCode && message.text.trim() === this.pairingCode) {
        this.claimOwner(message);
      }
      return;
    }
    const isOwner = message.senderOpenId === owner.openId;
    // 真实账号身处大量群聊：只有主人能驱动任务（单聊与群 @ 同口径）。
    if (!isOwner) return;

    const text = isGroup
      ? stripSelfMention(message.text, this.identity?.userName ?? '')
      : message.text.trim();
    if (text && this.pendingReplies.tryResolve(userId, text, isOwner)) return;

    const event: IMMessageEvent = {
      channelName: 'dingtalk',
      interactionSource: { senderName: message.senderName },
      senderId: userId,
      chatId: message.conversationId,
      contextId: this.contextId,
      messageId: message.messageId,
      // 群里纯 @（无正文）仍是召唤，补成裸 `@`，与机器人模式同口径。
      text: isGroup && !text ? '@' : text,
      ...(isGroup
        ? { speaker: { id: message.senderOpenId, name: message.senderName, isOwner } }
        : {}),
      ...(message.quoted
        ? { replyContext: { author: message.quoted.author, text: message.quoted.text } }
        : {}),
      attachments: [],
      unsupported: [],
    };
    if (!event.text) return;
    for (const handler of this.messageHandlers) {
      try {
        handler(event);
      } catch {
        // 单个订阅者异常不影响其他订阅者。
      }
    }
  }

  private claimOwner(message: DwsInboundMessage): OwnerRecord | null {
    const record: OwnerRecord = {
      contextId: this.contextId,
      openId: message.senderOpenId,
      name: message.senderName,
    };
    if (!this.host.secrets.write(OWNER_SECRET, JSON.stringify(record))) return null;
    // 配对码一次性：用过即作废，解除绑定后再生成新的。
    this.pairingCode = null;
    this.emitStateChange();
    return record;
  }

  private ensurePairingCode(): string {
    this.pairingCode ??= String(randomInt(100_000, 1_000_000));
    return this.pairingCode;
  }

  private readOwner(): OwnerRecord | null {
    const raw = this.host.secrets.read(OWNER_SECRET);
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (
        isRecord(parsed) &&
        typeof parsed.contextId === 'string' &&
        typeof parsed.openId === 'string' &&
        parsed.openId
      ) {
        return { contextId: parsed.contextId, openId: parsed.openId, name: str(parsed.name) };
      }
    } catch {
      // 损坏的记录按未绑定处理。
    }
    return null;
  }

  private isDuplicate(key: string): boolean {
    const now = Date.now();
    for (const [id, at] of this.seen) {
      if (now - at > DEDUP_TTL_MS) this.seen.delete(id);
    }
    if (this.seen.has(key)) return true;
    this.seen.set(key, now);
    while (this.seen.size > DEDUP_CAPACITY) {
      const oldest = this.seen.keys().next().value;
      if (!oldest) break;
      this.seen.delete(oldest);
    }
    return false;
  }

  // ── 出站 ────────────────────────────────────────────────────────────────

  private async sendBody(
    userId: string,
    body: string,
    flag: '--text' | '--markdown',
  ): Promise<{ messageId: string }> {
    const target = this.resolveTarget(userId);
    for (const chunk of chunkText(body)) {
      await this.runner.runJson(
        ['chat', '+messages-send', ...targetArgs(target), flag, chunk, '--yes', '-f', 'json'],
        { timeoutMs: SEND_TIMEOUT_MS },
      );
    }
    return { messageId: `out:${Date.now().toString(36)}` };
  }

  private resolveTarget(userId: string): Target {
    const lane = decodeLaneUserId(userId);
    return lane
      ? { kind: 'group', conversationId: lane.conversationId }
      : { kind: 'direct', openId: userId };
  }

  // ── 状态 ────────────────────────────────────────────────────────────────

  private setStatus(status: IMStatus): void {
    this.status = status;
    for (const handler of this.statusHandlers) {
      try {
        handler(status);
      } catch {
        // best effort
      }
    }
    this.emitStateChange();
  }

  private emitStateChange(): void {
    for (const handler of this.stateHandlers) {
      try {
        handler();
      } catch {
        // best effort
      }
    }
  }

  private async runInCapturedAccountScope<T>(
    token: unknown,
    operation: () => Promise<T>,
  ): Promise<T> {
    const scope = this.host.accountScope;
    if (!scope) return operation();
    if (token === null || !scope.isCurrent(token)) {
      throw new Error('[IM_NOT_READY] IM account changed');
    }
    return scope.run(token, operation);
  }
}

export function createDingTalkDwsIM(host: IMHost, runner: DwsRunner): DingTalkDwsIM {
  return new DingTalkDwsIM(host, runner);
}

function targetArgs(target: Target): string[] {
  return target.kind === 'group'
    ? ['--group', target.conversationId]
    : ['--open-dingtalk-id', target.openId];
}

function readLines(stream: NodeJS.ReadableStream, onLine: (line: string) => void): void {
  let buffer = '';
  stream.setEncoding?.('utf8');
  stream.on('data', (chunk: string | Buffer) => {
    buffer += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
    let index = buffer.indexOf('\n');
    while (index >= 0) {
      const line = buffer.slice(0, index).replace(/\r$/, '');
      buffer = buffer.slice(index + 1);
      if (line.trim()) onLine(line);
      index = buffer.indexOf('\n');
    }
    // 单行超长视为异常输出，丢弃以免无界增长。
    if (buffer.length > MAX_LINE_CHARS) buffer = '';
  });
}

function chunkText(text: string): string[] {
  const normalized = text.trim() || '（空回复）';
  const chunks: string[] = [];
  let remaining = normalized;
  while (remaining.length > OUTBOUND_CHUNK_SIZE) {
    let splitAt = remaining.lastIndexOf('\n', OUTBOUND_CHUNK_SIZE);
    if (splitAt < OUTBOUND_CHUNK_SIZE / 2) splitAt = OUTBOUND_CHUNK_SIZE;
    if (
      /[\uD800-\uDBFF]/.test(remaining[splitAt - 1] ?? '') &&
      /[\uDC00-\uDFFF]/.test(remaining[splitAt] ?? '')
    ) {
      splitAt -= 1;
    }
    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt).replace(/^\n+/, '');
  }
  if (remaining) chunks.push(remaining);
  return chunks;
}

function normalizeFinalText(text: string): string {
  return text.trim() || '✅ 本轮已完成，没有文本输出。';
}

function codedError(code: string): Error {
  return new Error(`[${code}] ${code}`);
}

function toCodedError(error: unknown): Error {
  const code = safeErrorCode(error);
  return code === 'DINGTALK_DWS_CONNECTION_ERROR' ? codedError(DINGTALK_DWS_ERROR.streamFailed) : codedError(code);
}

function safeErrorCode(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  for (const code of Object.values(DINGTALK_DWS_ERROR)) {
    if (message.startsWith(`[${code}]`)) return code;
  }
  return 'DINGTALK_DWS_CONNECTION_ERROR';
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
