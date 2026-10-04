import fs from 'node:fs';
import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { DingTalkChannelIM, RichChannelIM } from '@cindy/im';
import { decodeDingTalkLaneUserId } from '@cindy/im';

import { captureImContext } from '../../../shared/imMessageSource';
import { createLogger } from '../../logger';
import type { ImChannelAdapter, ImOrchestratorConfig } from '../shared/types';
import { ownerScopedImUserDataPath } from '../ownerScopedStorage';
import {
  buildDingTalkGroupContextPrefix,
  buildDingTalkReplyContextBlock,
  DINGTALK_GROUP_CONTEXT_LIMIT,
} from './groupContext';
import { handleDingTalkTextInteraction } from './interaction';
import { createDingTalkTurnPermissionPolicy } from './permissionPolicy';
import { ui } from './uiText';

const log = createLogger('im:dingtalk');

function ensureWorkingDir(appKey: string): string {
  const dir = ownerScopedImUserDataPath('im-working-dir', dingtalkManagedWorkingDirName(appKey));
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

export function dingtalkManagedWorkingDirName(appKey: string): string {
  if (/^[A-Za-z0-9_-]{1,128}$/.test(appKey)) return `dingtalk-${appKey}`;
  const digest = createHash('sha256').update(appKey, 'utf8').digest('hex').slice(0, 24);
  return `dingtalk-external-${digest}`;
}

export function dingtalkSessionIdFor(appKey: string, userId: string): string {
  const encodedIdentity = Buffer.from(JSON.stringify([appKey, userId]), 'utf8').toString(
    'base64url',
  );
  return `dingtalk_${encodedIdentity}`;
}

function sanitizeSpeaker(value: string): string {
  return value
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f\u200b]/g, ' ')
    .trim()
    .slice(0, 64);
}

export function buildDingTalkAdapter(
  dingtalkIm: DingTalkChannelIM,
  config: ImOrchestratorConfig,
): ImChannelAdapter {
  return {
    channel: 'dingtalk',
    // The shared card-action subscription still expects the rich interface.
    // Normal turn output is discriminated below and never calls card methods.
    im: dingtalkIm as RichChannelIM,
    output: {
      kind: 'chunked-text',
      im: dingtalkIm,
      commitFinal: (output) => dingtalkIm.commitFinal(output),
    },
    config,
    ui,
    sessions: {
      source: 'dingtalk',
      sessionIdFor: dingtalkSessionIdFor,
      defaultTitle: (userId) =>
        decodeDingTalkLaneUserId(userId)
          ? `钉钉群聊 · ${userId.slice(-6)}`
          : `钉钉 · ${userId.slice(-6)}`,
      generatedTitlePrefix: '钉钉 · ',
      workspaceKind: 'dialogue',
      ensureWorkingDir,
      extraInsertColumns: (appKey, userId) => ({
        imBotContextId: appKey,
        imUserId: userId,
      }),
    },
    processingEmoji: '',
    silentQueue: true,
    buildVendorOptions: (userId) => ({ dingtalkChatId: userId, source: 'dingtalk' }),
    handleTextInteraction: (userId, request, options) =>
      handleDingTalkTextInteraction(dingtalkIm, userId, request, options),
    // 对齐 Telegram / 飞书的边界：主人私聊完全遵循 session.permissionMode，
    // 因而可以显式选择 bypassPermissions（完全访问）；群聊携带成员可控上下文，
    // 无论谁 @ bot 都附加强确认策略，危险操作仍须主人在群里确认。
    turnPermissionPolicyFor: (event) =>
      event.speaker ? createDingTalkTurnPermissionPolicy(event.messageId, event.speaker.isOwner) : undefined,
    prepareAgentTurnText: async (event) => {
      // 引用回复（仅「钉钉账号」方式会带 replyContext）：私聊与群聊都注入。
      const replyPrefix = event.replyContext
        ? buildDingTalkReplyContextBlock(event.replyContext)
        : '';
      if (!event.speaker) {
        if (!replyPrefix) return null;
        return {
          agentText: `${replyPrefix}${event.text}`,
          contextSnapshot: captureImContext({ replyPrefix, replyMessageCount: 1 }),
        };
      }
      const speaker = sanitizeSpeaker(event.speaker.name);
      const speakerLine = `[发言人] ${speaker} · id:${event.speaker.id}${event.speaker.isOwner ? ' · 主人' : ''}\n`;
      // 群上下文：仅「钉钉账号（dws）」方式能以账号身份读取群历史；机器人方式没有该能力。
      let groupPrefix = '';
      let groupMessageCount = 0;
      const lane = decodeDingTalkLaneUserId(event.senderId);
      if (lane && dingtalkIm.supportsGroupHistory()) {
        try {
          const history = await dingtalkIm.fetchRecentGroupMessages(
            lane.conversationId,
            DINGTALK_GROUP_CONTEXT_LIMIT,
          );
          const built = buildDingTalkGroupContextPrefix(history, event.messageId);
          if (built) {
            groupPrefix = built.prefix;
            groupMessageCount = built.messageCount;
          }
        } catch (error) {
          // 拉取失败不阻断本轮：按无群上下文继续。
          log.warn(
            `dingtalk group context fetch failed: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      if (!groupPrefix && !replyPrefix) {
        return { agentText: `${speakerLine}${event.text}` };
      }
      // 顺序：群上下文（较远背景）→ 引用块（直接相关）→ 发言人 → 正文。
      return {
        agentText: `${groupPrefix}${replyPrefix}${speakerLine}${event.text}`,
        contextSnapshot: captureImContext({
          ...(groupPrefix ? { groupPrefix, groupMessageCount } : {}),
          ...(replyPrefix ? { replyPrefix, replyMessageCount: 1 } : {}),
        }),
      };
    },
  };
}
