import { createHash } from 'crypto';

import { DelayRange, MessagePriority } from './outbound-queue.types';

export function randomDelay(range: DelayRange): number {
  if (range.min <= 0 && range.max <= 0) return 0;
  return Math.floor(Math.random() * (range.max - range.min + 1)) + range.min;
}

export function contentHash(message: any): string {
  let content = '';

  if (typeof message === 'string') {
    content = message;
  } else if (message?.conversation) {
    content = message.conversation;
  } else if (message?.extendedTextMessage?.text) {
    content = message.extendedTextMessage.text;
  } else {
    try {
      content = JSON.stringify(message);
    } catch {
      content = String(message);
    }
  }

  // Hash dos primeiros 200 chars
  const truncated = content.substring(0, 200);
  return createHash('sha256').update(truncated).digest('hex').substring(0, 16);
}

export function isTextMessage(message: any): boolean {
  if (!message) return false;
  return !!(message.conversation || message.extendedTextMessage?.text);
}

export function getTextContent(message: any): string | null {
  if (message?.conversation) return message.conversation;
  if (message?.extendedTextMessage?.text) return message.extendedTextMessage.text;
  return null;
}

export function formatETA(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}m${remainingSeconds.toString().padStart(2, '0')}s`;
}

export function generateId(): string {
  return `qm_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
}

export function detectPriority(message: any, isIntegration: boolean): MessagePriority {
  // Status/broadcast → always low
  if (message?.statusJidList || message?.allContacts) {
    return 'low';
  }

  // Human messages via API (not integration) → high
  if (!isIntegration) {
    return 'high';
  }

  // Integration (Chatwoot/bot) → medium by default
  return 'medium';
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
