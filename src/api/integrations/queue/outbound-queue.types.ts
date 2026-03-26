export type MessagePriority = 'high' | 'medium' | 'low';

export type CongestionMode = 'normal' | 'congested' | 'critical';

export interface DelayRange {
  min: number;
  max: number;
}

export interface QueueConfig {
  enabled: boolean;

  delays: {
    normal: {
      high: DelayRange;
      medium: DelayRange;
      low: DelayRange;
    };
    congested: {
      high: DelayRange;
      medium: DelayRange;
      low: DelayRange;
    };
    critical: {
      high: DelayRange;
      medium: DelayRange;
      low: DelayRange;
    };
  };

  sla: {
    high: number;
    medium: number;
    low: number;
  };

  maxQueueSize: {
    high: number;
    medium: number;
    low: number;
  };

  maxPendingPerConversation: number;

  maxETAMs: number;

  consolidation: {
    enabled: boolean;
    windowMs: number;
    separator: string;
    maxMessages: number;
  };

  perConversation: {
    minIntervalMs: number;
    lockAfterSendMs: number;
  };

  congestion: {
    warnThresholdMs: number;
    criticalThresholdMs: number;
  };

  deduplication: {
    enabled: boolean;
    windowMs: number;
  };

  typing: {
    enabled: boolean;
    durationMs: DelayRange;
  };
}

export interface QueuedMessage {
  id: string;
  instanceId: string;
  conversationJid: string;
  priority: MessagePriority;
  enqueuedAt: number;
  deadlineAt: number;
  contentHash: string;
  isIntegration: boolean;

  // Payload for sending
  sender: string;
  message: any;
  options: any;
  mentions: string[] | undefined;
  linkPreview: any;
  quoted: any;
  messageId?: string;
  ephemeralExpiration?: number;
  contextInfo?: any;

  // Internal control
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  consolidatedWith?: string[];
  paused?: boolean;
}

export interface EnqueueParams {
  instanceId: string;
  conversationJid: string;
  priority: MessagePriority;
  isIntegration: boolean;
  sender: string;
  message: any;
  options: any;
  mentions?: string[];
  linkPreview?: any;
  quoted?: any;
  messageId?: string;
  ephemeralExpiration?: number;
  contextInfo?: any;
}

export interface DroppedResult {
  status: 'dropped';
  reason: 'queue_congestion' | 'sla_expired' | 'deduplicated' | 'consolidated';
  originalId: string;
  queueETA: number;
}

export interface QueueMetrics {
  queueSize: number;
  queueSizeByPriority: Record<MessagePriority, number>;
  etaMs: number;
  etaByPriority: Record<MessagePriority, number>;
  congestionMode: CongestionMode;
  droppedCount: number;
  droppedLast5min: number;
  sentCount: number;
  sentDelayAvgMs: number;
  promotedCount: number;
  consolidatedCount: number;
  modeChanges: number;
}

export interface QueueManagerOptions {
  instanceId: string;
  config: QueueConfig;
  sendFn: (sender: string, message: any, options: any, isIntegration: boolean) => Promise<any>;
  clientFn: () => {
    presenceSubscribe: (jid: string) => Promise<void>;
    sendPresenceUpdate: (type: string, jid: string) => Promise<void>;
  } | null;
  logger: any;
}
