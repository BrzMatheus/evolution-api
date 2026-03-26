import { QueueConfig } from './outbound-queue.types';

export const DEFAULT_QUEUE_CONFIG: QueueConfig = {
  enabled: false,

  delays: {
    normal: {
      high: { min: 4000, max: 10000 },
      medium: { min: 15000, max: 30000 },
      low: { min: 30000, max: 60000 },
    },
    congested: {
      high: { min: 2000, max: 6000 },
      medium: { min: 8000, max: 15000 },
      low: { min: 0, max: 0 }, // pausado
    },
    critical: {
      high: { min: 1000, max: 3000 },
      medium: { min: 0, max: 0 }, // pausado
      low: { min: 0, max: 0 }, // pausado
    },
  },

  sla: {
    high: 90_000, // 90s
    medium: 600_000, // 10min
    low: 1_800_000, // 30min
  },

  maxQueueSize: {
    high: 50,
    medium: 100,
    low: 50,
  },

  maxPendingPerConversation: 3,

  maxETAMs: 900_000, // 15min hard cap

  consolidation: {
    enabled: true,
    windowMs: 8000,
    separator: '\n',
    maxMessages: 5,
    mediaGroup: {
      enabled: true,
      windowMs: 8000,
      maxSize: 5,
      delayMs: 1500,
    },
  },

  perConversation: {
    minIntervalMs: 20000,
    lockAfterSendMs: 25000,
  },

  congestion: {
    warnThresholdMs: 600_000, // 10min
    criticalThresholdMs: 900_000, // 15min (alinhado com maxETAMs)
  },

  deduplication: {
    enabled: true,
    windowMs: 30000,
  },

  typing: {
    enabled: true,
    durationMs: { min: 2000, max: 5000 },
  },
};

export function mergeQueueConfig(overrides: Partial<QueueConfig>): QueueConfig {
  return {
    ...DEFAULT_QUEUE_CONFIG,
    ...overrides,
    delays: overrides.delays
      ? {
          normal: { ...DEFAULT_QUEUE_CONFIG.delays.normal, ...overrides.delays?.normal },
          congested: { ...DEFAULT_QUEUE_CONFIG.delays.congested, ...overrides.delays?.congested },
          critical: { ...DEFAULT_QUEUE_CONFIG.delays.critical, ...overrides.delays?.critical },
        }
      : DEFAULT_QUEUE_CONFIG.delays,
    sla: { ...DEFAULT_QUEUE_CONFIG.sla, ...overrides.sla },
    maxQueueSize: { ...DEFAULT_QUEUE_CONFIG.maxQueueSize, ...overrides.maxQueueSize },
    consolidation: overrides.consolidation
      ? {
          ...DEFAULT_QUEUE_CONFIG.consolidation,
          ...overrides.consolidation,
          mediaGroup: {
            ...DEFAULT_QUEUE_CONFIG.consolidation.mediaGroup,
            ...overrides.consolidation?.mediaGroup,
          },
        }
      : DEFAULT_QUEUE_CONFIG.consolidation,
    perConversation: { ...DEFAULT_QUEUE_CONFIG.perConversation, ...overrides.perConversation },
    congestion: { ...DEFAULT_QUEUE_CONFIG.congestion, ...overrides.congestion },
    deduplication: { ...DEFAULT_QUEUE_CONFIG.deduplication, ...overrides.deduplication },
    typing: { ...DEFAULT_QUEUE_CONFIG.typing, ...overrides.typing },
  };
}
