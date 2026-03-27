import {
  CongestionMode,
  DroppedResult,
  EnqueueParams,
  MessagePriority,
  QueueConfig,
  QueuedMessage,
  QueueManagerOptions,
  QueueMetrics,
} from './outbound-queue.types';
import {
  contentHash,
  formatETA,
  generateId,
  getMediaType,
  getTextContent,
  isTextMessage,
  isTransientError,
  randomDelay,
  sleep,
} from './outbound-queue.utils';

const PRIORITIES: MessagePriority[] = ['high', 'medium', 'low'];
const ANTI_STARVATION_RATIO = 5; // a cada 5 high, permite 1 medium em critical
const METRICS_LOG_INTERVAL = 10; // loga métricas a cada N mensagens processadas

export class OutboundQueueManager {
  // Per-conversation sub-queues
  private queues: Map<string, QueuedMessage[]> = new Map();
  // Conversation locks: jid → timestamp do último envio
  private conversationLocks: Map<string, number> = new Map();
  // Dedup: hash+jid → timestamp
  private recentHashes: Map<string, number> = new Map();
  // Round-robin index per priority
  private roundRobinIndex: Map<MessagePriority, number> = new Map([
    ['high', 0],
    ['medium', 0],
    ['low', 0],
  ]);

  private congestionMode: CongestionMode = 'normal';
  private workerRunning = false;
  private workerTimer: ReturnType<typeof setTimeout> | null = null;
  private draining = false;
  private highSentSinceMedium = 0;

  // Drop timestamps for droppedLast5min calculation
  private dropTimestamps: number[] = [];

  private metrics: QueueMetrics = {
    queueSize: 0,
    queueSizeByPriority: { high: 0, medium: 0, low: 0 },
    etaMs: 0,
    etaFormatted: '0s',
    etaByPriority: { high: 0, medium: 0, low: 0 },
    congestionMode: 'normal',
    droppedCount: 0,
    droppedLast5min: 0,
    sentCount: 0,
    sentDelayAvgMs: 0,
    sentDelayAvgByPriority: { high: 0, medium: 0, low: 0 },
    promotedCount: 0,
    consolidatedCount: 0,
    mediaGroupedCount: 0,
    modeChanges: 0,
  };

  private totalSentDelay = 0;
  private totalSentDelayByPriority: Record<string, number> = { high: 0, medium: 0, low: 0 };
  private sentCountByPriority: Record<string, number> = { high: 0, medium: 0, low: 0 };
  private processedSinceLastLog = 0;

  private readonly instanceId: string;
  private config: QueueConfig;
  private readonly sendFn: (sender: string, message: any, options: any, isIntegration: boolean) => Promise<any>;
  private readonly clientFn: () => any;
  private readonly logger: any;

  constructor(options: QueueManagerOptions) {
    this.instanceId = options.instanceId;
    this.config = options.config;
    this.sendFn = options.sendFn;
    this.clientFn = options.clientFn;
    this.logger = options.logger;
  }

  isEnabled(): boolean {
    return this.config.enabled;
  }

  getConfig(): QueueConfig {
    return { ...this.config };
  }

  updateConfig(partial: Partial<QueueConfig>): void {
    this.config = {
      ...this.config,
      ...partial,
      delays: partial.delays
        ? {
            normal: { ...this.config.delays.normal, ...partial.delays?.normal },
            congested: { ...this.config.delays.congested, ...partial.delays?.congested },
            critical: { ...this.config.delays.critical, ...partial.delays?.critical },
          }
        : this.config.delays,
      sla: { ...this.config.sla, ...partial.sla },
      maxQueueSize: { ...this.config.maxQueueSize, ...partial.maxQueueSize },
      consolidation: { ...this.config.consolidation, ...partial.consolidation },
      perConversation: { ...this.config.perConversation, ...partial.perConversation },
      congestion: { ...this.config.congestion, ...partial.congestion },
      deduplication: { ...this.config.deduplication, ...partial.deduplication },
      typing: { ...this.config.typing, ...partial.typing },
    };
    this.logger.info(`[Queue/${this.instanceId}] Config updated`);
    this.updateCongestionMode();
  }

  getMetrics(): QueueMetrics {
    this.refreshMetrics();
    return { ...this.metrics };
  }

  getCongestionMode(): CongestionMode {
    return this.congestionMode;
  }

  // ─── ENQUEUE ────────────────────────────────────────────────

  async enqueue(params: EnqueueParams): Promise<any> {
    const { conversationJid, priority } = params;

    // 1. Backpressure checks
    const backpressureResult = this.checkBackpressure(params);
    if (backpressureResult) return backpressureResult;

    // 2. Deduplication
    const dedupResult = this.checkDeduplication(params);
    if (dedupResult) return dedupResult;

    // 3. Create queued message wrapped in a Promise
    return new Promise<any>((resolve, reject) => {
      const msg: QueuedMessage = {
        id: generateId(),
        instanceId: params.instanceId,
        conversationJid,
        priority,
        enqueuedAt: Date.now(),
        deadlineAt: Date.now() + this.config.sla[priority],
        contentHash: contentHash(params.message),
        isIntegration: params.isIntegration,
        sender: params.sender,
        message: params.message,
        options: params.options,
        mentions: params.mentions,
        linkPreview: params.linkPreview,
        quoted: params.quoted,
        messageId: params.messageId,
        ephemeralExpiration: params.ephemeralExpiration,
        contextInfo: params.contextInfo,
        resolve,
        reject,
      };

      // 4. Consolidation check
      if (this.tryConsolidate(msg)) {
        return; // consolidated into existing message, promise will resolve when that one sends
      }

      // 5. Tag media group (before insert, so the queue already has the lead)
      this.tryGroupMedia(msg);

      // 6. Insert into queue
      this.insertMessage(msg);

      // 6. Store hash for dedup
      if (this.config.deduplication.enabled) {
        const hashKey = `${msg.contentHash}:${conversationJid}`;
        this.recentHashes.set(hashKey, Date.now());
      }

      // 7. Update congestion
      this.updateCongestionMode();

      // 8. Start worker if not running
      this.ensureWorkerRunning();
    });
  }

  // ─── BACKPRESSURE ───────────────────────────────────────────

  private checkBackpressure(params: EnqueueParams): DroppedResult | null {
    const { priority, conversationJid } = params;

    // a) maxQueueSize per priority
    const currentCount = this.countByPriority(priority);
    if (currentCount >= this.config.maxQueueSize[priority]) {
      if (priority === 'high') {
        const hardCap = this.config.maxQueueSize[priority] * 4;
        if (currentCount >= hardCap) {
          return this.createDroppedResult(params, 'queue_congestion');
        }
      } else if (priority === 'medium' && this.congestionMode === 'critical') {
        return this.createDroppedResult(params, 'queue_congestion');
      } else if (priority === 'low' && this.congestionMode !== 'normal') {
        return this.createDroppedResult(params, 'queue_congestion');
      }
    }

    // b) maxPendingPerConversation
    const conversationQueue = this.queues.get(conversationJid) || [];
    if (conversationQueue.length >= this.config.maxPendingPerConversation) {
      if (priority === 'low') {
        return this.createDroppedResult(params, 'queue_congestion');
      }
      // medium/high: still accept but will attempt consolidation in enqueue
    }

    // c) Hard cap ETA
    const eta = this.getETA();
    if (eta > this.config.maxETAMs) {
      if (this.congestionMode !== 'critical') {
        this.congestionMode = 'critical';
        this.onCongestionChange('normal', 'critical');
      }
      if (priority === 'low') {
        return this.createDroppedResult(params, 'queue_congestion');
      }
      if (priority === 'medium') {
        return this.createDroppedResult(params, 'queue_congestion');
      }
    }

    return null;
  }

  private createDroppedResult(params: EnqueueParams, reason: DroppedResult['reason']): DroppedResult {
    const result: DroppedResult = {
      status: 'dropped',
      reason,
      originalId: generateId(),
      queueETA: this.getETA(),
    };
    this.recordDrop();
    this.logger.warn(
      `[Queue] Dropped ${params.priority} message for ${params.conversationJid}: ${reason} (ETA: ${formatETA(result.queueETA)})`,
    );
    return result;
  }

  // ─── DEDUPLICATION ──────────────────────────────────────────

  private checkDeduplication(params: EnqueueParams): DroppedResult | null {
    if (!this.config.deduplication.enabled) return null;

    const hash = contentHash(params.message);
    const hashKey = `${hash}:${params.conversationJid}`;
    const lastSeen = this.recentHashes.get(hashKey);

    if (lastSeen && Date.now() - lastSeen < this.config.deduplication.windowMs) {
      this.logger.verbose(`[Queue] Deduplicated message for ${params.conversationJid}`);
      return {
        status: 'dropped',
        reason: 'deduplicated',
        originalId: generateId(),
        queueETA: this.getETA(),
      };
    }

    return null;
  }

  // ─── CONSOLIDATION ─────────────────────────────────────────

  private tryConsolidate(msg: QueuedMessage): boolean {
    if (!this.config.consolidation.enabled) return false;
    if (!isTextMessage(msg.message)) return false;
    if (msg.quoted) return false;

    const conversationQueue = this.queues.get(msg.conversationJid);
    if (!conversationQueue || conversationQueue.length === 0) return false;

    // Find the last pending text message for same conversation & priority
    const target = [...conversationQueue]
      .reverse()
      .find(
        (m) =>
          m.priority === msg.priority &&
          isTextMessage(m.message) &&
          !m.quoted &&
          (m.consolidatedWith?.length || 0) < this.config.consolidation.maxMessages - 1 &&
          Date.now() - m.enqueuedAt < this.config.consolidation.windowMs,
      );

    if (!target) return false;

    const targetText = getTextContent(target.message);
    const newText = getTextContent(msg.message);
    if (!targetText || !newText) return false;

    // Merge text
    const mergedText = targetText + this.config.consolidation.separator + newText;
    if (target.message.conversation) {
      target.message.conversation = mergedText;
    } else if (target.message.extendedTextMessage?.text) {
      target.message.extendedTextMessage.text = mergedText;
    }

    // Track consolidation
    if (!target.consolidatedWith) target.consolidatedWith = [];
    target.consolidatedWith.push(msg.id);

    // Update hash
    target.contentHash = contentHash(target.message);

    // Chain the new message's promise to the target (defensive: isolate each callback)
    const originalResolve = target.resolve;
    target.resolve = (value: any) => {
      try {
        originalResolve(value);
      } catch {
        void 0;
      }
      try {
        msg.resolve(value);
      } catch {
        void 0;
      }
    };
    const originalReject = target.reject;
    target.reject = (error: Error) => {
      try {
        originalReject(error);
      } catch {
        void 0;
      }
      try {
        msg.reject(error);
      } catch {
        void 0;
      }
    };

    this.metrics.consolidatedCount++;
    this.logger.verbose(
      `[Queue] Consolidated message into ${target.id} for ${msg.conversationJid} (${(target.consolidatedWith?.length || 0) + 1} messages)`,
    );
    return true;
  }

  private tryGroupMedia(msg: QueuedMessage): void {
    const cfg = this.config.consolidation.mediaGroup;
    if (!cfg.enabled) return;

    const mediaType = getMediaType(msg.message);
    if (!mediaType) return;

    const conversationQueue = this.queues.get(msg.conversationJid);
    if (!conversationQueue || conversationQueue.length === 0) return;

    // Encontrar líder: último media do mesmo tipo na mesma conversa dentro da janela
    const lead = [...conversationQueue]
      .reverse()
      .find(
        (m) =>
          m.priority === msg.priority &&
          getMediaType(m.message) === mediaType &&
          !m.quoted &&
          (m.consolidatedWith?.length || 0) < cfg.maxSize - 1 &&
          Date.now() - m.enqueuedAt < cfg.windowMs,
      );

    if (!lead) return;

    // Marcar líder se ainda não marcado
    if (!lead.mediaGroupId) {
      lead.mediaGroupId = lead.id;
      lead.isMediaGroupLead = true;
    }

    // Marcar nova mensagem como membro do grupo
    msg.mediaGroupId = lead.mediaGroupId;
    msg.isMediaGroupLead = false;

    // Usar consolidatedWith apenas como contador de membros (sem merge de conteúdo)
    if (!lead.consolidatedWith) lead.consolidatedWith = [];
    lead.consolidatedWith.push(msg.id);

    this.metrics.mediaGroupedCount++;
    this.logger.verbose(
      `[Queue] Media group: added ${msg.id} to group ${lead.mediaGroupId} for ${msg.conversationJid} (${(lead.consolidatedWith?.length || 0) + 1} items)`,
    );
  }

  // ─── QUEUE MANAGEMENT ──────────────────────────────────────

  private insertMessage(msg: QueuedMessage): void {
    const jid = msg.conversationJid;
    if (!this.queues.has(jid)) {
      this.queues.set(jid, []);
    }
    this.queues.get(jid)!.push(msg);
  }

  private removeMessage(msg: QueuedMessage): void {
    const jid = msg.conversationJid;
    const queue = this.queues.get(jid);
    if (!queue) return;

    const idx = queue.indexOf(msg);
    if (idx !== -1) queue.splice(idx, 1);
    if (queue.length === 0) this.queues.delete(jid);
  }

  private countByPriority(priority: MessagePriority): number {
    let count = 0;
    for (const msgs of this.queues.values()) {
      for (const m of msgs) {
        if (m.priority === priority && !m.paused) count++;
      }
    }
    return count;
  }

  private countActive(priority: MessagePriority): number {
    return this.countByPriority(priority);
  }

  private getTotalPending(): number {
    let total = 0;
    for (const msgs of this.queues.values()) {
      total += msgs.length;
    }
    return total;
  }

  private getAllConversationJids(): string[] {
    return Array.from(this.queues.keys());
  }

  // ─── WORKER ─────────────────────────────────────────────────

  private ensureWorkerRunning(): void {
    if (this.workerRunning) return;
    this.workerRunning = true;
    this.processLoop();
  }

  private async processLoop(): Promise<void> {
    try {
      while (this.workerRunning) {
        const msg = this.selectNext();
        if (!msg) {
          if (this.getTotalPending() > 0) {
            // Mensagens existem mas temporariamente inacessíveis (conversa locked ou pausadas).
            // Atualizar modo de congestionamento pode resumir pausadas; o sleep aguarda o lock expirar.
            this.updateCongestionMode();
            await sleep(2000);
            continue;
          }
          this.workerRunning = false;
          break;
        }

        try {
          await this.processMessage(msg);
        } catch (error) {
          this.logger.error(`[Queue] Error processing message ${msg.id}: ${error?.message}`);
          try {
            msg.reject(error instanceof Error ? error : new Error(String(error)));
          } catch {
            // reject callback itself failed — nothing more to do
          }
          this.removeMessage(msg);
        }

        this.processedSinceLastLog++;
        if (this.processedSinceLastLog >= METRICS_LOG_INTERVAL) {
          this.logMetrics();
          this.processedSinceLastLog = 0;
        }

        // Recalculate congestion after each send
        this.updateCongestionMode();

        // If queue is empty, stop
        if (this.getTotalPending() === 0) {
          this.workerRunning = false;
          break;
        }
      }
    } catch (error) {
      this.logger.error(
        `[Queue/${this.instanceId}] Worker loop crashed unexpectedly: ${error instanceof Error ? error.message : String(error)}`,
      );
    } finally {
      this.workerRunning = false;
      // Safety net: restart worker if messages remain (e.g. after unexpected crash)
      if (this.getTotalPending() > 0 && !this.draining) {
        this.logger.warn(
          `[Queue/${this.instanceId}] Worker restarting after exit (${this.getTotalPending()} messages pending)`,
        );
        this.ensureWorkerRunning();
      }
    }
  }

  private async processMessage(msg: QueuedMessage): Promise<void> {
    // Check deadline - promote or fast-track
    this.checkDeadline(msg);

    // Membros de grupo de mídia (não-líderes) usam delay pequeno sem typing
    const isMediaGroupMember = msg.mediaGroupId && !msg.isMediaGroupLead;

    // Detectar se a conversa está "quente" (envio recente para o mesmo JID)
    const lockUntil = this.conversationLocks.get(msg.conversationJid);
    const isWarmConversation =
      lockUntil !== undefined && Date.now() - lockUntil < this.config.perConversation.warmWindowMs;

    if (isMediaGroupMember) {
      const groupDelayMs = this.config.consolidation.mediaGroup.delayMs;
      if (groupDelayMs > 0) await sleep(groupDelayMs);
    } else {
      // Conversa quente → delay curto de follow-up; fria → delay normal por prioridade
      const delayRange = isWarmConversation
        ? this.config.perConversation.warmDelayMs
        : this.config.delays[this.congestionMode][msg.priority];
      const delayMs = this.draining ? Math.min(randomDelay(delayRange), 1000) : randomDelay(delayRange);

      // Typing presence
      if (this.config.typing.enabled && delayMs > 0) {
        await this.sendTyping(msg.sender, delayMs);
      } else if (delayMs > 0) {
        await sleep(delayMs);
      }
    }

    // Execute actual send (with retry for transient errors)
    const result = await this.executeSendWithRetry(msg);

    // Resolve promise
    msg.resolve(result);
    this.removeMessage(msg);

    // Update conversation lock.
    // Para membros de grupo de mídia, ajustar o lock para que expire após groupDelayMs.
    // A duração efetiva do lock depende do "calor" da conversa.
    if (isMediaGroupMember) {
      const groupDelayMs = this.config.consolidation.mediaGroup.delayMs;
      const effectiveLockMs = isWarmConversation
        ? this.config.perConversation.warmLockAfterSendMs
        : this.config.perConversation.lockAfterSendMs;
      const lockOffset = effectiveLockMs - groupDelayMs;
      this.conversationLocks.set(msg.conversationJid, Date.now() - Math.max(0, lockOffset));
    } else {
      this.conversationLocks.set(msg.conversationJid, Date.now());
    }

    // Track metrics
    this.metrics.sentCount++;
    const actualDelay = Date.now() - msg.enqueuedAt;
    this.totalSentDelay += actualDelay;
    this.metrics.sentDelayAvgMs = this.totalSentDelay / this.metrics.sentCount;
    this.totalSentDelayByPriority[msg.priority] += actualDelay;
    this.sentCountByPriority[msg.priority]++;
    this.metrics.sentDelayAvgByPriority[msg.priority] =
      this.totalSentDelayByPriority[msg.priority] / this.sentCountByPriority[msg.priority];

    // Anti-starvation tracking
    if (msg.priority === 'high' && this.congestionMode === 'critical') {
      this.highSentSinceMedium++;
    } else if (msg.priority === 'medium') {
      this.highSentSinceMedium = 0;
    }
  }

  private async executeSendWithRetry(msg: QueuedMessage): Promise<any> {
    const MAX_ATTEMPTS = 3;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        return await this.sendFn(msg.sender, msg.message, msg.options, msg.isIntegration);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        if (attempt < MAX_ATTEMPTS && isTransientError(err)) {
          const delayMs = 3000 * attempt; // 3s, 6s
          this.logger.warn(
            `[Queue/${this.instanceId}] Send failed for ${msg.id} (attempt ${attempt}/${MAX_ATTEMPTS}): ${err.message}. Retrying in ${delayMs / 1000}s...`,
          );
          await sleep(delayMs);
        } else {
          if (attempt > 1) {
            this.logger.error(
              `[Queue/${this.instanceId}] All ${MAX_ATTEMPTS} attempts failed for ${msg.id}: ${err.message}`,
            );
          }
          throw err;
        }
      }
    }
  }

  private selectNext(): QueuedMessage | null {
    const now = Date.now();

    // Clean expired hashes periodically
    this.cleanExpiredHashes();

    // In critical: anti-starvation check - allow medium every N high
    if (this.congestionMode === 'critical' && this.highSentSinceMedium >= ANTI_STARVATION_RATIO) {
      const medium = this.selectByPriority('medium', now);
      if (medium) return medium;
      // No medium available, continue with high
    }

    // Select by priority order
    for (const priority of PRIORITIES) {
      // Skip paused priorities in congestion modes
      if (this.congestionMode === 'congested' && priority === 'low') continue;
      if (this.congestionMode === 'critical' && priority !== 'high') continue;

      const msg = this.selectByPriority(priority, now);
      if (msg) return msg;
    }

    return null;
  }

  private selectByPriority(priority: MessagePriority, now: number): QueuedMessage | null {
    const jids = this.getAllConversationJids();
    if (jids.length === 0) return null;

    // Round-robin across conversations for fairness
    const startIdx = this.roundRobinIndex.get(priority) || 0;
    const len = jids.length;

    for (let i = 0; i < len; i++) {
      const idx = (startIdx + i) % len;
      const jid = jids[idx];

      // Check conversation lock (warm conversations use shorter lock)
      const lockUntil = this.conversationLocks.get(jid);
      if (lockUntil) {
        const timeSince = now - lockUntil;
        const isWarm = timeSince < this.config.perConversation.warmWindowMs;
        const effectiveLockMs = isWarm
          ? this.config.perConversation.warmLockAfterSendMs
          : this.config.perConversation.lockAfterSendMs;
        if (timeSince < effectiveLockMs) continue;
      }

      const queue = this.queues.get(jid);
      if (!queue) continue;

      const msg = queue.find((m) => m.priority === priority && !m.paused);
      if (msg) {
        this.roundRobinIndex.set(priority, (idx + 1) % len);
        return msg;
      }
    }

    return null;
  }

  // ─── DEADLINE & PRIORITY PROMOTION ─────────────────────────

  private checkDeadline(msg: QueuedMessage): void {
    const now = Date.now();
    const timeLeft = msg.deadlineAt - now;
    const totalSla = this.config.sla[msg.priority];

    // If less than 20% SLA remaining, promote priority
    if (timeLeft < totalSla * 0.2 && timeLeft > 0) {
      if (msg.priority === 'low') {
        msg.priority = 'medium';
        msg.deadlineAt = now + this.config.sla.medium;
        this.metrics.promotedCount++;
        this.logger.verbose(`[Queue] Promoted ${msg.id} from low → medium (deadline approaching)`);
      } else if (msg.priority === 'medium') {
        msg.priority = 'high';
        msg.deadlineAt = now + this.config.sla.high;
        this.metrics.promotedCount++;
        this.logger.verbose(`[Queue] Promoted ${msg.id} from medium → high (deadline approaching)`);
      }
    }
  }

  // ─── TYPING PRESENCE ───────────────────────────────────────

  private async sendTyping(sender: string, durationMs: number): Promise<void> {
    try {
      const client = this.clientFn();
      if (!client) {
        await sleep(durationMs);
        return;
      }

      const typingDuration = Math.min(durationMs, randomDelay(this.config.typing.durationMs));

      // For long delays, chunk into 20s segments like the original code
      if (typingDuration > 20000) {
        let remaining = typingDuration;
        while (remaining > 20000) {
          await client.presenceSubscribe(sender);
          await client.sendPresenceUpdate('composing', sender);
          await sleep(20000);
          await client.sendPresenceUpdate('paused', sender);
          remaining -= 20000;
        }
        if (remaining > 0) {
          await client.presenceSubscribe(sender);
          await client.sendPresenceUpdate('composing', sender);
          await sleep(remaining);
          await client.sendPresenceUpdate('paused', sender);
        }
      } else if (typingDuration > 0) {
        await client.presenceSubscribe(sender);
        await client.sendPresenceUpdate('composing', sender);
        await sleep(typingDuration);
        await client.sendPresenceUpdate('paused', sender);
      }

      // If there's remaining delay after typing, just sleep
      const remainingDelay = durationMs - typingDuration;
      if (remainingDelay > 0) {
        await sleep(remainingDelay);
      }
    } catch (error) {
      // Typing failures are non-critical, just sleep the delay
      const errMsg = error instanceof Error ? error.message : String(error);
      this.logger.verbose(`[Queue] Typing presence failed for ${sender}: ${errMsg}, falling back to sleep`);
      await sleep(durationMs);
    }
  }

  // ─── CONGESTION ─────────────────────────────────────────────

  getETA(): number {
    const delays = this.config.delays[this.congestionMode];

    const highCount = this.countActive('high');
    const mediumCount = this.countActive('medium');

    const avgDelayHigh = (delays.high.min + delays.high.max) / 2;
    const avgDelayMedium = (delays.medium.min + delays.medium.max) / 2;

    return highCount * avgDelayHigh + mediumCount * avgDelayMedium;
  }

  getETAByPriority(): Record<MessagePriority, number> {
    const delays = this.config.delays[this.congestionMode];

    return {
      high: this.countActive('high') * ((delays.high.min + delays.high.max) / 2),
      medium: this.countActive('medium') * ((delays.medium.min + delays.medium.max) / 2),
      low: this.countActive('low') * ((delays.low.min + delays.low.max) / 2),
    };
  }

  private updateCongestionMode(): void {
    const eta = this.getETA();
    const prev = this.congestionMode;

    if (eta > this.config.maxETAMs) {
      this.congestionMode = 'critical';
    } else if (eta >= this.config.congestion.criticalThresholdMs) {
      this.congestionMode = 'critical';
    } else if (eta >= this.config.congestion.warnThresholdMs) {
      this.congestionMode = 'congested';
    } else {
      this.congestionMode = 'normal';
    }

    if (this.congestionMode !== prev) {
      this.onCongestionChange(prev, this.congestionMode);
    }
  }

  private onCongestionChange(prev: CongestionMode, next: CongestionMode): void {
    this.metrics.modeChanges++;

    const eta = this.getETA();

    if (next === 'congested') {
      this.pauseByPriority('low');
      this.logger.warn(`[Queue/${this.instanceId}] Mode: ${prev} → CONGESTED | ETA: ${formatETA(eta)}`);
    }

    if (next === 'critical') {
      this.pauseByPriority('low');
      this.pauseByPriority('medium');
      this.dropExcessMessages();
      this.logger.error(`[Queue/${this.instanceId}] Mode: ${prev} → CRITICAL | ETA: ${formatETA(eta)}`);
    }

    if (next === 'normal' && prev !== 'normal') {
      this.resumeAll();
      this.logger.info(`[Queue/${this.instanceId}] Mode: ${prev} → NORMAL | ETA: ${formatETA(eta)}`);
    }

    this.logMetrics();
  }

  private pauseByPriority(priority: MessagePriority): void {
    for (const msgs of this.queues.values()) {
      for (const m of msgs) {
        if (m.priority === priority) m.paused = true;
      }
    }
  }

  private resumeAll(): void {
    for (const msgs of this.queues.values()) {
      for (const m of msgs) {
        m.paused = false;
      }
    }
  }

  private dropExcessMessages(): void {
    const now = Date.now();
    const toDrop: QueuedMessage[] = [];

    for (const [, msgs] of this.queues) {
      for (const m of msgs) {
        if (m.priority === 'high') continue;
        if (m.deadlineAt < now) {
          toDrop.push(m);
        } else if (m.priority === 'low') {
          toDrop.push(m);
        }
      }
    }

    for (const msg of toDrop) {
      msg.resolve({
        status: 'dropped',
        reason: msg.deadlineAt < now ? 'sla_expired' : 'queue_congestion',
        originalId: msg.id,
        queueETA: this.getETA(),
      } as DroppedResult);
      this.removeMessage(msg);
      this.recordDrop();
    }

    if (toDrop.length > 0) {
      this.logger.warn(`[Queue/${this.instanceId}] Dropped ${toDrop.length} excess messages in critical mode`);
    }
  }

  // ─── METRICS ────────────────────────────────────────────────

  private refreshMetrics(): void {
    this.metrics.queueSize = this.getTotalPending();
    this.metrics.queueSizeByPriority = {
      high: this.countByPriority('high'),
      medium: this.countByPriority('medium'),
      low: this.countByPriority('low'),
    };
    this.metrics.etaMs = this.getETA();
    this.metrics.etaFormatted = formatETA(this.metrics.etaMs);
    this.metrics.etaByPriority = this.getETAByPriority();
    this.metrics.congestionMode = this.congestionMode;

    // Clean old drop timestamps and count last 5min
    const fiveMinAgo = Date.now() - 300_000;
    this.dropTimestamps = this.dropTimestamps.filter((t) => t > fiveMinAgo);
    this.metrics.droppedLast5min = this.dropTimestamps.length;
  }

  private recordDrop(): void {
    this.metrics.droppedCount++;
    this.dropTimestamps.push(Date.now());
  }

  private logMetrics(): void {
    this.refreshMetrics();
    const m = this.metrics;
    this.logger.info(
      `[Queue/${this.instanceId}] mode=${m.congestionMode} | pending=${m.queueSize} (H:${m.queueSizeByPriority.high} M:${m.queueSizeByPriority.medium} L:${m.queueSizeByPriority.low}) | ETA=${m.etaFormatted} | dropped=${m.droppedCount} (5m:${m.droppedLast5min}) | sent=${m.sentCount} | avg_delay=${Math.round(m.sentDelayAvgMs / 1000)}s (H:${Math.round(m.sentDelayAvgByPriority.high / 1000)}s M:${Math.round(m.sentDelayAvgByPriority.medium / 1000)}s L:${Math.round(m.sentDelayAvgByPriority.low / 1000)}s) | grouped=${m.mediaGroupedCount}`,
    );
  }

  // ─── CLEANUP ────────────────────────────────────────────────

  private cleanExpiredHashes(): void {
    const now = Date.now();
    const windowMs = this.config.deduplication.windowMs;
    for (const [key, ts] of this.recentHashes) {
      if (now - ts > windowMs) {
        this.recentHashes.delete(key);
      }
    }
    this.cleanExpiredLocks(now);
  }

  private cleanExpiredLocks(now: number): void {
    const maxAge = this.config.perConversation.lockAfterSendMs * 2;
    for (const [jid, ts] of this.conversationLocks) {
      if (now - ts > maxAge) {
        this.conversationLocks.delete(jid);
      }
    }
  }

  // ─── DRAIN (graceful shutdown) ──────────────────────────────

  async drain(): Promise<void> {
    this.draining = true;
    this.logger.info(`[Queue/${this.instanceId}] Draining queue (${this.getTotalPending()} messages remaining)`);

    // Ensure worker is active so pending messages get processed during drain
    this.ensureWorkerRunning();

    // Process remaining with minimal delays
    const timeout = setTimeout(() => {
      // After 30s force-resolve remaining
      this.forceResolveAll();
    }, 30000);

    // Wait for worker to finish
    while (this.workerRunning && this.getTotalPending() > 0) {
      await sleep(500);
    }

    clearTimeout(timeout);
    this.draining = false;
    this.logger.info(`[Queue/${this.instanceId}] Queue drained`);
  }

  private forceResolveAll(): void {
    const pending = this.getTotalPending();
    if (pending > 0) {
      this.refreshMetrics();
      this.logger.warn(
        `[Queue/${this.instanceId}] Force-resolving ${pending} messages on shutdown | sent=${this.metrics.sentCount} dropped=${this.metrics.droppedCount}`,
      );
    }

    for (const [, msgs] of this.queues) {
      for (const msg of [...msgs]) {
        msg.resolve({
          status: 'dropped',
          reason: 'queue_congestion',
          originalId: msg.id,
          queueETA: 0,
        } as DroppedResult);
      }
    }
    this.queues.clear();
    this.workerRunning = false;
  }

  destroy(): void {
    this.workerRunning = false;
    if (this.workerTimer) {
      clearTimeout(this.workerTimer);
      this.workerTimer = null;
    }
    this.forceResolveAll();
  }
}
