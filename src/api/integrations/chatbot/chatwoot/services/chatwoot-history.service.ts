import { InstanceDto } from '@api/dto/instance.dto';
import {
  ChatwootHistoryAnalyzeDto,
  ChatwootHistoryContactActionDto,
  ChatwootHistoryExecuteDto,
  ChatwootHistoryReprocessDto,
} from '@api/integrations/chatbot/chatwoot/dto/chatwoot-history.dto';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import {
  CanonicalIdentityType,
  classifyChatwootHistoryContact,
  ContactClassification,
  IdentityResolutionStatus,
  SuggestedAction,
  UnsafeReason,
} from '@api/integrations/chatbot/chatwoot/services/chatwoot-history-classifier.service';
import { chatwootImport, FksChatwoot } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import { PrismaRepository } from '@api/repository/repository.service';
import { Logger } from '@config/logger.config';
import { BadRequestException, NotFoundException } from '@exceptions';
import { Chatwoot as ChatwootModel, Message as MessageModel, Prisma } from '@prisma/client';
import { getJidAliases, resolveCanonicalJid } from '@utils/whatsapp-jid';

type JobMode = 'dryRun' | 'importDirect' | 'rebuild';
type JobStatus = 'pending' | 'analyzing' | 'awaiting_execution' | 'running' | 'completed' | 'failed' | 'partial';
type SelectedAction = SuggestedAction;
type ExecutionStatus = 'pending' | 'completed' | 'failed' | 'skipped';

type InboxValidation = {
  code: string;
  label: string;
  ok: boolean;
  details?: string;
};

type DependencyNotice = {
  code: string;
  level: 'info' | 'warning';
  message: string;
};

type ExecutorDescriptor = {
  kind: 'embedded_chatwoot_db_import_helper';
  manualFirst: true;
  officialChatwootExecutor: false;
  writesDirectlyToChatwootDatabase: true;
};

type InboxStatusPayload = {
  enabled: boolean;
  accountId: string | null;
  nameInbox: string | null;
  webhookUrl: string | null;
  inboxId: number | null;
  inboxName: string | null;
  inboxStatus: 'resolved' | 'not_found' | 'invalid';
  inboxUrl: string | null;
  isReady: boolean;
  validations: InboxValidation[];
  dependencies: DependencyNotice[];
  executor: ExecutorDescriptor;
};

type HistoryAnalysisContext = {
  provider: ChatwootModel;
  inbox: any;
  inboxId: number;
  inboxStatus: InboxStatusPayload;
};

type ChatwootReviewPayload = {
  chatwootAccountId: string | null;
  chatwootInboxId: number | null;
  chatwootContactId: number | null;
  chatwootConversationId: number | null;
  chatwootReviewUrl: string | null;
  chatwootFallbackUrl: string | null;
};

type ConversationMetrics = {
  candidateConversationIds: number[];
  selectedConversationId: number | null;
  chatwootMessageCount: number;
  overlapCount: number;
  sourceIdCollisionRisk: boolean;
  firstMessageAt: string | null;
  lastMessageAt: string | null;
  matchedCanonicalSourceIds: string[];
  matchedFallbackSignatures: string[];
};

type PersistedContactRow = {
  remoteJid: string;
  canonicalJid: string | null;
  phoneJid: string | null;
  lidJid: string | null;
  canonicalIdentityType: CanonicalIdentityType;
  identityResolutionStatus: IdentityResolutionStatus;
  pushName: string | null;
  classification: ContactClassification;
  suggestedAction: SuggestedAction;
  selectedAction?: SelectedAction | null;
  executionStatus: ExecutionStatus;
  hasLidAlias: boolean;
  isSafeDirectImport: boolean;
  unsafeReasons: UnsafeReason[];
  evolutionMessageCount: number;
  chatwootMessageCount: number;
  overlapCount: number;
  chatwootContactId: number | null;
  existingConversationId: number | null;
  selectedConversationId: number | null;
  rebuiltConversationId: number | null;
  candidateConversationIds: number[];
  report: Record<string, unknown>;
};

type ContactAnalysis = PersistedContactRow & {
  phoneNumber: string | null;
};

type JobSummary = {
  totalContacts: number;
  safeDirectImport: number;
  eligible: number;
  needsReview: number;
  lidAlias: number;
  requiresRebuild: number;
  ignored: number;
  completed?: number;
  failed?: number;
  skipped?: number;
  imported?: number;
  rebuilt?: number;
  totalsByClassification: Record<ContactClassification, number>;
  totalsBySuggestedAction: Record<SuggestedAction, number>;
  totalsByExecutionStatus?: Record<ExecutionStatus, number>;
};

type JobReportOptions = {
  jobId: string;
  mode: JobMode;
  jobStatus: JobStatus;
  provider: ChatwootModel;
  inboxId: number;
  sourceJobId?: string;
  selectionMode?: 'allSafe' | 'selected';
  startedAt?: string | null;
  finishedAt?: string | null;
};

type ExecuteRuntimeOptions = {
  allowUnsafeOverrideRemoteJids?: Set<string>;
};

export class ChatwootHistoryService {
  private readonly logger = new Logger('ChatwootHistoryService');

  constructor(
    private readonly prismaRepository: PrismaRepository,
    private readonly chatwootService: ChatwootService,
  ) {}

  public async getInboxStatus(instance: InstanceDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const { inboxStatus } = await this.loadInboxContext(scopedInstance);

    return inboxStatus;
  }

  public async analyze(instance: InstanceDto, data: ChatwootHistoryAnalyzeDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const context = await this.requireAnalysisContext(scopedInstance);
    const remoteJids = await this.resolveScopeRemoteJids(scopedInstance, data);
    const startedAt = new Date();

    const job = await this.prismaRepository.chatwootHistoryJob.create({
      data: {
        instanceId: scopedInstance.instanceId,
        scopeType: data.scopeType,
        mode: 'dryRun',
        jobStatus: 'analyzing',
        filters: this.toJsonInput({
          remoteJids,
        }),
        summary: this.toJsonInput({
          totalContacts: remoteJids.length,
        }),
        report: this.toJsonInput(
          this.buildJobReport([], {
            jobId: 'pending',
            mode: 'dryRun',
            jobStatus: 'analyzing',
            provider: context.provider,
            inboxId: context.inboxId,
            startedAt: startedAt.toISOString(),
            finishedAt: null,
          }),
        ),
        startedAt,
      },
    });

    try {
      const analyses: ContactAnalysis[] = [];

      for (const remoteJid of remoteJids) {
        analyses.push(await this.analyzeContact(scopedInstance, remoteJid, context));
      }

      if (analyses.length > 0) {
        await this.prismaRepository.chatwootHistoryJobContact.createMany({
          data: analyses.map((analysis) => this.toHistoryJobContactCreate(job.id, scopedInstance.instanceId, analysis)),
        });
      }

      const finishedAt = new Date();
      const summary = this.buildSummary(analyses);
      await this.prismaRepository.chatwootHistoryJob.update({
        where: { id: job.id },
        data: {
          jobStatus: 'awaiting_execution',
          summary: this.toJsonInput(summary),
          report: this.toJsonInput(
            this.buildJobReport(analyses, {
              jobId: job.id,
              mode: 'dryRun',
              jobStatus: 'awaiting_execution',
              provider: context.provider,
              inboxId: context.inboxId,
              startedAt: startedAt.toISOString(),
              finishedAt: finishedAt.toISOString(),
            }),
          ),
          finishedAt,
        },
      });

      return this.getJob(scopedInstance, job.id);
    } catch (error) {
      await this.prismaRepository.chatwootHistoryJob.update({
        where: { id: job.id },
        data: {
          jobStatus: 'failed',
          errorMessage: error?.message || error?.toString?.() || 'Unknown error',
          finishedAt: new Date(),
        },
      });

      throw error;
    }
  }

  public async execute(
    instance: InstanceDto,
    data: ChatwootHistoryExecuteDto,
    runtimeOptions: ExecuteRuntimeOptions = {},
  ) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const sourceJob = await this.requireJob(scopedInstance, data.jobId);
    const sourceContacts = await this.prismaRepository.chatwootHistoryJobContact.findMany({
      where: { instanceId: scopedInstance.instanceId, jobId: sourceJob.id },
      orderBy: { createdAt: 'asc' },
    });

    if (sourceContacts.length === 0) {
      throw new BadRequestException('No contacts available to execute');
    }

    const selectedContacts = this.selectContactsForExecution(sourceContacts, data);
    if (selectedContacts.length === 0) {
      throw new BadRequestException('No contacts matched the execution criteria');
    }

    const context = await this.requireAnalysisContext(scopedInstance);
    const startedAt = new Date();
    const executionJob = await this.prismaRepository.chatwootHistoryJob.create({
      data: {
        instanceId: scopedInstance.instanceId,
        scopeType: sourceJob.scopeType,
        mode: data.mode,
        jobStatus: 'running',
        filters: this.toJsonInput({
          sourceJobId: sourceJob.id,
          selectionMode: data.selectionMode,
          remoteJids: selectedContacts.map((contact) => contact.remoteJid),
        }),
        summary: this.toJsonInput({
          totalContacts: selectedContacts.length,
        }),
        report: this.toJsonInput(
          this.buildJobReport([], {
            jobId: 'pending',
            mode: data.mode,
            jobStatus: 'running',
            provider: context.provider,
            inboxId: context.inboxId,
            sourceJobId: sourceJob.id,
            selectionMode: data.selectionMode,
            startedAt: startedAt.toISOString(),
            finishedAt: null,
          }),
        ),
        startedAt,
      },
    });

    await this.prismaRepository.chatwootHistoryJobContact.createMany({
      data: selectedContacts.map((contact) =>
        this.cloneContactForExecution(contact, executionJob.id, scopedInstance.instanceId, data.mode),
      ),
    });

    try {
      const executionContacts = await this.prismaRepository.chatwootHistoryJobContact.findMany({
        where: { instanceId: scopedInstance.instanceId, jobId: executionJob.id },
        orderBy: { createdAt: 'asc' },
      });

      const results: PersistedContactRow[] = [];
      for (const contact of executionContacts) {
        const updated = await this.executeContact(scopedInstance, context, data.mode, contact, {
          allowUnsafeOverride: !!runtimeOptions.allowUnsafeOverrideRemoteJids?.has(contact.remoteJid),
        });
        results.push(this.mapExecutionContact(updated));
      }

      const finishedAt = new Date();
      const summary = this.buildSummary(results, true, data.mode);
      const jobStatus = this.resolveExecutionJobStatus(summary);
      await this.prismaRepository.chatwootHistoryJob.update({
        where: { id: executionJob.id },
        data: {
          jobStatus,
          summary: this.toJsonInput(summary),
          report: this.toJsonInput(
            this.buildJobReport(results, {
              jobId: executionJob.id,
              mode: data.mode,
              jobStatus,
              provider: context.provider,
              inboxId: context.inboxId,
              sourceJobId: sourceJob.id,
              selectionMode: data.selectionMode,
              startedAt: startedAt.toISOString(),
              finishedAt: finishedAt.toISOString(),
            }),
          ),
          finishedAt,
        },
      });

      return this.getJob(scopedInstance, executionJob.id);
    } catch (error) {
      await this.prismaRepository.chatwootHistoryJob.update({
        where: { id: executionJob.id },
        data: {
          jobStatus: 'failed',
          errorMessage: error?.message || error?.toString?.() || 'Unknown error',
          finishedAt: new Date(),
        },
      });

      throw error;
    }
  }

  public async listJobs(instance: InstanceDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    return this.prismaRepository.chatwootHistoryJob.findMany({
      where: { instanceId: scopedInstance.instanceId },
      orderBy: { createdAt: 'desc' },
    });
  }

  public async getJob(instance: InstanceDto, jobId: string) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const job = await this.prismaRepository.chatwootHistoryJob.findFirst({
      where: { id: jobId, instanceId: scopedInstance.instanceId },
      include: {
        Contacts: {
          orderBy: { createdAt: 'asc' },
        },
      },
    });

    if (!job) {
      throw new NotFoundException('History job not found');
    }

    return job;
  }

  public async listConflicts(instance: InstanceDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const contacts = await this.prismaRepository.chatwootHistoryJobContact.findMany({
      where: {
        instanceId: scopedInstance.instanceId,
        classification: {
          in: ['needs_review', 'lid_alias', 'requires_rebuild'],
        },
      },
      orderBy: { createdAt: 'desc' },
      include: {
        Job: true,
      },
    });

    const latestPerRemoteJid = new Map<string, any>();
    contacts.forEach((contact) => {
      if (!latestPerRemoteJid.has(contact.remoteJid)) {
        latestPerRemoteJid.set(contact.remoteJid, contact);
      }
    });

    return Array.from(latestPerRemoteJid.values());
  }

  public async reprocess(instance: InstanceDto, data: ChatwootHistoryReprocessDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const job = await this.requireJob(scopedInstance, data.jobId);

    if (job.mode === 'dryRun') {
      const filters = this.asObject(job.filters);
      return this.analyze(scopedInstance, {
        scopeType: job.scopeType,
        remoteJids: data.remoteJid ? [data.remoteJid] : filters.remoteJids,
      });
    }

    return this.execute(scopedInstance, {
      jobId: data.jobId,
      mode: job.mode,
      selectionMode: 'selected',
      remoteJids: data.remoteJid
        ? [data.remoteJid]
        : (
            await this.prismaRepository.chatwootHistoryJobContact.findMany({
              where: { instanceId: scopedInstance.instanceId, jobId: job.id },
              select: { remoteJid: true },
            })
          ).map((contact) => contact.remoteJid),
    });
  }

  public async contactAction(instance: InstanceDto, data: ChatwootHistoryContactActionDto) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const job = await this.requireJob(scopedInstance, data.jobId);
    const contact = await this.prismaRepository.chatwootHistoryJobContact.findFirst({
      where: {
        instanceId: scopedInstance.instanceId,
        jobId: job.id,
        remoteJid: data.remoteJid,
      },
    });

    if (!contact) {
      throw new NotFoundException('History job contact not found');
    }

    if (data.action === 'openChatwootReview') {
      const reviewPayload = this.extractReviewPayload(contact.report);
      return {
        jobId: job.id,
        remoteJid: contact.remoteJid,
        action: data.action,
        url: reviewPayload.chatwootReviewUrl || reviewPayload.chatwootFallbackUrl,
        ...reviewPayload,
      };
    }

    if (data.action === 'ignore') {
      const updatedReport = this.mergeExecutionIntoReport(this.asObject(contact.report), {
        selectedAction: 'ignore',
        executionStatus: 'skipped',
        executionError: null,
        rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
        reviewPayload: this.extractReviewPayload(contact.report),
      });

      await this.prismaRepository.chatwootHistoryJobContact.update({
        where: { id: contact.id },
        data: {
          selectedAction: 'ignore',
          executionStatus: 'skipped',
          report: this.toJsonInput(updatedReport),
        },
      });

      return this.getJob(scopedInstance, job.id);
    }

    return this.execute(
      scopedInstance,
      {
        jobId: job.id,
        mode: data.action === 'createRebuild' ? 'rebuild' : 'importDirect',
        selectionMode: 'selected',
        remoteJids: [contact.remoteJid],
      },
      data.action === 'importDirect'
        ? {
            allowUnsafeOverrideRemoteJids: new Set([contact.remoteJid]),
          }
        : {},
    );
  }

  public async exportCsv(instance: InstanceDto, jobId: string) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const job = await this.getJob(scopedInstance, jobId);
    const rows = job.Contacts || [];
    const header = [
      'remoteJid',
      'canonicalJid',
      'phoneJid',
      'lidJid',
      'canonicalIdentityType',
      'identityResolutionStatus',
      'pushName',
      'classification',
      'suggestedAction',
      'selectedAction',
      'executionStatus',
      'hasLidAlias',
      'isSafeDirectImport',
      'unsafeReasons',
      'evolutionMessageCount',
      'chatwootMessageCount',
      'overlapCount',
      'chatwootContactId',
      'existingConversationId',
      'selectedConversationId',
      'candidateConversationIds',
      'rebuiltConversationId',
    ];

    const lines = [header.join(',')];
    rows.forEach((row) => {
      lines.push(
        [
          row.remoteJid,
          row.canonicalJid,
          row.phoneJid,
          row.lidJid,
          row.canonicalIdentityType,
          row.identityResolutionStatus,
          row.pushName,
          row.classification,
          row.suggestedAction,
          row.selectedAction,
          row.executionStatus,
          row.hasLidAlias,
          row.isSafeDirectImport,
          Array.isArray(row.unsafeReasons) ? row.unsafeReasons.join('|') : '',
          row.evolutionMessageCount,
          row.chatwootMessageCount,
          row.overlapCount,
          row.chatwootContactId,
          row.existingConversationId,
          row.selectedConversationId,
          Array.isArray(row.candidateConversationIds) ? row.candidateConversationIds.join('|') : '',
          row.rebuiltConversationId,
        ]
          .map((value) => this.toCsvCell(value))
          .join(','),
      );
    });

    return lines.join('\n');
  }

  public classifyContactAnalysis(args: {
    isGroup: boolean;
    isStatus: boolean;
    isBroadcast: boolean;
    evolutionMessageCount: number;
    hasLidAlias: boolean;
    candidateConversationCount: number;
    chatwootMessageCount: number;
    overlapCount: number;
    sourceIdCollisionRisk: boolean;
    canonicalIdentityType: CanonicalIdentityType;
    identityResolutionStatus: IdentityResolutionStatus;
  }) {
    return classifyChatwootHistoryContact(args);
  }

  private async requireJob(instance: InstanceDto, jobId: string) {
    const scopedInstance = await this.resolveInstanceScope(instance);
    const job = await this.prismaRepository.chatwootHistoryJob.findFirst({
      where: { id: jobId, instanceId: scopedInstance.instanceId },
    });

    if (!job) {
      throw new NotFoundException('History job not found');
    }

    return job;
  }

  private async resolveInstanceScope(instance: InstanceDto): Promise<InstanceDto & { instanceId: string }> {
    if (instance.instanceId) {
      return instance as InstanceDto & { instanceId: string };
    }

    const persistedInstance = await this.prismaRepository.instance.findFirst({
      where: { name: instance.instanceName },
      select: { id: true },
    });

    if (!persistedInstance?.id) {
      throw new NotFoundException(`Instance "${instance.instanceName}" not found`);
    }

    return {
      ...instance,
      instanceId: persistedInstance.id,
    };
  }

  private async loadInboxContext(instance: InstanceDto) {
    const provider = await this.chatwootService.getProvider(instance);
    const executor = this.getExecutorDescriptor();
    const dependencies = [this.getPaginationDependency()];
    const validations: InboxValidation[] = [];

    const enabled = !!provider?.enabled;
    const importHistoryAvailability = this.chatwootService.getImportHistoryAvailability();
    const importExecutorAvailable = importHistoryAvailability.available;
    const hasAccountId = !!provider?.accountId;
    const hasNameInbox = !!provider?.nameInbox;
    const hasBaseUrl = !!provider?.url;
    const hasToken = !!provider?.token;

    let inbox: any = null;
    if (enabled && hasAccountId && hasNameInbox && hasBaseUrl && hasToken) {
      try {
        inbox = await this.chatwootService.getInbox(instance);
      } catch (error) {
        this.logger.warn(`Unable to resolve inbox mapping: ${error?.message || error?.toString?.() || error}`);
      }
    }

    const inboxResolved = !!inbox?.id;
    const inboxLooksApi = inboxResolved ? this.isApiInbox(inbox) : false;
    const webhookUrl = typeof inbox?.webhook_url === 'string' ? inbox.webhook_url : null;
    const webhookConfigured = inboxResolved ? ('webhook_url' in inbox ? !!webhookUrl : true) : false;

    validations.push({
      code: 'chatwoot_integration_active',
      label: 'Integracao Chatwoot ativa',
      ok: enabled,
    });
    validations.push({
      code: 'chatwoot_import_executor_available',
      label: 'Executor manual-first disponivel',
      ok: importExecutorAvailable,
      details: importExecutorAvailable ? undefined : importHistoryAvailability.reason,
    });
    validations.push({
      code: 'chatwoot_account_id_present',
      label: 'accountId configurado',
      ok: hasAccountId,
    });
    validations.push({
      code: 'chatwoot_inbox_name_present',
      label: 'nameInbox configurado',
      ok: hasNameInbox,
    });
    validations.push({
      code: 'chatwoot_base_url_present',
      label: 'URL base configurada',
      ok: hasBaseUrl,
    });
    validations.push({
      code: 'chatwoot_token_present',
      label: 'Token configurado',
      ok: hasToken,
    });
    validations.push({
      code: 'chatwoot_mapping_unique_for_instance',
      label: 'Mapping unico por instancia',
      ok: !!provider,
      details: provider ? undefined : 'Nenhuma configuracao persistida do Chatwoot foi encontrada para esta instancia.',
    });
    validations.push({
      code: 'chatwoot_inbox_resolved',
      label: 'Inbox alvo resolvida',
      ok: inboxResolved,
      details: inboxResolved ? undefined : 'Nao foi possivel localizar a inbox configurada na conta do Chatwoot.',
    });
    validations.push({
      code: 'chatwoot_inbox_api_channel',
      label: 'Inbox alvo e do tipo API',
      ok: inboxResolved ? inboxLooksApi : false,
      details: inboxResolved && !inboxLooksApi ? 'A inbox resolvida nao parece ser um canal API.' : undefined,
    });
    validations.push({
      code: 'chatwoot_webhook_configured',
      label: 'Webhook configurado na inbox',
      ok: webhookConfigured,
      details:
        inboxResolved && !webhookConfigured ? 'A payload da inbox nao trouxe webhook_url configurado.' : undefined,
    });

    const inboxStatus: InboxStatusPayload = {
      enabled,
      accountId: provider?.accountId || null,
      nameInbox: provider?.nameInbox || null,
      webhookUrl,
      inboxId: inbox?.id ? Number(inbox.id) : null,
      inboxName: inbox?.name || provider?.nameInbox || null,
      inboxStatus: inboxResolved ? 'resolved' : enabled ? 'not_found' : 'invalid',
      inboxUrl: provider ? this.buildInboxUrl(provider) : null,
      isReady: validations.every((validation) => validation.ok),
      validations,
      dependencies,
      executor,
    };

    return {
      provider,
      inbox,
      inboxStatus,
    };
  }

  private async requireAnalysisContext(instance: InstanceDto): Promise<HistoryAnalysisContext> {
    const { provider, inbox, inboxStatus } = await this.loadInboxContext(instance);

    if (!provider?.enabled || !provider) {
      throw new BadRequestException('Chatwoot is not configured for this instance');
    }

    if (!inboxStatus.isReady || !inbox?.id) {
      const failedValidations = inboxStatus.validations
        .filter((validation) => !validation.ok)
        .map((validation) => validation.code);
      throw new BadRequestException(
        `Chatwoot inbox mapping is not ready for history import: ${failedValidations.join(', ')}`,
      );
    }

    return {
      provider,
      inbox,
      inboxId: Number(inbox.id),
      inboxStatus,
    };
  }

  private async resolveScopeRemoteJids(instance: InstanceDto, data: ChatwootHistoryAnalyzeDto) {
    if (data.scopeType === 'single') {
      if (!data.remoteJids?.[0]) {
        throw new BadRequestException('remoteJids[0] is required for single scope');
      }

      return [data.remoteJids[0]];
    }

    if (data.scopeType === 'selected') {
      if (!data.remoteJids?.length) {
        throw new BadRequestException('remoteJids is required for selected scope');
      }

      return [...new Set(data.remoteJids)];
    }

    const [chats, contacts] = await Promise.all([
      this.prismaRepository.chat.findMany({
        where: { instanceId: instance.instanceId },
        select: { remoteJid: true },
      }),
      this.prismaRepository.contact.findMany({
        where: { instanceId: instance.instanceId },
        select: { remoteJid: true },
      }),
    ]);

    return [...new Set([...chats, ...contacts].map((item) => item.remoteJid).filter(Boolean))];
  }

  private async analyzeContact(
    instance: InstanceDto,
    remoteJid: string,
    context: HistoryAnalysisContext,
  ): Promise<ContactAnalysis> {
    const resolved = resolveCanonicalJid({ remoteJid });
    const identity = this.resolveIdentityMetadata(resolved);
    const evolutionMessages = await this.loadEvolutionMessages(instance, remoteJid, resolved);
    const chatwootContact = await this.findChatwootContact(instance, remoteJid, resolved);
    const conversationMetrics = await this.loadConversationMetrics(
      instance,
      context,
      evolutionMessages,
      chatwootContact?.id ? Number(chatwootContact.id) : null,
    );
    const hasLidAlias = !!resolved.lidJid;
    const classification = this.classifyContactAnalysis({
      isGroup: resolved.isGroup,
      isStatus: resolved.isStatus,
      isBroadcast: resolved.isBroadcast,
      evolutionMessageCount: evolutionMessages.length,
      hasLidAlias,
      candidateConversationCount: conversationMetrics.candidateConversationIds.length,
      chatwootMessageCount: conversationMetrics.chatwootMessageCount,
      overlapCount: conversationMetrics.overlapCount,
      sourceIdCollisionRisk: conversationMetrics.sourceIdCollisionRisk,
      canonicalIdentityType: identity.canonicalIdentityType,
      identityResolutionStatus: identity.identityResolutionStatus,
    });
    const pushName = await this.resolvePushName(instance, remoteJid, evolutionMessages);
    const reviewPayload = this.buildReviewPayload(
      context.provider,
      context.inboxId,
      chatwootContact?.id ? Number(chatwootContact.id) : null,
      conversationMetrics.selectedConversationId,
    );
    const firstMessageTimestamp = evolutionMessages[0]?.messageTimestamp || null;
    const lastMessageTimestamp = evolutionMessages[evolutionMessages.length - 1]?.messageTimestamp || null;

    return {
      remoteJid,
      canonicalJid: resolved.canonicalJid,
      phoneJid: resolved.phoneJid,
      lidJid: resolved.lidJid,
      canonicalIdentityType: identity.canonicalIdentityType,
      identityResolutionStatus: identity.identityResolutionStatus,
      pushName,
      classification: classification.classification,
      suggestedAction: classification.suggestedAction,
      selectedAction: null,
      executionStatus: 'pending',
      hasLidAlias,
      isSafeDirectImport: classification.isSafeDirectImport,
      unsafeReasons: classification.unsafeReasons,
      evolutionMessageCount: evolutionMessages.length,
      chatwootMessageCount: conversationMetrics.chatwootMessageCount,
      overlapCount: conversationMetrics.overlapCount,
      chatwootContactId: chatwootContact?.id ? Number(chatwootContact.id) : null,
      existingConversationId: conversationMetrics.selectedConversationId,
      selectedConversationId: conversationMetrics.selectedConversationId,
      rebuiltConversationId: null,
      candidateConversationIds: conversationMetrics.candidateConversationIds,
      phoneNumber: resolved.phoneJid?.split('@')[0] || null,
      report: this.buildContactReport({
        remoteJid,
        resolved,
        pushName,
        classification: classification.classification,
        suggestedAction: classification.suggestedAction,
        selectedAction: null,
        executionStatus: 'pending',
        hasLidAlias,
        isSafeDirectImport: classification.isSafeDirectImport,
        unsafeReasons: classification.unsafeReasons,
        evolutionMessageCount: evolutionMessages.length,
        chatwootMessageCount: conversationMetrics.chatwootMessageCount,
        overlapCount: conversationMetrics.overlapCount,
        candidateConversationIds: conversationMetrics.candidateConversationIds,
        matchedCanonicalSourceIds: conversationMetrics.matchedCanonicalSourceIds,
        matchedFallbackSignatures: conversationMetrics.matchedFallbackSignatures,
        sourceIdCollisionRisk: conversationMetrics.sourceIdCollisionRisk,
        firstMessageTimestamp,
        lastMessageTimestamp,
        chatwootFirstMessageAt: conversationMetrics.firstMessageAt,
        chatwootLastMessageAt: conversationMetrics.lastMessageAt,
        canonicalIdentityType: identity.canonicalIdentityType,
        identityResolutionStatus: identity.identityResolutionStatus,
        reviewPayload,
      }),
    };
  }

  private async loadEvolutionMessages(
    instance: InstanceDto,
    remoteJid: string,
    resolved: ReturnType<typeof resolveCanonicalJid>,
  ) {
    const aliases = getJidAliases({
      remoteJid,
      canonicalJid: resolved.canonicalJid,
      phoneJid: resolved.phoneJid,
      lidJid: resolved.lidJid,
    });

    const aliasConditions = aliases.flatMap((alias) => [
      { canonicalJid: alias },
      { phoneJid: alias },
      { lidJid: alias },
      { key: { path: ['canonicalJid'], equals: alias } },
      { key: { path: ['remoteJid'], equals: alias } },
      { key: { path: ['remoteJidAlt'], equals: alias } },
    ]);

    if (aliasConditions.length === 0) {
      return this.prismaRepository.message.findMany({
        where: {
          instanceId: instance.instanceId,
          OR: [
            { canonicalJid: remoteJid },
            { phoneJid: remoteJid },
            { lidJid: remoteJid },
            { key: { path: ['canonicalJid'], equals: remoteJid } },
            { key: { path: ['remoteJid'], equals: remoteJid } },
            { key: { path: ['remoteJidAlt'], equals: remoteJid } },
          ],
        },
        orderBy: { messageTimestamp: 'asc' },
      });
    }

    return this.prismaRepository.message.findMany({
      where: {
        instanceId: instance.instanceId,
        OR: aliasConditions as any[],
      },
      orderBy: { messageTimestamp: 'asc' },
    });
  }

  private async findChatwootContact(
    instance: InstanceDto,
    remoteJid: string,
    resolved: ReturnType<typeof resolveCanonicalJid>,
  ) {
    const phoneNumber = resolved.phoneJid?.split('@')[0];
    if (phoneNumber) {
      const directContact = await this.chatwootService.findContact(instance, phoneNumber);
      if (directContact?.id) {
        return directContact;
      }
    }

    const aliases = getJidAliases({
      remoteJid,
      canonicalJid: resolved.canonicalJid,
      phoneJid: resolved.phoneJid,
      lidJid: resolved.lidJid,
    });
    for (const alias of aliases) {
      const byIdentifier = await this.chatwootService.findContactByIdentifier(instance, alias);
      if (byIdentifier?.id) {
        return byIdentifier;
      }
    }

    return null;
  }

  private async loadConversationMetrics(
    instance: InstanceDto,
    context: HistoryAnalysisContext,
    evolutionMessages: MessageModel[],
    chatwootContactId: number | null,
  ): Promise<ConversationMetrics> {
    if (!chatwootContactId) {
      return {
        candidateConversationIds: [],
        selectedConversationId: null,
        chatwootMessageCount: 0,
        overlapCount: 0,
        sourceIdCollisionRisk: false,
        firstMessageAt: null,
        lastMessageAt: null,
        matchedCanonicalSourceIds: [],
        matchedFallbackSignatures: [],
      };
    }

    const candidateConversationIds = (await this.chatwootService.listContactConversations(instance, chatwootContactId))
      .filter((conversation) => Number(conversation?.inbox_id) === context.inboxId)
      .map((conversation) => Number(conversation.id))
      .sort((left, right) => right - left);

    if (candidateConversationIds.length === 0) {
      return {
        candidateConversationIds: [],
        selectedConversationId: null,
        chatwootMessageCount: 0,
        overlapCount: 0,
        sourceIdCollisionRisk: false,
        firstMessageAt: null,
        lastMessageAt: null,
        matchedCanonicalSourceIds: [],
        matchedFallbackSignatures: [],
      };
    }

    const matchedTokens = new Set<string>();
    const matchedCanonicalSourceIds = new Set<string>();
    const matchedFallbackSignatures = new Set<string>();
    let chatwootMessageCount = 0;
    let sourceIdCollisionRisk = false;
    let firstMessageAt: string | null = null;
    let lastMessageAt: string | null = null;

    for (const conversationId of candidateConversationIds) {
      const [messageCount, inspection, window] = await Promise.all([
        this.chatwootService.countConversationMessages(instance, conversationId),
        chatwootImport.inspectConversationMessages(
          instance,
          this.chatwootService,
          context.provider,
          conversationId,
          evolutionMessages,
        ),
        this.chatwootService.getConversationMessageWindow(instance, conversationId),
      ]);

      chatwootMessageCount += messageCount;
      sourceIdCollisionRisk = sourceIdCollisionRisk || inspection.sourceIdCollisionRisk;
      inspection.matchedTokens.forEach((value) => matchedTokens.add(value));
      inspection.matchedCanonicalSourceIds.forEach((value) => matchedCanonicalSourceIds.add(value));
      inspection.matchedFallbackSignatures.forEach((value) => matchedFallbackSignatures.add(value));

      if (window.firstMessageAt && (!firstMessageAt || new Date(window.firstMessageAt) < new Date(firstMessageAt))) {
        firstMessageAt = window.firstMessageAt;
      }
      if (window.lastMessageAt && (!lastMessageAt || new Date(window.lastMessageAt) > new Date(lastMessageAt))) {
        lastMessageAt = window.lastMessageAt;
      }
    }

    return {
      candidateConversationIds,
      selectedConversationId: candidateConversationIds.length === 1 ? candidateConversationIds[0] : null,
      chatwootMessageCount,
      overlapCount: matchedTokens.size,
      sourceIdCollisionRisk,
      firstMessageAt,
      lastMessageAt,
      matchedCanonicalSourceIds: Array.from(matchedCanonicalSourceIds),
      matchedFallbackSignatures: Array.from(matchedFallbackSignatures),
    };
  }

  private async resolvePushName(instance: InstanceDto, remoteJid: string, messages: MessageModel[]) {
    const messagePushName = messages.find((message) => !!message.pushName)?.pushName;
    if (messagePushName) {
      return messagePushName;
    }

    const [contact, chat] = await Promise.all([
      this.prismaRepository.contact.findFirst({
        where: { instanceId: instance.instanceId, remoteJid },
      }),
      this.prismaRepository.chat.findFirst({
        where: { instanceId: instance.instanceId, remoteJid },
      }),
    ]);

    return contact?.pushName || chat?.name || remoteJid.split('@')[0];
  }

  private async executeContact(
    instance: InstanceDto,
    context: HistoryAnalysisContext,
    mode: Exclude<JobMode, 'dryRun'>,
    contact: any,
    options?: {
      allowUnsafeOverride?: boolean;
    },
  ) {
    const selectedAction = mode === 'rebuild' ? 'create_rebuild' : 'import_direct';
    const isSafeForMode = this.isSafeForMode(contact, mode);
    const allowUnsafeOverride = !!options?.allowUnsafeOverride;
    if (!isSafeForMode && !allowUnsafeOverride) {
      return this.updateExecutionContact(contact.id, {
        selectedAction,
        executionStatus: 'skipped',
        report: this.toJsonInput(
          this.mergeExecutionIntoReport(this.asObject(contact.report), {
            selectedAction,
            executionStatus: 'skipped',
            executionError: 'Contact does not meet the execution safety criteria for this mode',
            rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
            reviewPayload: this.extractReviewPayload(contact.report),
          }),
        ),
      });
    }

    const resolved = resolveCanonicalJid({
      remoteJid: contact.remoteJid,
      canonicalJid: contact.canonicalJid,
      phoneJid: contact.phoneJid,
      lidJid: contact.lidJid,
    });
    const messages = await this.loadEvolutionMessages(instance, contact.remoteJid, resolved);
    if (messages.length === 0) {
      return this.updateExecutionContact(contact.id, {
        selectedAction,
        executionStatus: 'skipped',
        report: this.toJsonInput(
          this.mergeExecutionIntoReport(this.asObject(contact.report), {
            selectedAction,
            executionStatus: 'skipped',
            executionError: 'No Evolution messages were found for this contact',
            rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
            reviewPayload: this.extractReviewPayload(contact.report),
          }),
        ),
      });
    }

    const phoneNumber = (contact.phoneJid || resolved.phoneJid)?.split('@')[0] || null;
    if (!phoneNumber) {
      return this.updateExecutionContact(contact.id, {
        selectedAction,
        executionStatus: 'failed',
        report: this.toJsonInput(
          this.mergeExecutionIntoReport(this.asObject(contact.report), {
            selectedAction,
            executionStatus: 'failed',
            executionError: 'Phone number could not be resolved',
            rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
            reviewPayload: this.extractReviewPayload(contact.report),
          }),
        ),
      });
    }

    try {
      chatwootImport.clearAll(instance);
      chatwootImport.addHistoryMessages(instance, messages as any);

      let targetConversationId = contact.selectedConversationId ? Number(contact.selectedConversationId) : null;
      let rebuiltConversationId = contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null;
      const forceFksByPhoneNumber = new Map<string, FksChatwoot>();

      if (mode === 'rebuild') {
        rebuiltConversationId = await this.createRebuildConversation(
          instance,
          context.inboxId,
          contact.remoteJid,
          contact.pushName,
        );
        if (!rebuiltConversationId) {
          throw new Error('Unable to create rebuilt conversation');
        }
        targetConversationId = rebuiltConversationId;
      }

      if (targetConversationId) {
        const resolvedContactId = Number(
          contact.chatwootContactId ||
            (await this.resolveContactId(instance, contact.remoteJid, contact.pushName, context.inboxId)),
        );
        forceFksByPhoneNumber.set(`+${phoneNumber}`, {
          phone_number: `+${phoneNumber}`,
          contact_id: String(resolvedContactId),
          conversation_id: String(targetConversationId),
        });
      }

      await chatwootImport.importHistoryMessages(instance, this.chatwootService, context.inbox, context.provider, {
        allowedPhoneNumbers: new Set([`+${phoneNumber}`]),
        forceFksByPhoneNumber: forceFksByPhoneNumber.size > 0 ? forceFksByPhoneNumber : undefined,
      });

      const latestContact = await this.findChatwootContact(instance, contact.remoteJid, resolved);
      const latestConversation =
        latestContact?.id && context.inboxId
          ? await this.chatwootService.getLatestInboxConversation(instance, Number(latestContact.id), context.inboxId)
          : null;
      const latestConversationId =
        mode === 'rebuild'
          ? rebuiltConversationId
          : targetConversationId || (latestConversation?.id ? Number(latestConversation.id) : null);
      const latestChatwootCount = latestConversationId
        ? await this.chatwootService.countConversationMessages(instance, latestConversationId)
        : contact.chatwootMessageCount;
      const reviewPayload = this.buildReviewPayload(
        context.provider,
        context.inboxId,
        latestContact?.id ? Number(latestContact.id) : contact.chatwootContactId || null,
        latestConversationId,
      );

      return this.updateExecutionContact(contact.id, {
        selectedAction,
        executionStatus: 'completed',
        chatwootContactId: latestContact?.id ? Number(latestContact.id) : contact.chatwootContactId,
        existingConversationId: mode === 'importDirect' ? latestConversationId : contact.existingConversationId,
        selectedConversationId: mode === 'importDirect' ? latestConversationId : contact.selectedConversationId,
        rebuiltConversationId,
        chatwootMessageCount: latestChatwootCount,
        report: this.toJsonInput(
          this.mergeExecutionIntoReport(this.asObject(contact.report), {
            selectedAction,
            executionStatus: 'completed',
            executionError: null,
            rebuiltConversationId,
            reviewPayload,
            executionWarning:
              !isSafeForMode && allowUnsafeOverride
                ? 'Explicit manual override executed despite direct import safety warnings'
                : null,
          }),
        ),
      });
    } catch (error) {
      this.logger.error(`Error executing history contact ${contact.remoteJid}: ${error?.toString?.() || error}`);
      return this.updateExecutionContact(contact.id, {
        selectedAction,
        executionStatus: 'failed',
        report: this.toJsonInput(
          this.mergeExecutionIntoReport(this.asObject(contact.report), {
            selectedAction,
            executionStatus: 'failed',
            executionError: error?.message || error?.toString?.() || 'Unknown error',
            rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
            reviewPayload: this.extractReviewPayload(contact.report),
          }),
        ),
      });
    }
  }

  private async resolveContactId(instance: InstanceDto, remoteJid: string, pushName: string | null, inboxId: number) {
    const resolved = resolveCanonicalJid({ remoteJid });
    const existingContact = await this.findChatwootContact(instance, remoteJid, resolved);
    if (existingContact?.id) {
      return Number(existingContact.id);
    }

    const phoneNumber = resolved.phoneJid?.split('@')[0];
    if (!phoneNumber) {
      throw new Error('Phone number could not be resolved');
    }

    const createdContact = await this.chatwootService.createContact(
      instance,
      phoneNumber,
      inboxId,
      false,
      pushName || phoneNumber,
      null,
      resolved.canonicalJid || remoteJid,
    );

    const createdContactId =
      createdContact?.payload?.id || createdContact?.payload?.contact?.id || createdContact?.id || null;
    if (!createdContactId) {
      throw new Error('Unable to create Chatwoot contact');
    }

    return Number(createdContactId);
  }

  private async createRebuildConversation(
    instance: InstanceDto,
    inboxId: number,
    remoteJid: string,
    pushName: string | null,
  ) {
    const contactId = await this.resolveContactId(instance, remoteJid, pushName, inboxId);
    const conversation = await this.chatwootService.createFreshConversation(instance, contactId, inboxId, true);
    return conversation?.id ? Number(conversation.id) : null;
  }

  private updateExecutionContact(contactId: string, data: Record<string, unknown>) {
    return this.prismaRepository.chatwootHistoryJobContact.update({
      where: { id: contactId },
      data,
    });
  }

  private selectContactsForExecution(sourceContacts: any[], data: ChatwootHistoryExecuteDto) {
    const selectedRemoteJids = new Set(data.remoteJids || []);

    return data.selectionMode === 'selected'
      ? sourceContacts.filter((contact) => selectedRemoteJids.has(contact.remoteJid))
      : sourceContacts.filter((contact) => this.isSafeForMode(contact, data.mode));
  }

  private isSafeForMode(contact: any, mode: Exclude<JobMode, 'dryRun'>) {
    if (mode === 'importDirect') {
      return !!contact.isSafeDirectImport;
    }

    return ['needs_review', 'requires_rebuild', 'lid_alias'].includes(contact.classification);
  }

  private cloneContactForExecution(contact: any, jobId: string, instanceId: string, mode: Exclude<JobMode, 'dryRun'>) {
    return {
      jobId,
      instanceId,
      remoteJid: contact.remoteJid,
      canonicalJid: contact.canonicalJid,
      phoneJid: contact.phoneJid,
      lidJid: contact.lidJid,
      canonicalIdentityType: contact.canonicalIdentityType,
      identityResolutionStatus: contact.identityResolutionStatus,
      pushName: contact.pushName,
      classification: contact.classification,
      suggestedAction: contact.suggestedAction,
      selectedAction: (mode === 'rebuild' ? 'create_rebuild' : 'import_direct') as SelectedAction,
      executionStatus: 'pending' as ExecutionStatus,
      hasLidAlias: contact.hasLidAlias,
      isSafeDirectImport: !!contact.isSafeDirectImport,
      unsafeReasons: this.toJsonInput(Array.isArray(contact.unsafeReasons) ? contact.unsafeReasons : []),
      evolutionMessageCount: contact.evolutionMessageCount,
      chatwootMessageCount: contact.chatwootMessageCount,
      overlapCount: contact.overlapCount,
      chatwootContactId: contact.chatwootContactId,
      existingConversationId: contact.existingConversationId,
      selectedConversationId: contact.selectedConversationId,
      rebuiltConversationId: contact.rebuiltConversationId,
      candidateConversationIds: this.toJsonInput(
        Array.isArray(contact.candidateConversationIds) ? contact.candidateConversationIds : [],
      ),
      report: this.toJsonInput(contact.report || {}),
    };
  }

  private toHistoryJobContactCreate(jobId: string, instanceId: string, analysis: ContactAnalysis) {
    return {
      jobId,
      instanceId,
      remoteJid: analysis.remoteJid,
      canonicalJid: analysis.canonicalJid,
      phoneJid: analysis.phoneJid,
      lidJid: analysis.lidJid,
      canonicalIdentityType: analysis.canonicalIdentityType,
      identityResolutionStatus: analysis.identityResolutionStatus,
      pushName: analysis.pushName,
      classification: analysis.classification,
      suggestedAction: analysis.suggestedAction,
      selectedAction: analysis.selectedAction,
      executionStatus: analysis.executionStatus,
      hasLidAlias: analysis.hasLidAlias,
      isSafeDirectImport: analysis.isSafeDirectImport,
      unsafeReasons: this.toJsonInput(analysis.unsafeReasons),
      evolutionMessageCount: analysis.evolutionMessageCount,
      chatwootMessageCount: analysis.chatwootMessageCount,
      overlapCount: analysis.overlapCount,
      chatwootContactId: analysis.chatwootContactId,
      existingConversationId: analysis.existingConversationId,
      selectedConversationId: analysis.selectedConversationId,
      rebuiltConversationId: analysis.rebuiltConversationId,
      candidateConversationIds: this.toJsonInput(analysis.candidateConversationIds),
      report: this.toJsonInput(analysis.report),
    };
  }

  private mapExecutionContact(contact: any): PersistedContactRow {
    return {
      remoteJid: contact.remoteJid,
      canonicalJid: contact.canonicalJid,
      phoneJid: contact.phoneJid,
      lidJid: contact.lidJid,
      canonicalIdentityType: contact.canonicalIdentityType,
      identityResolutionStatus: contact.identityResolutionStatus,
      pushName: contact.pushName,
      classification: contact.classification,
      suggestedAction: contact.suggestedAction,
      selectedAction: contact.selectedAction,
      executionStatus: contact.executionStatus,
      hasLidAlias: contact.hasLidAlias,
      isSafeDirectImport: !!contact.isSafeDirectImport,
      unsafeReasons: Array.isArray(contact.unsafeReasons) ? contact.unsafeReasons : [],
      evolutionMessageCount: contact.evolutionMessageCount,
      chatwootMessageCount: contact.chatwootMessageCount,
      overlapCount: contact.overlapCount,
      chatwootContactId: contact.chatwootContactId ? Number(contact.chatwootContactId) : null,
      existingConversationId: contact.existingConversationId ? Number(contact.existingConversationId) : null,
      selectedConversationId: contact.selectedConversationId ? Number(contact.selectedConversationId) : null,
      rebuiltConversationId: contact.rebuiltConversationId ? Number(contact.rebuiltConversationId) : null,
      candidateConversationIds: Array.isArray(contact.candidateConversationIds) ? contact.candidateConversationIds : [],
      report: this.asObject(contact.report),
    };
  }

  private buildSummary(
    rows: PersistedContactRow[],
    includeExecution = false,
    mode?: Exclude<JobMode, 'dryRun'>,
  ): JobSummary {
    const summary: JobSummary = {
      totalContacts: rows.length,
      safeDirectImport: 0,
      eligible: 0,
      needsReview: 0,
      lidAlias: 0,
      requiresRebuild: 0,
      ignored: 0,
      totalsByClassification: {
        eligible: 0,
        needs_review: 0,
        lid_alias: 0,
        requires_rebuild: 0,
        ignored: 0,
      },
      totalsBySuggestedAction: {
        import_direct: 0,
        create_rebuild: 0,
        open_chatwoot: 0,
        ignore: 0,
      },
    };

    rows.forEach((row) => {
      if (row.isSafeDirectImport) {
        summary.safeDirectImport += 1;
      }
      summary.totalsByClassification[row.classification] += 1;
      summary.totalsBySuggestedAction[row.suggestedAction] += 1;
    });

    summary.eligible = summary.totalsByClassification.eligible;
    summary.needsReview = summary.totalsByClassification.needs_review;
    summary.lidAlias = summary.totalsByClassification.lid_alias;
    summary.requiresRebuild = summary.totalsByClassification.requires_rebuild;
    summary.ignored = summary.totalsByClassification.ignored;

    if (includeExecution) {
      const totalsByExecutionStatus: Record<ExecutionStatus, number> = {
        pending: 0,
        completed: 0,
        failed: 0,
        skipped: 0,
      };
      rows.forEach((row) => {
        totalsByExecutionStatus[row.executionStatus] += 1;
      });

      summary.totalsByExecutionStatus = totalsByExecutionStatus;
      summary.completed = totalsByExecutionStatus.completed;
      summary.failed = totalsByExecutionStatus.failed;
      summary.skipped = totalsByExecutionStatus.skipped;

      if (mode === 'importDirect') {
        summary.imported = totalsByExecutionStatus.completed;
      }
      if (mode === 'rebuild') {
        summary.rebuilt = totalsByExecutionStatus.completed;
      }
    }

    return summary;
  }

  private resolveExecutionJobStatus(summary: JobSummary): JobStatus {
    const completed = summary.completed || 0;
    const failed = summary.failed || 0;
    const skipped = summary.skipped || 0;

    if (failed === 0 && skipped === 0) {
      return 'completed';
    }

    if (completed > 0) {
      return 'partial';
    }

    return failed > 0 ? 'failed' : 'partial';
  }

  private resolveIdentityMetadata(resolved: ReturnType<typeof resolveCanonicalJid>) {
    const canonicalIdentityType: CanonicalIdentityType = resolved.phoneJid ? 's_whatsapp_net' : 'unresolved';
    const identityResolutionStatus: IdentityResolutionStatus = resolved.phoneJid
      ? 'resolved'
      : resolved.lidJid
        ? 'alias_only'
        : 'ambiguous';

    return {
      canonicalIdentityType,
      identityResolutionStatus,
    };
  }

  private buildReviewPayload(
    provider: ChatwootModel,
    inboxId: number | null,
    chatwootContactId: number | null,
    chatwootConversationId: number | null,
  ): ChatwootReviewPayload {
    const fallbackUrl = this.buildInboxUrl(provider);
    let reviewUrl = fallbackUrl;

    if (chatwootConversationId) {
      reviewUrl = this.buildConversationUrl(provider, chatwootConversationId);
    } else if (chatwootContactId) {
      reviewUrl = this.buildContactUrl(provider, chatwootContactId);
    }

    return {
      chatwootAccountId: provider?.accountId || null,
      chatwootInboxId: inboxId,
      chatwootContactId,
      chatwootConversationId,
      chatwootReviewUrl: reviewUrl,
      chatwootFallbackUrl: fallbackUrl,
    };
  }

  private buildContactReport(args: {
    remoteJid: string;
    resolved: ReturnType<typeof resolveCanonicalJid>;
    pushName: string | null;
    classification: ContactClassification;
    suggestedAction: SuggestedAction;
    selectedAction: SelectedAction | null;
    executionStatus: ExecutionStatus;
    hasLidAlias: boolean;
    isSafeDirectImport: boolean;
    unsafeReasons: UnsafeReason[];
    evolutionMessageCount: number;
    chatwootMessageCount: number;
    overlapCount: number;
    candidateConversationIds: number[];
    matchedCanonicalSourceIds: string[];
    matchedFallbackSignatures: string[];
    sourceIdCollisionRisk: boolean;
    firstMessageTimestamp: number | null;
    lastMessageTimestamp: number | null;
    chatwootFirstMessageAt: string | null;
    chatwootLastMessageAt: string | null;
    canonicalIdentityType: CanonicalIdentityType;
    identityResolutionStatus: IdentityResolutionStatus;
    reviewPayload: ChatwootReviewPayload;
  }) {
    return {
      aliases: getJidAliases({
        remoteJid: args.remoteJid,
        canonicalJid: args.resolved.canonicalJid,
        phoneJid: args.resolved.phoneJid,
        lidJid: args.resolved.lidJid,
      }),
      diagnosis: {
        classification: args.classification,
        suggestedAction: args.suggestedAction,
        isSafeDirectImport: args.isSafeDirectImport,
        unsafeReasons: args.unsafeReasons,
        canonicalIdentityType: args.canonicalIdentityType,
        identityResolutionStatus: args.identityResolutionStatus,
      },
      decision: {
        suggestedAction: args.suggestedAction,
        appliedAction: args.selectedAction,
      },
      evidence: {
        hasLidAlias: args.hasLidAlias,
        candidateConversationIds: args.candidateConversationIds,
        matchedCanonicalSourceIds: args.matchedCanonicalSourceIds,
        matchedFallbackSignatures: args.matchedFallbackSignatures,
        sourceIdCollisionRisk: args.sourceIdCollisionRisk,
      },
      overlapMetrics: {
        evolutionMessageCount: args.evolutionMessageCount,
        chatwootMessageCount: args.chatwootMessageCount,
        overlapCount: args.overlapCount,
      },
      timeWindows: {
        evolution: {
          firstMessageTimestamp: args.firstMessageTimestamp,
          lastMessageTimestamp: args.lastMessageTimestamp,
        },
        chatwoot: {
          firstMessageAt: args.chatwootFirstMessageAt,
          lastMessageAt: args.chatwootLastMessageAt,
        },
      },
      review: args.reviewPayload,
      dedupeStrategy: {
        canonicalSourceId: 'wa:<id>',
        equivalentSourceIds: ['WAID:<id>', 'evo:wa:<id>', 'wa:<id>'],
        fallback: 'created_at_epoch + direction + normalized_content',
        collisionPreference: 'prefer_chatwoot',
      },
      executor: this.getExecutorDescriptor(),
      execution: {
        status: args.executionStatus,
      },
      chatwootConversationUrl: args.reviewPayload.chatwootConversationId ? args.reviewPayload.chatwootReviewUrl : null,
      rebuiltConversationUrl: null,
    };
  }

  private mergeExecutionIntoReport(
    report: Record<string, unknown>,
    args: {
      selectedAction: SelectedAction | 'ignore';
      executionStatus: ExecutionStatus;
      executionError: string | null;
      rebuiltConversationId: number | null;
      reviewPayload: ChatwootReviewPayload;
      executionWarning?: string | null;
    },
  ) {
    const existingDecision = this.asObject(report.decision);
    const execution = this.asObject(report.execution);

    return {
      ...report,
      decision: {
        ...existingDecision,
        appliedAction: args.selectedAction,
        appliedAt: new Date().toISOString(),
      },
      execution: {
        ...execution,
        status: args.executionStatus,
        error: args.executionError,
        warning: args.executionWarning || execution.warning || null,
        finishedAt: new Date().toISOString(),
      },
      review: args.reviewPayload,
      chatwootConversationUrl: args.reviewPayload.chatwootConversationId ? args.reviewPayload.chatwootReviewUrl : null,
      rebuiltConversationUrl: args.rebuiltConversationId
        ? this.buildConversationUrlFromReview(args.reviewPayload, args.rebuiltConversationId)
        : report.rebuiltConversationUrl || null,
    };
  }

  private buildJobReport(rows: PersistedContactRow[], options: JobReportOptions) {
    const summary = this.buildSummary(
      rows,
      options.mode !== 'dryRun',
      options.mode === 'dryRun' ? undefined : options.mode,
    );

    return {
      aggregated: {
        classifications: summary.totalsByClassification,
        suggestedActions: summary.totalsBySuggestedAction,
        executionStatuses: summary.totalsByExecutionStatus || null,
        safeDirectImport: summary.safeDirectImport,
      },
      execution: {
        mode: options.mode,
        jobStatus: options.jobStatus,
        sourceJobId: options.sourceJobId || null,
        selectionMode: options.selectionMode || null,
        startedAt: options.startedAt || null,
        finishedAt: options.finishedAt || null,
      },
      csv: {
        fileName: this.getCsvFileName(options.jobId),
      },
      target: {
        chatwootAccountId: options.provider?.accountId || null,
        chatwootInboxId: options.inboxId,
      },
      dependencies: [this.getPaginationDependency()],
      executor: this.getExecutorDescriptor(),
    };
  }

  private extractReviewPayload(report: unknown): ChatwootReviewPayload {
    const reportData = this.asObject(report);
    const review = this.asObject(reportData.review);

    return {
      chatwootAccountId: review.chatwootAccountId || null,
      chatwootInboxId: review.chatwootInboxId ? Number(review.chatwootInboxId) : null,
      chatwootContactId: review.chatwootContactId ? Number(review.chatwootContactId) : null,
      chatwootConversationId: review.chatwootConversationId ? Number(review.chatwootConversationId) : null,
      chatwootReviewUrl:
        review.chatwootReviewUrl || reportData.rebuiltConversationUrl || reportData.chatwootConversationUrl || null,
      chatwootFallbackUrl: review.chatwootFallbackUrl || null,
    };
  }

  private getExecutorDescriptor(): ExecutorDescriptor {
    return {
      kind: 'embedded_chatwoot_db_import_helper',
      manualFirst: true,
      officialChatwootExecutor: false,
      writesDirectlyToChatwootDatabase: true,
    };
  }

  private getPaginationDependency(): DependencyNotice {
    return {
      code: 'chatwoot_pagination_compound_cursor_recommended',
      level: 'warning',
      message:
        'O History Import funciona na v1, mas a correcao estrutural recomendada no Chatwoot continua sendo cursor composto por (created_at, id).',
    };
  }

  private getCsvFileName(jobId: string) {
    return `chatwoot-history-${jobId}.csv`;
  }

  private isApiInbox(inbox: any) {
    const channelType = String(inbox?.channel_type || inbox?.channelType || inbox?.type || '').toLowerCase();
    return !channelType || channelType.includes('api');
  }

  private buildConversationUrl(provider: ChatwootModel, conversationId: number) {
    return `${this.normalizeBaseUrl(provider.url)}/app/accounts/${provider.accountId}/conversations/${conversationId}`;
  }

  private buildContactUrl(provider: ChatwootModel, contactId: number) {
    return `${this.normalizeBaseUrl(provider.url)}/app/accounts/${provider.accountId}/contacts/${contactId}`;
  }

  private buildInboxUrl(provider: ChatwootModel) {
    return `${this.normalizeBaseUrl(provider.url)}/app/accounts/${provider.accountId}/settings/inboxes`;
  }

  private buildConversationUrlFromReview(reviewPayload: ChatwootReviewPayload, conversationId: number) {
    if (reviewPayload.chatwootReviewUrl && reviewPayload.chatwootReviewUrl.includes('/app/accounts/')) {
      const base = reviewPayload.chatwootReviewUrl.split('/app/accounts/')[0];
      return `${base}/app/accounts/${reviewPayload.chatwootAccountId}/conversations/${conversationId}`;
    }

    if (reviewPayload.chatwootFallbackUrl && reviewPayload.chatwootFallbackUrl.includes('/app/accounts/')) {
      const base = reviewPayload.chatwootFallbackUrl.split('/app/accounts/')[0];
      return `${base}/app/accounts/${reviewPayload.chatwootAccountId}/conversations/${conversationId}`;
    }

    return reviewPayload.chatwootReviewUrl;
  }

  private normalizeBaseUrl(url: string) {
    return String(url || '').replace(/\/$/, '');
  }

  private asObject(value: unknown): Record<string, any> {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return value as Record<string, any>;
    }

    return {};
  }

  private toCsvCell(value: unknown) {
    if (value === null || value === undefined) {
      return '""';
    }

    return `"${String(value).replace(/"/g, '""')}"`;
  }

  private toJsonInput(value: unknown) {
    if (value === null || value === undefined) {
      return {} as Prisma.InputJsonValue;
    }

    return value as Prisma.InputJsonValue;
  }
}
