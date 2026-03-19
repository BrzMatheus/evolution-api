import { InstanceDto } from '@api/dto/instance.dto';
import { Options, Quoted, SendAudioDto, SendMediaDto, SendTextDto } from '@api/dto/sendMessage.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { postgresClient } from '@api/integrations/chatbot/chatwoot/libs/postgres.client';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import { PrismaRepository } from '@api/repository/repository.service';
import { CacheService } from '@api/services/cache.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { Events } from '@api/types/wa.types';
import { Chatwoot, ConfigService, Database, HttpServer } from '@config/env.config';
import { Logger } from '@config/logger.config';
import ChatwootClient, {
  ChatwootAPIConfig,
  contact,
  contact_inboxes,
  conversation,
  conversation_show,
  generic_id,
  inbox,
} from '@figuro/chatwoot-sdk';
import { request as chatwootRequest } from '@figuro/chatwoot-sdk/dist/core/request';
import { Chatwoot as ChatwootModel, Contact as ContactModel, Message as MessageModel } from '@prisma/client';
import i18next from '@utils/i18n';
import { sendTelemetry } from '@utils/sendTelemetry';
import {
  getChatwootIdentifier,
  getChatwootPhoneNumber,
  getJidAliases,
  isTechnicalDisplayName,
  resolveCanonicalJid,
  resolveChatwootDisplayName,
} from '@utils/whatsapp-jid';
import axios from 'axios';
import { WAMessageContent, WAMessageKey } from 'baileys';
import dayjs from 'dayjs';
import FormData from 'form-data';
import { Jimp, JimpMime } from 'jimp';
import { parsePhoneNumberFromString } from 'libphonenumber-js';
import Long from 'long';
import mimeTypes from 'mime-types';
import path from 'path';
import { Readable } from 'stream';

interface ChatwootMessage {
  messageId?: number;
  inboxId?: number;
  conversationId?: number;
  contactInboxSourceId?: string;
  isRead?: boolean;
}

export interface ChatwootConversationCandidate {
  internalId: number;
  displayId: number;
  inboxId: number;
  status: 'open' | 'resolved' | 'pending' | 'snoozed' | 'unknown';
  messageCount: number;
  attachmentMessageCount: number;
  firstMessageAt: string | null;
  lastMessageAt: string | null;
  lastActivityAt: string | null;
}

export class ChatwootService {
  private readonly logger = new Logger('ChatwootService');
  private readonly CHATWOOT_STATUS_OPEN = 0;
  private readonly CHATWOOT_STATUS_RESOLVED = 1;

  // Lock polling delay
  private readonly LOCK_POLLING_DELAY_MS = 300; // Delay between lock status checks

  private provider: any;

  constructor(
    private readonly waMonitor: WAMonitoringService,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly cache: CacheService,
  ) {}

  private pgClient = postgresClient.getChatwootConnection();

  public async getProvider(instance: InstanceDto): Promise<ChatwootModel | null> {
    const cacheKey = `${instance.instanceName}:getProvider`;
    if (await this.cache.has(cacheKey)) {
      const provider = (await this.cache.get(cacheKey)) as ChatwootModel;

      return provider;
    }

    const provider = await this.waMonitor.waInstances[instance.instanceName]?.findChatwoot();

    if (!provider) {
      this.logger.warn('provider not found');
      return null;
    }

    this.cache.set(cacheKey, provider);

    return provider;
  }

  private async clientCw(instance: InstanceDto) {
    const provider = await this.getProvider(instance);

    if (!provider) {
      this.logger.error('provider not found');
      return null;
    }

    this.provider = provider;

    const client = new ChatwootClient({
      config: this.getClientCwConfig(),
    });

    return client;
  }

  public getClientCwConfig(): ChatwootAPIConfig & { nameInbox: string; mergeBrazilContacts: boolean } {
    return {
      basePath: this.provider.url,
      with_credentials: true,
      credentials: 'include',
      token: this.provider.token,
      nameInbox: this.provider.nameInbox,
      mergeBrazilContacts: this.provider.mergeBrazilContacts,
    };
  }

  public getCache() {
    return this.cache;
  }

  public async create(instance: InstanceDto, data: ChatwootDto) {
    await this.waMonitor.waInstances[instance.instanceName].setChatwoot(data);

    if (data.autoCreate) {
      this.logger.log('Auto create chatwoot instance');
      const urlServer = this.configService.get<HttpServer>('SERVER').URL;

      await this.initInstanceChatwoot(
        instance,
        data.nameInbox ?? instance.instanceName.split('-cwId-')[0],
        `${urlServer}/chatwoot/webhook/${encodeURIComponent(instance.instanceName)}`,
        true,
        data.number,
        data.organization,
        data.logo,
      );
    }
    return data;
  }

  public async find(instance: InstanceDto): Promise<ChatwootDto> {
    try {
      return await this.waMonitor.waInstances[instance.instanceName].findChatwoot();
    } catch {
      this.logger.error('chatwoot not found');
      return { enabled: null, url: '' };
    }
  }

  public async getContact(instance: InstanceDto, id: number) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!id) {
      this.logger.warn('id is required');
      return null;
    }

    const contact = await client.contact.getContactable({
      accountId: this.provider.accountId,
      id,
    });

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    return contact;
  }

  public async initInstanceChatwoot(
    instance: InstanceDto,
    inboxName: string,
    webhookUrl: string,
    qrcode: boolean,
    number: string,
    organization?: string,
    logo?: string,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const findInbox: any = await client.inboxes.list({
      accountId: this.provider.accountId,
    });

    const checkDuplicate = findInbox.payload.map((inbox) => inbox.name).includes(inboxName);

    let inboxId: number;

    this.logger.log('Creating chatwoot inbox');
    if (!checkDuplicate) {
      const data = {
        type: 'api',
        webhook_url: webhookUrl,
      };

      const inbox = await client.inboxes.create({
        accountId: this.provider.accountId,
        data: {
          name: inboxName,
          channel: data as any,
        },
      });

      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      inboxId = inbox.id;
    } else {
      const inbox = findInbox.payload.find((inbox) => inbox.name === inboxName);

      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      inboxId = inbox.id;
    }
    this.logger.log(`Inbox created - inboxId: ${inboxId}`);

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');

      return true;
    }

    this.logger.log('Creating chatwoot bot contact');
    const contact =
      (await this.findContact(instance, '123456')) ||
      ((await this.createContact(
        instance,
        '123456',
        inboxId,
        false,
        organization ? organization : 'EvolutionAPI',
        logo ? logo : 'https://evolution-api.com/files/evolution-api-favicon.png',
      )) as any);

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const contactId = contact.id || contact.payload.contact.id;
    this.logger.log(`Contact created - contactId: ${contactId}`);

    if (qrcode) {
      this.logger.log('QR code enabled');
      const data = {
        contact_id: contactId.toString(),
        inbox_id: inboxId.toString(),
      };

      const conversation = await client.conversations.create({
        accountId: this.provider.accountId,
        data,
      });

      if (!conversation) {
        this.logger.warn('conversation not found');
        return null;
      }

      let contentMsg = 'init';

      if (number) {
        contentMsg = `init:${number}`;
      }

      const message = await client.messages.create({
        accountId: this.provider.accountId,
        conversationId: conversation.id,
        data: {
          content: contentMsg,
          message_type: 'outgoing',
        },
      });

      if (!message) {
        this.logger.warn('conversation not found');
        return null;
      }
      this.logger.log('Init message sent');
    }

    return true;
  }

  public async createContact(
    instance: InstanceDto,
    phoneNumber: string,
    inboxId: number,
    isGroup: boolean,
    name?: string,
    avatar_url?: string,
    jid?: string,
  ) {
    try {
      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      let data: any = {};
      if (!isGroup) {
        data = {
          inbox_id: inboxId,
          name: name || phoneNumber,
          identifier: jid,
          avatar_url: avatar_url,
        };

        if ((jid && jid.includes('@')) || !jid) {
          data['phone_number'] = `+${phoneNumber}`;
        }
      } else {
        data = {
          inbox_id: inboxId,
          name: name || phoneNumber,
          identifier: phoneNumber,
          avatar_url: avatar_url,
        };
      }

      const contact = await client.contacts.create({
        accountId: this.provider.accountId,
        data,
      });

      if (!contact) {
        this.logger.warn('contact not found');
        return null;
      }

      const findContact = await this.findContact(instance, phoneNumber);

      const contactId = findContact?.id;

      await this.addLabelToContact(this.provider.nameInbox, contactId);

      return contact;
    } catch (error) {
      if ((error.status === 422 || error.response?.status === 422) && jid) {
        this.logger.warn(`Contact with identifier ${jid} creation failed (422). Checking if it already exists...`);
        const existingContact = await this.findContactByIdentifier(instance, jid);
        if (existingContact) {
          const contactId = existingContact.id;
          await this.addLabelToContact(this.provider.nameInbox, contactId);
          return existingContact;
        }
      }

      this.logger.error('Error creating contact');
      console.log(error);
      return null;
    }
  }

  public async updateContact(instance: InstanceDto, id: number, data: any) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!id) {
      this.logger.warn('id is required');
      return null;
    }

    try {
      const contact = await client.contacts.update({
        accountId: this.provider.accountId,
        id,
        data,
      });

      return contact;
    } catch {
      return null;
    }
  }

  private getResponsePayload<T = any>(response: any): T[] {
    if (Array.isArray(response?.payload)) {
      return response.payload;
    }

    if (Array.isArray(response?.data?.payload)) {
      return response.data.payload;
    }

    return [];
  }

  private selectExactIdentifierContact(contacts: any[], identifier: string) {
    return contacts.find((contact) => contact?.identifier === identifier) || contacts[0] || null;
  }

  public async addLabelToContact(nameInbox: string, contactId: number) {
    try {
      const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI;

      if (!uri) return false;

      const sqlTags = `SELECT id, taggings_count FROM tags WHERE name = $1 LIMIT 1`;
      const tagData = (await this.pgClient.query(sqlTags, [nameInbox]))?.rows[0];
      let tagId = tagData?.id;
      const taggingsCount = tagData?.taggings_count || 0;

      const sqlTag = `INSERT INTO tags (name, taggings_count) 
                      VALUES ($1, $2) 
                      ON CONFLICT (name) 
                      DO UPDATE SET taggings_count = tags.taggings_count + 1 
                      RETURNING id`;

      tagId = (await this.pgClient.query(sqlTag, [nameInbox, taggingsCount + 1]))?.rows[0]?.id;

      const sqlCheckTagging = `SELECT 1 FROM taggings 
                               WHERE tag_id = $1 AND taggable_type = 'Contact' AND taggable_id = $2 AND context = 'labels' LIMIT 1`;

      const taggingExists = (await this.pgClient.query(sqlCheckTagging, [tagId, contactId]))?.rowCount > 0;

      if (!taggingExists) {
        const sqlInsertLabel = `INSERT INTO taggings (tag_id, taggable_type, taggable_id, context, created_at) 
                                VALUES ($1, 'Contact', $2, 'labels', NOW())`;

        await this.pgClient.query(sqlInsertLabel, [tagId, contactId]);
      }

      return true;
    } catch {
      return false;
    }
  }

  public async findContactByIdentifier(instance: InstanceDto, identifier: string) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const filteredContacts = await chatwootRequest(this.getClientCwConfig(), {
      method: 'POST',
      url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`,
      body: {
        payload: [
          {
            attribute_key: 'identifier',
            filter_operator: 'equal_to',
            values: [identifier],
            query_operator: null,
          },
        ],
      },
    });

    const filteredPayload = this.getResponsePayload(filteredContacts);
    if (filteredPayload.length > 0) {
      return this.selectExactIdentifierContact(filteredPayload, identifier);
    }

    const searchedContacts = await client.contacts.search({
      accountId: this.provider.accountId,
      q: identifier,
    });
    const searchedPayload = this.getResponsePayload(searchedContacts);
    if (searchedPayload.length > 0) {
      return this.selectExactIdentifierContact(searchedPayload, identifier);
    }

    return null;
  }

  public async findContact(instance: InstanceDto, phoneNumber: string) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    let query: any;
    const isGroup = phoneNumber.includes('@g.us');

    if (!isGroup) {
      query = `+${phoneNumber}`;
    } else {
      query = phoneNumber;
    }

    let contact: any;

    if (isGroup) {
      contact = await client.contacts.search({
        accountId: this.provider.accountId,
        q: query,
      });
    } else {
      contact = await chatwootRequest(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/contacts/filter`,
        body: {
          payload: this.getFilterPayload(query),
        },
      });
    }

    if (!contact && contact?.payload?.length === 0) {
      this.logger.warn('contact not found');
      return null;
    }

    if (!isGroup) {
      return contact.payload.length > 1 ? this.findContactInContactList(contact.payload, query) : contact.payload[0];
    } else {
      return contact.payload.find((contact) => contact.identifier === query);
    }
  }

  private async mergeContacts(baseId: number, mergeId: number) {
    try {
      const contact = await chatwootRequest(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/actions/contact_merge`,
        body: {
          base_contact_id: baseId,
          mergee_contact_id: mergeId,
        },
      });

      return contact;
    } catch {
      this.logger.error('Error merging contacts');
      return null;
    }
  }

  private async mergeBrazilianContacts(contacts: any[]) {
    try {
      const contact = await chatwootRequest(this.getClientCwConfig(), {
        method: 'POST',
        url: `/api/v1/accounts/${this.provider.accountId}/actions/contact_merge`,
        body: {
          base_contact_id: contacts.find((contact) => contact.phone_number.length === 14)?.id,
          mergee_contact_id: contacts.find((contact) => contact.phone_number.length === 13)?.id,
        },
      });

      return contact;
    } catch {
      this.logger.error('Error merging contacts');
      return null;
    }
  }

  private findContactInContactList(contacts: any[], query: string) {
    const phoneNumbers = this.getNumbers(query);
    const searchableFields = this.getSearchableFields();

    // eslint-disable-next-line prettier/prettier
    if (
      contacts.length === 2 &&
      this.getClientCwConfig().mergeBrazilContacts &&
      query.startsWith('+55') &&
      !query.includes('@')
    ) {
      const contact = this.mergeBrazilianContacts(contacts);
      if (contact) {
        return contact;
      }
    }

    const phone = phoneNumbers.reduce(
      (savedNumber, number) => (number.length > savedNumber.length ? number : savedNumber),
      '',
    );

    const contact_with9 = contacts.find((contact) => contact.phone_number === phone);
    if (contact_with9) {
      return contact_with9;
    }

    for (const contact of contacts) {
      for (const field of searchableFields) {
        if (contact[field] && phoneNumbers.includes(contact[field])) {
          return contact;
        }
      }
    }

    return null;
  }

  private getNumbers(query: string) {
    const numbers = [query];

    if (query.startsWith('+55') && query.length === 14) {
      return numbers;
    } else if (query.startsWith('+55') && query.length === 13) {
      const withNine = query.slice(0, 5) + '9' + query.slice(5);
      numbers.push(withNine);
    }

    return numbers;
  }

  private getSearchableFields() {
    return ['phone_number'];
  }

  private getFilterPayload(query: string) {
    const filterPayload = [];

    const numbers = this.getNumbers(query);
    const fieldsToSearch = this.getSearchableFields();

    fieldsToSearch.forEach((field, index1) => {
      numbers.forEach((number, index2) => {
        const queryOperator = fieldsToSearch.length - 1 === index1 && numbers.length - 1 === index2 ? null : 'OR';
        filterPayload.push({
          attribute_key: field,
          filter_operator: 'equal_to',
          values: [number.replace('+', '')],
          query_operator: queryOperator,
        });
      });
    });

    return filterPayload;
  }

  private resolveWhatsappIdentity(body: any) {
    const key = body?.key || {};
    const resolved = resolveCanonicalJid(key);
    const canonicalIdentifier = getChatwootIdentifier(key);
    const phoneNumber = getChatwootPhoneNumber(key);

    return {
      ...resolved,
      canonicalIdentifier,
      phoneNumber,
      aliases: getJidAliases(key),
      isGroup: resolved.isGroup,
    };
  }

  private getConversationCacheKey(instance: InstanceDto, identifier: string) {
    return `${instance.instanceName}:createConversation-${identifier}`;
  }

  private getConversationLockKey(instance: InstanceDto, identifier: string) {
    return `${instance.instanceName}:lock:createConversation-${identifier}`;
  }

  private sortConversationsByRecency<T extends { id?: number | null }>(conversations: T[]) {
    return [...conversations].sort((left, right) => (right?.id || 0) - (left?.id || 0));
  }

  private normalizeConversationStatus(status: number | string | null | undefined) {
    switch (Number(status)) {
      case 0:
        return 'open';
      case 1:
        return 'resolved';
      case 2:
        return 'pending';
      case 3:
        return 'snoozed';
      default:
        return 'unknown';
    }
  }

  private pickPreferredInboxConversation(
    conversations: conversation[],
    inboxId: number,
    options?: {
      allowResolvedFallback?: boolean;
    },
  ) {
    const inboxConversations = this.sortConversationsByRecency(
      conversations.filter((item) => item?.inbox_id === inboxId),
    );
    const activeConversation = inboxConversations.find((item) => item?.status !== 'resolved');

    if (activeConversation) {
      return activeConversation;
    }

    return options?.allowResolvedFallback === false ? null : inboxConversations[0] || null;
  }

  public async setConversationCacheForIdentifiers(
    instance: InstanceDto,
    identifiers: (string | null | undefined)[],
    conversationId: number,
    ttlSeconds = 1800,
  ) {
    const uniqueIdentifiers = Array.from(new Set(identifiers.filter(Boolean)));

    await Promise.all(
      uniqueIdentifiers.map((identifier) =>
        this.cache.set(this.getConversationCacheKey(instance, String(identifier)), conversationId, ttlSeconds),
      ),
    );
  }

  private async syncCanonicalIdentifier(
    instance: InstanceDto,
    identity: ReturnType<ChatwootService['resolveWhatsappIdentity']>,
  ) {
    if (!identity.canonicalIdentifier || !identity.phoneNumber || identity.isGroup || !identity.lidJid) {
      return null;
    }

    const phoneDigits = identity.phoneNumber.split('@')[0].split(':')[0];
    const canonicalContact = await this.findContactByIdentifier(instance, identity.canonicalIdentifier);
    if (canonicalContact) {
      return canonicalContact;
    }

    const phoneContact = await this.findContact(instance, phoneDigits);
    if (!phoneContact || phoneContact.identifier === identity.canonicalIdentifier) {
      return phoneContact;
    }

    this.logger.verbose(
      `Identifier promotion detected: ${phoneContact.identifier} -> ${identity.canonicalIdentifier} (phone: ${identity.phoneNumber})`,
    );

    const updatedContact: any = await this.updateContact(instance, phoneContact.id, {
      identifier: identity.canonicalIdentifier,
      phone_number: `+${phoneDigits}`,
    });

    for (const alias of identity.aliases) {
      await this.cache.delete(this.getConversationCacheKey(instance, alias));
    }

    if (updatedContact === null) {
      const baseContact = await this.findContactByIdentifier(instance, identity.canonicalIdentifier);
      if (baseContact && baseContact.id !== phoneContact.id) {
        await this.mergeContacts(baseContact.id, phoneContact.id);
        this.logger.verbose(
          `Merge contacts after identifier promotion: (${baseContact.id}) ${baseContact.identifier} and (${phoneContact.id}) ${phoneContact.identifier}`,
        );
        return baseContact;
      }
    }

    return updatedContact || phoneContact;
  }

  private async findContactByWhatsappIdentity(
    instance: InstanceDto,
    identity: ReturnType<ChatwootService['resolveWhatsappIdentity']>,
  ) {
    for (const alias of identity.aliases) {
      const byIdentifier = await this.findContactByIdentifier(instance, alias);
      if (byIdentifier) {
        return byIdentifier;
      }
    }

    const phoneDigits = identity.phoneNumber?.split('@')[0].split(':')[0];
    if (phoneDigits) {
      return await this.findContact(instance, phoneDigits);
    }

    return null;
  }

  public async createConversation(instance: InstanceDto, body: any) {
    const identity = this.resolveWhatsappIdentity(body);
    const isGroup = identity.isGroup;
    const phoneNumber = identity.phoneNumber || identity.canonicalIdentifier || body.key.remoteJid;
    const remoteJid = identity.canonicalIdentifier || body.key.remoteJid;
    const cacheKey = this.getConversationCacheKey(instance, remoteJid);
    const lockKey = this.getConversationLockKey(instance, remoteJid);
    const maxWaitTime = 5000; // 5 seconds
    const client = await this.clientCw(instance);
    if (!client) return null;

    try {
      // Processa atualização de contatos já criados @lid
      if (!isGroup) {
        await this.syncCanonicalIdentifier(instance, identity);
      }
      this.logger.verbose(`--- Start createConversation ---`);
      this.logger.verbose(`Instance: ${JSON.stringify(instance)}`);

      // If it already exists in the cache, return conversationId
      if (await this.cache.has(cacheKey)) {
        const conversationId = (await this.cache.get(cacheKey)) as number;
        this.logger.verbose(`Found conversation to: ${remoteJid}, conversation ID: ${conversationId}`);
        let conversationExists: any;
        try {
          conversationExists = await client.conversations.get({
            accountId: this.provider.accountId,
            conversationId: conversationId,
          });
          this.logger.verbose(
            `Conversation exists: ID: ${conversationExists.id} - Name: ${conversationExists.meta.sender.name} - Identifier: ${conversationExists.meta.sender.identifier}`,
          );
        } catch (error) {
          this.logger.error(`Error getting conversation: ${error}`);
          conversationExists = false;
        }
        if (!conversationExists || conversationExists?.status === 'resolved') {
          this.logger.verbose('Conversation does not exist, re-calling createConversation');
          this.cache.delete(cacheKey);
          return await this.createConversation(instance, body);
        }
        return conversationId;
      }

      // If lock already exists, wait until release or timeout
      if (await this.cache.has(lockKey)) {
        this.logger.verbose(`Operação de criação já em andamento para ${remoteJid}, aguardando resultado...`);
        const start = Date.now();
        while (await this.cache.has(lockKey)) {
          if (Date.now() - start > maxWaitTime) {
            this.logger.warn(`Timeout aguardando lock para ${remoteJid}`);
            break;
          }
          await new Promise((res) => setTimeout(res, this.LOCK_POLLING_DELAY_MS));
          if (await this.cache.has(cacheKey)) {
            const conversationId = (await this.cache.get(cacheKey)) as number;
            this.logger.verbose(`Resolves creation of: ${remoteJid}, conversation ID: ${conversationId}`);
            return conversationId;
          }
        }
      }

      // Adquire lock
      await this.cache.set(lockKey, true, 30);
      this.logger.verbose(`Bloqueio adquirido para: ${lockKey}`);

      try {
        /*
        Double check after lock
        Utilizei uma nova verificação para evitar que outra thread execute entre o terminio do while e o set lock
        */
        if (await this.cache.has(cacheKey)) {
          return (await this.cache.get(cacheKey)) as number;
        }

        const chatId = isGroup ? remoteJid : phoneNumber.split('@')[0].split(':')[0];
        const filterInbox = await this.getInbox(instance);
        if (!filterInbox) return null;

        // Display name for the group contact itself (used in createContact below)
        let groupDisplayName: string | undefined;

        if (isGroup) {
          this.logger.verbose(`Processing group conversation`);
          const group = await this.waMonitor.waInstances[instance.instanceName].client.groupMetadata(chatId);
          this.logger.verbose(`Group metadata: JID:${group.JID} - Subject:${group?.subject || group?.Name}`);

          const participantJid =
            identity.lidJid && !body.key.fromMe && body.key.participantAlt
              ? body.key.participantAlt
              : body.key.participant;
          groupDisplayName = `${group.subject} (GROUP)`;

          const picture_url = await this.waMonitor.waInstances[instance.instanceName].profilePicture(
            participantJid.split('@')[0],
          );
          this.logger.verbose(`Participant profile picture URL: ${JSON.stringify(picture_url)}`);

          const participantPhoneNumber = participantJid.split('@')[0].split(':')[0];
          const findParticipant = await this.findContact(instance, participantPhoneNumber);

          if (findParticipant) {
            this.logger.verbose(
              `Found participant: ID:${findParticipant.id} - Name: ${findParticipant.name} - identifier: ${findParticipant.identifier}`,
            );
            if (isTechnicalDisplayName(findParticipant.name, [participantPhoneNumber])) {
              await this.updateContact(instance, findParticipant.id, {
                name: resolveChatwootDisplayName({
                  pushName: body.pushName,
                  phoneNumber: participantPhoneNumber,
                }),
                avatar_url: picture_url.profilePictureUrl || null,
              });
            }
          } else {
            await this.createContact(
              instance,
              participantPhoneNumber,
              filterInbox.id,
              false,
              resolveChatwootDisplayName({
                pushName: body.pushName,
                phoneNumber: participantPhoneNumber,
              }),
              picture_url.profilePictureUrl || null,
              participantJid,
            );
          }
        }

        const picture_url = await this.waMonitor.waInstances[instance.instanceName].profilePicture(chatId);
        this.logger.verbose(`Contact profile picture URL: ${JSON.stringify(picture_url)}`);

        this.logger.verbose(`Searching contact for identity aliases: ${identity.aliases.join(', ') || remoteJid}`);
        let contact = !isGroup ? await this.findContactByWhatsappIdentity(instance, identity) : null;

        if (contact) {
          this.logger.verbose(`Found contact: ID:${contact.id} - Name:${contact.name}`);
          if (!body.key.fromMe) {
            const waProfilePictureFile =
              picture_url?.profilePictureUrl?.split('#')[0].split('?')[0].split('/').pop() || '';
            const chatwootProfilePictureFile = contact?.thumbnail?.split('#')[0].split('?')[0].split('/').pop() || '';
            const pictureNeedsUpdate = waProfilePictureFile !== chatwootProfilePictureFile;
            const nameNeedsUpdate = isTechnicalDisplayName(contact.name, identity.aliases);
            this.logger.verbose(`Picture needs update: ${pictureNeedsUpdate}`);
            this.logger.verbose(`Name needs update: ${nameNeedsUpdate}`);
            if (pictureNeedsUpdate || nameNeedsUpdate) {
              contact = await this.updateContact(instance, contact.id, {
                ...(nameNeedsUpdate && {
                  name: resolveChatwootDisplayName({
                    pushName: body.pushName,
                    phoneNumber: chatId,
                    currentName: contact.name,
                    identifiers: identity.aliases,
                  }),
                }),
                ...(waProfilePictureFile === '' && { avatar: null }),
                ...(pictureNeedsUpdate && { avatar_url: picture_url?.profilePictureUrl }),
              });
            }
          }
        } else {
          contact = await this.createContact(
            instance,
            chatId,
            filterInbox.id,
            isGroup,
            isGroup
              ? groupDisplayName
              : resolveChatwootDisplayName({
                  pushName: body.pushName,
                  phoneNumber: chatId,
                  identifiers: identity.aliases,
                }),
            picture_url.profilePictureUrl || null,
            remoteJid,
          );
        }

        if (!contact) {
          this.logger.warn(`Contact not created or found`);
          return null;
        }

        const contactId = contact?.payload?.id || contact?.payload?.contact?.id || contact?.id;
        this.logger.verbose(`Contact ID: ${contactId}`);

        const contactConversations = (await client.contacts.listConversations({
          accountId: this.provider.accountId,
          id: contactId,
        })) as any;

        if (!contactConversations || !contactConversations.payload) {
          this.logger.error(`No conversations found or payload is undefined`);
          return null;
        }

        let inboxConversation = this.pickPreferredInboxConversation(contactConversations.payload, filterInbox.id);
        if (inboxConversation) {
          if (this.provider.reopenConversation) {
            const inboxConversationDetails = inboxConversation as any;
            this.logger.verbose(
              `Found conversation in reopenConversation mode: ID: ${inboxConversation.id} - Name: ${inboxConversationDetails?.meta?.sender?.name} - Identifier: ${inboxConversationDetails?.meta?.sender?.identifier}`,
            );
            if (inboxConversation && this.provider.conversationPending && inboxConversation.status !== 'open') {
              await client.conversations.toggleStatus({
                accountId: this.provider.accountId,
                conversationId: inboxConversation.id,
                data: {
                  status: 'pending',
                },
              });
            }
          } else {
            inboxConversation = this.pickPreferredInboxConversation(contactConversations.payload, filterInbox.id, {
              allowResolvedFallback: false,
            });
            this.logger.verbose(`Found conversation: ${JSON.stringify(inboxConversation)}`);
          }

          if (inboxConversation) {
            this.logger.verbose(`Returning existing conversation ID: ${inboxConversation.id}`);
            await this.setConversationCacheForIdentifiers(instance, identity.aliases, inboxConversation.id, 1800);
            return inboxConversation.id;
          }
        }

        const data = {
          contact_id: contactId.toString(),
          inbox_id: filterInbox.id.toString(),
        };

        if (this.provider.conversationPending) {
          data['status'] = 'pending';
        }

        const conversation = await client.conversations.create({
          accountId: this.provider.accountId,
          data,
        });

        if (!conversation) {
          this.logger.warn(`Conversation not created or found`);
          return null;
        }

        this.logger.verbose(`New conversation created of ${remoteJid} with ID: ${conversation.id}`);
        await this.setConversationCacheForIdentifiers(instance, identity.aliases, conversation.id, 1800);
        return conversation.id;
      } finally {
        await this.cache.delete(lockKey);
        this.logger.verbose(`Block released for: ${lockKey}`);
      }
    } catch (error) {
      this.logger.error(`Error in createConversation: ${error}`);
      return null;
    }
  }

  public async getInbox(instance: InstanceDto): Promise<inbox | null> {
    const cacheKey = `${instance.instanceName}:getInbox`;
    if (await this.cache.has(cacheKey)) {
      return (await this.cache.get(cacheKey)) as inbox;
    }

    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const inbox = (await client.inboxes.list({
      accountId: this.provider.accountId,
    })) as any;

    if (!inbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const findByName = inbox.payload.find((inbox) => inbox.name === this.getClientCwConfig().nameInbox);

    if (!findByName) {
      this.logger.warn('inbox not found');
      return null;
    }

    this.cache.set(cacheKey, findByName);
    return findByName;
  }

  public async createMessage(
    instance: InstanceDto,
    conversationId: number,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    privateMessage?: boolean,
    attachments?: {
      content: unknown;
      encoding: string;
      filename: string;
    }[],
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: MessageModel,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const replyToIds = await this.getReplyToIds(messageBody, instance);

    const sourceReplyId = quotedMsg?.chatwootMessageId || null;

    const message = await client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversationId,
      data: {
        content: content,
        message_type: messageType,
        attachments: attachments,
        private: privateMessage || false,
        source_id: sourceId,
        content_attributes: {
          ...replyToIds,
        },
        source_reply_id: sourceReplyId ? sourceReplyId.toString() : null,
      },
    });

    if (!message) {
      this.logger.warn('message not found');
      return null;
    }

    return message;
  }

  public async getOpenConversationByContact(
    instance: InstanceDto,
    inbox: inbox,
    contact: generic_id & contact,
  ): Promise<conversation> {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const conversations = (await client.contacts.listConversations({
      accountId: this.provider.accountId,
      id: contact.id,
    })) as any;

    return (
      conversations.payload.find(
        (conversation) => conversation.inbox_id === inbox.id && conversation.status === 'open',
      ) || undefined
    );
  }

  public async listContactConversations(instance: InstanceDto, contactId: number): Promise<conversation[]> {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return [];
    }

    const conversations = (await client.contacts.listConversations({
      accountId: this.provider.accountId,
      id: contactId,
    })) as any;

    return Array.isArray(conversations?.payload) ? conversations.payload : [];
  }

  public async getLatestInboxConversation(
    instance: InstanceDto,
    contactId: number,
    inboxId: number,
  ): Promise<conversation | null> {
    const conversations = await this.listContactConversations(instance, contactId);
    return this.pickPreferredInboxConversation(conversations, inboxId);
  }

  public async listInboxConversationCandidates(
    instance: InstanceDto,
    contactId: number,
    inboxId: number,
  ): Promise<ChatwootConversationCandidate[]> {
    const provider = await this.getProvider(instance);
    if (!provider || !contactId || !inboxId) {
      return [];
    }

    const result = (await this.pgClient.query(
      `SELECT conversations.id AS internal_id,
              conversations.display_id,
              conversations.inbox_id,
              conversations.status,
              conversations.last_activity_at,
              COUNT(messages.id)::INTEGER AS message_count,
              COUNT(messages.id) FILTER (
                WHERE COALESCE(messages.content_type, 0) <> 0
              )::INTEGER AS attachment_message_count,
              MIN(messages.created_at) AS first_message_at,
              MAX(messages.created_at) AS last_message_at
         FROM conversations
         LEFT JOIN messages
           ON messages.account_id = conversations.account_id
          AND messages.conversation_id = conversations.id
        WHERE conversations.account_id = $1
          AND conversations.contact_id = $2
          AND conversations.inbox_id = $3
        GROUP BY conversations.id, conversations.display_id, conversations.inbox_id, conversations.status, conversations.last_activity_at
        ORDER BY COALESCE(conversations.last_activity_at, MAX(messages.created_at), MIN(messages.created_at)) DESC NULLS LAST,
                 conversations.id DESC`,
      [provider.accountId, contactId, inboxId],
    )) as {
      rows: {
        internal_id: number;
        display_id: number;
        inbox_id: number;
        status: number;
        last_activity_at: Date | null;
        message_count: number;
        attachment_message_count: number;
        first_message_at: Date | null;
        last_message_at: Date | null;
      }[];
    };

    return (result?.rows || []).map((row) => ({
      internalId: Number(row.internal_id),
      displayId: Number(row.display_id),
      inboxId: Number(row.inbox_id),
      status: this.normalizeConversationStatus(row.status),
      messageCount: Number(row.message_count || 0),
      attachmentMessageCount: Number(row.attachment_message_count || 0),
      firstMessageAt: row.first_message_at ? new Date(row.first_message_at).toISOString() : null,
      lastMessageAt: row.last_message_at ? new Date(row.last_message_at).toISOString() : null,
      lastActivityAt: row.last_activity_at ? new Date(row.last_activity_at).toISOString() : null,
    }));
  }

  public async listContactConversationCandidates(
    instance: InstanceDto,
    contactId: number,
  ): Promise<ChatwootConversationCandidate[]> {
    const provider = await this.getProvider(instance);
    if (!provider || !contactId) {
      return [];
    }

    const result = (await this.pgClient.query(
      `SELECT conversations.id AS internal_id,
              conversations.display_id,
              conversations.inbox_id,
              conversations.status,
              conversations.last_activity_at,
              COUNT(messages.id)::INTEGER AS message_count,
              COUNT(messages.id) FILTER (
                WHERE COALESCE(messages.content_type, 0) <> 0
              )::INTEGER AS attachment_message_count,
              MIN(messages.created_at) AS first_message_at,
              MAX(messages.created_at) AS last_message_at
         FROM conversations
         LEFT JOIN messages
           ON messages.account_id = conversations.account_id
          AND messages.conversation_id = conversations.id
        WHERE conversations.account_id = $1
          AND conversations.contact_id = $2
        GROUP BY conversations.id, conversations.display_id, conversations.inbox_id, conversations.status, conversations.last_activity_at
        ORDER BY COALESCE(conversations.last_activity_at, MAX(messages.created_at), MIN(messages.created_at)) DESC NULLS LAST,
                 conversations.id DESC`,
      [provider.accountId, contactId],
    )) as {
      rows: {
        internal_id: number;
        display_id: number;
        inbox_id: number;
        status: number;
        last_activity_at: Date | null;
        message_count: number;
        attachment_message_count: number;
        first_message_at: Date | null;
        last_message_at: Date | null;
      }[];
    };

    return (result?.rows || []).map((row) => ({
      internalId: Number(row.internal_id),
      displayId: Number(row.display_id),
      inboxId: Number(row.inbox_id),
      status: this.normalizeConversationStatus(row.status),
      messageCount: Number(row.message_count || 0),
      attachmentMessageCount: Number(row.attachment_message_count || 0),
      firstMessageAt: row.first_message_at ? new Date(row.first_message_at).toISOString() : null,
      lastMessageAt: row.last_message_at ? new Date(row.last_message_at).toISOString() : null,
      lastActivityAt: row.last_activity_at ? new Date(row.last_activity_at).toISOString() : null,
    }));
  }

  public async getLatestInboxConversationCandidate(
    instance: InstanceDto,
    contactId: number,
    inboxId: number,
  ): Promise<ChatwootConversationCandidate | null> {
    const candidates = await this.listInboxConversationCandidates(instance, contactId, inboxId);
    return candidates[0] || null;
  }

  public async getConversationCandidateByInternalId(
    instance: InstanceDto,
    conversationId: number,
  ): Promise<ChatwootConversationCandidate | null> {
    const provider = await this.getProvider(instance);
    if (!provider || !conversationId) {
      return null;
    }

    const result = (await this.pgClient.query(
      `SELECT conversations.id AS internal_id,
              conversations.display_id,
              conversations.inbox_id,
              conversations.status,
              conversations.last_activity_at,
              COUNT(messages.id)::INTEGER AS message_count,
              COUNT(messages.id) FILTER (
                WHERE COALESCE(messages.content_type, 0) <> 0
              )::INTEGER AS attachment_message_count,
              MIN(messages.created_at) AS first_message_at,
              MAX(messages.created_at) AS last_message_at
         FROM conversations
         LEFT JOIN messages
           ON messages.account_id = conversations.account_id
          AND messages.conversation_id = conversations.id
        WHERE conversations.account_id = $1
          AND conversations.id = $2
        GROUP BY conversations.id, conversations.display_id, conversations.inbox_id, conversations.status, conversations.last_activity_at
        LIMIT 1`,
      [provider.accountId, conversationId],
    )) as {
      rows: {
        internal_id: number;
        display_id: number;
        inbox_id: number;
        status: number;
        last_activity_at: Date | null;
        message_count: number;
        attachment_message_count: number;
        first_message_at: Date | null;
        last_message_at: Date | null;
      }[];
    };

    const row = result?.rows?.[0];
    if (!row) {
      return null;
    }

    return {
      internalId: Number(row.internal_id),
      displayId: Number(row.display_id),
      inboxId: Number(row.inbox_id),
      status: this.normalizeConversationStatus(row.status),
      messageCount: Number(row.message_count || 0),
      attachmentMessageCount: Number(row.attachment_message_count || 0),
      firstMessageAt: row.first_message_at ? new Date(row.first_message_at).toISOString() : null,
      lastMessageAt: row.last_message_at ? new Date(row.last_message_at).toISOString() : null,
      lastActivityAt: row.last_activity_at ? new Date(row.last_activity_at).toISOString() : null,
    };
  }

  public async createFreshConversation(
    instance: InstanceDto,
    contactId: number,
    inboxId: number,
    pending = false,
  ): Promise<conversation | null> {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const data: Record<string, string> = {
      contact_id: contactId.toString(),
      inbox_id: inboxId.toString(),
    };

    if (pending) {
      data.status = 'pending';
    }

    const freshConversation = await client.conversations.create({
      accountId: this.provider.accountId,
      data,
    });

    if (!freshConversation) {
      this.logger.warn('conversation not created');
      return null;
    }

    return freshConversation as unknown as conversation;
  }

  public async createHistoricalShadowConversation(
    instance: InstanceDto,
    contactId: number,
    inboxId: number,
    options?: {
      sourceConversationId?: number | null;
      remoteJid?: string | null;
    },
  ): Promise<conversation | null> {
    const provider = await this.getProvider(instance);
    if (!provider) {
      return null;
    }

    const contactInboxRow = (await this.pgClient.query(
      `SELECT id
         FROM contact_inboxes
        WHERE contact_id = $1
          AND inbox_id = $2
        ORDER BY id DESC
        LIMIT 1`,
      [contactId, inboxId],
    )) as { rows: { id: number }[] };

    const contactInboxId = contactInboxRow.rows[0]?.id;
    if (!contactInboxId) {
      this.logger.warn(
        `Contact inbox not found for contact ${contactId} and inbox ${inboxId}, falling back to API conversation creation`,
      );
      return this.createFreshConversation(instance, contactId, inboxId, false);
    }

    let sourceAdditionalAttributes: Record<string, unknown> = {};
    const sourceConversationId = Number(options?.sourceConversationId);

    if (Number.isFinite(sourceConversationId) && sourceConversationId > 0) {
      const sourceConversation = (await this.pgClient.query(
        `SELECT additional_attributes
           FROM conversations
          WHERE account_id = $1
            AND id = $2
          LIMIT 1`,
        [provider.accountId, sourceConversationId],
      )) as { rows: { additional_attributes: Record<string, unknown> | null }[] };

      const rawAdditionalAttributes = sourceConversation.rows[0]?.additional_attributes;
      if (rawAdditionalAttributes && typeof rawAdditionalAttributes === 'object') {
        sourceAdditionalAttributes = rawAdditionalAttributes;
      }
    }

    const additionalAttributes = {
      ...sourceAdditionalAttributes,
      historical_import: true,
      historical_import_source: 'evolution_api_rebuild_merge',
      historical_import_jid: options?.remoteJid || null,
      historical_import_origin_conversation_id:
        Number.isFinite(sourceConversationId) && sourceConversationId > 0 ? sourceConversationId : null,
      historical_import_preserve_chatwoot_media: true,
    };

    const insertedConversation = (await this.pgClient.query(
      `INSERT INTO conversations
         (account_id, inbox_id, status, contact_id, contact_inbox_id, uuid, last_activity_at, created_at, updated_at,
          additional_attributes)
       VALUES ($1, $2, $3, $4, $5, gen_random_uuid(), NOW(), NOW(), NOW(), $6::jsonb)
       RETURNING *`,
      [
        provider.accountId,
        inboxId,
        this.CHATWOOT_STATUS_OPEN,
        contactId,
        contactInboxId,
        JSON.stringify(additionalAttributes),
      ],
    )) as {
      rows: (conversation & {
        display_id?: number;
      })[];
    };

    return (insertedConversation.rows[0] as unknown as conversation) || null;
  }

  public async consolidateConversationHistory(
    instance: InstanceDto,
    targetConversationId: number,
    sourceConversationIds: number[],
  ): Promise<{
    movedMessageCount: number;
    supersededConversationIds: number[];
    resolvedConversationIds: number[];
    failedConversationIds: number[];
  }> {
    const provider = await this.getProvider(instance);
    const supersededConversationIds = Array.from(
      new Set(
        sourceConversationIds
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value) && value > 0 && value !== targetConversationId),
      ),
    );

    if (!provider || supersededConversationIds.length === 0) {
      return {
        movedMessageCount: 0,
        supersededConversationIds,
        resolvedConversationIds: [],
        failedConversationIds: [],
      };
    }

    const movedMessageCount =
      (
        await this.pgClient.query(
          `UPDATE messages AS messages
              SET conversation_id = $1,
                  inbox_id = target.inbox_id,
                  updated_at = NOW()
             FROM conversations AS target
            WHERE target.account_id = $2
              AND target.id = $1
              AND messages.account_id = $2
              AND messages.conversation_id = ANY($3::integer[])`,
          [targetConversationId, provider.accountId, supersededConversationIds],
        )
      )?.rowCount || 0;

    // Deduplicate messages by source_id within the target conversation, preferring
    // the copy that has attachments (audio, video, image). If neither has attachments,
    // keep the one with the lower id (first imported). This handles the scenario where
    // a past bad import created two conversations for the same contact, one with media
    // and one without.
    await this.pgClient.query(
      `DELETE FROM messages
        WHERE account_id = $1
          AND conversation_id = $2
          AND source_id IS NOT NULL
          AND id NOT IN (
            SELECT DISTINCT ON (source_id)
              -- prefer message with attachments; break ties by lowest id
              m.id
            FROM messages m
            LEFT JOIN attachments a ON a.message_id = m.id
            WHERE m.account_id = $1
              AND m.conversation_id = $2
              AND m.source_id IS NOT NULL
            ORDER BY source_id,
                     (a.id IS NOT NULL) DESC,
                     m.id ASC
          )`,
      [provider.accountId, targetConversationId],
    );

    // Deduplicate messages without source_id using a fallback fingerprint:
    // message_type + md5(content) + timestamp truncated to the minute.
    // Same preference: keep the copy with attachments, then lowest id.
    await this.pgClient.query(
      `DELETE FROM messages
        WHERE account_id = $1
          AND conversation_id = $2
          AND source_id IS NULL
          AND id NOT IN (
            SELECT DISTINCT ON (m.message_type, md5(COALESCE(m.content, '')), date_trunc('minute', m.created_at))
              m.id
            FROM messages m
            LEFT JOIN attachments a ON a.message_id = m.id
            WHERE m.account_id = $1
              AND m.conversation_id = $2
              AND m.source_id IS NULL
            ORDER BY m.message_type, md5(COALESCE(m.content, '')), date_trunc('minute', m.created_at),
                     (a.id IS NOT NULL) DESC,
                     m.id ASC
          )`,
      [provider.accountId, targetConversationId],
    );

    await this.pgClient.query(
      `UPDATE conversations
          SET last_activity_at = COALESCE(
                (SELECT MAX(created_at) FROM messages WHERE account_id = $1 AND conversation_id = $2),
                last_activity_at
              ),
              updated_at = NOW()
        WHERE account_id = $1
          AND id = $2`,
      [provider.accountId, targetConversationId],
    );

    await this.pgClient.query(
      `UPDATE conversations
          SET last_activity_at = COALESCE(
                (SELECT MAX(created_at) FROM messages WHERE account_id = $1 AND conversation_id = conversations.id),
                conversations.created_at
              ),
              updated_at = NOW()
        WHERE account_id = $1
          AND id = ANY($2::integer[])`,
      [provider.accountId, supersededConversationIds],
    );

    const client = await this.clientCw(instance);
    const resolutionResults = await Promise.allSettled(
      supersededConversationIds.map(async (conversationId) => {
        if (client) {
          try {
            await client.conversations.toggleStatus({
              accountId: this.provider.accountId,
              conversationId,
              data: {
                status: 'resolved',
              },
            });
            return conversationId;
          } catch (error) {
            this.logger.warn(`Conversation ${conversationId} resolve via API failed, applying DB fallback: ${error}`);
          }
        }

        await this.pgClient.query(
          `UPDATE conversations
              SET status = $1,
                  updated_at = NOW()
            WHERE account_id = $2
              AND id = $3`,
          [this.CHATWOOT_STATUS_RESOLVED, provider.accountId, conversationId],
        );

        return conversationId;
      }),
    );

    const resolvedConversationIds: number[] = [];
    const failedConversationIds: number[] = [];

    resolutionResults.forEach((result, index) => {
      const conversationId = supersededConversationIds[index];
      if (result.status === 'fulfilled') {
        resolvedConversationIds.push(result.value);
        return;
      }

      failedConversationIds.push(conversationId);
    });

    return {
      movedMessageCount,
      supersededConversationIds,
      resolvedConversationIds,
      failedConversationIds,
    };
  }

  public async refreshHistoricalConversation(
    instance: InstanceDto,
    conversationId: number,
    options?: {
      forceOpen?: boolean;
    },
  ) {
    const provider = await this.getProvider(instance);
    if (!provider || !conversationId) {
      return null;
    }

    const aggregatesResult = (await this.pgClient.query(
      `SELECT COUNT(*)::INTEGER AS total_messages,
              MIN(created_at) AS first_message_at,
              MAX(created_at) AS last_message_at,
              MIN(created_at) FILTER (WHERE message_type = 1 AND private = FALSE) AS first_reply_at,
              MAX(created_at) FILTER (WHERE message_type = 0 AND private = FALSE) AS last_incoming_at,
              MAX(created_at) FILTER (WHERE message_type = 1 AND private = FALSE) AS last_outgoing_at
         FROM messages
        WHERE account_id = $1
          AND conversation_id = $2`,
      [provider.accountId, conversationId],
    )) as {
      rows: {
        total_messages: number;
        first_message_at: Date | null;
        last_message_at: Date | null;
        first_reply_at: Date | null;
        last_incoming_at: Date | null;
        last_outgoing_at: Date | null;
      }[];
    };

    const aggregates = aggregatesResult.rows[0];
    if (!aggregates) {
      return null;
    }

    await this.pgClient.query(
      `UPDATE conversations
          SET status = CASE WHEN $3 THEN $4 ELSE status END,
              created_at = COALESCE($5, created_at),
              last_activity_at = COALESCE($6, last_activity_at, created_at),
              first_reply_created_at = COALESCE($7, first_reply_created_at),
              waiting_since = CASE
                                WHEN $8 IS NULL THEN NULL
                                WHEN $9 IS NULL OR $8 > $9 THEN $8
                                ELSE NULL
                              END,
              updated_at = NOW()
        WHERE account_id = $1
          AND id = $2`,
      [
        provider.accountId,
        conversationId,
        !!options?.forceOpen,
        this.CHATWOOT_STATUS_OPEN,
        aggregates.first_message_at,
        aggregates.last_message_at,
        aggregates.first_reply_at,
        aggregates.last_incoming_at,
        aggregates.last_outgoing_at,
      ],
    );

    return aggregates;
  }

  public async countConversationMessages(instance: InstanceDto, conversationId: number): Promise<number> {
    const provider = await this.getProvider(instance);
    if (!provider) {
      return 0;
    }

    const result = await this.pgClient.query(
      'SELECT COUNT(*)::INTEGER as total FROM messages WHERE account_id = $1 AND conversation_id = $2',
      [provider.accountId, conversationId],
    );

    return Number(result?.rows?.[0]?.total || 0);
  }

  public async getConversationMessageWindow(
    instance: InstanceDto,
    conversationId: number,
  ): Promise<{ firstMessageAt: string | null; lastMessageAt: string | null }> {
    const provider = await this.getProvider(instance);
    if (!provider) {
      return {
        firstMessageAt: null,
        lastMessageAt: null,
      };
    }

    const result = await this.pgClient.query(
      `SELECT
          MIN(created_at) AS "firstMessageAt",
          MAX(created_at) AS "lastMessageAt"
         FROM messages
        WHERE account_id = $1
          AND conversation_id = $2`,
      [provider.accountId, conversationId],
    );

    return {
      firstMessageAt: result?.rows?.[0]?.firstMessageAt ? new Date(result.rows[0].firstMessageAt).toISOString() : null,
      lastMessageAt: result?.rows?.[0]?.lastMessageAt ? new Date(result.rows[0].lastMessageAt).toISOString() : null,
    };
  }

  public async createBotMessage(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    attachments?: {
      content: unknown;
      encoding: string;
      filename: string;
    }[],
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    const contact = await this.findContact(instance, '123456');

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const filterInbox = await this.getInbox(instance);

    if (!filterInbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);

    if (!conversation) {
      this.logger.warn('conversation not found');
      return;
    }

    const message = await client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversation.id,
      data: {
        content: content,
        message_type: messageType,
        attachments: attachments,
      },
    });

    if (!message) {
      this.logger.warn('message not found');
      return null;
    }

    return message;
  }

  private async sendData(
    conversationId: number,
    fileStream: Readable,
    fileName: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    content?: string,
    instance?: InstanceDto,
    messageBody?: any,
    sourceId?: string,
    quotedMsg?: MessageModel,
  ) {
    if (sourceId && this.isImportHistoryAvailable()) {
      const messageAlreadySaved = await chatwootImport.getExistingSourceIds([sourceId], conversationId);
      if (messageAlreadySaved) {
        if (messageAlreadySaved.size > 0) {
          this.logger.warn('Message already saved on chatwoot');
          return null;
        }
      }
    }
    const data = new FormData();

    if (content) {
      data.append('content', content);
    }

    data.append('message_type', messageType);

    data.append('attachments[]', fileStream, { filename: fileName });

    const sourceReplyId = quotedMsg?.chatwootMessageId || null;

    if (messageBody && instance) {
      const replyToIds = await this.getReplyToIds(messageBody, instance);

      if (replyToIds.in_reply_to || replyToIds.in_reply_to_external_id) {
        const content = JSON.stringify({
          ...replyToIds,
        });
        data.append('content_attributes', content);
      }
    }

    if (sourceReplyId) {
      data.append('source_reply_id', sourceReplyId.toString());
    }

    if (sourceId) {
      data.append('source_id', sourceId);
    }

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversationId}/messages`,
      headers: {
        api_access_token: this.provider.token,
        ...data.getHeaders(),
      },
      data: data,
    };

    try {
      const { data } = await axios.request(config);

      return data;
    } catch (error) {
      this.logger.error(error);
    }
  }

  public async sendHistoryMediaMessage(options: {
    conversationId: number;
    fileStream: Readable;
    fileName: string;
    messageType: 'incoming' | 'outgoing';
    content?: string;
    sourceId?: string;
    timestamp?: number;
  }): Promise<any> {
    const data = new FormData();

    if (options.content) {
      data.append('content', options.content);
    }

    data.append('message_type', options.messageType);
    data.append('attachments[]', options.fileStream, { filename: options.fileName });

    if (options.sourceId) {
      data.append('source_id', options.sourceId);
    }

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${options.conversationId}/messages`,
      headers: {
        api_access_token: this.provider.token,
        ...data.getHeaders(),
      },
      data: data,
    };

    try {
      const { data: responseData } = await axios.request(config);

      // If we have the original timestamp, update created_at to preserve chronological order
      if (options.timestamp && responseData?.id) {
        await this.pgClient.query(
          `UPDATE messages SET created_at = to_timestamp($1), updated_at = to_timestamp($1) WHERE id = $2`,
          [options.timestamp, responseData.id],
        );
      }

      return responseData;
    } catch (error) {
      this.logger.error(`Error sending history media message: ${error}`);
      return null;
    }
  }

  public async createBotQr(
    instance: InstanceDto,
    content: string,
    messageType: 'incoming' | 'outgoing' | undefined,
    fileStream?: Readable,
    fileName?: string,
  ) {
    const client = await this.clientCw(instance);

    if (!client) {
      this.logger.warn('client not found');
      return null;
    }

    if (!this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT) {
      this.logger.log('Chatwoot bot contact is disabled');

      return true;
    }

    const contact = await this.findContact(instance, '123456');

    if (!contact) {
      this.logger.warn('contact not found');
      return null;
    }

    const filterInbox = await this.getInbox(instance);

    if (!filterInbox) {
      this.logger.warn('inbox not found');
      return null;
    }

    const conversation = await this.getOpenConversationByContact(instance, filterInbox, contact);

    if (!conversation) {
      this.logger.warn('conversation not found');
      return;
    }

    const data = new FormData();

    if (content) {
      data.append('content', content);
    }

    data.append('message_type', messageType);

    if (fileStream && fileName) {
      data.append('attachments[]', fileStream, { filename: fileName });
    }

    const config = {
      method: 'post',
      maxBodyLength: Infinity,
      url: `${this.provider.url}/api/v1/accounts/${this.provider.accountId}/conversations/${conversation.id}/messages`,
      headers: {
        api_access_token: this.provider.token,
        ...data.getHeaders(),
      },
      data: data,
    };

    try {
      const { data } = await axios.request(config);

      return data;
    } catch (error) {
      this.logger.error(error);
    }
  }

  public async sendAttachment(waInstance: any, number: string, media: any, caption?: string, options?: Options) {
    try {
      const parsedMedia = path.parse(decodeURIComponent(media));
      let mimeType = mimeTypes.lookup(parsedMedia?.ext) || '';
      let fileName = parsedMedia?.name + parsedMedia?.ext;

      if (!mimeType) {
        const parts = media.split('/');
        fileName = decodeURIComponent(parts[parts.length - 1]);

        const response = await axios.get(media, {
          responseType: 'arraybuffer',
        });
        mimeType = response.headers['content-type'];
      }

      let type = 'document';

      switch (mimeType.split('/')[0]) {
        case 'image':
          type = 'image';
          break;
        case 'video':
          type = 'video';
          break;
        case 'audio':
          type = 'audio';
          break;
        default:
          type = 'document';
          break;
      }

      if (type === 'audio') {
        const data: SendAudioDto = {
          number: number,
          audio: media,
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
          quoted: options?.quoted,
        };

        sendTelemetry('/message/sendWhatsAppAudio');

        const messageSent = await waInstance?.audioWhatsapp(data, null, true);

        return messageSent;
      }

      const documentExtensions = ['.gif', '.svg', '.tiff', '.tif', '.dxf', '.dwg'];
      if (type === 'image' && parsedMedia && documentExtensions.includes(parsedMedia?.ext)) {
        type = 'document';
      }

      const data: SendMediaDto = {
        number: number,
        mediatype: type as any,
        fileName: fileName,
        media: media,
        delay: 1200,
        quoted: options?.quoted,
      };

      sendTelemetry('/message/sendMedia');

      if (caption) {
        data.caption = caption;
      }

      const messageSent = await waInstance?.mediaMessage(data, null, true);

      return messageSent;
    } catch (error) {
      this.logger.error(error);
      throw error; // Re-throw para que o erro seja tratado pelo caller
    }
  }

  public async onSendMessageError(instance: InstanceDto, conversation: number, error?: any) {
    this.logger.verbose(`onSendMessageError ${JSON.stringify(error)}`);

    const client = await this.clientCw(instance);

    if (!client) {
      return;
    }

    if (error && error?.status === 400 && error?.message[0]?.exists === false) {
      client.messages.create({
        accountId: this.provider.accountId,
        conversationId: conversation,
        data: {
          content: `${i18next.t('cw.message.numbernotinwhatsapp')}`,
          message_type: 'outgoing',
          private: true,
        },
      });

      return;
    }

    client.messages.create({
      accountId: this.provider.accountId,
      conversationId: conversation,
      data: {
        content: i18next.t('cw.message.notsent', {
          error: error ? `_${error.toString()}_` : '',
        }),
        message_type: 'outgoing',
        private: true,
      },
    });
  }

  public async receiveWebhook(instance: InstanceDto, body: any) {
    try {
      await new Promise((resolve) => setTimeout(resolve, 500));

      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      if (
        this.provider.reopenConversation === false &&
        body.event === 'conversation_status_changed' &&
        body.status === 'resolved' &&
        body.meta?.sender?.identifier
      ) {
        const keyToDelete = `${instance.instanceName}:createConversation-${body.meta.sender.identifier}`;
        this.cache.delete(keyToDelete);
      }

      if (
        !body?.conversation ||
        body.private ||
        (body.event === 'message_updated' && !body.content_attributes?.deleted)
      ) {
        return { message: 'bot' };
      }

      const chatId =
        body.conversation.meta.sender?.identifier || body.conversation.meta.sender?.phone_number.replace('+', '');
      // Chatwoot to Whatsapp
      const messageReceived = body.content
        ? body.content
            .replaceAll(/(?<!\*)\*((?!\s)([^\n*]+?)(?<!\s))\*(?!\*)/g, '_$1_') // Substitui * por _
            .replaceAll(/\*{2}((?!\s)([^\n*]+?)(?<!\s))\*{2}/g, '*$1*') // Substitui ** por *
            .replaceAll(/~{2}((?!\s)([^\n*]+?)(?<!\s))~{2}/g, '~$1~') // Substitui ~~ por ~
            .replaceAll(/(?<!`)`((?!\s)([^`*]+?)(?<!\s))`(?!`)/g, '```$1```') // Substitui ` por ```
        : body.content;

      const senderName = body?.conversation?.messages[0]?.sender?.available_name || body?.sender?.name;
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      instance.instanceId = waInstance.instanceId;

      if (body.event === 'message_updated' && body.content_attributes?.deleted) {
        const message = await this.prismaRepository.message.findFirst({
          where: {
            chatwootMessageId: body.id,
            instanceId: instance.instanceId,
          },
        });

        if (message) {
          const key = message.key as WAMessageKey;

          await waInstance?.client.sendMessage(key.remoteJid, { delete: key });

          await this.prismaRepository.message.deleteMany({
            where: {
              instanceId: instance.instanceId,
              chatwootMessageId: body.id,
            },
          });
        }
        return { message: 'bot' };
      }

      const cwBotContact = this.configService.get<Chatwoot>('CHATWOOT').BOT_CONTACT;

      if (chatId === '123456' && body.message_type === 'outgoing') {
        const command = messageReceived.replace('/', '');

        if (cwBotContact && (command.includes('init') || command.includes('iniciar'))) {
          const state = waInstance?.connectionStatus?.state;

          if (state !== 'open') {
            const number = command.split(':')[1];
            await waInstance.connectToWhatsapp(number);
          } else {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.alreadyConnected', {
                inboxName: body.inbox.name,
              }),
              'incoming',
            );
          }
        }

        if (command === 'clearcache') {
          waInstance.clearCacheChatwoot();
          await this.createBotMessage(
            instance,
            i18next.t('cw.inbox.clearCache', {
              inboxName: body.inbox.name,
            }),
            'incoming',
          );
        }

        if (command === 'status') {
          const state = waInstance?.connectionStatus?.state;

          if (!state) {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.notFound', {
                inboxName: body.inbox.name,
              }),
              'incoming',
            );
          }

          if (state) {
            await this.createBotMessage(
              instance,
              i18next.t('cw.inbox.status', {
                inboxName: body.inbox.name,
                state: state,
              }),
              'incoming',
            );
          }
        }

        if (cwBotContact && (command === 'disconnect' || command === 'desconectar')) {
          const msgLogout = i18next.t('cw.inbox.disconnect', {
            inboxName: body.inbox.name,
          });

          await this.createBotMessage(instance, msgLogout, 'incoming');

          await waInstance?.client?.logout('Log out instance: ' + instance.instanceName);
          await waInstance?.client?.ws?.close();
        }
      }

      if (body.message_type === 'outgoing' && body?.conversation?.messages?.length && chatId !== '123456') {
        if (body?.conversation?.messages[0]?.source_id?.substring(0, 5) === 'WAID:') {
          return { message: 'bot' };
        }

        if (!waInstance && body.conversation?.id) {
          this.onSendMessageError(instance, body.conversation?.id, 'Instance not found');
          return { message: 'bot' };
        }

        let formatText: string;
        if (senderName === null || senderName === undefined) {
          formatText = messageReceived;
        } else {
          const formattedDelimiter = this.provider.signDelimiter
            ? this.provider.signDelimiter.replaceAll('\\n', '\n')
            : '\n';
          const textToConcat = this.provider.signMsg ? [`*${senderName}:*`] : [];
          textToConcat.push(messageReceived);

          formatText = textToConcat.join(formattedDelimiter);
        }

        for (const message of body.conversation.messages) {
          if (message.attachments && message.attachments.length > 0) {
            for (const attachment of message.attachments) {
              if (!messageReceived) {
                formatText = null;
              }

              const options: Options = {
                quoted: await this.getQuotedMessage(body, instance),
              };

              const messageSent = await this.sendAttachment(
                waInstance,
                chatId,
                attachment.data_url,
                formatText,
                options,
              );
              if (!messageSent && body.conversation?.id) {
                this.onSendMessageError(instance, body.conversation?.id);
              }

              await this.updateChatwootMessageId(
                {
                  ...messageSent,
                },
                {
                  messageId: body.id,
                  inboxId: body.inbox?.id,
                  conversationId: body.conversation?.id,
                  contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
                },
                instance,
              );
            }
          } else {
            const data: SendTextDto = {
              number: chatId,
              text: formatText,
              delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
              quoted: await this.getQuotedMessage(body, instance),
            };

            sendTelemetry('/message/sendText');

            let messageSent: any;
            try {
              messageSent = await waInstance?.textMessage(data, true);
              if (!messageSent) {
                throw new Error('Message not sent');
              }

              if (Long.isLong(messageSent?.messageTimestamp)) {
                messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();
              }

              await this.updateChatwootMessageId(
                {
                  ...messageSent,
                },
                {
                  messageId: body.id,
                  inboxId: body.inbox?.id,
                  conversationId: body.conversation?.id,
                  contactInboxSourceId: body.conversation?.contact_inbox?.source_id,
                },
                instance,
              );
            } catch (error) {
              if (!messageSent && body.conversation?.id) {
                this.onSendMessageError(instance, body.conversation?.id, error);
              }
              throw error;
            }
          }
        }

        const chatwootRead = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_READ;
        if (chatwootRead) {
          const lastMessage = await this.prismaRepository.message.findFirst({
            where: {
              key: {
                path: ['fromMe'],
                equals: false,
              },
              instanceId: instance.instanceId,
            },
          });
          if (lastMessage && !lastMessage.chatwootIsRead) {
            const key = lastMessage.key as WAMessageKey;

            waInstance?.markMessageAsRead({
              readMessages: [
                {
                  id: key.id,
                  fromMe: key.fromMe,
                  remoteJid: key.remoteJid,
                },
              ],
            });
            const updateMessage = {
              chatwootMessageId: lastMessage.chatwootMessageId,
              chatwootConversationId: lastMessage.chatwootConversationId,
              chatwootInboxId: lastMessage.chatwootInboxId,
              chatwootContactInboxSourceId: lastMessage.chatwootContactInboxSourceId,
              chatwootIsRead: true,
            };

            await this.prismaRepository.message.updateMany({
              where: {
                instanceId: instance.instanceId,
                keyId: key.id,
              },
              data: updateMessage,
            });
          }
        }
      }

      if (body.message_type === 'template' && body.event === 'message_created') {
        const data: SendTextDto = {
          number: chatId,
          text: body.content.replace(/\\\r\n|\\\n|\n/g, '\n'),
          delay: Math.floor(Math.random() * (2000 - 500 + 1)) + 500,
        };

        sendTelemetry('/message/sendText');

        await waInstance?.textMessage(data);
      }

      return { message: 'bot' };
    } catch (error) {
      this.logger.error(error);

      return { message: 'bot' };
    }
  }

  private async updateChatwootMessageId(
    message: MessageModel,
    chatwootMessageIds: ChatwootMessage,
    instance: InstanceDto,
  ) {
    const key = message.key as WAMessageKey;

    if (!chatwootMessageIds.messageId || !key?.id) {
      return;
    }

    // Use raw SQL to avoid JSON path issues
    const result = await this.prismaRepository.$executeRaw`
      UPDATE "Message" 
      SET 
        "chatwootMessageId" = ${chatwootMessageIds.messageId},
        "chatwootConversationId" = ${chatwootMessageIds.conversationId},
        "chatwootInboxId" = ${chatwootMessageIds.inboxId},
        "chatwootContactInboxSourceId" = ${chatwootMessageIds.contactInboxSourceId},
        "chatwootIsRead" = ${chatwootMessageIds.isRead || false}
      WHERE "instanceId" = ${instance.instanceId} 
      AND "key"->>'id' = ${key.id}
    `;

    this.logger.verbose(`Update result: ${result} rows affected`);

    if (this.isImportHistoryAvailable()) {
      try {
        await chatwootImport.updateMessageSourceID(chatwootMessageIds.messageId, key.id);
      } catch (error) {
        this.logger.error(`Error updating Chatwoot message source ID: ${error}`);
      }
    }
  }

  private async getMessageByKeyId(instance: InstanceDto, keyId: string): Promise<MessageModel> {
    return await this.prismaRepository.message.findFirst({
      where: {
        instanceId: instance.instanceId,
        OR: [
          { keyId },
          {
            key: {
              path: ['id'],
              equals: keyId,
            },
          },
        ],
      },
    });
  }

  private async getReplyToIds(
    msg: any,
    instance: InstanceDto,
  ): Promise<{ in_reply_to: string; in_reply_to_external_id: string }> {
    let inReplyTo = null;
    let inReplyToExternalId = null;

    if (msg) {
      inReplyToExternalId = msg.message?.extendedTextMessage?.contextInfo?.stanzaId ?? msg.contextInfo?.stanzaId;
      if (inReplyToExternalId) {
        const message = await this.getMessageByKeyId(instance, inReplyToExternalId);
        if (message?.chatwootMessageId) {
          inReplyTo = message.chatwootMessageId;
        }
      }
    }

    return {
      in_reply_to: inReplyTo,
      in_reply_to_external_id: inReplyToExternalId,
    };
  }

  private async getQuotedMessage(msg: any, instance: InstanceDto): Promise<Quoted> {
    if (msg?.content_attributes?.in_reply_to) {
      const message = await this.prismaRepository.message.findFirst({
        where: {
          chatwootMessageId: msg?.content_attributes?.in_reply_to,
          instanceId: instance.instanceId,
        },
      });

      const key = message?.key as WAMessageKey;
      const messageContent = message?.message as WAMessageContent;

      if (messageContent && key?.id) {
        return {
          key: key,
          message: messageContent,
        };
      }
    }

    return null;
  }

  private isMediaMessage(message: any) {
    const media = [
      'imageMessage',
      'documentMessage',
      'documentWithCaptionMessage',
      'audioMessage',
      'videoMessage',
      'stickerMessage',
      'viewOnceMessageV2',
    ];

    const messageKeys = Object.keys(message);

    const result = messageKeys.some((key) => media.includes(key));

    return result;
  }

  private isInteractiveButtonMessage(messageType: string, message: any) {
    return messageType === 'interactiveMessage' && message.interactiveMessage?.nativeFlowMessage?.buttons?.length > 0;
  }

  private getAdsMessage(msg: any) {
    interface AdsMessage {
      title: string;
      body: string;
      thumbnailUrl: string;
      sourceUrl: string;
    }

    const adsMessage: AdsMessage | undefined = {
      title: msg.extendedTextMessage?.contextInfo?.externalAdReply?.title || msg.contextInfo?.externalAdReply?.title,
      body: msg.extendedTextMessage?.contextInfo?.externalAdReply?.body || msg.contextInfo?.externalAdReply?.body,
      thumbnailUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.thumbnailUrl ||
        msg.contextInfo?.externalAdReply?.thumbnailUrl,
      sourceUrl:
        msg.extendedTextMessage?.contextInfo?.externalAdReply?.sourceUrl || msg.contextInfo?.externalAdReply?.sourceUrl,
    };

    return adsMessage;
  }

  private getReactionMessage(msg: any) {
    interface ReactionMessage {
      key: {
        id: string;
        fromMe: boolean;
        remoteJid: string;
        participant?: string;
      };
      text: string;
    }
    const reactionMessage: ReactionMessage | undefined = msg?.reactionMessage;

    return reactionMessage;
  }

  private getTypeMessage(msg: any) {
    const types = {
      conversation: msg.conversation,
      imageMessage: msg.imageMessage?.caption,
      videoMessage: msg.videoMessage?.caption,
      extendedTextMessage: msg.extendedTextMessage?.text,
      messageContextInfo: msg.messageContextInfo?.stanzaId,
      stickerMessage: undefined,
      documentMessage: msg.documentMessage?.caption,
      documentWithCaptionMessage: msg.documentWithCaptionMessage?.message?.documentMessage?.caption,
      audioMessage: msg.audioMessage ? (msg.audioMessage.caption ?? '') : undefined,
      contactMessage: msg.contactMessage?.vcard,
      contactsArrayMessage: msg.contactsArrayMessage,
      locationMessage: msg.locationMessage,
      liveLocationMessage: msg.liveLocationMessage,
      listMessage: msg.listMessage,
      listResponseMessage: msg.listResponseMessage,
      viewOnceMessageV2:
        msg?.message?.viewOnceMessageV2?.message?.imageMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.videoMessage?.url ||
        msg?.message?.viewOnceMessageV2?.message?.audioMessage?.url,
    };

    return types;
  }

  private getMessageContent(types: any) {
    const typeKey = Object.keys(types).find((key) => types[key] !== undefined);

    let result = typeKey ? types[typeKey] : undefined;

    // Remove externalAdReplyBody| in Chatwoot (Already Have)
    if (result && typeof result === 'string' && result.includes('externalAdReplyBody|')) {
      result = result.split('externalAdReplyBody|').filter(Boolean).join('');
    }

    if (typeKey === 'locationMessage' || typeKey === 'liveLocationMessage') {
      const latitude = result.degreesLatitude;
      const longitude = result.degreesLongitude;

      const locationName = result?.name;
      const locationAddress = result?.address;

      const formattedLocation =
        `*${i18next.t('cw.locationMessage.location')}:*\n\n` +
        `_${i18next.t('cw.locationMessage.latitude')}:_ ${latitude} \n` +
        `_${i18next.t('cw.locationMessage.longitude')}:_ ${longitude} \n` +
        (locationName ? `_${i18next.t('cw.locationMessage.locationName')}:_ ${locationName}\n` : '') +
        (locationAddress ? `_${i18next.t('cw.locationMessage.locationAddress')}:_ ${locationAddress} \n` : '') +
        `_${i18next.t('cw.locationMessage.locationUrl')}:_ ` +
        `https://www.google.com/maps/search/?api=1&query=${latitude},${longitude}`;

      return formattedLocation;
    }

    if (typeKey === 'contactMessage') {
      const vCardData = result.split('\n');
      const contactInfo = {};

      vCardData.forEach((line) => {
        const [key, value] = line.split(':');
        if (key && value) {
          contactInfo[key] = value;
        }
      });

      let formattedContact =
        `*${i18next.t('cw.contactMessage.contact')}:*\n\n` +
        `_${i18next.t('cw.contactMessage.name')}:_ ${contactInfo['FN']}`;

      let numberCount = 1;
      Object.keys(contactInfo).forEach((key) => {
        if (key.startsWith('item') && key.includes('TEL')) {
          const phoneNumber = contactInfo[key];
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
          numberCount++;
        } else if (key.includes('TEL')) {
          const phoneNumber = contactInfo[key];
          formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
          numberCount++;
        }
      });

      return formattedContact;
    }

    if (typeKey === 'contactsArrayMessage') {
      const formattedContacts = result.contacts.map((contact) => {
        const vCardData = contact.vcard.split('\n');
        const contactInfo = {};

        vCardData.forEach((line) => {
          const [key, value] = line.split(':');
          if (key && value) {
            contactInfo[key] = value;
          }
        });

        let formattedContact = `*${i18next.t('cw.contactMessage.contact')}:*\n\n_${i18next.t(
          'cw.contactMessage.name',
        )}:_ ${contact.displayName}`;

        let numberCount = 1;
        Object.keys(contactInfo).forEach((key) => {
          if (key.startsWith('item') && key.includes('TEL')) {
            const phoneNumber = contactInfo[key];
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
            numberCount++;
          } else if (key.includes('TEL')) {
            const phoneNumber = contactInfo[key];
            formattedContact += `\n_${i18next.t('cw.contactMessage.number')} (${numberCount}):_ ${phoneNumber}`;
            numberCount++;
          }
        });

        return formattedContact;
      });

      const formattedContactsArray = formattedContacts.join('\n\n');

      return formattedContactsArray;
    }

    if (typeKey === 'listMessage') {
      const listTitle = result?.title || 'Unknown';
      const listDescription = result?.description || 'Unknown';
      const listFooter = result?.footerText || 'Unknown';

      let formattedList =
        '*List Menu:*\n\n' +
        '_Title_: ' +
        listTitle +
        '\n' +
        '_Description_: ' +
        listDescription +
        '\n' +
        '_Footer_: ' +
        listFooter;

      if (result.sections && result.sections.length > 0) {
        result.sections.forEach((section, sectionIndex) => {
          formattedList += '\n\n*Section ' + (sectionIndex + 1) + ':* ' + section.title || 'Unknown\n';

          if (section.rows && section.rows.length > 0) {
            section.rows.forEach((row, rowIndex) => {
              formattedList += '\n*Line ' + (rowIndex + 1) + ':*\n';
              formattedList += '_▪️ Title:_ ' + (row.title || 'Unknown') + '\n';
              formattedList += '_▪️ Description:_ ' + (row.description || 'Unknown') + '\n';
              formattedList += '_▪️ ID:_ ' + (row.rowId || 'Unknown') + '\n';
            });
          } else {
            formattedList += '\nNo lines found in this section.\n';
          }
        });
      } else {
        formattedList += '\nNo sections found.\n';
      }

      return formattedList;
    }

    if (typeKey === 'listResponseMessage') {
      const responseTitle = result?.title || 'Unknown';
      const responseDescription = result?.description || 'Unknown';
      const responseRowId = result?.singleSelectReply?.selectedRowId || 'Unknown';

      const formattedResponseList =
        '*List Response:*\n\n' +
        '_Title_: ' +
        responseTitle +
        '\n' +
        '_Description_: ' +
        responseDescription +
        '\n' +
        '_ID_: ' +
        responseRowId;
      return formattedResponseList;
    }

    return result;
  }

  public getConversationMessage(msg: any) {
    const types = this.getTypeMessage(msg);

    const messageContent = this.getMessageContent(types);

    return messageContent;
  }

  public async eventWhatsapp(event: string, instance: InstanceDto, body: any) {
    try {
      body = {
        ...body,
        key: body?.key ? { ...body.key } : undefined,
      };

      const waInstance = this.waMonitor.waInstances[instance.instanceName];

      if (!waInstance) {
        this.logger.warn('wa instance not found');
        return null;
      }

      const client = await this.clientCw(instance);

      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      const identity = this.resolveWhatsappIdentity(body);
      body.key = {
        ...body.key,
        canonicalJid: identity.canonicalJid,
        phoneJid: identity.phoneJid,
        lidJid: identity.lidJid,
      };
      const canonicalRemoteJid = identity.canonicalIdentifier || body?.key?.remoteJid;

      if (this.provider?.ignoreJids && this.provider?.ignoreJids.length > 0) {
        const ignoreJids: any = this.provider?.ignoreJids;

        let ignoreGroups = false;
        let ignoreContacts = false;

        if (ignoreJids.includes('@g.us')) {
          ignoreGroups = true;
        }

        if (ignoreJids.includes('@s.whatsapp.net')) {
          ignoreContacts = true;
        }

        if (ignoreGroups && canonicalRemoteJid?.endsWith('@g.us')) {
          this.logger.warn('Ignoring message from group: ' + canonicalRemoteJid);
          return;
        }

        if (ignoreContacts && canonicalRemoteJid?.endsWith('@s.whatsapp.net')) {
          this.logger.warn('Ignoring message from contact: ' + canonicalRemoteJid);
          return;
        }

        if (ignoreJids.includes(canonicalRemoteJid)) {
          this.logger.warn('Ignoring message from jid: ' + canonicalRemoteJid);
          return;
        }
      }

      if (event === 'messages.upsert' || event === 'send.message') {
        this.logger.info(`[${event}] New message received - Instance: ${JSON.stringify(body, null, 2)}`);
        if (canonicalRemoteJid === 'status@broadcast') {
          return;
        }

        if (body.message?.ephemeralMessage?.message) {
          body.message = {
            ...body.message?.ephemeralMessage?.message,
          };
        }

        const originalMessage = await this.getConversationMessage(body.message);
        const bodyMessage = originalMessage
          ? originalMessage
              .replaceAll(/\*((?!\s)([^\n*]+?)(?<!\s))\*/g, '**$1**')
              .replaceAll(/_((?!\s)([^\n_]+?)(?<!\s))_/g, '*$1*')
              .replaceAll(/~((?!\s)([^\n~]+?)(?<!\s))~/g, '~~$1~~')
          : originalMessage;

        if (bodyMessage && bodyMessage.includes('/survey/responses/') && bodyMessage.includes('http')) {
          return;
        }

        const quotedId = body.contextInfo?.stanzaId || body.message?.contextInfo?.stanzaId;

        let quotedMsg = null;

        if (quotedId)
          quotedMsg = await this.prismaRepository.message.findFirst({
            where: {
              keyId: quotedId,
              chatwootMessageId: {
                not: null,
              },
            },
          });

        const isMedia = this.isMediaMessage(body.message);

        const adsMessage = this.getAdsMessage(body);

        const reactionMessage = this.getReactionMessage(body.message);
        const isInteractiveButtonMessage = this.isInteractiveButtonMessage(body.messageType, body.message);

        if (!bodyMessage && !isMedia && !reactionMessage && !isInteractiveButtonMessage) {
          this.logger.warn('no body message found');
          return;
        }

        const getConversation = await this.createConversation(instance, body);

        if (!getConversation) {
          this.logger.warn('conversation not found');
          return;
        }

        const messageType = body.key.fromMe ? 'outgoing' : 'incoming';

        if (isMedia) {
          const downloadBase64 = await waInstance?.getBase64FromMediaMessage({
            message: {
              ...body,
            },
          });

          let nameFile: string;
          const messageBody = body?.message[body?.messageType];
          const originalFilename =
            messageBody?.fileName || messageBody?.filename || messageBody?.message?.documentMessage?.fileName;
          if (originalFilename) {
            const parsedFile = path.parse(originalFilename);
            if (parsedFile.name && parsedFile.ext) {
              nameFile = `${parsedFile.name}-${Math.floor(Math.random() * (99 - 10 + 1) + 10)}${parsedFile.ext}`;
            }
          }

          if (!nameFile) {
            nameFile = `${Math.random().toString(36).substring(7)}.${mimeTypes.extension(downloadBase64.mimetype) || ''}`;
          }

          const fileData = Buffer.from(downloadBase64.base64, 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          if (body.key.remoteJid.includes('@g.us')) {
            const participantName = body.pushName;
            const rawPhoneNumber =
              body.key.addressingMode === 'lid' && !body.key.fromMe && body.key.participantAlt
                ? body.key.participantAlt.split('@')[0].split(':')[0]
                : body.key.participant.split('@')[0].split(':')[0];
            const formattedPhoneNumber = parsePhoneNumberFromString(`+${rawPhoneNumber}`).formatInternational();

            let content: string;

            if (!body.key.fromMe) {
              content = bodyMessage
                ? `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}`
                : `**${formattedPhoneNumber} - ${participantName}:**`;
            } else {
              content = bodyMessage || '';
            }

            const send = await this.sendData(
              getConversation,
              fileStream,
              nameFile,
              messageType,
              content,
              instance,
              body,
              'WAID:' + body.key.id,
              quotedMsg,
            );

            if (!send) {
              this.logger.warn('message not sent');
              return;
            }

            return send;
          } else {
            const send = await this.sendData(
              getConversation,
              fileStream,
              nameFile,
              messageType,
              bodyMessage,
              instance,
              body,
              'WAID:' + body.key.id,
              quotedMsg,
            );

            if (!send) {
              this.logger.warn('message not sent');
              return;
            }

            return send;
          }
        }

        if (reactionMessage) {
          if (reactionMessage.text) {
            const send = await this.createMessage(
              instance,
              getConversation,
              reactionMessage.text,
              messageType,
              false,
              [],
              {
                message: { extendedTextMessage: { contextInfo: { stanzaId: reactionMessage.key.id } } },
              },
              'WAID:' + body.key.id,
              quotedMsg,
            );
            if (!send) {
              this.logger.warn('message not sent');
              return;
            }
          }

          return;
        }

        if (isInteractiveButtonMessage) {
          const buttons = body.message.interactiveMessage.nativeFlowMessage.buttons;
          this.logger.info('is Interactive Button Message: ' + JSON.stringify(buttons));

          for (const button of buttons) {
            const buttonParams = JSON.parse(button.buttonParamsJson);
            const paymentSettings = buttonParams.payment_settings;

            if (button.name === 'payment_info' && paymentSettings[0].type === 'pix_static_code') {
              const pixSettings = paymentSettings[0].pix_static_code;
              const pixKeyType = (() => {
                switch (pixSettings.key_type) {
                  case 'EVP':
                    return 'Chave Aleatória';
                  case 'EMAIL':
                    return 'E-mail';
                  case 'PHONE':
                    return 'Telefone';
                  default:
                    return pixSettings.key_type;
                }
              })();
              const pixKey = pixSettings.key_type === 'PHONE' ? pixSettings.key.replace('+55', '') : pixSettings.key;
              const content = `*${pixSettings.merchant_name}*\nChave PIX: ${pixKey} (${pixKeyType})`;

              const send = await this.createMessage(
                instance,
                getConversation,
                content,
                messageType,
                false,
                [],
                body,
                'WAID:' + body.key.id,
                quotedMsg,
              );
              if (!send) this.logger.warn('message not sent');
            } else {
              this.logger.warn('Interactive Button Message not mapped');
            }
          }
          return;
        }

        const isAdsMessage = (adsMessage && adsMessage.title) || adsMessage.body || adsMessage.thumbnailUrl;
        if (isAdsMessage) {
          const imgBuffer = await axios.get(adsMessage.thumbnailUrl, { responseType: 'arraybuffer' });

          const extension = mimeTypes.extension(imgBuffer.headers['content-type']);
          const mimeType = extension && mimeTypes.lookup(extension);

          if (!mimeType) {
            this.logger.warn('mimetype of Ads message not found');
            return;
          }

          const random = Math.random().toString(36).substring(7);
          const nameFile = `${random}.${mimeTypes.extension(mimeType)}`;
          const fileData = Buffer.from(imgBuffer.data, 'binary');

          const img = await Jimp.read(fileData);
          await img.cover({
            w: 320,
            h: 180,
          });
          const processedBuffer = await img.getBuffer(JimpMime.png);

          const fileStream = new Readable();
          fileStream._read = () => {}; // _read is required but you can noop it
          fileStream.push(processedBuffer);
          fileStream.push(null);

          const truncStr = (str: string, len: number) => {
            if (!str) return '';

            return str.length > len ? str.substring(0, len) + '...' : str;
          };

          const title = truncStr(adsMessage.title, 40);
          const description = truncStr(adsMessage?.body, 75);

          const send = await this.sendData(
            getConversation,
            fileStream,
            nameFile,
            messageType,
            `${bodyMessage}\n\n\n**${title}**\n${description}\n${adsMessage.sourceUrl}`,
            instance,
            body,
            'WAID:' + body.key.id,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        }

        if (body.key.remoteJid.includes('@g.us')) {
          const participantName = body.pushName;
          const rawPhoneNumber =
            body.key.addressingMode === 'lid' && !body.key.fromMe && body.key.participantAlt
              ? body.key.participantAlt.split('@')[0].split(':')[0]
              : body.key.participant.split('@')[0].split(':')[0];
          const formattedPhoneNumber = parsePhoneNumberFromString(`+${rawPhoneNumber}`).formatInternational();

          let content: string;

          if (!body.key.fromMe) {
            content = `**${formattedPhoneNumber} - ${participantName}:**\n\n${bodyMessage}`;
          } else {
            content = `${bodyMessage}`;
          }

          const send = await this.createMessage(
            instance,
            getConversation,
            content,
            messageType,
            false,
            [],
            body,
            'WAID:' + body.key.id,
            quotedMsg,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        } else {
          const send = await this.createMessage(
            instance,
            getConversation,
            bodyMessage,
            messageType,
            false,
            [],
            body,
            'WAID:' + body.key.id,
            quotedMsg,
          );

          if (!send) {
            this.logger.warn('message not sent');
            return;
          }

          return send;
        }
      }

      if (event === Events.MESSAGES_DELETE) {
        const chatwootDelete = this.configService.get<Chatwoot>('CHATWOOT').MESSAGE_DELETE;

        if (chatwootDelete === true) {
          if (!body?.key?.id) {
            this.logger.warn('message id not found');
            return;
          }

          const message = await this.getMessageByKeyId(instance, body.key.id);

          if (message?.chatwootMessageId && message?.chatwootConversationId) {
            await this.prismaRepository.message.deleteMany({
              where: {
                keyId: body.key.id,
                instanceId: instance.instanceId,
              },
            });

            return await client.messages.delete({
              accountId: this.provider.accountId,
              conversationId: message.chatwootConversationId,
              messageId: message.chatwootMessageId,
            });
          }
        }
      }

      if (event === 'messages.edit' || event === 'send.message.update') {
        const editedMessageContentRaw =
          body?.editedMessage?.conversation ??
          body?.editedMessage?.extendedTextMessage?.text ??
          body?.editedMessage?.imageMessage?.caption ??
          body?.editedMessage?.videoMessage?.caption ??
          body?.editedMessage?.documentMessage?.caption ??
          (typeof body?.text === 'string' ? body.text : undefined);

        const editedMessageContent = (editedMessageContentRaw ?? '').trim();

        if (!editedMessageContent) {
          this.logger.info('[CW.EDIT] Conteúdo vazio — ignorando (DELETE tratará se for revoke).');
          return;
        }

        const message = await this.getMessageByKeyId(instance, body?.key?.id);

        if (!message) {
          this.logger.warn('Message not found for edit event');
          return;
        }

        const key = message.key as WAMessageKey;

        const messageType = key?.fromMe ? 'outgoing' : 'incoming';

        if (message && message.chatwootConversationId && message.chatwootMessageId) {
          // Criar nova mensagem com formato: "Mensagem editada:\n\nteste1"
          const editedText = `\n\n\`${i18next.t('cw.message.edited')}:\`\n\n${editedMessageContent}`;

          const send = await this.createMessage(
            instance,
            message.chatwootConversationId,
            editedText,
            messageType,
            false,
            [],
            {
              message: { extendedTextMessage: { contextInfo: { stanzaId: key.id } } },
            },
            'WAID:' + body.key.id,
            null,
          );
          if (!send) {
            this.logger.warn('edited message not sent');
            return;
          }
        }
        return;
      }

      if (event === 'messages.read') {
        if (!body?.key?.id || !body?.key?.remoteJid) {
          this.logger.warn('message id not found');
          return;
        }

        const message = await this.getMessageByKeyId(instance, body.key.id);
        const conversationId = message?.chatwootConversationId;
        const contactInboxSourceId = message?.chatwootContactInboxSourceId;

        if (conversationId) {
          let sourceId = contactInboxSourceId;
          const inbox = (await this.getInbox(instance)) as inbox & {
            inbox_identifier?: string;
          };

          if (!sourceId && inbox) {
            const conversation = (await client.conversations.get({
              accountId: this.provider.accountId,
              conversationId: conversationId,
            })) as conversation_show & {
              last_non_activity_message: { conversation: { contact_inbox: contact_inboxes } };
            };
            sourceId = conversation.last_non_activity_message?.conversation?.contact_inbox?.source_id;
          }

          if (sourceId && inbox?.inbox_identifier) {
            const url =
              `/public/api/v1/inboxes/${inbox.inbox_identifier}/contacts/${sourceId}` +
              `/conversations/${conversationId}/update_last_seen`;
            await chatwootRequest(this.getClientCwConfig(), {
              method: 'POST',
              url: url,
            });
          }
        }
        return;
      }

      if (event === 'status.instance') {
        const data = body;
        const inbox = await this.getInbox(instance);

        if (!inbox) {
          this.logger.warn('inbox not found');
          return;
        }

        const msgStatus = i18next.t('cw.inbox.status', {
          inboxName: inbox.name,
          state: data.status,
        });

        await this.createBotMessage(instance, msgStatus, 'incoming');
      }

      if (event === 'connection.update' && body.status === 'open') {
        const waInstance = this.waMonitor.waInstances[instance.instanceName];
        if (!waInstance) return;

        const now = Date.now();
        const timeSinceLastNotification = now - (waInstance.lastConnectionNotification || 0);

        // Se a conexão foi estabelecida via QR code, notifica imediatamente.
        if (waInstance.qrCode && waInstance.qrCode.count > 0) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.qrCode.count = 0;
          waInstance.lastConnectionNotification = now;
          chatwootImport.clearAll(instance);
        }
        // Se não foi via QR code, verifica o throttling.
        else if (timeSinceLastNotification >= 30000) {
          const msgConnection = i18next.t('cw.inbox.connected');
          await this.createBotMessage(instance, msgConnection, 'incoming');
          waInstance.lastConnectionNotification = now;
        } else {
          this.logger.warn(
            `Connection notification skipped for ${instance.instanceName} - too frequent (${timeSinceLastNotification}ms since last)`,
          );
        }
      }

      if (event === 'qrcode.updated') {
        if (body.statusCode === 500) {
          const erroQRcode = `🚨 ${i18next.t('qrlimitreached')}`;
          return await this.createBotMessage(instance, erroQRcode, 'incoming');
        } else {
          const fileData = Buffer.from(body?.qrcode.base64.replace('data:image/png;base64,', ''), 'base64');

          const fileStream = new Readable();
          fileStream._read = () => {};
          fileStream.push(fileData);
          fileStream.push(null);

          await this.createBotQr(
            instance,
            i18next.t('qrgeneratedsuccesfully'),
            'incoming',
            fileStream,
            `${instance.instanceName}.png`,
          );

          let msgQrCode = `⚡️${i18next.t('qrgeneratedsuccesfully')}\n\n${i18next.t('scanqr')}`;

          if (body?.qrcode?.pairingCode) {
            msgQrCode =
              msgQrCode +
              `\n\n*Pairing Code:* ${body.qrcode.pairingCode.substring(0, 4)}-${body.qrcode.pairingCode.substring(
                4,
                8,
              )}`;
          }

          await this.createBotMessage(instance, msgQrCode, 'incoming');
        }
      }
    } catch (error) {
      this.logger.error(error);
    }
  }

  public normalizeJidIdentifier(remoteJid: string) {
    if (!remoteJid) {
      return '';
    }
    if (remoteJid.includes('@lid')) {
      return remoteJid;
    }
    return remoteJid.replace(/:\d+/, '').split('@')[0];
  }

  public startImportHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    this.createBotMessage(instance, i18next.t('cw.import.startImport'), 'incoming');
  }

  public isImportHistoryAvailable() {
    return this.getImportHistoryAvailability().available;
  }

  public getImportHistoryAvailability() {
    const uri = this.configService.get<Chatwoot>('CHATWOOT').IMPORT.DATABASE.CONNECTION.URI?.trim();

    if (!uri) {
      return {
        available: false,
        reason: 'Configure CHATWOOT_IMPORT_DATABASE_CONNECTION_URI com a URI do PostgreSQL do Chatwoot.',
      };
    }

    if (uri === 'postgres://user:password@hostname:port/dbname') {
      return {
        available: false,
        reason: 'Substitua a URI placeholder do importador por uma conexao real do PostgreSQL do Chatwoot.',
      };
    }

    try {
      const parsedUri = new URL(uri);
      const hostname = parsedUri.hostname?.trim().toLowerCase();

      if (!hostname || hostname === 'host' || hostname === 'hostname') {
        return {
          available: false,
          reason:
            'CHATWOOT_IMPORT_DATABASE_CONNECTION_URI ainda usa host placeholder. Informe o hostname real do PostgreSQL do Chatwoot.',
        };
      }
    } catch {
      return {
        available: false,
        reason: 'CHATWOOT_IMPORT_DATABASE_CONNECTION_URI esta invalida.',
      };
    }

    return {
      available: true,
      reason: null,
    };
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: MessageModel[]) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    chatwootImport.addHistoryMessages(instance, messagesRaw);
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: ContactModel[]) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    return chatwootImport.addHistoryContacts(instance, contactsRaw);
  }

  public async importHistoryMessages(instance: InstanceDto) {
    if (!this.isImportHistoryAvailable()) {
      return;
    }

    this.createBotMessage(instance, i18next.t('cw.import.importingMessages'), 'incoming');

    const totalMessagesImported = await chatwootImport.importHistoryMessages(
      instance,
      this,
      await this.getInbox(instance),
      this.provider,
    );
    this.updateContactAvatarInRecentConversations(instance);

    const msg = Number.isInteger(totalMessagesImported)
      ? i18next.t('cw.import.messagesImported', { totalMessagesImported })
      : i18next.t('cw.import.messagesException');

    this.createBotMessage(instance, msg, 'incoming');

    return totalMessagesImported;
  }

  public async updateContactAvatarInRecentConversations(instance: InstanceDto, limitContacts = 100) {
    try {
      if (!this.isImportHistoryAvailable()) {
        return;
      }

      const client = await this.clientCw(instance);
      if (!client) {
        this.logger.warn('client not found');
        return null;
      }

      const inbox = await this.getInbox(instance);
      if (!inbox) {
        this.logger.warn('inbox not found');
        return null;
      }

      const recentContacts = await chatwootImport.getContactsOrderByRecentConversations(
        inbox,
        this.provider,
        limitContacts,
      );

      const contactIdentifiers = recentContacts
        .map((contact) => contact.identifier)
        .filter((identifier) => identifier !== null);

      const contactsWithProfilePicture = (
        await this.prismaRepository.contact.findMany({
          where: {
            instanceId: instance.instanceId,
            id: {
              in: contactIdentifiers,
            },
            profilePicUrl: {
              not: null,
            },
          },
        })
      ).reduce((acc: Map<string, ContactModel>, contact: ContactModel) => acc.set(contact.id, contact), new Map());

      recentContacts.forEach(async (contact) => {
        if (contactsWithProfilePicture.has(contact.identifier)) {
          client.contacts.update({
            accountId: this.provider.accountId,
            id: contact.id,
            data: {
              avatar_url: contactsWithProfilePicture.get(contact.identifier).profilePictureUrl || null,
            },
          });
        }
      });
    } catch (error) {
      this.logger.error(`Error on update avatar in recent conversations: ${error.toString()}`);
    }
  }

  public async syncLostMessages(
    instance: InstanceDto,
    chatwootConfig: ChatwootDto,
    prepareMessage: (message: any) => any,
  ) {
    try {
      if (!this.isImportHistoryAvailable()) {
        return;
      }
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
        return;
      }

      const inbox = await this.getInbox(instance);

      const sqlMessages = `select * from messages m
      where account_id = ${chatwootConfig.accountId}
      and inbox_id = ${inbox.id}
      and created_at >= now() - interval '6h'
      order by created_at desc`;

      const messagesData = (await this.pgClient.query(sqlMessages))?.rows;
      const ids: string[] = messagesData
        .filter((message) => !!message.source_id)
        .map((message) => message.source_id.replace('WAID:', ''));

      const savedMessages = await this.prismaRepository.message.findMany({
        where: {
          Instance: { name: instance.instanceName },
          messageTimestamp: { gte: Number(dayjs().subtract(6, 'hours').unix()) },
          AND: ids.map((id) => ({ keyId: { not: id } })),
        },
      });

      const filteredMessages = savedMessages.filter(
        (msg: any) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid),
      );
      const messagesRaw: any[] = [];
      for (const m of filteredMessages) {
        if (!m.message || !m.key || !m.messageTimestamp) {
          continue;
        }

        if (Long.isLong(m?.messageTimestamp)) {
          m.messageTimestamp = m.messageTimestamp?.toNumber();
        }

        messagesRaw.push(prepareMessage(m as any));
      }

      this.addHistoryMessages(
        instance,
        messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)),
      );

      await chatwootImport.importHistoryMessages(instance, this, inbox, this.provider);
      const waInstance = this.waMonitor.waInstances[instance.instanceName];
      waInstance.clearCacheChatwoot();
    } catch {
      return;
    }
  }
}
