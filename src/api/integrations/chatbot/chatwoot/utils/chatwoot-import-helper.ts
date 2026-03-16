import { InstanceDto } from '@api/dto/instance.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import { postgresClient } from '@api/integrations/chatbot/chatwoot/libs/postgres.client';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import { Chatwoot, configService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { inbox } from '@figuro/chatwoot-sdk';
import { Chatwoot as ChatwootModel, Contact, Message } from '@prisma/client';
import { getChatwootPhoneNumber, resolveCanonicalJid } from '@utils/whatsapp-jid';
import { proto } from 'baileys';

type ChatwootUser = {
  user_type: string;
  user_id: number;
};

export type FksChatwoot = {
  phone_number: string;
  contact_id: string;
  conversation_id: string;
};

type firstLastTimestamp = {
  first: number;
  last: number;
};

type IWebMessageInfo = Omit<proto.IWebMessageInfo, 'key'> & Partial<Pick<proto.IWebMessageInfo, 'key'>>;

type ImportHistoryOptions = {
  allowedPhoneNumbers?: Set<string>;
  forceFksByPhoneNumber?: Map<string, FksChatwoot>;
};

type NormalizedDirection = 'incoming' | 'outgoing';

type MessageFingerprint = {
  canonicalSourceId: string | null;
  fallbackSignature: string | null;
  token: string;
};

type ConversationInspectionResult = {
  overlapCount: number;
  matchedTokens: Set<string>;
  matchedCanonicalSourceIds: Set<string>;
  matchedFallbackSignatures: Set<string>;
  sourceIdCollisionRisk: boolean;
};

type MessageKeyCarrier = {
  remoteJid?: string | null;
  remoteJidAlt?: string | null;
  canonicalJid?: string | null;
  phoneJid?: string | null;
  lidJid?: string | null;
  id?: string | null;
  fromMe?: boolean | null;
};

type HistoryMessageCarrier = Message & {
  canonicalJid?: string | null;
  phoneJid?: string | null;
  lidJid?: string | null;
};

class ChatwootImport {
  private logger = new Logger('ChatwootImport');
  private repositoryMessagesCache = new Map<string, Set<string>>();
  private historyMessages = new Map<string, Message[]>();
  private historyContacts = new Map<string, Contact[]>();

  public getRepositoryMessagesCache(instance: InstanceDto) {
    return this.repositoryMessagesCache.has(instance.instanceName)
      ? this.repositoryMessagesCache.get(instance.instanceName)
      : null;
  }

  public setRepositoryMessagesCache(instance: InstanceDto, repositoryMessagesCache: Set<string>) {
    this.repositoryMessagesCache.set(instance.instanceName, repositoryMessagesCache);
  }

  public deleteRepositoryMessagesCache(instance: InstanceDto) {
    this.repositoryMessagesCache.delete(instance.instanceName);
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: Message[]) {
    const actualValue = this.historyMessages.has(instance.instanceName)
      ? this.historyMessages.get(instance.instanceName)
      : [];
    this.historyMessages.set(instance.instanceName, [...actualValue, ...messagesRaw]);
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: Contact[]) {
    const actualValue = this.historyContacts.has(instance.instanceName)
      ? this.historyContacts.get(instance.instanceName)
      : [];
    this.historyContacts.set(instance.instanceName, actualValue.concat(contactsRaw));
  }

  public deleteHistoryMessages(instance: InstanceDto) {
    this.historyMessages.delete(instance.instanceName);
  }

  public deleteHistoryContacts(instance: InstanceDto) {
    this.historyContacts.delete(instance.instanceName);
  }

  public clearAll(instance: InstanceDto) {
    this.deleteRepositoryMessagesCache(instance);
    this.deleteHistoryMessages(instance);
    this.deleteHistoryContacts(instance);
  }

  public getHistoryMessagesLenght(instance: InstanceDto) {
    return this.historyMessages.get(instance.instanceName)?.length ?? 0;
  }

  public async importHistoryContacts(instance: InstanceDto, provider: ChatwootDto) {
    try {
      if (this.getHistoryMessagesLenght(instance) > 0) {
        return;
      }

      const pgClient = postgresClient.getChatwootConnection();

      let totalContactsImported = 0;

      const contacts = this.historyContacts.get(instance.instanceName) || [];
      if (contacts.length === 0) {
        return 0;
      }

      let contactsChunk: Contact[] = this.sliceIntoChunks(contacts, 3000);
      while (contactsChunk.length > 0) {
        const labelSql = `SELECT id FROM labels WHERE title = '${provider.nameInbox}' AND account_id = ${provider.accountId} LIMIT 1`;

        let labelId = (await pgClient.query(labelSql))?.rows[0]?.id;

        if (!labelId) {
          // creating label in chatwoot db and getting the id
          const sqlLabel = `INSERT INTO labels (title, color, show_on_sidebar, account_id, created_at, updated_at) VALUES ('${provider.nameInbox}', '#34039B', true, ${provider.accountId}, NOW(), NOW()) RETURNING id`;

          labelId = (await pgClient.query(sqlLabel))?.rows[0]?.id;
        }

        // inserting contacts in chatwoot db
        let sqlInsert = `INSERT INTO contacts
          (name, phone_number, account_id, identifier, created_at, updated_at) VALUES `;
        const bindInsert = [provider.accountId];

        for (const contact of contactsChunk) {
          const isGroup = this.isIgnorePhoneNumber(contact.remoteJid);

          const contactName = isGroup ? `${contact.pushName} (GROUP)` : contact.pushName;
          bindInsert.push(contactName);
          const bindName = `$${bindInsert.length}`;

          let bindPhoneNumber: string;
          if (!isGroup) {
            bindInsert.push(`+${contact.remoteJid.split('@')[0]}`);
            bindPhoneNumber = `$${bindInsert.length}`;
          } else {
            bindPhoneNumber = 'NULL';
          }
          bindInsert.push(contact.remoteJid);
          const bindIdentifier = `$${bindInsert.length}`;

          sqlInsert += `(${bindName}, ${bindPhoneNumber}, $1, ${bindIdentifier}, NOW(), NOW()),`;
        }
        if (sqlInsert.slice(-1) === ',') {
          sqlInsert = sqlInsert.slice(0, -1);
        }
        sqlInsert += ` ON CONFLICT (identifier, account_id)
                       DO UPDATE SET
                        name = EXCLUDED.name,
                        phone_number = EXCLUDED.phone_number,
                        updated_at = NOW()`;

        totalContactsImported += (await pgClient.query(sqlInsert, bindInsert))?.rowCount ?? 0;

        const sqlTags = `SELECT id FROM tags WHERE name = '${provider.nameInbox}' LIMIT 1`;

        const tagData = (await pgClient.query(sqlTags))?.rows[0];
        let tagId = tagData?.id;

        const sqlTag = `INSERT INTO tags (name, taggings_count) VALUES ('${provider.nameInbox}', ${totalContactsImported}) ON CONFLICT (name) DO UPDATE SET taggings_count = tags.taggings_count + ${totalContactsImported} RETURNING id`;

        tagId = (await pgClient.query(sqlTag))?.rows[0]?.id;

        await pgClient.query(sqlTag);

        let sqlInsertLabel = `INSERT INTO taggings (tag_id, taggable_type, taggable_id, context, created_at) VALUES `;

        contactsChunk.forEach((contact) => {
          const bindTaggableId = `(SELECT id FROM contacts WHERE identifier = '${contact.remoteJid}' AND account_id = ${provider.accountId})`;
          sqlInsertLabel += `($1, $2, ${bindTaggableId}, $3, NOW()),`;
        });

        if (sqlInsertLabel.slice(-1) === ',') {
          sqlInsertLabel = sqlInsertLabel.slice(0, -1);
        }

        await pgClient.query(sqlInsertLabel, [tagId, 'Contact', 'labels']);

        contactsChunk = this.sliceIntoChunks(contacts, 3000);
      }

      this.deleteHistoryContacts(instance);

      return totalContactsImported;
    } catch (error) {
      this.logger.error(`Error on import history contacts: ${error.toString()}`);
    }
  }

  public async getExistingSourceIds(sourceIds: string[], conversationId?: number): Promise<Set<string>> {
    try {
      const existingSourceIdsSet = new Set<string>();

      if (sourceIds.length === 0) {
        return existingSourceIdsSet;
      }

      const formattedSourceIds = Array.from(
        new Set(
          sourceIds
            .map((sourceId) => this.normalizeSourceId(sourceId))
            .filter(Boolean)
            .map((sourceId) => sourceId.replace(/^wa:/, 'WAID:')),
        ),
      );
      const pgClient = postgresClient.getChatwootConnection();

      const params = conversationId ? [formattedSourceIds, conversationId] : [formattedSourceIds];

      const query = conversationId
        ? 'SELECT source_id FROM messages WHERE source_id = ANY($1) AND conversation_id = $2'
        : 'SELECT source_id FROM messages WHERE source_id = ANY($1)';

      const result = await pgClient.query(query, params);
      for (const row of result.rows) {
        const normalizedSourceId = this.normalizeSourceId(row.source_id);
        if (normalizedSourceId) {
          existingSourceIdsSet.add(normalizedSourceId);
        }
      }

      return existingSourceIdsSet;
    } catch (error) {
      this.logger.error(`Error on getExistingSourceIds: ${error.toString()}`);
      return new Set<string>();
    }
  }

  public async importHistoryMessages(
    instance: InstanceDto,
    chatwootService: ChatwootService,
    inbox: inbox,
    provider: ChatwootModel,
    options?: ImportHistoryOptions,
  ) {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const chatwootUser = await this.getChatwootUser(provider);
      if (!chatwootUser) {
        throw new Error('User not found to import messages.');
      }

      let totalMessagesImported = 0;

      let messagesOrdered = this.historyMessages.get(instance.instanceName) || [];
      if (messagesOrdered.length === 0) {
        return 0;
      }

      // ordering messages by number and timestamp asc
      messagesOrdered.sort((a, b) => {
        const aMessageTimestamp = a.messageTimestamp as any as number;
        const bMessageTimestamp = b.messageTimestamp as any as number;

        return (
          this.getMessageConversationKey(a).localeCompare(this.getMessageConversationKey(b)) ||
          aMessageTimestamp - bMessageTimestamp
        );
      });

      const allMessagesMappedByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesOrdered);
      // Map structure: +552199999999 => { first message timestamp from number, last message timestamp from number}
      const phoneNumbersWithTimestamp = new Map<string, firstLastTimestamp>();
      allMessagesMappedByPhoneNumber.forEach((messages: Message[], phoneNumber: string) => {
        phoneNumbersWithTimestamp.set(phoneNumber, {
          first: messages[0]?.messageTimestamp as any as number,
          last: messages[messages.length - 1]?.messageTimestamp as any as number,
        });
      });

      // When a history job already resolved the target conversation, dedupe must happen against that
      // conversation only. A global source_id prefilter can hide messages that are still missing from
      // the canonical timeline because the same WAID exists elsewhere in Chatwoot.
      if (!options?.forceFksByPhoneNumber?.size) {
        const existingSourceIds = await this.getExistingSourceIds(
          messagesOrdered.map((message: any) => message.key.id),
        );
        messagesOrdered = messagesOrdered.filter((message: any) => {
          const sourceId = this.normalizeSourceId(message?.key?.id);
          return !sourceId || !existingSourceIds.has(sourceId);
        });
      }
      // processing messages in batch
      const batchSize = 4000;
      let messagesChunk: Message[] = this.sliceIntoChunks(messagesOrdered, batchSize);
      while (messagesChunk.length > 0) {
        // Map structure: +552199999999 => Message[]
        const messagesByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesChunk, options?.allowedPhoneNumbers);

        if (messagesByPhoneNumber.size > 0) {
          const forcedPhoneNumbers = new Set(Array.from(options?.forceFksByPhoneNumber?.keys() || []));
          const autoMessagesByPhoneNumber = new Map(
            Array.from(messagesByPhoneNumber.entries()).filter(([phoneNumber]) => !forcedPhoneNumbers.has(phoneNumber)),
          );
          const autoPhoneNumbersWithTimestamp = new Map(
            Array.from(phoneNumbersWithTimestamp.entries()).filter(
              ([phoneNumber]) => !forcedPhoneNumbers.has(phoneNumber),
            ),
          );

          const fksByNumber = new Map<string, FksChatwoot>(options?.forceFksByPhoneNumber || []);

          if (autoMessagesByPhoneNumber.size > 0) {
            const autoFksByNumber = await this.selectOrCreateFksFromChatwoot(
              provider,
              inbox,
              autoPhoneNumbersWithTimestamp,
              autoMessagesByPhoneNumber,
            );

            autoFksByNumber.forEach((value, key) => fksByNumber.set(key, value));
          }

          // inserting messages in chatwoot db
          let sqlInsertMsg = `INSERT INTO messages
            (content, processed_message_content, account_id, inbox_id, conversation_id, message_type, private, content_type,
            sender_type, sender_id, source_id, created_at, updated_at) VALUES `;
          const bindInsertMsg = [provider.accountId, inbox.id];

          for (const [phoneNumber, rawMessages] of messagesByPhoneNumber.entries()) {
            const fksChatwoot = fksByNumber.get(phoneNumber);
            if (!fksChatwoot?.conversation_id || !fksChatwoot?.contact_id) {
              continue;
            }

            const conversationMessages = await this.filterAlreadyImportedMessages(
              instance,
              chatwootService,
              provider,
              Number(fksChatwoot.conversation_id),
              rawMessages,
            );

            conversationMessages.forEach((message) => {
              const messageKey = (message.key || {}) as {
                id?: string;
                fromMe?: boolean;
              };

              if (!message.message) {
                return;
              }

              const contentMessage = this.getContentMessage(chatwootService, message as any);
              if (!contentMessage) {
                return;
              }

              bindInsertMsg.push(contentMessage);
              const bindContent = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(fksChatwoot.conversation_id);
              const bindConversationId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(messageKey.fromMe ? '1' : '0');
              const bindMessageType = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(messageKey.fromMe ? chatwootUser.user_type : 'Contact');
              const bindSenderType = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(messageKey.fromMe ? chatwootUser.user_id : fksChatwoot.contact_id);
              const bindSenderId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(this.toChatwootSourceId(messageKey.id));
              const bindSourceId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.messageTimestamp as number);
              const bindmessageTimestamp = `$${bindInsertMsg.length}`;

              sqlInsertMsg += `(${bindContent}, ${bindContent}, $1, $2, ${bindConversationId}, ${bindMessageType}, FALSE, 0,
                  ${bindSenderType},${bindSenderId},${bindSourceId}, to_timestamp(${bindmessageTimestamp}), to_timestamp(${bindmessageTimestamp})),`;
            });
          }
          if (bindInsertMsg.length > 2) {
            if (sqlInsertMsg.slice(-1) === ',') {
              sqlInsertMsg = sqlInsertMsg.slice(0, -1);
            }
            totalMessagesImported += (await pgClient.query(sqlInsertMsg, bindInsertMsg))?.rowCount ?? 0;
          }
        }
        messagesChunk = this.sliceIntoChunks(messagesOrdered, batchSize);
      }

      this.deleteHistoryMessages(instance);
      this.deleteRepositoryMessagesCache(instance);

      const providerData: ChatwootDto = {
        ...provider,
        ignoreJids: Array.isArray(provider.ignoreJids) ? provider.ignoreJids.map((event) => String(event)) : [],
      };

      this.importHistoryContacts(instance, providerData);

      return totalMessagesImported;
    } catch (error) {
      this.logger.error(`Error on import history messages: ${error.toString()}`);

      this.deleteHistoryMessages(instance);
      this.deleteRepositoryMessagesCache(instance);
    }
  }

  public async selectOrCreateFksFromChatwoot(
    provider: ChatwootModel,
    inbox: inbox,
    phoneNumbersWithTimestamp: Map<string, firstLastTimestamp>,
    messagesByPhoneNumber: Map<string, Message[]>,
  ): Promise<Map<string, FksChatwoot>> {
    const pgClient = postgresClient.getChatwootConnection();

    const bindValues = [provider.accountId, inbox.id];
    const phoneNumberBind = Array.from(messagesByPhoneNumber.keys())
      .map((phoneNumber) => {
        const phoneNumberTimestamp = phoneNumbersWithTimestamp.get(phoneNumber);

        if (phoneNumberTimestamp) {
          bindValues.push(phoneNumber);
          let bindStr = `($${bindValues.length},`;

          bindValues.push(phoneNumberTimestamp.first);
          bindStr += `$${bindValues.length},`;

          bindValues.push(phoneNumberTimestamp.last);
          return `${bindStr}$${bindValues.length})`;
        }
      })
      .join(',');

    // select (or insert when necessary) data from tables contacts, contact_inboxes, conversations from chatwoot db
    const sqlFromChatwoot = `WITH
              phone_number AS (
                SELECT phone_number, created_at::INTEGER, last_activity_at::INTEGER FROM (
                  VALUES 
                   ${phoneNumberBind}
                 ) as t (phone_number, created_at, last_activity_at)
              ),

              only_new_phone_number AS (
                SELECT * FROM phone_number
                WHERE phone_number NOT IN (
                  SELECT phone_number
                  FROM contacts
                    JOIN contact_inboxes ci ON ci.contact_id = contacts.id AND ci.inbox_id = $2
                    JOIN conversations con ON con.contact_inbox_id = ci.id 
                      AND con.account_id = $1
                      AND con.inbox_id = $2
                      AND con.contact_id = contacts.id
                  WHERE contacts.account_id = $1
                )
              ),

              new_contact AS (
                INSERT INTO contacts (name, phone_number, account_id, identifier, created_at, updated_at)
                SELECT REPLACE(p.phone_number, '+', ''), p.phone_number, $1, CONCAT(REPLACE(p.phone_number, '+', ''),
                  '@s.whatsapp.net'), to_timestamp(p.created_at), to_timestamp(p.last_activity_at)
                FROM only_new_phone_number AS p
                ON CONFLICT(identifier, account_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                RETURNING id, phone_number, created_at, updated_at
              ),

              new_contact_inbox AS (
                INSERT INTO contact_inboxes (contact_id, inbox_id, source_id, created_at, updated_at)
                SELECT new_contact.id, $2, gen_random_uuid(), new_contact.created_at, new_contact.updated_at
                FROM new_contact 
                RETURNING id, contact_id, created_at, updated_at
              ),

              new_conversation AS (
                INSERT INTO conversations (account_id, inbox_id, status, contact_id,
                  contact_inbox_id, uuid, last_activity_at, created_at, updated_at)
                SELECT $1, $2, 0, new_contact_inbox.contact_id, new_contact_inbox.id, gen_random_uuid(),
                  new_contact_inbox.updated_at, new_contact_inbox.created_at, new_contact_inbox.updated_at
                FROM new_contact_inbox
                RETURNING id, contact_id
              )

              SELECT new_contact.phone_number, new_conversation.contact_id, new_conversation.id AS conversation_id
              FROM new_conversation 
              JOIN new_contact ON new_conversation.contact_id = new_contact.id

              UNION

              SELECT p.phone_number, c.id contact_id, con.id conversation_id
                FROM phone_number p
              JOIN contacts c ON c.phone_number = p.phone_number
              JOIN contact_inboxes ci ON ci.contact_id = c.id AND ci.inbox_id = $2
              JOIN conversations con ON con.contact_inbox_id = ci.id AND con.account_id = $1
                AND con.inbox_id = $2 AND con.contact_id = c.id`;

    const fksFromChatwoot = await pgClient.query(sqlFromChatwoot, bindValues);

    return new Map(fksFromChatwoot.rows.map((item: FksChatwoot) => [item.phone_number, item]));
  }

  public async getChatwootUser(provider: ChatwootModel): Promise<ChatwootUser> {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const sqlUser = `SELECT owner_type AS user_type, owner_id AS user_id
                         FROM access_tokens
                       WHERE token = $1`;

      return (await pgClient.query(sqlUser, [provider.token]))?.rows[0] || false;
    } catch (error) {
      this.logger.error(`Error on getChatwootUser: ${error.toString()}`);
    }
  }

  public createMessagesMapByPhoneNumber(
    messages: Message[],
    allowedPhoneNumbers?: Set<string>,
  ): Map<string, Message[]> {
    return messages.reduce((acc: Map<string, Message[]>, message: Message) => {
      const identity = this.getMessageIdentity(message);
      if (!this.isIgnorePhoneNumber(identity.canonicalJid || identity.remoteJid || '')) {
        const phoneNumber = this.getMessagePhoneNumber(message);
        if (!phoneNumber) {
          return acc;
        }

        const phoneNumberPlus = `+${phoneNumber}`;
        if (allowedPhoneNumbers && !allowedPhoneNumbers.has(phoneNumberPlus)) {
          return acc;
        }
        const groupedMessages = acc.has(phoneNumberPlus) ? acc.get(phoneNumberPlus) : [];
        groupedMessages.push(message);
        acc.set(phoneNumberPlus, groupedMessages);
      }

      return acc;
    }, new Map());
  }

  public async getContactsOrderByRecentConversations(
    inbox: inbox,
    provider: ChatwootModel,
    limit = 50,
  ): Promise<{ id: number; phone_number: string; identifier: string }[]> {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const sql = `SELECT contacts.id, contacts.identifier, contacts.phone_number
                     FROM conversations
                   JOIN contacts ON contacts.id = conversations.contact_id
                   WHERE conversations.account_id = $1
                     AND inbox_id = $2
                   ORDER BY conversations.last_activity_at DESC
                   LIMIT $3`;

      return (await pgClient.query(sql, [provider.accountId, inbox.id, limit]))?.rows;
    } catch (error) {
      this.logger.error(`Error on get recent conversations: ${error.toString()}`);
    }
  }

  public getContentMessage(chatwootService: ChatwootService, msg: IWebMessageInfo) {
    const normalizedMessage = this.unwrapMessagePayload(msg.message as Record<string, any> | undefined);
    const contentMessage = chatwootService.getConversationMessage(normalizedMessage);
    if (contentMessage) {
      return contentMessage;
    }

    if (!configService.get<Chatwoot>('CHATWOOT').IMPORT.PLACEHOLDER_MEDIA_MESSAGE) {
      return '';
    }

    const types = {
      documentMessage: normalizedMessage.documentMessage,
      documentWithCaptionMessage: normalizedMessage.documentWithCaptionMessage?.message?.documentMessage,
      imageMessage: normalizedMessage.imageMessage,
      videoMessage: normalizedMessage.videoMessage,
      audioMessage: normalizedMessage.audioMessage,
      stickerMessage: normalizedMessage.stickerMessage,
      templateMessage: normalizedMessage.templateMessage?.hydratedTemplate?.hydratedContentText,
    };

    const typeKey = Object.keys(types).find((key) => types[key] !== undefined && types[key] !== null);
    switch (typeKey) {
      case 'documentMessage': {
        const doc = normalizedMessage.documentMessage;
        const fileName = doc?.fileName || 'document';
        const caption = doc?.caption ? ` ${doc.caption}` : '';
        return `_<File: ${fileName}${caption}>_`;
      }

      case 'documentWithCaptionMessage': {
        const doc = normalizedMessage.documentWithCaptionMessage?.message?.documentMessage;
        const fileName = doc?.fileName || 'document';
        const caption = doc?.caption ? ` ${doc.caption}` : '';
        return `_<File: ${fileName}${caption}>_`;
      }

      case 'templateMessage': {
        const template = normalizedMessage.templateMessage?.hydratedTemplate;
        return (
          (template?.hydratedTitleText ? `*${template.hydratedTitleText}*\n` : '') +
          (template?.hydratedContentText || '')
        );
      }

      case 'imageMessage':
        return '_<Image Message>_';

      case 'videoMessage':
        return '_<Video Message>_';

      case 'audioMessage':
        return '_<Audio Message>_';

      case 'stickerMessage':
        return '_<Sticker Message>_';

      default: {
        const rawTypeKey = Object.keys(normalizedMessage || {}).find(
          (key) =>
            normalizedMessage?.[key] !== undefined && normalizedMessage?.[key] !== null && key !== 'messageContextInfo',
        );

        if (rawTypeKey) {
          return `_<${this.humanizeMessageType(rawTypeKey)}>_`;
        }

        return '';
      }
    }
  }

  public sliceIntoChunks(arr: any[], chunkSize: number) {
    return arr.splice(0, chunkSize);
  }

  public isGroup(remoteJid: string) {
    return remoteJid.includes('@g.us');
  }

  public isIgnorePhoneNumber(remoteJid: string) {
    return this.isGroup(remoteJid) || remoteJid === 'status@broadcast' || remoteJid === '0@s.whatsapp.net';
  }

  public updateMessageSourceID(messageId: string | number, sourceId: string) {
    const pgClient = postgresClient.getChatwootConnection();

    const sql = `UPDATE messages SET source_id = $1, status = 0, created_at = NOW(), updated_at = NOW() WHERE id = $2;`;

    return pgClient.query(sql, [this.toChatwootSourceId(sourceId), messageId]);
  }

  public normalizeSourceId(sourceId: string | null | undefined) {
    if (!sourceId) {
      return null;
    }

    const normalized = String(sourceId).trim();
    if (!normalized) {
      return null;
    }

    return `wa:${normalized
      .replace(/^WAID:/i, '')
      .replace(/^evo:wa:/i, '')
      .replace(/^wa:/i, '')}`;
  }

  public toChatwootSourceId(sourceId: string | null | undefined) {
    const normalized = this.normalizeSourceId(sourceId);
    if (!normalized) {
      return null;
    }

    return normalized.replace(/^wa:/, 'WAID:');
  }

  public normalizeContent(content: string | null | undefined) {
    return String(content || '')
      .trim()
      .replace(/\s+/g, ' ')
      .toLowerCase();
  }

  public buildFallbackSignature(
    createdAtEpoch: number | null | undefined,
    direction: NormalizedDirection,
    content: string | null | undefined,
  ) {
    if (!createdAtEpoch) {
      return null;
    }

    return `${createdAtEpoch}:${direction}:${this.normalizeContent(content)}`;
  }

  public buildMessageFingerprint(chatwootService: ChatwootService, message: Message): MessageFingerprint {
    const messageKey = (message.key || {}) as {
      id?: string;
      fromMe?: boolean;
    };
    const content = this.getContentMessage(chatwootService, message as any);
    const canonicalSourceId = this.normalizeSourceId(messageKey.id || message.keyId);
    const fallbackSignature = this.buildFallbackSignature(
      Number(message.messageTimestamp || 0),
      messageKey.fromMe ? 'outgoing' : 'incoming',
      content,
    );

    return {
      canonicalSourceId,
      fallbackSignature,
      token: canonicalSourceId ? `source:${canonicalSourceId}` : `fallback:${fallbackSignature || 'unknown'}`,
    };
  }

  public async inspectConversationMessages(
    instance: InstanceDto,
    chatwootService: ChatwootService,
    provider: ChatwootModel,
    conversationId: number,
    messages: Message[],
  ): Promise<ConversationInspectionResult> {
    const fingerprints = messages.map((message) => this.buildMessageFingerprint(chatwootService, message));
    const canonicalSourceIds = Array.from(
      new Set(fingerprints.map((fingerprint) => fingerprint.canonicalSourceId).filter(Boolean)),
    ) as string[];
    const timestamps = messages
      .map((message) => Number(message.messageTimestamp || 0))
      .filter((timestamp) => Number.isFinite(timestamp) && timestamp > 0);

    if (fingerprints.length === 0 || timestamps.length === 0) {
      return {
        overlapCount: 0,
        matchedTokens: new Set<string>(),
        matchedCanonicalSourceIds: new Set<string>(),
        matchedFallbackSignatures: new Set<string>(),
        sourceIdCollisionRisk: false,
      };
    }

    const sourceToFallbackMap = new Map<string, Set<string>>();
    fingerprints.forEach((fingerprint) => {
      if (!fingerprint.canonicalSourceId || !fingerprint.fallbackSignature) {
        return;
      }

      const values = sourceToFallbackMap.get(fingerprint.canonicalSourceId) || new Set<string>();
      values.add(fingerprint.fallbackSignature);
      sourceToFallbackMap.set(fingerprint.canonicalSourceId, values);
    });

    const pgClient = postgresClient.getChatwootConnection();
    const minTimestamp = Math.min(...timestamps) - 86400;
    const maxTimestamp = Math.max(...timestamps) + 86400;
    const rows =
      (
        await pgClient.query(
          `SELECT source_id, created_at, message_type, content, processed_message_content
           FROM messages
          WHERE account_id = $1
            AND conversation_id = $2
            AND (
              source_id = ANY($3)
              OR created_at BETWEEN to_timestamp($4) AND to_timestamp($5)
            )`,
          [
            provider.accountId,
            conversationId,
            canonicalSourceIds.map((sourceId) => sourceId.replace(/^wa:/, 'WAID:')),
            minTimestamp,
            maxTimestamp,
          ],
        )
      )?.rows || [];

    const matchedTokens = new Set<string>();
    const matchedCanonicalSourceIds = new Set<string>();
    const matchedFallbackSignatures = new Set<string>();
    let sourceIdCollisionRisk = Array.from(sourceToFallbackMap.values()).some((values) => values.size > 1);

    const existingFallbackBySource = new Map<string, Set<string>>();
    const existingFallbackSignatures = new Set<string>();
    rows.forEach((row) => {
      const canonicalSourceId = this.normalizeSourceId(row.source_id);
      const fallbackSignature = this.buildFallbackSignature(
        row.created_at ? Math.floor(new Date(row.created_at).getTime() / 1000) : null,
        this.normalizeDirection(row.message_type),
        row.processed_message_content || row.content,
      );

      if (canonicalSourceId) {
        const values = existingFallbackBySource.get(canonicalSourceId) || new Set<string>();
        if (fallbackSignature) {
          values.add(fallbackSignature);
        }
        existingFallbackBySource.set(canonicalSourceId, values);
      }

      if (fallbackSignature) {
        existingFallbackSignatures.add(fallbackSignature);
      }
    });

    fingerprints.forEach((fingerprint) => {
      const sourceFallbacks =
        fingerprint.canonicalSourceId && fingerprint.fallbackSignature
          ? existingFallbackBySource.get(fingerprint.canonicalSourceId)
          : null;

      if (fingerprint.canonicalSourceId && sourceFallbacks) {
        matchedCanonicalSourceIds.add(fingerprint.canonicalSourceId);
        matchedTokens.add(fingerprint.token);

        if (
          fingerprint.fallbackSignature &&
          sourceFallbacks.size > 0 &&
          !sourceFallbacks.has(fingerprint.fallbackSignature)
        ) {
          sourceIdCollisionRisk = true;
        }
      }

      if (fingerprint.fallbackSignature && existingFallbackSignatures.has(fingerprint.fallbackSignature)) {
        matchedFallbackSignatures.add(fingerprint.fallbackSignature);
        matchedTokens.add(fingerprint.token);
      }
    });

    return {
      overlapCount: matchedTokens.size,
      matchedTokens,
      matchedCanonicalSourceIds,
      matchedFallbackSignatures,
      sourceIdCollisionRisk,
    };
  }

  public async filterAlreadyImportedMessages(
    instance: InstanceDto,
    chatwootService: ChatwootService,
    provider: ChatwootModel,
    conversationId: number,
    messages: Message[],
  ) {
    const inspection = await this.inspectConversationMessages(
      instance,
      chatwootService,
      provider,
      conversationId,
      messages,
    );

    if (inspection.matchedTokens.size === 0) {
      return messages;
    }

    return messages.filter((message) => {
      const fingerprint = this.buildMessageFingerprint(chatwootService, message);
      return !inspection.matchedTokens.has(fingerprint.token);
    });
  }

  private normalizeDirection(messageType: unknown): NormalizedDirection {
    return String(messageType) === '1' || String(messageType) === 'outgoing' ? 'outgoing' : 'incoming';
  }

  private unwrapMessagePayload(message?: Record<string, any>) {
    let current = message || {};

    for (let index = 0; index < 4; index += 1) {
      if (current?.ephemeralMessage?.message) {
        current = current.ephemeralMessage.message;
        continue;
      }

      if (current?.viewOnceMessageV2?.message) {
        current = current.viewOnceMessageV2.message;
        continue;
      }

      if (current?.viewOnceMessage?.message) {
        current = current.viewOnceMessage.message;
        continue;
      }

      if (current?.viewOnceMessageV2Extension?.message) {
        current = current.viewOnceMessageV2Extension.message;
        continue;
      }

      break;
    }

    return current || {};
  }

  private humanizeMessageType(typeKey: string) {
    return typeKey
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (char) => char.toUpperCase());
  }

  private getMessageIdentity(message: Message) {
    const key = ((message?.key || {}) as MessageKeyCarrier) || {};
    const source = message as HistoryMessageCarrier;

    return resolveCanonicalJid({
      remoteJid: key.remoteJid,
      remoteJidAlt: key.remoteJidAlt,
      canonicalJid: key.canonicalJid || source.canonicalJid,
      phoneJid: key.phoneJid || source.phoneJid,
      lidJid: key.lidJid || source.lidJid,
    });
  }

  private getMessagePhoneNumber(message: Message) {
    const phoneNumber = getChatwootPhoneNumber(this.getMessageIdentity(message));
    return phoneNumber ? String(phoneNumber).replace(/^\+/, '').split('@')[0] : null;
  }

  private getMessageConversationKey(message: Message) {
    const phoneNumber = this.getMessagePhoneNumber(message);
    if (phoneNumber) {
      return phoneNumber;
    }

    const identity = this.getMessageIdentity(message);
    return identity.canonicalJid || identity.remoteJid || '';
  }
}

export const chatwootImport = new ChatwootImport();
