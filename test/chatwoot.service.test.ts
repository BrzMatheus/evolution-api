import assert from 'node:assert/strict';

import { ChatwootService } from '../src/api/integrations/chatbot/chatwoot/services/chatwoot.service';

export async function runChatwootServiceTests() {
  const service = new ChatwootService(
    {
      waInstances: {
        demo: {},
      },
    } as any,
    {} as any,
    {} as any,
    {} as any,
  );

  (service as any).clientCw = async () => ({});

  const contactPayload: any = {
    remoteJid: '5511999999999@s.whatsapp.net',
    pushName: 'Nova mensagem',
    profilePicUrl: 'https://example.com/avatar.png',
    instanceId: 'instance-1',
  };

  await service.eventWhatsapp('contacts.update', { instanceName: 'demo', instanceId: 'instance-1' } as any, contactPayload);

  assert.equal(contactPayload.key, undefined);
  assert.equal(contactPayload.remoteJid, '5511999999999@s.whatsapp.net');

  const brazilianCanonicalNumbers = (service as any).getNumbers('+5521988874200');
  assert.deepEqual(brazilianCanonicalNumbers, ['+5521988874200']);

  const brazilianLegacyNumbers = (service as any).getNumbers('+552188874200');
  assert.deepEqual(brazilianLegacyNumbers, ['+552188874200', '+5521988874200']);

  const identity = (service as any).resolveWhatsappIdentity({
    key: {
      remoteJid: '5511999999999@lid',
      remoteJidAlt: '5511999999999@s.whatsapp.net',
    },
  });

  assert.equal(identity.canonicalIdentifier, '5511999999999@lid');
  assert.equal(identity.phoneNumber, '5511999999999');
  assert.deepEqual(identity.aliases, ['5511999999999@lid', '5511999999999@s.whatsapp.net']);

  let findIdentifierCalls: string[] = [];
  (service as any).findContactByIdentifier = async (_instance: any, identifier: string) => {
    findIdentifierCalls.push(identifier);
    return identifier === '5511999999999@s.whatsapp.net'
      ? { id: 7, identifier: '5511999999999@s.whatsapp.net', phone_number: '+5511999999999' }
      : null;
  };
  (service as any).findContact = async () => null;

  const foundByAlias = await (service as any).findContactByWhatsappIdentity(
    { instanceName: 'demo', instanceId: 'instance-1' },
    identity,
  );
  assert.equal(foundByAlias.id, 7);
  assert.deepEqual(findIdentifierCalls, ['5511999999999@lid', '5511999999999@s.whatsapp.net']);

  const promotedContact = { id: 10, identifier: '5511999999999@lid', phone_number: '+5511999999999' };
  let updatePayload: any = null;
  let clearedKeys: string[] = [];
  (service as any).findContactByIdentifier = async (_instance: any, identifier: string) => {
    if (identifier === '5511999999999@lid') {
      return null;
    }

    return null;
  };
  (service as any).findContact = async () => ({ id: 10, identifier: '5511999999999@s.whatsapp.net' });
  (service as any).updateContact = async (_instance: any, _id: number, data: any) => {
    updatePayload = data;
    return promotedContact;
  };
  (service as any).cache = {
    delete: async (key: string) => {
      clearedKeys.push(key);
    },
  };

  const syncResult = await (service as any).syncCanonicalIdentifier(
    { instanceName: 'demo', instanceId: 'instance-1' },
    identity,
  );
  assert.equal(syncResult.identifier, '5511999999999@lid');
  assert.deepEqual(updatePayload, {
    identifier: '5511999999999@lid',
    phone_number: '+5511999999999',
  });
  assert.deepEqual(clearedKeys, [
    'demo:createConversation-5511999999999@lid',
    'demo:createConversation-5511999999999@s.whatsapp.net',
  ]);

  let cachedEntries: { key: string; value: number; ttl: number }[] = [];
  (service as any).cache = {
    set: async (key: string, value: number, ttl: number) => {
      cachedEntries.push({ key, value, ttl });
    },
  };

  await service.setConversationCacheForIdentifiers(
    { instanceName: 'demo', instanceId: 'instance-1' } as any,
    ['5511999999999@lid', '5511999999999@s.whatsapp.net', '5511999999999@lid'],
    321,
    1800,
  );

  assert.deepEqual(cachedEntries, [
    { key: 'demo:createConversation-5511999999999@lid', value: 321, ttl: 1800 },
    { key: 'demo:createConversation-5511999999999@s.whatsapp.net', value: 321, ttl: 1800 },
  ]);

  // ── Display name resolution in createConversation ───────────────────────────

  const instanceCtx = { instanceName: 'demo', instanceId: 'instance-1' } as any;

  /**
   * Helper that drives createConversation up to the contact create/update call
   * and returns the name that was passed to the mocked method.
   */
  async function captureContactName(opts: {
    remoteJid: string;
    remoteJidAlt?: string;
    fromMe: boolean;
    pushName?: string;
    existingContactName?: string | null;
  }): Promise<string | undefined> {
    let capturedName: string | undefined;

    const svc = new ChatwootService(
      { waInstances: { demo: { profilePicture: async () => ({ profilePictureUrl: null }) } } } as any,
      {} as any,
      {} as any,
      {} as any,
    );

    (svc as any).provider = { accountId: 1 };
    (svc as any).getInbox = async () => ({ id: 99 });
    (svc as any).cache = {
      has: async () => false,
      get: async () => null,
      set: async () => {},
    };
    (svc as any).clientCw = async () => ({});

    if (opts.existingContactName !== undefined) {
      (svc as any).findContactByWhatsappIdentity = async () => ({
        id: 42,
        name: opts.existingContactName,
        identifier: opts.remoteJid,
        thumbnail: null,
      });
      (svc as any).updateContact = async (_inst: any, _id: number, data: any) => {
        capturedName = data.name;
        return { id: 42, name: capturedName };
      };
    } else {
      (svc as any).findContactByWhatsappIdentity = async () => null;
      (svc as any).createContact = async (_inst: any, _phone: string, _inbox: number, _group: boolean, name: string) => {
        capturedName = name;
        return { id: 42, name };
      };
    }

    const body: any = {
      key: {
        remoteJid: opts.remoteJid,
        remoteJidAlt: opts.remoteJidAlt ?? null,
        fromMe: opts.fromMe,
        id: 'msgid',
      },
      pushName: opts.pushName ?? null,
      message: { conversation: 'hello' },
    };

    // Silence errors that come from post-contact conversation logic (not under test)
    try {
      await (svc as any).createConversation(instanceCtx, body);
    } catch {
      // expected — mocks are partial
    }

    return capturedName;
  }

  // Case A: received message with pushName → "pushName — phone"
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@s.whatsapp.net',
      fromMe: false,
      pushName: 'João Silva',
    });
    assert.equal(name, 'João Silva — 5511999999999');
  }

  // Case B: sent message (fromMe) without pushName → just phone digits (Rule 5)
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@s.whatsapp.net',
      fromMe: true,
      pushName: undefined,
    });
    assert.equal(name, '5511999999999');
  }

  // Case C: contact exists with technical name (raw phone digits) → promote to "pushName — phone"
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@s.whatsapp.net',
      fromMe: false,
      pushName: 'Maria',
      existingContactName: '5511999999999',
    });
    assert.equal(name, 'Maria — 5511999999999');
  }

  // Case D: contact exists with LID as name → promote to "pushName — phone"
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@lid',
      remoteJidAlt: '5511999999999@s.whatsapp.net',
      fromMe: false,
      pushName: 'Carlos',
      existingContactName: '5511999999999@lid',
    });
    assert.equal(name, 'Carlos — 5511999999999');
  }

  // Case E: contact exists with a human name → preserve and compose with phone
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@s.whatsapp.net',
      fromMe: false,
      pushName: 'Different Name',
      existingContactName: 'Ana Souza',
    });
    assert.equal(name, 'Ana Souza — 5511999999999');
  }

  // Case F: no pushName, contact has technical name → fallback to phone
  {
    const name = await captureContactName({
      remoteJid: '5511999999999@s.whatsapp.net',
      fromMe: false,
      pushName: undefined,
      existingContactName: '5511999999999',
    });
    assert.equal(name, '5511999999999');
  }
}
