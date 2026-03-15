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
}
