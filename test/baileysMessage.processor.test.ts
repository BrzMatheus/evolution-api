import assert from 'node:assert/strict';

import {
  BaileysMessageProcessor,
  UnreconciledMessageUpdateError,
} from '../src/api/integrations/channel/whatsapp/baileysMessage.processor';

export async function runBaileysMessageProcessorTests() {
  const processor = new BaileysMessageProcessor();
  const processedEvents: string[] = [];
  let unresolvedAttempts = 0;

  (processor as any).sleep = async () => undefined;

  processor.mount({
    onEvent: async (eventName) => {
      processedEvents.push(eventName);

      if (eventName === 'messages.update' && unresolvedAttempts < 3) {
        unresolvedAttempts += 1;
        throw new UnreconciledMessageUpdateError('message not ready yet');
      }
    },
  });

  await Promise.all([
    processor.processEvent('messages.upsert', { messages: [], type: 'notify' } as any, {}),
    processor.processEvent('messages.update', [] as any, {}),
  ]);

  assert.deepEqual(processedEvents, [
    'messages.upsert',
    'messages.update',
    'messages.update',
    'messages.update',
    'messages.update',
  ]);
  assert.equal(unresolvedAttempts, 3);

  processor.onDestroy();

  const failingProcessor = new BaileysMessageProcessor();
  const permanentFailures: string[] = [];
  let finalAttempts = 0;

  (failingProcessor as any).sleep = async () => undefined;

  failingProcessor.mount({
    onEvent: async () => {
      finalAttempts += 1;
      throw new UnreconciledMessageUpdateError('still unresolved');
    },
    onFinalError: (eventName) => {
      permanentFailures.push(eventName);
    },
  });

  await assert.rejects(() => failingProcessor.processEvent('messages.update', [] as any, {}));
  assert.equal(finalAttempts, 4);
  assert.deepEqual(permanentFailures, ['messages.update']);

  failingProcessor.onDestroy();
}
