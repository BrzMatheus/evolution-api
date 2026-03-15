import { runBaileysMessageProcessorTests } from './baileysMessage.processor.test';
import { runChatwootHistoryServiceTests } from './chatwoot-history.service.test';
import { runChatwootServiceTests } from './chatwoot.service.test';
import { runWhatsappJidTests } from './whatsapp-jid.test';

async function main() {
  await runWhatsappJidTests();
  await runBaileysMessageProcessorTests();
  await runChatwootServiceTests();
  await runChatwootHistoryServiceTests();
  console.log('All tests passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
