import { runBaileysMessageProcessorTests } from './baileysMessage.processor.test';
import { runChatwootServiceTests } from './chatwoot.service.test';
import { runWhatsappJidTests } from './whatsapp-jid.test';

async function main() {
  await runWhatsappJidTests();
  await runBaileysMessageProcessorTests();
  await runChatwootServiceTests();
  console.log('All tests passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
