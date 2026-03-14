import { runBaileysMessageProcessorTests } from './baileysMessage.processor.test';
import { runWhatsappJidTests } from './whatsapp-jid.test';

async function main() {
  await runWhatsappJidTests();
  await runBaileysMessageProcessorTests();
  console.log('All tests passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
