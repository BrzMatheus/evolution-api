import assert from 'node:assert/strict';

import {
  enrichWhatsappKey,
  getChatwootIdentifier,
  getChatwootPhoneNumber,
  getJidAliases,
  resolveCanonicalJid,
} from '../src/utils/whatsapp-jid';

export async function runWhatsappJidTests() {
  const directIdentity = resolveCanonicalJid({
    remoteJid: '5511999999999@lid',
    remoteJidAlt: '5511999999999@s.whatsapp.net',
  });

  assert.equal(directIdentity.canonicalJid, '5511999999999@s.whatsapp.net');
  assert.equal(directIdentity.phoneJid, '5511999999999@s.whatsapp.net');
  assert.equal(directIdentity.lidJid, '5511999999999@lid');

  const lidOnlyIdentity = resolveCanonicalJid({
    remoteJid: '5511888888888@lid',
  });

  assert.equal(lidOnlyIdentity.canonicalJid, '5511888888888@lid');
  assert.equal(getChatwootIdentifier({ remoteJid: '5511888888888@lid' }), '5511888888888@lid');
  assert.equal(getChatwootPhoneNumber({ remoteJid: '5511888888888@lid' }), '5511888888888');

  const groupIdentity = resolveCanonicalJid({
    remoteJid: '5511999999999-123456@g.us',
    remoteJidAlt: '5511999999999@s.whatsapp.net',
  });

  assert.equal(groupIdentity.canonicalJid, '5511999999999-123456@g.us');
  assert.equal(groupIdentity.isGroup, true);
  assert.equal(getChatwootPhoneNumber({ remoteJid: '5511999999999-123456@g.us' }), '5511999999999-123456@g.us');

  const statusIdentity = resolveCanonicalJid({
    remoteJid: 'status@broadcast',
  });

  assert.equal(statusIdentity.canonicalJid, 'status@broadcast');
  assert.equal(statusIdentity.isStatus, true);

  const enriched = enrichWhatsappKey({
    remoteJid: '5511777777777@lid',
    remoteJidAlt: '5511777777777@s.whatsapp.net',
  });

  assert.equal(enriched.canonicalJid, '5511777777777@s.whatsapp.net');
  assert.deepEqual(getJidAliases(enriched), [
    '5511777777777@s.whatsapp.net',
    '5511777777777@lid',
  ]);
}
