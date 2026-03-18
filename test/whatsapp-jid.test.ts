import assert from 'node:assert/strict';

import {
  enrichWhatsappKey,
  getChatwootIdentifier,
  getChatwootPhoneNumber,
  getJidAliases,
  isTechnicalDisplayName,
  resolveChatwootDisplayName,
  resolveCanonicalJid,
} from '../src/utils/whatsapp-jid';

export async function runWhatsappJidTests() {
  const directIdentity = resolveCanonicalJid({
    remoteJid: '5511999999999@lid',
    remoteJidAlt: '5511999999999@s.whatsapp.net',
  });

  assert.equal(directIdentity.canonicalJid, '5511999999999@lid');
  assert.equal(directIdentity.phoneJid, '5511999999999@s.whatsapp.net');
  assert.equal(directIdentity.lidJid, '5511999999999@lid');
  assert.equal(getChatwootPhoneNumber(directIdentity), '5511999999999');

  const inverseDirectIdentity = resolveCanonicalJid({
    remoteJid: '5511999999999@s.whatsapp.net',
    remoteJidAlt: '5511999999999@lid',
  });

  assert.equal(inverseDirectIdentity.canonicalJid, '5511999999999@lid');
  assert.equal(inverseDirectIdentity.phoneJid, '5511999999999@s.whatsapp.net');
  assert.equal(inverseDirectIdentity.lidJid, '5511999999999@lid');

  const phoneOnlyIdentity = resolveCanonicalJid({
    remoteJid: '5511666666666@s.whatsapp.net',
  });

  assert.equal(phoneOnlyIdentity.canonicalJid, '5511666666666@s.whatsapp.net');
  assert.equal(phoneOnlyIdentity.phoneJid, '5511666666666@s.whatsapp.net');
  assert.equal(phoneOnlyIdentity.lidJid, null);

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

  const broadcastIdentity = resolveCanonicalJid({
    remoteJid: 'newsletter@broadcast',
  });
  assert.equal(broadcastIdentity.canonicalJid, 'newsletter@broadcast');
  assert.equal(broadcastIdentity.isBroadcast, true);

  const newsletterIdentity = resolveCanonicalJid({
    remoteJid: '1203630@newsletter',
    remoteJidAlt: '5511777777777@lid',
  });
  assert.equal(newsletterIdentity.canonicalJid, '1203630@newsletter');

  const enriched = enrichWhatsappKey({
    remoteJid: '5511777777777@lid',
    remoteJidAlt: '5511777777777@s.whatsapp.net',
  });

  assert.equal(enriched.canonicalJid, '5511777777777@lid');
  assert.deepEqual(getJidAliases(enriched), [
    '5511777777777@lid',
    '5511777777777@s.whatsapp.net',
  ]);

  // ── isTechnicalDisplayName ──────────────────────────────────────────────────

  // null / empty → technical
  assert.equal(isTechnicalDisplayName(null), true);
  assert.equal(isTechnicalDisplayName(''), true);
  assert.equal(isTechnicalDisplayName('   '), true);

  // full JID alias → technical
  assert.equal(isTechnicalDisplayName('5511999@lid', ['5511999@lid']), true);
  assert.equal(isTechnicalDisplayName('5511999@s.whatsapp.net', ['5511999@s.whatsapp.net']), true);

  // local part of an alias (raw digits) → technical
  assert.equal(isTechnicalDisplayName('5511999999999', ['5511999999999@lid', '5511999999999@s.whatsapp.net']), true);

  // bare phone number pattern → technical (no identifiers needed)
  assert.equal(isTechnicalDisplayName('5511999999999'), true);
  assert.equal(isTechnicalDisplayName('+5511999999999'), true);
  assert.equal(isTechnicalDisplayName('1234567'), true);

  // human name → NOT technical
  assert.equal(isTechnicalDisplayName('João Silva'), false);
  assert.equal(isTechnicalDisplayName('Maria'), false);
  assert.equal(isTechnicalDisplayName('Cliente'), false);

  // name with letters that also contains digits → NOT technical (human wins)
  assert.equal(isTechnicalDisplayName('João 55'), false);

  // ── resolveChatwootDisplayName ──────────────────────────────────────────────

  const aliases = ['5511999999999@lid', '5511999999999@s.whatsapp.net'];

  // Rule 1: human currentName + phone → "currentName — phone"
  assert.equal(
    resolveChatwootDisplayName({ currentName: 'João Silva', phoneNumber: '5511999999999', identifiers: aliases }),
    'João Silva — 5511999999999',
  );

  // Rule 2: human currentName + no phone → currentName
  assert.equal(
    resolveChatwootDisplayName({ currentName: 'João Silva', identifiers: aliases }),
    'João Silva',
  );

  // Rule 3: pushName + phone → "pushName — phone"
  assert.equal(
    resolveChatwootDisplayName({ pushName: 'Maria', phoneNumber: '5521988888888', identifiers: aliases }),
    'Maria — 5521988888888',
  );

  // Rule 4: pushName + no phone → pushName
  assert.equal(
    resolveChatwootDisplayName({ pushName: 'Maria', identifiers: aliases }),
    'Maria',
  );

  // Rule 5: phone only (currentName is technical, no pushName)
  assert.equal(
    resolveChatwootDisplayName({
      currentName: '5511999999999',
      phoneNumber: '5511999999999',
      identifiers: aliases,
    }),
    '5511999999999',
  );

  // Rule 6: nothing useful → fallback
  assert.equal(resolveChatwootDisplayName({}), 'Contato WhatsApp');
  assert.equal(resolveChatwootDisplayName({ fallback: 'Unknown' }), 'Unknown');

  // Anti-duplication: currentName already contains phone → no suffix
  assert.equal(
    resolveChatwootDisplayName({
      currentName: 'João 5511999999999',
      phoneNumber: '5511999999999',
      identifiers: aliases,
    }),
    'João 5511999999999',
  );

  // Anti-duplication: pushName is numerically equal to phone → use phone only
  assert.equal(
    resolveChatwootDisplayName({ pushName: '5511999999999', phoneNumber: '5511999999999' }),
    '5511999999999',
  );
  assert.equal(
    resolveChatwootDisplayName({ pushName: '+5511999999999', phoneNumber: '5511999999999' }),
    '5511999999999',
  );

  // Anti-duplication: pushName already contains phone as substring → no suffix
  assert.equal(
    resolveChatwootDisplayName({ pushName: 'Carlos 5511999', phoneNumber: '5511999' }),
    'Carlos 5511999',
  );

  // Technical currentName (LID alias) is NOT preserved → falls through to pushName
  assert.equal(
    resolveChatwootDisplayName({
      currentName: '5511999999999@lid',
      pushName: 'Ana',
      phoneNumber: '5511999999999',
      identifiers: aliases,
    }),
    'Ana — 5511999999999',
  );

  // Technical currentName (raw digits) is NOT preserved → falls through to pushName
  assert.equal(
    resolveChatwootDisplayName({
      currentName: '5511999999999',
      pushName: 'Pedro',
      phoneNumber: '5511999999999',
      identifiers: aliases,
    }),
    'Pedro — 5511999999999',
  );
}
