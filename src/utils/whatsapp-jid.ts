type NullableJid = string | null | undefined;

export interface WhatsappJidCarrier {
  remoteJid?: NullableJid;
  remoteJidAlt?: NullableJid;
  canonicalJid?: NullableJid;
  phoneJid?: NullableJid;
  lidJid?: NullableJid;
}

export interface ResolvedWhatsappJid {
  canonicalJid: string | null;
  phoneJid: string | null;
  lidJid: string | null;
  remoteJid: string | null;
  remoteJidAlt: string | null;
  isDirect: boolean;
  isGroup: boolean;
  isStatus: boolean;
  isBroadcast: boolean;
}

function normalizeJid(jid?: NullableJid): string | null {
  if (!jid) {
    return null;
  }

  const normalized = String(jid).trim();
  if (!normalized) {
    return null;
  }

  return normalized.startsWith('+') ? normalized.slice(1) : normalized;
}

function isGroupJid(jid?: NullableJid) {
  return !!jid && jid.endsWith('@g.us');
}

function isStatusJid(jid?: NullableJid) {
  return jid === 'status@broadcast';
}

function isBroadcastJid(jid?: NullableJid) {
  return !!jid && jid.endsWith('@broadcast');
}

function isNewsletterJid(jid?: NullableJid) {
  return !!jid && jid.endsWith('@newsletter');
}

function isPhoneJid(jid?: NullableJid) {
  return !!jid && (jid.endsWith('@s.whatsapp.net') || jid.endsWith('@c.us'));
}

function isLidJid(jid?: NullableJid) {
  return !!jid && jid.endsWith('@lid');
}

export function resolveCanonicalJid(key: WhatsappJidCarrier): ResolvedWhatsappJid {
  const remoteJid = normalizeJid(key?.remoteJid);
  const remoteJidAlt = normalizeJid(key?.remoteJidAlt);
  const explicitCanonicalJid = normalizeJid(key?.canonicalJid);
  const explicitPhoneJid = normalizeJid(key?.phoneJid);
  const explicitLidJid = normalizeJid(key?.lidJid);

  const specialJid = [remoteJid, remoteJidAlt].find(
    (jid) => isGroupJid(jid) || isStatusJid(jid) || isBroadcastJid(jid) || isNewsletterJid(jid),
  );

  const phoneJid = explicitPhoneJid || [remoteJid, remoteJidAlt].find((jid) => isPhoneJid(jid)) || null;
  const lidJid = explicitLidJid || [remoteJid, remoteJidAlt].find((jid) => isLidJid(jid)) || null;
  const canonicalJid = explicitCanonicalJid || specialJid || phoneJid || lidJid || remoteJid || remoteJidAlt || null;

  return {
    canonicalJid,
    phoneJid,
    lidJid,
    remoteJid,
    remoteJidAlt,
    isDirect: !!canonicalJid && !specialJid,
    isGroup: isGroupJid(canonicalJid),
    isStatus: isStatusJid(canonicalJid),
    isBroadcast: isBroadcastJid(canonicalJid),
  };
}

export function enrichWhatsappKey<T extends WhatsappJidCarrier>(
  key: T,
): T & Required<Pick<ResolvedWhatsappJid, 'canonicalJid' | 'phoneJid' | 'lidJid'>> {
  const resolved = resolveCanonicalJid(key);

  return {
    ...key,
    canonicalJid: resolved.canonicalJid,
    phoneJid: resolved.phoneJid,
    lidJid: resolved.lidJid,
  };
}

export function getJidAliases(key: WhatsappJidCarrier): string[] {
  const resolved = resolveCanonicalJid(key);

  return [
    ...new Set(
      [resolved.canonicalJid, resolved.phoneJid, resolved.lidJid, resolved.remoteJid, resolved.remoteJidAlt].filter(
        Boolean,
      ),
    ),
  ];
}

export function getChatwootIdentifier(key: WhatsappJidCarrier): string | null {
  return resolveCanonicalJid(key).canonicalJid;
}

export function getChatwootPhoneNumber(key: WhatsappJidCarrier): string | null {
  const { canonicalJid, isDirect, phoneJid } = resolveCanonicalJid(key);

  if (!isDirect) {
    return canonicalJid;
  }

  return (phoneJid || canonicalJid)?.split('@')[0] || null;
}

export function normalizeWhatsappJid(jid?: NullableJid) {
  return normalizeJid(jid);
}
