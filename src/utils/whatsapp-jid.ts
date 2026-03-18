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
  const canonicalJid = explicitCanonicalJid || specialJid || lidJid || phoneJid || remoteJid || remoteJidAlt || null;

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

/**
 * Returns true if the given name looks like a technical identifier rather than a
 * human-readable display name. Technical names include full JIDs, the local part of
 * any known alias (e.g. raw phone digits), and bare phone-number strings.
 *
 * Limitation: generic weak human names (e.g. "Cliente", "Fulano") are treated as
 * human — detecting "low-quality human names" is out of scope for this version.
 */
export function isTechnicalDisplayName(name: string | null | undefined, identifiers: string[] = []): boolean {
  if (!name || !name.trim()) {
    return true;
  }

  const trimmed = name.trim();

  // Matches a full JID alias (e.g. "5511999@lid", "5511999@s.whatsapp.net")
  if (identifiers.some((id) => id === trimmed)) {
    return true;
  }

  // Matches the local part of any alias (the digits/hex before the "@")
  if (identifiers.some((id) => id.split('@')[0] === trimmed)) {
    return true;
  }

  // Bare phone number: only digits and optional leading "+", 7–15 chars
  if (/^\+?[0-9]{7,15}$/.test(trimmed)) {
    return true;
  }

  return false;
}

export interface ResolveChatwootDisplayNameParams {
  /** Raw pushName from the WhatsApp message event */
  pushName?: string | null;
  /** Clean phone digits (no "@" domain), e.g. "5511999999999" */
  phoneNumber?: string | null;
  /** Existing contact name already stored in Chatwoot */
  currentName?: string | null;
  /** All known JID aliases for this contact (used by isTechnicalDisplayName) */
  identifiers?: string[];
  /** Fallback when no useful name can be resolved. Defaults to "Contato WhatsApp" */
  fallback?: string;
}

/**
 * Resolves the human-readable display name to send to Chatwoot.
 *
 * Priority policy:
 *  1. currentName is human + phoneNumber exists  → "currentName — phoneNumber"
 *  2. currentName is human + no phoneNumber      → currentName
 *  3. pushName is useful + phoneNumber exists    → "pushName — phoneNumber"
 *  4. pushName is useful + no phoneNumber        → pushName
 *  5. phoneNumber only                           → phoneNumber
 *  6. nothing useful                             → fallback ("Contato WhatsApp")
 *
 * Anti-duplication rules:
 *  - If the chosen name already contains the phoneNumber as a substring, the
 *    "— phoneNumber" suffix is omitted to avoid redundancy.
 *  - If pushName is numerically equivalent to phoneNumber (after stripping "+" and
 *    spaces), the composition is skipped and only phoneNumber is returned.
 */
export function resolveChatwootDisplayName({
  pushName,
  phoneNumber,
  currentName,
  identifiers = [],
  fallback = 'Contato WhatsApp',
}: ResolveChatwootDisplayNameParams): string {
  const phone = phoneNumber?.trim() || null;

  const compose = (humanName: string): string => {
    if (!phone) {
      return humanName;
    }
    // Skip suffix if the name already embeds the phone digits
    if (humanName.includes(phone)) {
      return humanName;
    }
    return `${humanName} — ${phone}`;
  };

  // 1 & 2: preserve a genuinely human currentName
  if (currentName && !isTechnicalDisplayName(currentName, identifiers)) {
    return compose(currentName.trim());
  }

  const push = pushName?.trim() || null;

  if (push) {
    // Guard: if pushName is numerically the same as the phone number, treat as
    // phone-only to avoid display names like "5511999 — 5511999"
    const normalizedPush = push.replace(/^\+/, '');
    const normalizedPhone = phone?.replace(/^\+/, '') || null;
    if (normalizedPhone && normalizedPush === normalizedPhone) {
      return phone!;
    }

    // 3 & 4: use pushName (with optional phone suffix)
    return compose(push);
  }

  // 5: phone only
  if (phone) {
    return phone;
  }

  // 6: nothing useful
  return fallback;
}
