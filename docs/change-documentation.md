# Change Documentation Guide

Use this guide for every meaningful code change in Delta Evolution API / ChatBravo foundations.

## Goal

Preserve decision context while changes are fresh, especially for WhatsApp capture, message identity, routing, and integration behavior.

## Default Rule

If a change alters behavior, assumptions, contracts, mappings, integration flow, or operational setup, document it in the same work item even if the user did not explicitly ask for documentation.

## Keep It Lightweight

Prefer concise updates. A short, accurate note is better than a long document that nobody maintains.

## What Must Be Documented

- Runtime behavior changes
- JID / identifier normalization or resolution changes
- API or webhook contract changes
- Integration-specific assumptions or limitations
- Migration or environment changes
- New operational risks, fallback logic, or compatibility constraints

## Minimum Template

Use this format wherever documentation is added:

```md
## Change
- What changed

## Reason
- Why this was necessary

## Impact
- Affected flows, compatibility notes, and operational risk

## References
- Relevant files, endpoints, providers, or follow-up tasks
```

## Preferred Locations

1. `CHANGELOG.md`
   Use for breaking changes, externally visible behavior, and upgrade-impact notes.
2. `README.md`
   Use for setup, public API usage, and operational workflows.
3. Feature-adjacent documentation
   Use when the change is specific to one integration or subsystem.
4. Inline code comments
   Use only for logic that is easy to misread later.

## Practical Heuristics

- Document semantic changes, not trivial refactors.
- If another engineer could ask "why is it like this now?", add a note.
- If the change affects ChatBravo capture fidelity or identity resolution, document it.
- If behavior differs from upstream Evolution API, document the divergence clearly.

## Examples That Should Trigger Documentation

- Changing canonical JID selection priority
- Introducing `lid` fallback behavior
- Updating Chatwoot identifier mapping
- Altering webhook retry semantics
- Adding or changing environment variables
- Adjusting tenant isolation rules

## Agent Reminder

Do not wait for a separate documentation request. Documentation is part of delivering the change.

## Change
- Direct WhatsApp chats now resolve `canonicalJid` as `lidJid` when both LID and PN aliases are available, while preserving `phoneJid` as the auxiliary phone identity.
- Chatwoot online reconciliation now reuses aliases first, phone second, and can promote existing PN-first contacts to LID-first identifiers during runtime.

## Reason
- PN-first resolution was creating inconsistent identifiers across WhatsApp events, Evolution persistence, and Chatwoot contact reuse.
- Centralizing the technical identity on LID reduces contact/conversation fragmentation without losing phone-based matching.

## Impact
- Direct-chat `canonicalJid` changes from PN-first to LID-first when both aliases exist.
- Chatwoot contacts keep `phone_number` based on PN, while `identifier` can be promoted to LID on demand for backward compatibility.
- No bulk migration is introduced in this round; compatibility is handled in the runtime flow.

## References
- `src/utils/whatsapp-jid.ts`
- `src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts`
- `test/whatsapp-jid.test.ts`

---

## Change
- Chatwoot contact display name now uses a dedicated `resolveChatwootDisplayName` function instead of ad-hoc fallbacks.
- Display name policy (in order): `currentName (human) — phone` → `currentName (human)` → `pushName — phone` → `pushName` → `phone` → `"Contato WhatsApp"`.
- Added `isTechnicalDisplayName` helper to detect raw phone digits, LID strings, and JID aliases as non-human names.
- `nameNeedsUpdate` check in `createConversation` now uses `isTechnicalDisplayName(contact.name, identity.aliases)` instead of `contact.name === chatId`, so LID-as-name is also caught and promoted.
- `chatwoot-history.service.ts`: `resolvePushName` fallback now extracts phone digits via `getChatwootPhoneNumber` to avoid exposing a LID hex local-part as a name; `resolveContactId` now calls `resolveChatwootDisplayName` for contact creation display name.
- The phone suffix (`— phone`) is omitted when the chosen name already contains the phone as a substring, or when pushName is numerically equal to the phone number.

## Reason
- Contacts were appearing in Chatwoot with LID strings (e.g. `5511999@lid`) or raw phone digits as their visible name, making them unrecognisable to agents.
- The old `nameContact = !fromMe ? pushName : chatId` logic silently degraded to phone digits for all sent messages with no pushName.
- Combining name + phone in the display name improves operational triage (agents can see both the human name and the number at a glance).

## Impact
- Existing contacts in Chatwoot whose name equals a known JID alias or raw phone digits will be renamed on the next incoming message (triggered by `nameNeedsUpdate = true`).
- Contacts with a genuine human name already set will have `— phoneNumber` appended on the next update (if no suffix present yet). This is a one-time update per contact.
- Groups: participant display names now use `resolveChatwootDisplayName`; the group contact itself still uses `group.subject (GROUP)` — no change there.
- Import SQL path (`chatwoot-import-helper.ts`): not touched; still uses raw `pushName` — flagged as a follow-up.
- **Limitation**: "weak human names" (e.g. `"Cliente"`) are treated as human and preserved. Detecting low-quality human names is deferred.

## References
- `src/utils/whatsapp-jid.ts` — `isTechnicalDisplayName`, `resolveChatwootDisplayName`
- `src/api/integrations/chatbot/chatwoot/services/chatwoot.service.ts` — `createConversation`
- `src/api/integrations/chatbot/chatwoot/services/chatwoot-history.service.ts` — `resolvePushName`, `resolveContactId`
- `test/whatsapp-jid.test.ts` — unit tests for both new functions
- `test/chatwoot.service.test.ts` — integration-style tests for display name in contact payloads
