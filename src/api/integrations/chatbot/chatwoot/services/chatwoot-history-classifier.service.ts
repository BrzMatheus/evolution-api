export type ContactClassification = 'eligible' | 'needs_review' | 'lid_alias' | 'requires_rebuild' | 'ignored';
export type SuggestedAction = 'import_direct' | 'create_rebuild' | 'open_chatwoot' | 'ignore';
export type UnsafeReason =
  | 'existing_conversation_overlap'
  | 'lid_alias_detected'
  | 'multiple_candidate_conversations'
  | 'source_id_collision_risk'
  | 'chatwoot_history_already_present'
  | 'identity_conflict';
export type CanonicalIdentityType = 's_whatsapp_net' | 'unresolved';
export type IdentityResolutionStatus = 'resolved' | 'alias_only' | 'ambiguous';

type ClassifyChatwootHistoryContactArgs = {
  isGroup: boolean;
  isStatus: boolean;
  isBroadcast: boolean;
  evolutionMessageCount: number;
  hasLidAlias: boolean;
  candidateConversationCount: number;
  chatwootMessageCount: number;
  overlapCount: number;
  sourceIdCollisionRisk: boolean;
  canonicalIdentityType: CanonicalIdentityType;
  identityResolutionStatus: IdentityResolutionStatus;
};

type ChatwootHistoryContactDecision = {
  classification: ContactClassification;
  suggestedAction: SuggestedAction;
  isSafeDirectImport: boolean;
  unsafeReasons: UnsafeReason[];
};

export function classifyChatwootHistoryContact(
  args: ClassifyChatwootHistoryContactArgs,
): ChatwootHistoryContactDecision {
  if (args.isGroup || args.isStatus || args.isBroadcast || args.evolutionMessageCount === 0) {
    return {
      classification: 'ignored',
      suggestedAction: 'ignore',
      isSafeDirectImport: false,
      unsafeReasons: [],
    };
  }

  const unsafeReasons = new Set<UnsafeReason>();

  if (args.hasLidAlias) {
    unsafeReasons.add('lid_alias_detected');
  }

  if (args.candidateConversationCount > 1) {
    unsafeReasons.add('multiple_candidate_conversations');
  }

  if (args.overlapCount > 0) {
    unsafeReasons.add('existing_conversation_overlap');
  }

  if (args.sourceIdCollisionRisk) {
    unsafeReasons.add('source_id_collision_risk');
  }

  if (args.chatwootMessageCount > 0) {
    unsafeReasons.add('chatwoot_history_already_present');
  }

  if (args.canonicalIdentityType !== 's_whatsapp_net' || args.identityResolutionStatus !== 'resolved') {
    unsafeReasons.add('identity_conflict');
  }

  const unsafeReasonList = Array.from(unsafeReasons);
  const isSafeDirectImport = unsafeReasonList.length === 0;

  if (args.hasLidAlias) {
    return {
      classification: 'lid_alias',
      suggestedAction: args.candidateConversationCount > 0 ? 'create_rebuild' : 'open_chatwoot',
      isSafeDirectImport,
      unsafeReasons: unsafeReasonList,
    };
  }

  if (
    unsafeReasons.has('multiple_candidate_conversations') ||
    unsafeReasons.has('existing_conversation_overlap') ||
    unsafeReasons.has('source_id_collision_risk') ||
    unsafeReasons.has('identity_conflict')
  ) {
    return {
      classification: 'needs_review',
      suggestedAction: args.candidateConversationCount > 0 ? 'create_rebuild' : 'open_chatwoot',
      isSafeDirectImport,
      unsafeReasons: unsafeReasonList,
    };
  }

  if (unsafeReasons.has('chatwoot_history_already_present')) {
    return {
      classification:
        args.candidateConversationCount === 1 && args.evolutionMessageCount > args.chatwootMessageCount
          ? 'requires_rebuild'
          : 'needs_review',
      suggestedAction:
        args.candidateConversationCount === 1 && args.evolutionMessageCount > args.chatwootMessageCount
          ? 'create_rebuild'
          : 'open_chatwoot',
      isSafeDirectImport,
      unsafeReasons: unsafeReasonList,
    };
  }

  return {
    classification: 'eligible',
    suggestedAction: 'import_direct',
    isSafeDirectImport,
    unsafeReasons: unsafeReasonList,
  };
}
