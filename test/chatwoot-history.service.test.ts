import assert from 'node:assert/strict';

import { classifyChatwootHistoryContact } from '../src/api/integrations/chatbot/chatwoot/services/chatwoot-history-classifier.service';

export async function runChatwootHistoryServiceTests() {
  const eligible = classifyChatwootHistoryContact({
    isGroup: false,
    isStatus: false,
    isBroadcast: false,
    evolutionMessageCount: 12,
    hasLidAlias: false,
    candidateConversationCount: 0,
    chatwootMessageCount: 0,
    overlapCount: 0,
    sourceIdCollisionRisk: false,
    canonicalIdentityType: 's_whatsapp_net',
    identityResolutionStatus: 'resolved',
  });
  assert.equal(eligible.classification, 'eligible');
  assert.equal(eligible.suggestedAction, 'import_direct');
  assert.equal(eligible.isSafeDirectImport, true);
  assert.deepEqual(eligible.unsafeReasons, []);

  const needsReview = classifyChatwootHistoryContact({
    isGroup: false,
    isStatus: false,
    isBroadcast: false,
    evolutionMessageCount: 12,
    hasLidAlias: false,
    candidateConversationCount: 2,
    chatwootMessageCount: 6,
    overlapCount: 3,
    sourceIdCollisionRisk: true,
    canonicalIdentityType: 's_whatsapp_net',
    identityResolutionStatus: 'resolved',
  });
  assert.equal(needsReview.classification, 'needs_review');
  assert.equal(needsReview.suggestedAction, 'create_rebuild');
  assert.equal(needsReview.isSafeDirectImport, false);
  assert.ok(needsReview.unsafeReasons.includes('existing_conversation_overlap'));
  assert.ok(needsReview.unsafeReasons.includes('multiple_candidate_conversations'));
  assert.ok(needsReview.unsafeReasons.includes('source_id_collision_risk'));

  const lidAlias = classifyChatwootHistoryContact({
    isGroup: false,
    isStatus: false,
    isBroadcast: false,
    evolutionMessageCount: 8,
    hasLidAlias: true,
    candidateConversationCount: 0,
    chatwootMessageCount: 0,
    overlapCount: 0,
    sourceIdCollisionRisk: false,
    canonicalIdentityType: 'unresolved',
    identityResolutionStatus: 'alias_only',
  });
  assert.equal(lidAlias.classification, 'lid_alias');
  assert.equal(lidAlias.suggestedAction, 'open_chatwoot');
  assert.ok(lidAlias.unsafeReasons.includes('lid_alias_detected'));
  assert.ok(lidAlias.unsafeReasons.includes('identity_conflict'));

  const requiresRebuild = classifyChatwootHistoryContact({
    isGroup: false,
    isStatus: false,
    isBroadcast: false,
    evolutionMessageCount: 20,
    hasLidAlias: false,
    candidateConversationCount: 1,
    chatwootMessageCount: 5,
    overlapCount: 0,
    sourceIdCollisionRisk: false,
    canonicalIdentityType: 's_whatsapp_net',
    identityResolutionStatus: 'resolved',
  });
  assert.equal(requiresRebuild.classification, 'requires_rebuild');
  assert.equal(requiresRebuild.suggestedAction, 'create_rebuild');
  assert.equal(requiresRebuild.isSafeDirectImport, false);
  assert.ok(requiresRebuild.unsafeReasons.includes('chatwoot_history_already_present'));
}
