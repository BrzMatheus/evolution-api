import { JSONSchema7 } from 'json-schema';
import { v4 } from 'uuid';

export const chatwootHistoryAnalyzeSchema: JSONSchema7 = {
  $id: v4(),
  type: 'object',
  properties: {
    scopeType: {
      type: 'string',
      enum: ['single', 'selected', 'eligibleAll'],
    },
    remoteJids: {
      type: 'array',
      items: { type: 'string', minLength: 1 },
    },
  },
  required: ['scopeType'],
};

export const chatwootHistoryExecuteSchema: JSONSchema7 = {
  $id: v4(),
  type: 'object',
  properties: {
    jobId: { type: 'string', minLength: 1 },
    mode: {
      type: 'string',
      enum: ['importDirect', 'rebuild'],
    },
    selectionMode: {
      type: 'string',
      enum: ['allSafe', 'selected'],
    },
    remoteJids: {
      type: 'array',
      items: { type: 'string', minLength: 1 },
    },
  },
  required: ['jobId', 'mode', 'selectionMode'],
};

export const chatwootHistoryContactActionSchema: JSONSchema7 = {
  $id: v4(),
  type: 'object',
  properties: {
    jobId: { type: 'string', minLength: 1 },
    remoteJid: { type: 'string', minLength: 1 },
    action: {
      type: 'string',
      enum: ['importDirect', 'createRebuild', 'ignore', 'openChatwootReview'],
    },
  },
  required: ['jobId', 'remoteJid', 'action'],
};

export const chatwootHistoryReprocessSchema: JSONSchema7 = {
  $id: v4(),
  type: 'object',
  properties: {
    jobId: { type: 'string', minLength: 1 },
    remoteJid: { type: 'string' },
  },
  required: ['jobId'],
};
