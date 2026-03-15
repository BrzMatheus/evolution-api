export class ChatwootHistoryAnalyzeDto {
  scopeType: 'single' | 'selected' | 'eligibleAll';
  remoteJids?: string[];
}

export class ChatwootHistoryExecuteDto {
  jobId: string;
  mode: 'importDirect' | 'rebuild';
  selectionMode: 'allSafe' | 'selected';
  remoteJids?: string[];
}

export class ChatwootHistoryContactActionDto {
  jobId: string;
  remoteJid: string;
  action: 'importDirect' | 'createRebuild' | 'ignore' | 'openChatwootReview';
}

export class ChatwootHistoryReprocessDto {
  jobId: string;
  remoteJid?: string;
}
