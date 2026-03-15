import { InstanceDto } from '@api/dto/instance.dto';
import {
  ChatwootHistoryAnalyzeDto,
  ChatwootHistoryContactActionDto,
  ChatwootHistoryExecuteDto,
  ChatwootHistoryReprocessDto,
} from '@api/integrations/chatbot/chatwoot/dto/chatwoot-history.dto';
import { ChatwootHistoryService } from '@api/integrations/chatbot/chatwoot/services/chatwoot-history.service';

export class ChatwootHistoryController {
  constructor(private readonly historyService: ChatwootHistoryService) {}

  public async getInboxStatus(instance: InstanceDto) {
    return this.historyService.getInboxStatus(instance);
  }

  public async analyze(instance: InstanceDto, data: ChatwootHistoryAnalyzeDto) {
    return this.historyService.analyze(instance, data);
  }

  public async execute(instance: InstanceDto, data: ChatwootHistoryExecuteDto) {
    return this.historyService.execute(instance, data);
  }

  public async listJobs(instance: InstanceDto) {
    return this.historyService.listJobs(instance);
  }

  public async getJob(instance: InstanceDto, jobId: string) {
    return this.historyService.getJob(instance, jobId);
  }

  public async listConflicts(instance: InstanceDto) {
    return this.historyService.listConflicts(instance);
  }

  public async reprocess(instance: InstanceDto, data: ChatwootHistoryReprocessDto) {
    return this.historyService.reprocess(instance, data);
  }

  public async contactAction(instance: InstanceDto, data: ChatwootHistoryContactActionDto) {
    return this.historyService.contactAction(instance, data);
  }

  public async exportCsv(instance: InstanceDto, jobId: string) {
    return this.historyService.exportCsv(instance, jobId);
  }
}
